# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Correctness tests for the libcudf hybrid scan Parquet reader.

The reader runs only when several settings line up: the hybrid scan reader is enabled, the reader
type is PERFILE, and the chunked reader is off. A read that does not qualify falls back to the
parquet-mr path and still returns the right answer, so `hybrid_conf` makes a fallback fail the
query instead; a test that is meant to fall back uses `fallback_conf`.
"""

import pytest

from datetime import date, datetime, timezone

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, \
    assert_gpu_and_cpu_row_counts_equal, assert_spark_exception, collect_plan_nodes, plan_metric
from conftest import is_databricks_runtime, is_not_utc
from data_gen import *
from marks import inject_oom
from parquet_test import hybrid_fallback_parquet_file_reader_conf, hybrid_parquet_file_reader_conf, \
    rebase_write_legacy_conf
from spark_session import with_cpu_session, with_gpu_session


def hybrid_conf(**overrides):
    return copy_and_update(hybrid_parquet_file_reader_conf, overrides)


def fallback_conf(**overrides):
    return copy_and_update(hybrid_fallback_parquet_file_reader_conf, overrides)


def write_test_data(spark, path, num_rows=20000, row_group_size=8192, files=1):
    """Files of several row groups each, with columns shaped for each way of pruning.

    `sorted_int` ascends, so row group statistics rule out most groups on a range filter.
    `filter_int` and `filter_str` have few distinct values, so they are dictionary encoded and
    an equality filter can be answered from the dictionary pages alone.

    Several files means each holds a contiguous ascending stretch of the rows, since `range`
    divides its output that way rather than shuffling it.
    """
    spark.range(0, num_rows, 1, files).selectExpr(
        'CAST(id AS INT) AS sorted_int',
        'CAST(id % 100 AS INT) AS filter_int',
        "CONCAT('key_', CAST(id % 50 AS STRING)) AS filter_str",
        'CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id % 1000 AS INT) END AS nullable_int',
        'CAST(id * 2 AS LONG) AS payload_long',
        'CAST(id AS DOUBLE) / 3 AS payload_double',
        "CONCAT('payload_', CAST(id AS STRING)) AS payload_str"
    ).write.option('parquet.block.size', row_group_size).parquet(path)


@pytest.mark.parametrize('where', [
    'sorted_int < 100',
    'sorted_int >= 19900',
    'sorted_int > 5000 AND sorted_int <= 5100',
    'filter_int = 7',
    'filter_int IN (1, 2, 3)',
    "filter_str = 'key_3'",
    'nullable_int IS NULL',
    'nullable_int IS NOT NULL AND sorted_int < 200',
    'sorted_int < 100 OR sorted_int > 19900',
    # A comparison against a null is unknown rather than false, so these decide what the reader
    # does with an unknown: Spark keeps a row where the other side of an OR is true, and rules out
    # a row where the other side of an AND is false.
    'nullable_int < 5 OR sorted_int > 19900',
    'nullable_int = 500 OR sorted_int < 100',
    'nullable_int > 500 AND sorted_int < 300',
    'nullable_int IS NULL AND filter_int = 7',
    'NOT (nullable_int = 500) AND sorted_int < 300',
    'NOT (filter_int = 5) AND sorted_int < 300',
    'filter_int != 7 AND sorted_int < 300',
    'filter_int NOT IN (1, 2, 3) AND sorted_int < 300',
    "filter_str != 'key_3' AND sorted_int < 300",
    'NOT (sorted_int >= 100)',
], ids=lambda w: w.replace(' ', '_'))
def test_hybrid_scan_filters(spark_tmp_path, where):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=hybrid_conf())


@pytest.mark.parametrize('where', [
    # No row group holds a value below zero, so statistics rule all of them out.
    'sorted_int < 0',
    # 'key_500' sorts between the smallest and largest string in every row group, so statistics
    # cannot rule any of them out, but no dictionary holds the value.
    "filter_str = 'key_500'",
    # Nothing is ruled out.
    'sorted_int >= 0',
], ids=['none_survive_stats', 'none_survive_dict', 'all_survive'])
def test_hybrid_scan_pruning_extremes(spark_tmp_path, where):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=hybrid_conf())


_rows_per_row_group = 1000


def write_pruning_data(spark, path):
    """One file of exactly 4 row groups, laid out so each filter below rules out known ones.

    Row group i holds `a` from 1000 * i to 1000 * i + 999. Every row group's `b` spans 0 to 1000,
    so statistics never rule one out on `b`, but in between it holds only 100 * (i + 1) to
    100 * (i + 1) + 4, so its dictionary does.

    parquet-mr checks the row group size every `row.check` rows, and a 1-byte limit makes every
    check flush, which pins each row group to that many rows.
    """
    n = _rows_per_row_group
    spark.range(0, 4 * n, 1, 1).selectExpr(
        'CAST(id AS INT) AS a',
        f'CAST(CASE id % {n} WHEN 0 THEN 0 WHEN 1 THEN 1000 '
        f'ELSE 100 * (FLOOR(id / {n}) + 1) + id % 5 END AS INT) AS b'
    ).write.options(**{
        'parquet.block.size': 1,
        'parquet.page.size.row.check.min': n,
        'parquet.page.size.row.check.max': n,
    }).parquet(path)


# AQE off: collect_plan_nodes does not descend into AdaptiveSparkPlanExec. The pruning metrics
# are DEBUG.
_pruning_metrics_conf = {'spark.sql.adaptive.enabled': False,
                         'spark.rapids.sql.metrics.level': 'DEBUG'}


def _assert_row_groups(in_input, pruned_by_stats, pruned_by_dict, read, min_splits=1):
    def check(cpu_plan, gpu_plan):
        scan_classes = ('GpuFileSourceScanExec', 'GpuBatchScanExec')
        scans = [node for node in collect_plan_nodes(gpu_plan)
                 if node.getClass().getSimpleName() in scan_classes]
        assert len(scans) == 1, 'Expected one GPU Parquet scan in:\n{}'.format(gpu_plan)
        actual = {name: plan_metric(scans[0], name) for name in
                  ['rowGroupsInInput', 'rowGroupsPrunedByStats', 'rowGroupsPrunedByDict',
                   'rowGroupsRead']}
        expected = {'rowGroupsInInput': in_input, 'rowGroupsPrunedByStats': pruned_by_stats,
                    'rowGroupsPrunedByDict': pruned_by_dict, 'rowGroupsRead': read}
        assert actual == expected
        bytes_read = plan_metric(scans[0], 'hybridScanBytesRead')
        assert (bytes_read > 0) == (read > 0), \
            'Read {} row groups but {} bytes'.format(read, bytes_read)
        splits = plan_metric(scans[0], 'hybridScanSplits')
        assert splits >= min_splits, \
            'Expected at least {} splits, got {}'.format(min_splits, splits)
    return check


@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.parametrize('where,pruned,read', [
    ('a >= 2500', 2, 2),
    ('a < 0', 4, 0),
    ('a >= 0', 0, 4),
], ids=['some', 'all', 'none'])
def test_hybrid_scan_stats_pruning(spark_tmp_path, v1_enabled_list, where, pruned, read):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=hybrid_conf(**_pruning_metrics_conf,
                         **{'spark.sql.sources.useV1SourceList': v1_enabled_list}),
        gpu_plan_assertion=_assert_row_groups(4, pruned, 0, read))


def test_hybrid_scan_stage_timings(spark_tmp_path):
    def check(cpu_plan, gpu_plan):
        scan = [node for node in collect_plan_nodes(gpu_plan)
                if node.getClass().getSimpleName() == 'GpuFileSourceScanExec'][0]
        for name in ['filterTime', 'bufferTime', 'gpuDecodeTime', 'hybridScanFooterParseTime',
                     'hybridScanOutputFilterTime']:
            assert plan_metric(scan, name) > 0, '{} was not recorded'.format(name)

    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=hybrid_conf(**_pruning_metrics_conf),
        gpu_plan_assertion=check)


def test_hybrid_scan_dictionary_pruning(spark_tmp_path):
    """302 is inside every row group's `b` range, but only row group 2's dictionary holds it."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('b = 302'),
        conf=hybrid_conf(**_pruning_metrics_conf),
        gpu_plan_assertion=_assert_row_groups(4, 0, 3, 1))


def test_hybrid_scan_pruning_off_with_filter_pushdown(spark_tmp_path):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=hybrid_conf(**_pruning_metrics_conf,
                         **{'spark.sql.parquet.filterPushdown': False}),
        gpu_plan_assertion=_assert_row_groups(4, 0, 0, 4))


def test_hybrid_scan_pruning_with_file_split_across_tasks(spark_tmp_path):
    """Summed over the splits, each row group is in exactly one split's input."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=hybrid_conf(**_pruning_metrics_conf, **{
            'spark.sql.files.maxPartitionBytes': '4k',
            'spark.sql.files.openCostInBytes': '0',
        }),
        gpu_plan_assertion=_assert_row_groups(4, 2, 0, 2, min_splits=2))


@pytest.mark.parametrize('columns', [
    ['sorted_int'],                                 # the projection is only the filter column
    ['payload_str'],                                # the filter column is not projected
    ['sorted_int', 'payload_long', 'payload_str'],
], ids=['filter_col_only', 'payload_col_only', 'both'])
def test_hybrid_scan_projections(spark_tmp_path, columns):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500').select(*columns),
        conf=hybrid_conf())


@pytest.mark.parametrize('keep_threshold', ['1.0', '0.5'], ids=['reader_filter', 'output_filter'])
def test_hybrid_scan_projection_order_differs_from_file(spark_tmp_path, keep_threshold):
    """Columns come back in the read schema's order, not the file's.

    The decoded-schema check and the output filter both index the decoded table by read schema
    position. Both columns are INT, so a table in file order would pass the type check and return
    swapped values. Below the keep threshold the filter is applied by position rather than by name.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema('filter_int INT, sorted_int INT').parquet(data_path)
            .filter('sorted_int < 500'),
        conf=hybrid_conf(**{
            'spark.rapids.sql.format.parquet.hybridScan.singleStage.filterKeepThreshold':
                keep_threshold}))


def test_hybrid_scan_residual_filter(spark_tmp_path):
    """A filter the reader only partly understands must still return exactly the right rows.

    `startswith` has no AST equivalent, so only the range conjunct is pushed and Spark applies
    the rest above the scan.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .filter("sorted_int < 2000 AND filter_str LIKE 'key_1%'"),
        conf=hybrid_conf())


@inject_oom
def test_hybrid_scan_retry_oom(spark_tmp_path):
    """An OOM during dictionary pruning or decode is retried rather than failing the task.

    The filter needs the dictionary pages, so both device stages run.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=hybrid_conf())


@inject_oom('num_ooms=1,skip=1,type=GPU')
def test_hybrid_scan_retry_oom_in_reader(spark_tmp_path):
    """An OOM inside the reader's own calls is retried on the same reader.

    One file of one row group, filtered and projected on one column, makes each retry block copy
    exactly one buffer to the device, so skipping one GPU allocation lands the OOM inside the
    dictionary pruning and the decode rather than in the copies.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(
        spark, data_path, num_rows=1000, row_group_size=128 * 1024 * 1024))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7').select('filter_int'),
        conf=hybrid_conf())


@inject_oom('num_ooms=1,type=CPU')
def test_hybrid_scan_retry_cpu_oom(spark_tmp_path):
    """A host OOM while fetching the column chunks is retried rather than failing the task."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=hybrid_conf())


@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
def test_hybrid_scan_legacy_old_date_filter_throws(spark_tmp_path):
    """A LEGACY file stores pre-1582 dates in another calendar, which the reader cannot tell.

    Pruning on the stored days would drop the matching row group without an error, so the
    filter is withheld and the decoded dates fail the rebase check instead.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(
        lambda spark: spark.range(0, 100).selectExpr(
            "DATE_ADD(DATE'1000-01-01', CAST(id AS INT)) AS d").write.parquet(data_path),
        conf=rebase_write_legacy_conf)
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter("d = DATE'1000-01-01'").collect(),
            conf=hybrid_conf()),
        'SparkUpgradeException')


def test_hybrid_scan_file_split_across_tasks(spark_tmp_path):
    """Each task owns part of the file, so every row group must be read by exactly one of them."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    conf = hybrid_conf(**{
        'spark.sql.files.maxPartitionBytes': '16k',
        'spark.sql.files.openCostInBytes': '0',
    })
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int >= 0'), conf=conf)


# Packs every file into one task, so one task opens and closes a reader per file.
_one_task_conf = {
    'spark.sql.files.maxPartitionBytes': '1g',
    'spark.sql.files.minPartitionNum': '1',
    'spark.sql.files.openCostInBytes': '0',
}


@pytest.mark.parametrize('where', ['filter_int = 7', "filter_str = 'key_500'", 'sorted_int < 3000'],
                         ids=['dict_keeps_some', 'dict_keeps_none', 'stats_keep_first_files'])
def test_hybrid_scan_many_files_in_one_task(spark_tmp_path, where):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=16))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=hybrid_conf(**_one_task_conf))


def test_hybrid_scan_early_limit(spark_tmp_path):
    """A limit stops the task partway through its files, so readers are closed unconsumed.

    Which row the limit keeps is not fixed, so only the count is compared.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=16))
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7').limit(1),
        conf=hybrid_conf(**_one_task_conf))


_supported_type_gens = [
    ('filter_col', IntegerGen(nullable=False)),
    ('bool_col', boolean_gen),
    ('byte_col', byte_gen),
    ('short_col', short_gen),
    ('int_col', int_gen),
    ('long_col', long_gen),
    ('float_col', float_gen),
    ('double_col', double_gen),
    ('string_col', string_gen),
    ('decimal_col', decimal_gen_64bit),
    ('date_col', DateGen(start=date(1900, 1, 1))),
    ('ts_col', TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
]


def test_hybrid_scan_supported_types(spark_tmp_path):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    write_conf = {
        'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',
    }
    with_cpu_session(
        lambda spark: gen_df(spark, _supported_type_gens, length=4096).write.parquet(data_path),
        conf=write_conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_col > 0'),
        conf=hybrid_conf())


@pytest.mark.parametrize('overrides', [
    {'spark.rapids.sql.reader.chunked': 'true'},
    {'spark.sql.parquet.fieldId.read.enabled': 'true'},
    {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'},
    {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'},
    {'spark.rapids.sql.format.parquet.decompressCpu': 'true'},
], ids=['chunked', 'field_id', 'multithreaded', 'coalescing', 'decompress_cpu'])
def test_hybrid_scan_ineligible_reads_are_still_correct(spark_tmp_path, overrides):
    """Anything the hybrid path does not take has to come back from parquet-mr unchanged."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500'),
        conf=fallback_conf(**overrides))


def test_hybrid_scan_nested_column_read_is_still_correct(spark_tmp_path):
    """A nested column keeps the whole read on parquet-mr; the answer must not change."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    gens = [
        ('filter_col', IntegerGen(nullable=False)),
        ('struct_col', StructGen([['child', int_gen]])),
    ]
    with_cpu_session(lambda spark: gen_df(spark, gens, length=2048).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_col > 0'),
        conf=fallback_conf())


def test_hybrid_scan_unfiltered_read(spark_tmp_path):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path), conf=hybrid_conf())


_missing_col_schema = 'added_int INT, sorted_int INT, filter_int INT'


def test_hybrid_scan_filter_on_missing_column(spark_tmp_path):
    """The file lacks `added_int`, so the reader must fail loudly rather than return wrong rows.

    The filter is only on `sorted_int`, so statistics pruning succeeds and the read reaches the
    decoded-schema check.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
                .filter('sorted_int < 500').collect(),
            conf=fallback_conf(**{'spark.sql.parquet.fieldId.read.enabled': False})),
        error_message='a column is missing from the file')


def test_hybrid_scan_ignore_corrupt_files_keeps_readable_file(spark_tmp_path):
    """A readable file the reader cannot handle must still return its rows, not be skipped."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
            .filter('sorted_int < 500'),
        conf=fallback_conf(**{'spark.sql.files.ignoreCorruptFiles': 'true'}))


@pytest.mark.parametrize('ignore_missing', [True, False], ids=['ignored', 'raised'])
def test_hybrid_scan_missing_file(spark_tmp_path, ignore_missing):
    """A file deleted after planning is skipped when ignoreMissingFiles is set, else it fails.

    A path that is missing at planning fails or is dropped by the file listing and never reaches
    the reader, so each run lists its own copy of the data and then deletes one file. The CPU and
    GPU runs each take a copy.
    """
    copies = [spark_tmp_path + '/HYBRID_SCAN_DATA_' + str(i) for i in range(2)]
    for copy in copies:
        with_cpu_session(lambda spark: write_test_data(spark, copy, files=2))

    def read_then_delete(spark):
        df = spark.read.parquet(copies.pop(0))
        path = spark._jvm.org.apache.hadoop.fs.Path(sorted(df.inputFiles())[0])
        assert path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).delete(path, False)
        return df

    conf = hybrid_conf(**{'spark.sql.files.ignoreMissingFiles': str(ignore_missing).lower()})
    if ignore_missing:
        assert_gpu_and_cpu_are_equal_collect(read_then_delete, conf=conf)
    else:
        assert_gpu_and_cpu_error(lambda spark: read_then_delete(spark).collect(), conf,
                                 'FileNotFoundException')


def truncate_one_file(spark, data_path):
    """Overwrite one file with its first half, so its leading bytes are valid but its footer is
    gone."""
    path = spark._jvm.org.apache.hadoop.fs.Path(
        sorted(spark.read.parquet(data_path).inputFiles())[0])
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(path)
    try:
        data = spark._jvm.org.apache.commons.io.IOUtils.toByteArray(
            stream, fs.getFileStatus(path).getLen() // 2)
    finally:
        stream.close()
    out = fs.create(path, True)
    try:
        out.write(data)
    finally:
        out.close()


@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
@pytest.mark.parametrize('ignore_corrupt', [True, False], ids=['ignored', 'raised'])
def test_hybrid_scan_corrupt_file(spark_tmp_path, ignore_corrupt):
    """A truncated file is skipped when ignoreCorruptFiles is set, else it fails the read.

    Neither reader can read the file, so a fallback is allowed. The schema is given so schema
    inference does not read the truncated footer on the driver.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=2))
    with_cpu_session(lambda spark: truncate_one_file(spark, data_path))
    conf = fallback_conf(**{'spark.sql.files.ignoreCorruptFiles': str(ignore_corrupt).lower()})

    def read(spark):
        return spark.read.schema('sorted_int INT, filter_int INT').parquet(data_path) \
            .filter('sorted_int < 15000')
    if ignore_corrupt:
        assert_gpu_and_cpu_are_equal_collect(read, conf=conf)
    else:
        assert_gpu_and_cpu_error(lambda spark: read(spark).collect(), conf, 'SparkException')
