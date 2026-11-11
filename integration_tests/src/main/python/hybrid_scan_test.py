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
type is PERFILE or MULTITHREADED, and the chunked reader is off. A read that does not qualify falls back to the
parquet-mr path and still returns the right answer, so `hybrid_conf` makes a fallback fail the
query instead; a test that is meant to fall back uses `fallback_conf`.
"""

import pytest

from datetime import date, datetime, timezone

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_spark_exception
from conftest import is_not_utc
from data_gen import *
from marks import allow_non_gpu, inject_oom
from parquet_test import hybrid_fallback_parquet_file_reader_conf, hybrid_parquet_file_reader_conf, \
    rebase_write_legacy_conf
from spark_session import with_cpu_session, with_gpu_session


def hybrid_conf(**overrides):
    return copy_and_update(hybrid_parquet_file_reader_conf, overrides)


def fallback_conf(**overrides):
    return copy_and_update(hybrid_fallback_parquet_file_reader_conf, overrides)


def hybrid_mt_conf(**overrides):
    """The hybrid conf on the MULTITHREADED reader. failOnFallback stays on, so a read that drops
    to parquet-mr fails the query, proving the multithreaded hybrid path actually ran."""
    return hybrid_conf(**{'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
                          **overrides})


# The hybrid reader runs on both PERFILE and MULTITHREADED; the generic correctness tests run on
# each so the same answers are checked on both paths.
hybrid_reader_confs = [
    pytest.param(hybrid_conf(), id='perfile'),
    pytest.param(hybrid_mt_conf(), id='multithreaded'),
]

# The fallback variant (failOnFallback off) on both modes, for reads the hybrid path declines: the
# gate must hand them to parquet-mr unchanged on whichever reader is in use.
fallback_reader_confs = [
    pytest.param(fallback_conf(), id='perfile'),
    pytest.param(fallback_conf(**{'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}),
                 id='multithreaded'),
]


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
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_filters(spark_tmp_path, where, reader_confs):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=reader_confs)


@pytest.mark.parametrize('where', [
    # No row group holds a value below zero, so statistics rule all of them out.
    'sorted_int < 0',
    # 'key_500' sorts between the smallest and largest string in every row group, so statistics
    # cannot rule any of them out, but no dictionary holds the value.
    "filter_str = 'key_500'",
    # Nothing is ruled out.
    'sorted_int >= 0',
], ids=['none_survive_stats', 'none_survive_dict', 'all_survive'])
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_pruning_extremes(spark_tmp_path, where, reader_confs):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=reader_confs)


@pytest.mark.parametrize('columns', [
    ['sorted_int'],                                 # the projection is only the filter column
    ['payload_str'],                                # the filter column is not projected
    ['sorted_int', 'payload_long', 'payload_str'],
], ids=['filter_col_only', 'payload_col_only', 'both'])
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_projections(spark_tmp_path, columns, reader_confs):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500').select(*columns),
        conf=reader_confs)


@pytest.mark.parametrize('keep_threshold', ['1.0', '0.5'], ids=['reader_filter', 'output_filter'])
@pytest.mark.parametrize('reader_mode', ['PERFILE', 'MULTITHREADED'])
def test_hybrid_scan_projection_order_differs_from_file(spark_tmp_path, keep_threshold,
                                                        reader_mode):
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
            'spark.rapids.sql.format.parquet.reader.type': reader_mode,
            'spark.rapids.sql.format.parquet.hybridScan.singleStage.filterKeepThreshold':
                keep_threshold}))


@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_residual_filter(spark_tmp_path, reader_confs):
    """A filter the reader only partly understands must still return exactly the right rows.

    `startswith` has no AST equivalent, so only the range conjunct is pushed and Spark applies
    the rest above the scan.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .filter("sorted_int < 2000 AND filter_str LIKE 'key_1%'"),
        conf=reader_confs)


@inject_oom
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_retry_oom(spark_tmp_path, reader_confs):
    """An OOM in the device work is retried rather than failing the task. Both modes run the
    dictionary-pruning compare and the decode under the retry; MULTITHREADED reaches them through
    the spillable carrier.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=reader_confs)


def write_partial_dict_data(spark, path):
    """One file of many row groups where the dictionaries rule out only some of them.

    In the first half `dict_int` is only 0 or 2, so those row groups' statistics span 1 but their
    dictionaries do not hold it; in the second half it is 0, 1 or 2. `plain_str` is written without
    a dictionary, so the reader reports an empty dictionary range for it.
    """
    spark.range(0, 20000, 1, 1).selectExpr(
        'CAST(id AS INT) AS sorted_int',
        'CAST(CASE WHEN id < 10000 THEN (id % 2) * 2 ELSE id % 3 END AS INT) AS dict_int',
        "CONCAT('plain_', CAST(id AS STRING)) AS plain_str",
        'CAST(id * 2 AS LONG) AS payload_long'
    ).write.option('parquet.block.size', 8192) \
        .option('parquet.enable.dictionary#plain_str', 'false').parquet(path)


@pytest.mark.parametrize('where', [
    'dict_int = 1',
    # A conjunct on a column with no dictionary leaves an empty slot among the dictionary pages.
    "dict_int = 1 AND plain_str != 'none'",
], ids=['some_survive_dict', 'with_non_dict_column'])
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_partial_dictionary_pruning(spark_tmp_path, where, reader_confs):
    """Dictionaries drop some of a file's row groups but not all. MULTITHREADED prunes chunks it
    already fetched, so it must keep exactly the surviving row groups' chunks."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_partial_dict_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=reader_confs)


@inject_oom('num_ooms=1,skip=1,type=GPU')
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_retry_oom_in_reader(spark_tmp_path, reader_confs):
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
        conf=reader_confs)


@inject_oom('num_ooms=1,type=GPU')
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_retry_oom_in_stats_pruning(spark_tmp_path, reader_confs):
    """An OOM while statistics pruning evaluates the filter on the GPU is retried.

    Statistics rule out every row group, so the statistics call is the only GPU work in the read
    and the injected OOM lands there.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 0'),
        conf=reader_confs)


@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_legacy_old_date_filter_throws(spark_tmp_path, reader_confs):
    """A LEGACY file stores pre-1582 dates in another calendar, which the reader cannot tell.

    Pruning on the stored days would drop the matching row group without an error, so the
    filter is withheld and the decoded dates fail the rebase check instead. The check is in the
    shared decode, so both reader modes must throw.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(
        lambda spark: spark.range(0, 100).selectExpr(
            "DATE_ADD(DATE'1000-01-01', CAST(id AS INT)) AS d").write.parquet(data_path),
        conf=rebase_write_legacy_conf)
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter("d = DATE'1000-01-01'").collect(),
            conf=reader_confs),
        'SparkUpgradeException')


@pytest.mark.parametrize('reader_mode', ['PERFILE', 'MULTITHREADED'])
def test_hybrid_scan_file_split_across_tasks(spark_tmp_path, reader_mode):
    """Each task owns part of the file, so every row group must be read by exactly one of them."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    conf = hybrid_conf(**{
        'spark.rapids.sql.format.parquet.reader.type': reader_mode,
        'spark.sql.files.maxPartitionBytes': '16k',
        'spark.sql.files.openCostInBytes': '0',
    })
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int >= 0'), conf=conf)


def test_hybrid_scan_multithreaded_prunes_whole_files(spark_tmp_path):
    """Each file holds a contiguous ascending stretch, so a low filter leaves the later files with
    no surviving row group. The task reads some files to empty rows on the MULTITHREADED path,
    exercising the carrier's positive-rowcount handling against the framework's empty-buffer skip.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 100'),
        conf=hybrid_mt_conf())


def test_hybrid_scan_multithreaded_all_files_pruned(spark_tmp_path):
    """No row group in any file survives, so a multi-file task produces no batches at all."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 0'),
        conf=hybrid_mt_conf())


@allow_non_gpu('CollectLimitExec')
def test_hybrid_scan_multithreaded_early_limit_closes_cleanly(spark_tmp_path):
    """A limit stops the task before every file's batches are drained, so the reader closes with
    reads still queued (the MULTITHREADED cancellation path). Which rows come back is not fixed,
    so only the count is checked. The top-level limit lands on the CPU as CollectLimitExec.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=8))
    rows = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int >= 0').limit(5).collect(),
        conf=hybrid_mt_conf())
    assert len(rows) == 5


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


@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_supported_types(spark_tmp_path, reader_confs):
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
        conf=reader_confs)


@pytest.mark.parametrize('overrides', [
    {'spark.rapids.sql.reader.chunked': 'true'},
    {'spark.sql.parquet.fieldId.read.enabled': 'true'},
    {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'},
], ids=['chunked', 'field_id', 'coalescing'])
def test_hybrid_scan_ineligible_reads_are_still_correct(spark_tmp_path, overrides):
    """Anything the hybrid path does not take has to come back from parquet-mr unchanged."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500'),
        conf=fallback_conf(**overrides))


@pytest.mark.parametrize('reader_confs', fallback_reader_confs)
def test_hybrid_scan_nested_column_read_is_still_correct(spark_tmp_path, reader_confs):
    """A nested column keeps the whole read on parquet-mr; the answer must not change."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    gens = [
        ('filter_col', IntegerGen(nullable=False)),
        ('struct_col', StructGen([['child', int_gen]])),
    ]
    with_cpu_session(lambda spark: gen_df(spark, gens, length=2048).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_col > 0'),
        conf=reader_confs)


@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_unfiltered_read(spark_tmp_path, reader_confs):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path), conf=reader_confs)


@pytest.mark.parametrize('reader_confs', hybrid_reader_confs)
def test_hybrid_scan_partitioned_read(spark_tmp_path, reader_confs):
    """Partition columns are attached per file after decode, so a multi-partition read must pair
    each file's rows with its own partition value. On MULTITHREADED this is done inline in the
    hybrid task phase, a path the flat-directory tests never reach."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: spark.range(0, 20000).selectExpr(
        'CAST(id AS INT) AS sorted_int',
        'CAST(id % 100 AS INT) AS filter_int',
        'CAST(id % 4 AS INT) AS part'
    ).write.partitionBy('part').parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500'),
        conf=reader_confs)


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


@pytest.mark.parametrize('reader_confs', fallback_reader_confs)
def test_hybrid_scan_ignore_corrupt_files_keeps_readable_file(spark_tmp_path, reader_confs):
    """A readable file the reader cannot handle must still return its rows, not be skipped."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
            .filter('sorted_int < 500'),
        conf=copy_and_update(reader_confs, {'spark.sql.files.ignoreCorruptFiles': 'true'}))
