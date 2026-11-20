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

import os
import pytest

from datetime import date, datetime, timezone
from urllib.parse import unquote, urlparse

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, \
    assert_gpu_and_cpu_row_counts_equal, assert_spark_exception, collect_plan_nodes, plan_metric
from conftest import is_databricks_runtime, is_not_utc
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
    """The hybrid conf on the MULTITHREADED reader. failOnFallback stays on, so a read the
    eligibility check rejects fails the query instead of dropping to parquet-mr."""
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


_rows_per_row_group = 1000


def _pruning_b(a):
    """The `b` that write_pruning_data writes for row `a`."""
    n = _rows_per_row_group
    return {0: 0, 1: 1000}.get(a % n, 100 * (a // n + 1) + a % 5)


def write_pruning_data(spark, path):
    """One file meant to hold 4 row groups, laid out so each filter below rules out known ones.
    Returns each row group's (min a, max a, min b, max b) from the footer, for the tests to
    derive their expected counts: not every writer honors the options that pin the row groups.

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
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    dir_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    files = [status.getPath() for status in dir_path.getFileSystem(conf).listStatus(dir_path)
             if status.getPath().getName().endswith('.parquet')]
    assert len(files) == 1, files
    footer = spark._jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(conf, files[0])
    stats = [[c.getStatistics() for c in block.getColumns()] for block in footer.getBlocks()]
    return [(a.genericGetMin(), a.genericGetMax(), b.genericGetMin(), b.genericGetMax())
            for a, b in stats]


def _dictionary_pruned(row_groups, value):
    """The row groups whose `b` statistics admit `value` but whose rows do not hold it."""
    return sum(1 for min_a, max_a, min_b, max_b in row_groups
               if min_b <= value <= max_b
               and all(_pruning_b(a) != value for a in range(min_a, max_a + 1)))


# AQE off: collect_plan_nodes does not descend into AdaptiveSparkPlanExec. The pruning metrics
# are DEBUG.
_pruning_metrics_conf = {'spark.sql.adaptive.enabled': False,
                         'spark.rapids.sql.metrics.level': 'DEBUG'}

# Both readers count on different threads, so each metric test runs on both.
_metric_reader_modes = pytest.mark.parametrize('reader_mode', ['PERFILE', 'MULTITHREADED'])


def _metrics_conf(reader_mode, **overrides):
    return hybrid_conf(**_pruning_metrics_conf,
                       **{'spark.rapids.sql.format.parquet.reader.type': reader_mode},
                       **overrides)


def _assert_row_groups(in_input, pruned_by_stats, pruned_by_dict, read, min_splits=1):
    def check(cpu_plan, gpu_plan):
        scan_classes = ('GpuFileSourceScanExec', 'GpuBatchScanExec')
        scans = [node for node in collect_plan_nodes(gpu_plan)
                 if node.getClass().getSimpleName() in scan_classes]
        assert len(scans) == 1, 'Expected one GPU Parquet scan in:\n{}'.format(gpu_plan)
        actual = {name: plan_metric(scans[0], name) for name in
                  ['rowGroupsInInput', 'rowGroupsPrunedByStats', 'rowGroupsPrunedByDict',
                   'rowGroupsFetched']}
        expected = {'rowGroupsInInput': in_input, 'rowGroupsPrunedByStats': pruned_by_stats,
                    'rowGroupsPrunedByDict': pruned_by_dict, 'rowGroupsFetched': read}
        assert actual == expected
        bytes_requested = plan_metric(scans[0], 'hybridScanBytesRequested')
        assert (bytes_requested > 0) == (read > 0), \
            'Read {} row groups but requested {} bytes'.format(read, bytes_requested)
        splits = plan_metric(scans[0], 'hybridScanSplits')
        assert splits >= min_splits, \
            'Expected at least {} splits, got {}'.format(min_splits, splits)
    return check


@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.parametrize('where,is_pruned', [
    ('a >= 2500', lambda min_a, max_a: max_a < 2500),
    ('a < 0', lambda min_a, max_a: min_a >= 0),
    ('a >= 0', lambda min_a, max_a: max_a < 0),
], ids=['some', 'all', 'none'])
@_metric_reader_modes
def test_hybrid_scan_stats_pruning(spark_tmp_path, v1_enabled_list, where, is_pruned,
                                   reader_mode):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    row_groups = with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    pruned = sum(1 for min_a, max_a, _, _ in row_groups if is_pruned(min_a, max_a))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter(where),
        conf=_metrics_conf(reader_mode,
                           **{'spark.sql.sources.useV1SourceList': v1_enabled_list}),
        gpu_plan_assertion=_assert_row_groups(len(row_groups), pruned, 0,
                                              len(row_groups) - pruned))


@_metric_reader_modes
def test_hybrid_scan_stage_timings(spark_tmp_path, reader_mode):
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
        conf=_metrics_conf(reader_mode),
        gpu_plan_assertion=check)


@_metric_reader_modes
def test_hybrid_scan_reader_runs(spark_tmp_path, reader_mode):
    """failOnFallback only fails a read the eligibility check rejects, so this checks that the
    reader built for an eligible read is the hybrid one: only it counts hybridScanSplits."""
    def check(cpu_plan, gpu_plan):
        scan = [node for node in collect_plan_nodes(gpu_plan)
                if node.getClass().getSimpleName() == 'GpuFileSourceScanExec'][0]
        assert plan_metric(scan, 'hybridScanSplits') > 0, 'the hybrid reader did not run'

    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=_metrics_conf(reader_mode),
        gpu_plan_assertion=check)


def _local_paths(spark, data_path):
    """The local paths of the Parquet files under `data_path`, or None if any is not local."""
    files = [urlparse(f) for f in spark.read.parquet(data_path).inputFiles()]
    if not all(f.scheme == 'file' for f in files):
        return None
    return sorted(unquote(f.path) for f in files)


def _checksum_file(local_path):
    """The `.crc` sidecar Hadoop's local filesystem writes beside each file."""
    head, tail = os.path.split(local_path)
    return os.path.join(head, '.' + tail + '.crc')


@_metric_reader_modes
def test_hybrid_scan_native_host_reads(spark_tmp_path, reader_mode):
    """A local file's bytes are all read with KvikIO, and any other file's with none. The local
    files' checksum sidecars are removed, since a file with one is read through Hadoop."""
    def check(cpu_plan, gpu_plan):
        scan = [node for node in collect_plan_nodes(gpu_plan)
                if node.getClass().getSimpleName() == 'GpuFileSourceScanExec'][0]
        bytes_requested = plan_metric(scan, 'hybridScanBytesRequested')
        assert bytes_requested > 0, 'no column chunks were requested'
        native_bytes = plan_metric(scan, 'hybridScanNativeHostBytesRead')
        if is_local:
            assert native_bytes == bytes_requested, \
                'requested {} bytes, read {} with KvikIO'.format(bytes_requested, native_bytes)
        else:
            assert native_bytes in (-1, 0), 'read {} bytes of a remote file with KvikIO'.format(
                native_bytes)

    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=4))
    local_paths = with_cpu_session(lambda spark: _local_paths(spark, data_path))
    is_local = local_paths is not None
    for path in local_paths or []:
        if os.path.exists(_checksum_file(path)):
            os.remove(_checksum_file(path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=_metrics_conf(reader_mode),
        gpu_plan_assertion=check)


@_metric_reader_modes
def test_hybrid_scan_local_checksum(spark_tmp_path, reader_mode):
    """A local file changed after Spark wrote it fails the read on its checksum, as it does
    through Hadoop, rather than returning the changed data."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=2))
    local_paths = with_cpu_session(lambda spark: _local_paths(spark, data_path))
    if local_paths is None:
        pytest.skip('only local files have checksum sidecars')
    # Flip a byte of the first column chunk, which starts after the 4-byte magic.
    with open(local_paths[0], 'r+b') as f:
        f.seek(4)
        changed = bytes([f.read(1)[0] ^ 0xFF])
        f.seek(4)
        f.write(changed)
    assert_gpu_and_cpu_error(lambda spark: spark.read.parquet(data_path).collect(),
                             _metrics_conf(reader_mode), 'ChecksumException')


@_metric_reader_modes
def test_hybrid_scan_dictionary_pruning(spark_tmp_path, reader_mode):
    """302 is inside every row group's `b` range, but only row group 2's dictionary holds it.
    MULTITHREADED prunes with dictionaries after the fetch, so its pruned row groups are still
    read."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    row_groups = with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    by_stats = sum(1 for _, _, min_b, max_b in row_groups if not min_b <= 302 <= max_b)
    by_dict = _dictionary_pruned(row_groups, 302)
    read = len(row_groups) - by_stats
    if reader_mode == 'PERFILE':
        read -= by_dict
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('b = 302'),
        conf=_metrics_conf(reader_mode),
        gpu_plan_assertion=_assert_row_groups(len(row_groups), by_stats, by_dict, read))


@_metric_reader_modes
def test_hybrid_scan_pruning_off_with_filter_pushdown(spark_tmp_path, reader_mode):
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    row_groups = with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=_metrics_conf(reader_mode, **{'spark.sql.parquet.filterPushdown': False}),
        gpu_plan_assertion=_assert_row_groups(len(row_groups), 0, 0, len(row_groups)))


@_metric_reader_modes
def test_hybrid_scan_pruning_with_file_split_across_tasks(spark_tmp_path, reader_mode):
    """Summed over the splits, each row group is in exactly one split's input."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    row_groups = with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    if len(row_groups) < 2:
        pytest.skip('the writer put every row in one row group')
    pruned = sum(1 for _, max_a, _, _ in row_groups if max_a < 2500)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter('a >= 2500'),
        conf=_metrics_conf(reader_mode, **{
            'spark.sql.files.maxPartitionBytes': '4k',
            'spark.sql.files.openCostInBytes': '0',
        }),
        gpu_plan_assertion=_assert_row_groups(len(row_groups), pruned, 0,
                                              len(row_groups) - pruned, min_splits=2))


@pytest.mark.parametrize('footer_reader', ['NATIVE', 'JAVA'])
def test_parquet_scan_row_groups_fetched_without_columns(spark_tmp_path, footer_reader):
    """A count reads no columns, so the hybrid reader falls back, and the native footer reader
    merges the row groups into one block; the metric must still count every one."""
    def check(cpu_plan, gpu_plan):
        scans = [node for node in collect_plan_nodes(gpu_plan)
                 if node.getClass().getSimpleName() == 'GpuFileSourceScanExec']
        assert len(scans) == 1, 'Expected one GPU Parquet scan in:\n{}'.format(gpu_plan)
        assert plan_metric(scans[0], 'rowGroupsFetched') == len(row_groups)

    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    row_groups = with_cpu_session(lambda spark: write_pruning_data(spark, data_path))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).selectExpr('COUNT(*)'),
        conf=fallback_conf(**_pruning_metrics_conf, **{
            'spark.rapids.sql.format.parquet.reader.footer.type': footer_reader}),
        gpu_plan_assertion=check)


@pytest.mark.parametrize('reader_conf', [
    pytest.param(hybrid_mt_conf(), id='hybrid'),
    pytest.param({'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'},
                 id='parquet_mr'),
])
def test_parquet_scan_row_groups_fetched_from_pool_threads(spark_tmp_path, reader_conf):
    """Both multithreaded readers count each file on a pool thread and add it on the task thread,
    so each file must still count once. `parquet_mr` sets no hybrid conf, so the plain reader
    runs."""
    files = 64
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: spark.range(0, files, 1, files)
                     .selectExpr('CAST(id AS INT) AS a').write.parquet(data_path))

    def check(cpu_plan, gpu_plan):
        scans = [node for node in collect_plan_nodes(gpu_plan)
                 if node.getClass().getSimpleName() == 'GpuFileSourceScanExec']
        assert len(scans) == 1, 'Expected one GPU Parquet scan in:\n{}'.format(gpu_plan)
        assert plan_metric(scans[0], 'rowGroupsFetched') == files

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path),
        conf=copy_and_update(_pruning_metrics_conf, _one_task_conf, reader_conf),
        gpu_plan_assertion=check)


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
    """An OOM while statistics pruning evaluates the filter on the GPU is retried and the read
    still succeeds.

    The injection only arms inside retry blocks, so this shows the retry recovers, not that it is
    there. Statistics rule out every row group, so after compiling the filter, the statistics
    retry is the only one the read runs.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 0'),
        conf=reader_confs)


@inject_oom('num_ooms=1,type=CPU')
def test_hybrid_scan_retry_cpu_oom(spark_tmp_path):
    """A host OOM while fetching the column chunks is retried rather than failing the task."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=hybrid_conf())


@pytest.mark.parametrize('max_pack', ['1', '64k'], ids=['pack_per_chunk', 'several_packs'])
def test_hybrid_scan_small_h2d_packs(spark_tmp_path, max_pack):
    """With a small pack cap the chunks span several device allocations, and every chunk
    larger than the cap gets one of its own, so each must still map to its own slice."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7'),
        conf=hybrid_conf(**{'spark.rapids.sql.format.parquet.hybridScan.maxH2DPackSize': max_pack,
                            'spark.rapids.sql.format.parquet.hybridScan.minH2DPackSize': '1'}))


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

    The filter coalesces its output up to batchSizeBytes, so a 1-byte batch size hands the limit
    each batch as it is read rather than after every file. Which row the limit keeps is not
    fixed, so only the count is compared.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path, files=16))
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark: spark.read.parquet(data_path).filter('filter_int = 7').limit(1),
        conf=hybrid_conf(**_one_task_conf, **{'spark.rapids.sql.batchSizeBytes': '1'}))


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
    {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'},
    {'spark.rapids.sql.format.parquet.decompressCpu': 'true'},
], ids=['chunked', 'coalescing', 'decompress_cpu'])
def test_hybrid_scan_ineligible_reads_are_still_correct(spark_tmp_path, overrides):
    """Anything the hybrid path does not take has to come back from parquet-mr unchanged."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500'),
        conf=fallback_conf(**overrides))


def test_hybrid_scan_field_id_read_schema_falls_back(spark_tmp_path):
    """The read schema renames the columns and finds them by field ID; only parquet-mr does that."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    write_schema = StructType([
        StructField('sorted_int', IntegerType(), metadata={'parquet.field.id': 1}),
        StructField('payload_long', LongType(), metadata={'parquet.field.id': 2})])
    read_schema = StructType([
        StructField('a', IntegerType(), metadata={'parquet.field.id': 1}),
        StructField('b', LongType(), metadata={'parquet.field.id': 2})])
    with_cpu_session(
        lambda spark: spark.createDataFrame([(i, i * 2) for i in range(1000)], write_schema)
            .repartition(1).write.parquet(data_path),
        conf=enable_parquet_field_id_write)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).filter('a < 500'),
        conf=fallback_conf(**enable_parquet_field_id_read))


def test_hybrid_scan_field_id_read_enabled_without_field_ids(spark_tmp_path):
    """Field ID reads are on by default on Databricks; a schema with no field IDs stays hybrid."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500'),
        conf=hybrid_conf(**enable_parquet_field_id_read))


@pytest.mark.parametrize('reader_type,error', [
    ('COALESCING', 'the hybrid scan is not supported for this read in this configuration'),
    pytest.param('AUTO', 'support the COALESCING reader, which the AUTO reader type chose',
                 marks=pytest.mark.skipif(is_databricks_runtime(),
                                          reason='AUTO picks MULTITHREADED for DBFS paths')),
], ids=['coalescing', 'auto_local'])
def test_hybrid_scan_coalescing_reader_reports_fallback(spark_tmp_path, reader_type, error):
    """The coalescing reader never runs hybrid, so a read it takes reports the fallback. AUTO
    chooses it for local files."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    if reader_type == 'AUTO' and urlparse(data_path).scheme not in ('', 'file', 'hdfs'):
        pytest.skip('AUTO picks MULTITHREADED for cloud paths')
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter('sorted_int < 500').collect(),
            conf=hybrid_conf(**{'spark.rapids.sql.format.parquet.reader.type': reader_type})),
        error_message=error)


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


@pytest.mark.parametrize('reader_confs', fallback_reader_confs)
def test_hybrid_scan_filter_on_missing_column(spark_tmp_path, reader_confs):
    """The file lacks `added_int`, so the reader must fail loudly rather than return wrong rows.

    The filter is only on `sorted_int`, so statistics pruning succeeds and the read reaches the
    decoded-schema check. parquet-mr would return nulls instead, so this also shows that each
    reader type really ran the hybrid scan.
    """
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
                .filter('sorted_int < 500').collect(),
            conf=reader_confs),
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


@pytest.mark.parametrize('ignore_missing', [
    pytest.param(True, id='ignored', marks=pytest.mark.skipif(
        is_databricks_runtime(), reason="Databricks handles missing files differently")),
    pytest.param(False, id='raised')])
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
