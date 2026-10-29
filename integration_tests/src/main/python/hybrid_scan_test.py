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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_spark_exception
from conftest import is_not_utc
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
], ids=['chunked', 'field_id', 'multithreaded', 'coalescing'])
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


_missing_col_schema = 'sorted_int INT, filter_int INT, added_int INT'


@pytest.mark.xfail(strict=True, reason='a filter on a column the file lacks returns no rows '
                   'until schema mismatches are detected')
def test_hybrid_scan_filter_on_missing_column(spark_tmp_path):
    """The file lacks `added_int`, so every row reads it as null and the filter keeps them all."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
            .filter('added_int IS NULL AND sorted_int < 500'),
        conf=fallback_conf())


def test_hybrid_scan_ignore_corrupt_files_keeps_readable_file(spark_tmp_path):
    """A readable file the reader cannot handle must still return its rows, not be skipped."""
    data_path = spark_tmp_path + '/HYBRID_SCAN_DATA'
    with_cpu_session(lambda spark: write_test_data(spark, data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(_missing_col_schema).parquet(data_path)
            .filter('sorted_int < 500'),
        conf=fallback_conf(**{'spark.sql.files.ignoreCorruptFiles': 'true'}))
