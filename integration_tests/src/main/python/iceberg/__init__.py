# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import os
import tempfile
import logging
from types import MappingProxyType
from typing import Callable, List, Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import FloatType, DoubleType, BinaryType

import pytest

from conftest import spark_jvm
from data_gen import *
from spark_session import is_iceberg_supported_spark, with_cpu_session

iceberg_unsupported_mark = pytest.mark.skipif(
    not is_iceberg_supported_spark(),
    reason="Iceberg acceleration requires Spark 3.5.x, 4.0.x, or 4.1.x")

# iceberg supported types
iceberg_table_gen = MappingProxyType({
    '_c0': byte_gen, '_c1': short_gen, '_c2': int_gen,
    '_c3': long_gen, '_c4': float_gen, '_c5': double_gen,
    '_c6': string_gen,
    '_c7': boolean_gen, '_c8': date_gen, '_c9': timestamp_gen, '_c10': decimal_gen_32bit,
    '_c11': decimal_gen_64bit, '_c12': decimal_gen_128bit,
    # Add columns c13, c14, c15 to avoid Iceberg known issue:
    # https://github.com/NVIDIA/spark-rapids-jni/issues/4016
    # Disable special cases to avoid generating overflow values for truncate transform.
    # when this issue is fixed, we can remove these extra columns.
    '_c13': DecimalGen(precision=7, scale=3, special_cases=[]),
    '_c14': DecimalGen(precision=12, scale=2, special_cases=[]),
    '_c15': DecimalGen(precision=20, scale=2, special_cases=[]),
})
iceberg_base_table_cols = list(iceberg_table_gen.keys())
iceberg_gens_list = [iceberg_table_gen[col] for col in iceberg_base_table_cols]

rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']

# Anchor list used by the single partition-transform coverage test
# (iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage).
# That test exercises the partition writer against all 26 transforms. Every other
# DML op only sanity-checks its own SQL path against a few distinct transforms
# picked from this list via the per-op constants further down; the anchor is what
# guarantees full transform coverage of the partition writer.
full_coverage_partition_transforms = [
    pytest.param("year(_c8)", id="year(date_col)"),
    pytest.param("month(_c8)", id="month(date_col)"),
    pytest.param("day(_c8)", id="day(date_col)"),
    pytest.param("month(_c9)", id="month(timestamp_col)"),
    pytest.param("day(_c9)", id="day(timestamp_col)"),
    pytest.param("hour(_c9)", id="hour(timestamp_col)"),
    pytest.param("truncate(10, _c2)", id="truncate(10, int_col)"),
    pytest.param("truncate(10, _c3)", id="truncate(10, long_col)"),
    pytest.param("truncate(5, _c6)", id="truncate(5, string_col)"),
    pytest.param("truncate(10, _c13)", id="truncate(10, decimal32_col)"),
    pytest.param("truncate(10, _c14)", id="truncate(10, decimal64_col)"),
    pytest.param("truncate(10, _c15)", id="truncate(10, decimal128_col)"),
    pytest.param("bucket(16, _c2)", id="bucket(16, int_col)"),
    pytest.param("bucket(16, _c3)", id="bucket(16, long_col)"),
    pytest.param("bucket(16, _c8)", id="bucket(16, date_col)"),
    pytest.param("bucket(16, _c9)", id="bucket(16, timestamp_col)"),
    pytest.param("bucket(16, _c6)", id="bucket(16, string_col)"),
    pytest.param("bucket(16, _c13)", id="bucket(16, decimal32_col)"),
    pytest.param("bucket(16, _c14)", id="bucket(16, decimal64_col)"),
    pytest.param("bucket(16, _c15)", id="bucket(16, decimal128_col)"),
    pytest.param("_c0", id="identity(byte)"),
    pytest.param("_c2", id="identity(int)"),
    pytest.param("_c3", id="identity(long)"),
    pytest.param("_c6", id="identity(string)"),
    pytest.param("_c8", id="identity(date)"),
    pytest.param("_c10", id="identity(decimal)"),
]

# Per-op subsets. Each transform picked here is distinct from every other pick
# across all 7 op subsets (22 distinct transforms total). The four transforms
# not picked here are still exercised by the anchor. Each 4-pick op covers one
# transform from each family (datetime/truncate/bucket/identity); the single-mode
# slots in delete/update/merge each pick from one family.
ctas_partition_transforms = [
    pytest.param("year(_c8)", id="year(date_col)"),
    pytest.param("truncate(10, _c2)", id="truncate(10, int_col)"),
    pytest.param("bucket(16, _c2)", id="bucket(16, int_col)"),
    pytest.param("_c0", id="identity(byte)"),
]

rtas_partition_transforms = [
    pytest.param("month(_c8)", id="month(date_col)"),
    pytest.param("truncate(10, _c3)", id="truncate(10, long_col)"),
    pytest.param("bucket(16, _c3)", id="bucket(16, long_col)"),
    pytest.param("_c2", id="identity(int)"),
]

overwrite_static_partition_transforms = [
    pytest.param("day(_c8)", id="day(date_col)"),
    pytest.param("truncate(5, _c6)", id="truncate(5, string_col)"),
    pytest.param("bucket(16, _c8)", id="bucket(16, date_col)"),
    pytest.param("_c3", id="identity(long)"),
]

overwrite_dynamic_partition_transforms = [
    pytest.param("month(_c9)", id="month(timestamp_col)"),
    pytest.param("truncate(10, _c13)", id="truncate(10, decimal32_col)"),
    pytest.param("bucket(16, _c9)", id="bucket(16, timestamp_col)"),
    pytest.param("_c6", id="identity(string)"),
]

# Each list pairs (partition_col_sql, <mode>). Always used together as
# `@pytest.mark.parametrize("partition_col_sql,<mode_arg>", <list>)`, so the
# tuple order must match the order of names in the parametrize arg string
# (`partition_col_sql` first, then the mode arg). Pytest binds parameters to the
# test function by name, so the function-signature order does not matter.
#
# Each list is just two cases (one per mode, exercising one transform family):
# the 26-transform partition-writer matrix lives in the append anchor
# (test_insert_into_partitioned_table_full_coverage); per-op cross-mode coverage
# of year(_c9) is preserved in the non-_full_coverage basic tests.
delete_partition_transforms_distributed = [
    pytest.param("day(_c9)", 'copy-on-write', id="copy-on-write-day(timestamp_col)"),
    pytest.param("hour(_c9)", 'merge-on-read', id="merge-on-read-hour(timestamp_col)"),
]

update_partition_transforms_distributed = [
    pytest.param("truncate(10, _c14)", 'copy-on-write', id="copy-on-write-truncate(10, decimal64_col)"),
    pytest.param("truncate(10, _c15)", 'merge-on-read', id="merge-on-read-truncate(10, decimal128_col)"),
]

merge_partition_transforms_distributed = [
    pytest.param("bucket(16, _c6)", 'copy-on-write', id="copy-on-write-bucket(16, string_col)"),
    pytest.param("bucket(16, _c13)", 'merge-on-read', id="merge-on-read-bucket(16, decimal32_col)"),
]

# All data types of iceberg, not all of them are supported by spark-rapids for now
iceberg_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen ]] + \
                   [simple_string_to_string_map_gen,
                    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
                    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
                    MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

# Broad Iceberg end-to-end coverage including nested types
iceberg_full_gens_list = ([byte_gen, short_gen, IntegerGen(nullable=False), LongGen(nullable=False),
                          float_gen, double_gen, string_gen, boolean_gen, date_gen,
                          timestamp_gen, binary_gen, ArrayGen(binary_gen), ArrayGen(byte_gen),
                          ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
                          ArrayGen(timestamp_gen), ArrayGen(decimal_gen_64bit),
                          ArrayGen(ArrayGen(byte_gen)),
                          StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen],
                                     ['child2', float_gen], ['child3', decimal_gen_64bit]]),
                          ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen],
                                              ['child2', int_gen]]))] +
                          iceberg_map_gens + decimal_gens)

# Nested-type coverage for the focused positive GPU Iceberg DML tests reuses
# `iceberg_full_gens_list`. The merge/update/delete `_nested_types` tests use `_c0` for arithmetic
# (`_c0 = _c0 + 100`, `_c0 % 3 = 0`) and joins (`ON t._c0 = s._c0`), so we override that single
# slot with a non-null `IntegerGen` instead of the nullable `byte_gen` `iceberg_full_gens_list`
# puts there.
iceberg_nested_write_gens_list = (
    [IntegerGen(nullable=False)] + iceberg_full_gens_list[1:])

iceberg_write_enabled_conf = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.rapids.sql.format.iceberg.enabled": "true",
    "spark.rapids.sql.format.iceberg.write.enabled": "true",
    # WriteDeltaExec is disabled by default as it's experimental, but we need it enabled
    # for merge-on-read (MOR) DML operations (UPDATE/DELETE/MERGE with write.*.mode='merge-on-read')
    "spark.rapids.sql.exec.WriteDeltaExec": "true",
}

def can_be_eq_delete_col(data_gen: DataGen) -> bool:
    return (not isinstance(data_gen.data_type, FloatType) and
            not isinstance(data_gen.data_type, DoubleType) and
            # See https://github.com/NVIDIA/spark-rapids/issues/12469, iceberg spec doesn't prevent
            # binary type as eq delete column, but it seems there are bugs in iceberg equality
            # loader, we should remove this after the bug is fixed.
            not isinstance(data_gen.data_type, BinaryType))

# Representative eq-delete column pairs. The full C(14, 2) = 91-pair matrix
# previously tested every column combination, but eq-delete correctness only
# requires that every eligible column type appears in some pair (not every pair
# of types). The 10 pairs below cover all 14 eligible eq-delete columns of
# iceberg_table_gen (_c0..c3, _c6..c15; _c4 float and _c5 double are excluded by
# can_be_eq_delete_col), spanning every distinct primitive Spark type used there
# while mixing numeric/string/temporal/decimal across each pair.
representative_eq_column_combinations = [
    pytest.param(("_c0", "_c12"), id="(byte, decimal128)"),
    pytest.param(("_c1", "_c6"),  id="(short, string)"),
    pytest.param(("_c2", "_c8"),  id="(int, date)"),
    pytest.param(("_c3", "_c9"),  id="(long, timestamp)"),
    pytest.param(("_c7", "_c10"), id="(boolean, decimal32)"),
    pytest.param(("_c11", "_c13"), id="(decimal64, decimal32_special)"),
    pytest.param(("_c14", "_c15"), id="(decimal64_special, decimal128_special)"),
    pytest.param(("_c6", "_c9"),  id="(string, timestamp)"),
    pytest.param(("_c0", "_c11"), id="(byte, decimal64)"),
    pytest.param(("_c2", "_c12"), id="(int, decimal128)"),
]

# Reader-type canary: pairs to exercise the three rapids_reader_types against the
# eq-delete read path. Three pairs is enough to confirm that reader-type selection
# composes with eq-delete handling; the full per-pair coverage runs against the
# default reader only in test_iceberg_v2_eq_deletes. Each canary pair is distinct
# from representative_eq_column_combinations so the two tests don't overlap.
eq_reader_canary_pairs = [
    pytest.param(("_c2", "_c6"),  id="(int, string)"),
    pytest.param(("_c9", "_c12"), id="(timestamp, decimal128)"),
    pytest.param(("_c1", "_c10"), id="(short, decimal32)"),
]

def setup_base_iceberg_table(spark_tmp_table_factory,
                             seed: Optional[int] = None,
                             table_prop: Optional[Dict[str, str]] = None) -> str:

    gen_list = list(zip(iceberg_base_table_cols, iceberg_gens_list))
    table_name = get_full_table_name(spark_tmp_table_factory)
    tmp_view_name = spark_tmp_table_factory.get()

    if table_prop is None:
        table_prop = {'format-version':'2', 'write.delete.mode': 'merge-on-read'}
    else:
        table_prop = {**table_prop, 'format-version': '2', 'write.delete.mode': 'merge-on-read'}
    table_prop = _build_tblprops(table_prop)

    table_prop_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])

    def set_iceberg_table(spark: SparkSession):
        df = gen_df(spark, gen_list, seed=seed)
        df.createOrReplaceTempView(tmp_view_name)
        spark.sql(f"CREATE TABLE {table_name} USING ICEBERG "
                  f"TBLPROPERTIES ({table_prop_sql}) "
                  f"PARTITIONED BY (bucket(16, _c2), bucket(16, _c3)) "
                  f"AS SELECT * FROM {tmp_view_name}")

    with_cpu_session(set_iceberg_table)
    return table_name


def _add_eq_deletes(spark: SparkSession, eq_delete_cols: List[str], row_count: int, table_name: str,
                    spark_tmp_path):
    for eq_delete_col in eq_delete_cols:
        assert can_be_eq_delete_col(iceberg_table_gen[eq_delete_col]), \
            f"{eq_delete_col} can't be used as eq delete column"


    spark_warehouse_dir = spark.conf.get("spark.sql.catalog.spark_catalog.warehouse")

    temp_dir = tempfile.mkdtemp(dir=spark_tmp_path)
    deletes = (spark.table(table_name)
               .select(eq_delete_cols + ["_partition"])
               .distinct()
               .orderBy(eq_delete_cols)
               .limit(row_count)
               .repartition(1))
    deletes.write.parquet(temp_dir, mode='overwrite')
    parquet_files = [f for f in os.listdir(temp_dir) if f.endswith(".parquet")]
    assert len(parquet_files) == 1, "Only one delete parquet file should be created"
    delete_parquet_file_path = os.path.join(temp_dir, parquet_files[0])
    spark.sql(f"select iceberg_add_eq_deletes('{spark_warehouse_dir}', '{table_name}', "
              f"'{delete_parquet_file_path}')").collect()
    spark.sql(f"REFRESH TABLE {table_name}")


def _change_table(table_name, table_func: Callable[[SparkSession], None], message: str):
    def change_table(spark: SparkSession):
        before_count = spark.table(table_name).count()
        table_func(spark)
        spark.sql(f"REFRESH TABLE {table_name}")
        after_count = spark.table(table_name).count()
        if before_count == after_count:
            logging.warning(message)

    with_cpu_session(change_table,
                     conf = {"spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                         "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED"})

def get_full_table_name(spark_tmp_table_factory):
    return f"default.{spark_tmp_table_factory.get()}"


def schema_to_ddl(spark, schema):
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json()).toDDL()


# Base table properties applied to every Iceberg test table.
# Disables the fanout writer to prevent OOM in CI.  S3TablesCatalog does not
# honor catalog-level table-default properties, so this must be set per table.
_BASE_TBLPROPS = {'write.spark.fanout.enabled': False}


def _to_tblprops_str(props: dict) -> dict:
    """Convert property values to their SQL-safe string representation."""
    return {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in props.items()}


def _build_tblprops(extra_props: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Build table properties dict with base props merged in.
    Caller properties take precedence over base.
    Returns string-valued dict suitable for SQL interpolation."""
    if extra_props is None:
        return _to_tblprops_str(_BASE_TBLPROPS)
    return _to_tblprops_str({**_BASE_TBLPROPS, **extra_props})


# SQL fragment for raw CREATE TABLE statements that have no other TBLPROPERTIES.
_BASE_TBLPROPS_SQL = "TBLPROPERTIES (" + \
    ", ".join(f"'{k}' = '{v}'" for k, v in _build_tblprops().items()) + ")"


def read_parquet_codec(spark: SparkSession, path: str) -> str:
    """Open the Parquet footer of `path` and return the codec name of the first column of
    the first row group (uppercase, e.g. "ZSTD", "UNCOMPRESSED")."""
    jvm = spark_jvm()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(hadoop_conf, hadoop_path)
    try:
        blocks = reader.getFooter().getBlocks()
        assert blocks.size() > 0, f"Parquet file {path} has no row groups"
        cols = blocks.get(0).getColumns()
        assert cols.size() > 0, f"Row group 0 in {path} has no column chunks"
        return cols.get(0).getCodec().name()
    finally:
        reader.close()


def assert_iceberg_files_use_codec(spark: SparkSession, table_name: str, expected_codec: str,
                                   files_table: str = "files") -> None:
    """Assert every Parquet file listed in `{table_name}.{files_table}` uses `expected_codec`.
    Parquet reports the "uncompressed" Iceberg codec as "UNCOMPRESSED" in the footer."""
    file_rows = spark.sql(f"SELECT file_path FROM {table_name}.{files_table}").collect()
    assert len(file_rows) > 0, f"no files found in {table_name}.{files_table}"
    expected_footer = expected_codec.upper()
    for row in file_rows:
        actual = read_parquet_codec(spark, row.file_path)
        assert actual == expected_footer, \
            f"expected codec {expected_footer} but file {row.file_path} used {actual}"


def create_iceberg_table(table_name: str,
                         partition_col_sql: Optional[str] = None,
                         table_prop: Optional[Dict[str, str]] = None,
                         df_gen: Optional[Callable[[SparkSession], DataFrame]] = None) -> str:
    if table_prop is None:
        table_prop = {'format-version':'1'}
    table_prop = _build_tblprops(table_prop)

    if df_gen is None:
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))


    table_prop_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])

    def set_iceberg_table(spark: SparkSession):
        df = df_gen(spark)
        ddl = schema_to_ddl(spark, df.schema)

        if partition_col_sql is None:
            sql = (f"CREATE TABLE {table_name} "
                   f"({ddl}) "
                   f"USING ICEBERG "
                   f"TBLPROPERTIES ({table_prop_sql})")
        else:
            sql = (f"CREATE TABLE {table_name} "
                   f"({ddl}) "
                   f"USING ICEBERG "
                   f"PARTITIONED BY ({partition_col_sql}) "
                   f"TBLPROPERTIES ({table_prop_sql})")

        spark.sql(sql)

    with_cpu_session(set_iceberg_table)
    return table_name
