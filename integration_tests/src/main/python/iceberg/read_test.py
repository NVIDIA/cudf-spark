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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import spark_jvm
from iceberg import _build_tblprops, get_full_table_name, iceberg_unsupported_mark, \
    supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import with_cpu_session, with_gpu_session

pytestmark = iceberg_unsupported_mark


def _setup_iceberg_v3_defaults_table(table_name):
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, s STRUCT<present: BIGINT>) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        spark.sql(
            f"INSERT INTO {table_name} VALUES "
            "(1, named_struct('present', 10L)), "
            "(2, named_struct('present', 20L)), "
            "(3, CAST(NULL AS STRUCT<present: BIGINT>))")

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        literals = jvm.org.apache.iceberg.expressions.Literal
        types = jvm.org.apache.iceberg.types.Types
        update = table.updateSchema()

        def add_default(name, iceberg_type, value):
            update.addColumn(name, iceberg_type, literals.of(value).to(iceberg_type))

        # Cover every Iceberg 1.9 primitive type that Spark SQL can query. Iceberg TIME is omitted
        # because Iceberg's Spark adapter rejects it with "Spark does not support time fields".
        update.addRequiredColumn(
            "required_added", types.IntegerType.get(), None, literals.of(7))
        update.addColumn("optional_added", types.StringType.get(), literals.of("legacy"))
        update.addColumn("s", "nested_added", types.IntegerType.get(), literals.of(11))
        add_default("boolean_added", types.BooleanType.get(), True)
        add_default("long_added", types.LongType.get(), jvm.java.lang.Long.valueOf(5000000000))
        add_default("float_added", types.FloatType.get(), jvm.java.lang.Float.valueOf("1.25"))
        add_default("double_added", types.DoubleType.get(), 2.5)
        add_default("date_added", types.DateType.get(), "2024-01-02")
        add_default(
            "timestamp_added",
            types.TimestampType.withZone(),
            "2024-01-02T03:04:05Z")
        add_default(
            "binary_added", types.BinaryType.get(),
            jvm.java.nio.ByteBuffer.wrap(bytearray([1, 2, 3])))
        add_default(
            "decimal_added", types.DecimalType.of(9, 2),
            jvm.java.math.BigDecimal("12345.67"))

        # These types are representable by Iceberg/Spark but are not supported by the GPU
        # default-materialization path and must cause scan fallback when projected.
        add_default(
            "timestamp_ntz_added",
            types.TimestampType.withoutZone(),
            "2024-01-02T03:04:05")
        add_default(
            "uuid_added", types.UUIDType.get(),
            "123e4567-e89b-12d3-a456-426614174000")
        add_default(
            "fixed_added", types.FixedType.ofLength(3),
            jvm.java.nio.ByteBuffer.wrap(bytearray([4, 5, 6])))
        update.commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_initial_defaults_all_types(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}
    query = (
        f"SELECT id, s.present, s.nested_added, required_added, optional_added, "
        "boolean_added, long_added, float_added, double_added, date_added, "
        "timestamp_added, binary_added, decimal_added "
        f"FROM {table_name} ORDER BY id")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(query),
        conf=v3_conf)

    def assert_gpu_scan(spark):
        df = spark.sql(query)
        df.collect()
        spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertContains(
            df._jdf, "GpuBatchScanExec")

    with_gpu_session(assert_gpu_scan, conf=v3_conf)

@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_unsupported_initial_defaults_fallback(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}

    for unsupported_column in ["timestamp_ntz_added", "uuid_added", "fixed_added"]:
        assert_gpu_fallback_collect(
            lambda spark, column=unsupported_column: spark.sql(
                f"SELECT id, {column} FROM {table_name}"),
            "BatchScanExec",
            conf=v3_conf)
