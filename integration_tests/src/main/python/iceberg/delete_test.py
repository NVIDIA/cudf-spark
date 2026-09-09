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
from pyspark.sql import Row, functions as f

from asserts import assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import LongGen, StringGen, StructGen, TimestampGen, UniqueLongGen, gen_df
from iceberg import (_add_eq_deletes_from_df, _build_tblprops, get_full_table_name,
                     iceberg_unsupported_mark, supports_iceberg_v3,
                     ICEBERG_V3_UNSUPPORTED_REASON)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import with_cpu_session

pytestmark = iceberg_unsupported_mark


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Requires local equality-delete UDF")
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_default_on_implicit_equality_delete_field(
        spark_tmp_table_factory,
        spark_tmp_path,
        register_iceberg_add_eq_deletes_udf):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())
    initial_data_length = 2
    current_id = None

    def setup_table(spark):
        nonlocal current_id
        spark.sql(
            f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG PARTITIONED BY (id) "
            f"TBLPROPERTIES ({props_sql})")
        initial_data = gen_df(
            spark,
            [("id", UniqueLongGen())],
            length=initial_data_length).select(f.abs(f.col("id")).alias("id"))
        target_id = initial_data.orderBy("id").first().id
        initial_data.coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        timestamp_ntz_type = jvm.org.apache.iceberg.types.Types.TimestampType.withoutZone()
        default_value = jvm.org.apache.iceberg.expressions.Literal.of(
            "2024-01-02T03:04:05").to(timestamp_ntz_type)
        table.updateSchema().addColumn(
            "_c9",
            timestamp_ntz_type,
            default_value).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        current_data = (gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("_c9", TimestampGen(nullable=False, tzinfo=None))],
            length=1)
            .select(
                (f.abs(f.col("id")) + initial_data_length).alias("id"),
                f.col("_c9")))
        current_id = current_data.first().id
        current_data.coalesce(1).writeTo(table_name).append()

        # _c9 is intentionally omitted from the query below. The equality-delete file makes it an
        # implicit required read field, and the old data file requires its initial default.
        deletes = (spark.table(table_name)
                   .where(f.col("id") == target_id)
                   .select("_c9", "_partition")
                   .coalesce(1))
        _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)

    with_cpu_session(setup_table)
    remaining_rows = with_cpu_session(
        lambda spark: spark.sql(f"SELECT id FROM {table_name} ORDER BY id").collect())
    assert len(remaining_rows) == initial_data_length
    assert Row(current_id) in remaining_rows
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(f"SELECT id FROM {table_name}"),
        "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true"})


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Requires local equality-delete UDF")
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_nested_equality_delete_field_falls_back(
        spark_tmp_table_factory,
        spark_tmp_path,
        register_iceberg_add_eq_deletes_udf):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())
    initial_data_length = 2
    current_data_length = 1
    current_ids = []

    def setup_table(spark):
        nonlocal current_ids
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, payload STRUCT<existing: STRING>) USING ICEBERG PARTITIONED BY (id) "
            f"TBLPROPERTIES ({props_sql})")
        initial_data = gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("payload", StructGen(
                 [("existing", StringGen(nullable=False))], nullable=False))],
            length=initial_data_length).select(
                f.abs(f.col("id")).alias("id"), f.col("payload"))
        target_id = initial_data.orderBy("id").first().id
        initial_data.coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        string_type = jvm.org.apache.iceberg.types.Types.StringType.get()
        default_value = jvm.org.apache.iceberg.expressions.Literal.of("legacy")
        table.updateSchema().addColumn(
            "payload", "delete_key", string_type, default_value).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        current_data = (gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("payload", StructGen([
                 ("existing", StringGen(nullable=False)),
                 ("delete_key", StringGen(pattern="current_[0-9]{4}", nullable=False))
             ], nullable=False))],
            length=current_data_length)
            .select(
                (f.abs(f.col("id")) + initial_data_length).alias("id"),
                f.col("payload")))
        current_ids = [row.id for row in current_data.select("id").collect()]
        current_data.coalesce(1).writeTo(table_name).append()

        # Keep the parent struct in the Parquet delete row so Iceberg can preserve the nested field
        # ID. Only delete_key is included below, making it the equality field rather than payload.
        deletes = (spark.table(table_name)
                   .where(f.col("id") == target_id)
                   .select(
                       f.struct(f.col("payload.delete_key").alias("delete_key")).alias("payload"),
                       f.col("_partition"))
                   .coalesce(1))
        _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)

    with_cpu_session(setup_table)
    # Keep the nested equality field projected so the CPU baseline can apply the delete. The
    # implicit-field fallback path is covered by GpuPostProcessorSuite.
    read_query = f"SELECT id, payload.delete_key AS delete_key FROM {table_name}"
    remaining_rows = with_cpu_session(
        lambda spark: spark.sql(f"{read_query} ORDER BY id").collect())
    assert len(remaining_rows) == initial_data_length - 1 + current_data_length
    remaining_ids = {row.id for row in remaining_rows}
    assert all(current_id in remaining_ids for current_id in current_ids)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(read_query),
        "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true"})
