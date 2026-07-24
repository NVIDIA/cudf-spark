# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import pytest

from delta_lake_merge_common import *
from marks import *
from pyspark.sql.types import *
from spark_session import (is_before_spark_320, is_databricks_runtime, spark_version,
                           supports_delta_lake_deletion_vectors, is_before_spark_353,
                           is_spark_400_or_later, is_databricks143,
                           is_databricks173_or_later)

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true"})

if is_spark_400_or_later():
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    delta_merge_enabled_conf = copy_and_update(delta_merge_enabled_conf, {"spark.sql.adaptive.enabled": "false"})

fallback_test_params = [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                        {"spark.rapids.sql.format.parquet.enabled": "false"},
                        {"spark.rapids.sql.format.parquet.write.enabled": "false"},
                        {"spark.rapids.sql.command.MergeIntoCommand": "false"},
                        ]
if is_before_spark_353():
    # MergeCommand is disabled by default before Spark 3.5.3.
    # In Spark 3.5.3 and later, MergeCommand is enabled by default, but may not run on GPU yet
    # because of https://github.com/NVIDIA/spark-rapids/issues/8042.
    # See https://github.com/NVIDIA/spark-rapids/issues/13021#issuecomment-3166724473 for details.
    fallback_test_params.append(delta_writes_enabled_conf)

@allow_non_gpu(delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf", fallback_test_params, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_disabled_fallback(spark_tmp_path, spark_tmp_table_factory, disable_conf, enable_deletion_vectors):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path,
                                  delta_write_fallback_check, conf=disable_conf)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False, enable_deletion_vectors=enable_deletion_vectors,
                         src_table_func=lambda spark: unary_op_df(spark, SetValuesGen(IntegerType(), range(100))),
                         dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu('ColumnarToRowExec', 'BroadcastExchangeExec','BroadcastHashJoinExec', delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(supports_delta_lake_deletion_vectors(), reason="Deletion Vectors aren't supported")
def test_delta_merge_fallback_with_deletion_vectors(spark_tmp_path, spark_tmp_table_factory):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path,
                                  delta_write_fallback_check,
                                  conf=copy_and_update(delta_merge_enabled_conf, {"spark.rapids.sql.format.delta.write.enabled": "false"}))
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False, enable_deletion_vectors=True,
                         src_table_func=lambda spark: unary_op_df(spark, SetValuesGen(IntegerType(), range(100))),
                         dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu("ExecutedCommandExec,BroadcastHashJoinExec,ColumnarToRowExec,BroadcastExchangeExec,DataWritingCommandExec", delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and spark_version() < "3.3.2", reason="NOT MATCHED BY SOURCE added in DBR 12.2")
@pytest.mark.skipif((not is_databricks_runtime()) and is_before_spark_340(), reason="NOT MATCHED BY SOURCE added in Delta Lake 2.4")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_not_matched_by_source_fallback(spark_tmp_path, spark_tmp_table_factory, enable_deletion_vectors):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path, "ExecutedCommandExec", conf = delta_merge_enabled_conf)
    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.a == {dest_table}.a " \
                "WHEN MATCHED THEN " \
                "  UPDATE SET {dest_table}.b = {src_table}.b " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT (a, b) VALUES ({src_table}.a, {src_table}.b) " \
                "WHEN NOT MATCHED BY SOURCE AND {dest_table}.b > 0 THEN " \
                "  UPDATE SET {dest_table}.b = 0"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False, enable_deletion_vectors=enable_deletion_vectors,
                         src_table_func=lambda spark: binary_op_df(spark, SetValuesGen(IntegerType(), range(10))),
                         dest_table_func=lambda spark: binary_op_df(spark, SetValuesGen(IntegerType(), range(20, 30))),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu("BroadcastHashJoinExec,ColumnarToRowExec,BroadcastExchangeExec,"
               "UnionExec,UnionWithLocalDataExec,RangeExec",
               delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks143(),
                    reason="The shimmed UnionWithLocalDataExec fallback is DBR 14.3-specific")
def test_delta_merge_not_matched_by_source_union_source_fallback(
        spark_tmp_path, spark_tmp_table_factory):
    materialize_conf = copy_and_update(delta_merge_enabled_conf, {
        "spark.databricks.delta.merge.materializeSource": "all"})

    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path,
                                  ["ExecutedCommandExec", "UnionExec"],
                                  conf=materialize_conf)

    def create_test_data(spark, num=120):
        from datetime import datetime, timedelta, timezone
        import pandas as pd

        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        categories = ["electronics", "books", "home_garden", "toys"]
        rows = {
            "product_id": [],
            "sale_datetime": [],
            "sale_date": [],
            "product_category": [],
            "sales_amount": [],
            "quantity": []
        }
        for i in range(num):
            if i % 10 == 0:
                rows["sale_datetime"].append(None)
                rows["sale_date"].append(None)
            else:
                dt = start + timedelta(days=i * 3)
                rows["sale_datetime"].append(dt)
                rows["sale_date"].append(dt.date())
            rows["product_id"].append(i)
            rows["product_category"].append(categories[i % len(categories)])
            rows["sales_amount"].append(float(100 + i))
            rows["quantity"].append((i % 10) + 1)

        df = spark.createDataFrame(pd.DataFrame(rows))
        return df.withColumn("year", f.year("sale_datetime"))

    def extra_source_df(spark, start, end):
        return spark.range(start, end).select(
            f.col("id").alias("product_id"),
            f.lit(None).cast(TimestampType()).alias("sale_datetime"),
            f.lit(None).cast(DateType()).alias("sale_date"),
            f.lit("books").alias("product_category"),
            (f.col("id").cast(DoubleType()) * 10.0).alias("sales_amount"),
            f.lit(1).cast(LongType()).alias("quantity"),
            f.lit(2025).cast(IntegerType()).alias("year"))

    def source_df(spark):
        target_df = create_test_data(spark, num=120)
        matched = target_df.filter(f.col("product_id") < 60)
        inserted = extra_source_df(spark, 200, 260)
        return matched.unionByName(inserted)

    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.product_id == {dest_table}.product_id " \
                "WHEN MATCHED THEN " \
                "  UPDATE SET {dest_table}.sales_amount = {src_table}.sales_amount " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT * " \
                "WHEN NOT MATCHED BY SOURCE THEN " \
                "  DELETE"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False,
                         enable_deletion_vectors=False,
                         src_table_func=source_df,
                         dest_table_func=lambda spark: create_test_data(spark, num=120),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("disable_conf", [
    "spark.rapids.sql.exec.RapidsProcessDeltaMergeJoinExec",
    "spark.rapids.sql.expression.Add"], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_partial_fallback_via_conf(spark_tmp_path, spark_tmp_table_factory,
                                               use_cdf, partition_columns, num_slices, disable_conf, enable_deletion_vectors):
    src_range, dest_range = range(20), range(10, 30)
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices) \
        .groupBy("a").agg(f.max("b").alias("b"),f.min("c").alias("c"))
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
                " WHEN MATCHED THEN UPDATE SET d.a = s.a + 4 WHEN NOT MATCHED THEN INSERT *"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    conf = copy_and_update(delta_merge_enabled_conf, { disable_conf: "false" })
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf, enable_deletion_vectors,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns=partition_columns, conf=conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("table_ranges", [(range(20), range(10)),  # partial insert of source
                                          (range(5), range(5)),  # no-op insert
                                          (range(10), range(20, 30))  # full insert of source
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                           use_cdf, partition_columns, num_slices, enable_deletion_vector):
    do_test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory,
                                              table_ranges, use_cdf, enable_deletion_vector, partition_columns,
                                              num_slices, num_slices == 1, delta_merge_enabled_conf)

@allow_non_gpu(delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="The conf merge.materializeSource isn't available before Delta 3.3.x")
# As part of the merge command, Delta Lake will try to materialize tables in the process.
# This materialize will not show up as part of the original plan as it's done dynamically from GpuMergeIntoCommand
# therefore we will use the same technique OSS is using to assert it was materialized
# ref: https://github.com/delta-io/delta/blob/4b2beb096017d813475f692270bc20c113c0d974/spark/src/main/scala/org/apache/spark/sql/delta/commands/merge/MergeIntoMaterializeSource.scala#L364
# This test forces materialization by setting the delta property
def test_delta_materialize_merge(spark_tmp_path, spark_tmp_table_factory):
    materialize_conf = copy_and_update(delta_merge_enabled_conf, {"spark.databricks.delta.merge.materializeSource": "all"})
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), range(20)), num_slices=10)
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), range(10)), num_slices=10)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"

    def read_data(spark, path):
        df = read_delta_path(spark, path)
        return df.sort(df.columns)

    def checker(data_path, do_merge):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        # compare resulting dataframe from the merge operation (some older Spark versions return empty here)
        assert_collect(do_merge, data_path, materialize_conf)
        # compare merged table data results, read both via CPU to make sure GPU write can be read by CPU
        cpu_is_checkpointed = with_cpu_session(
            lambda spark: read_data(spark, cpu_path).rdd.isCheckpointed(), conf=materialize_conf)
        gpu_is_checkpointed = with_cpu_session(
            lambda spark: read_data(spark, gpu_path).rdd.isCheckpointed(), conf=materialize_conf)
        assert_equal(cpu_is_checkpointed, gpu_is_checkpointed)

    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                            use_cdf=False,
                            enable_deletion_vectors=False,
                            src_table_func=src_table_func,
                            dest_table_func=dest_table_func,
                            merge_sql=merge_sql,
                            check_func=checker,
                            partition_columns=None)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("table_ranges_expect_write", [
    ((range(10), range(20)), True),  # partial delete of target
    ((range(5), range(5)), True),  # full delete of target
    ((range(10), range(20, 30)), False)  # no-op delete. gpu write is not expected
], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges_expect_write,
                                       use_cdf, partition_columns, num_slices, enable_deletion_vector):
    table_ranges, expect_write = table_ranges_expect_write
    do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, enable_deletion_vector, partition_columns, num_slices,
                                          num_slices == 1, delta_merge_enabled_conf, expect_write=expect_write)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices, enable_deletion_vector):
    do_test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, enable_deletion_vector,
                                        num_slices, num_slices == 1, delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Issue-specific MERGE smoke coverage for Databricks 17.3+")
def test_delta_merge_standard_upsert_db173_smoke(spark_tmp_path, spark_tmp_table_factory):
    # Keep the issue-specific DBR 17.3 coverage small and deterministic so a single cloud run can
    # quickly validate that the MERGE GPU path is wired up before the broader matrix is exercised.
    do_test_delta_merge_standard_upsert(
        spark_tmp_path,
        spark_tmp_table_factory,
        use_cdf=False,
        enable_deletion_vectors=False,
        num_slices=10,
        compare_logs=False,
        conf=delta_merge_enabled_conf)


@allow_non_gpu("ExecutedCommandExec,BroadcastHashJoinExec,ColumnarToRowExec,BroadcastExchangeExec,DataWritingCommandExec", delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Issue-specific fallback coverage for Databricks 17.3+")
def test_delta_merge_not_matched_by_source_db173_fallback(spark_tmp_path, spark_tmp_table_factory):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path, "ExecutedCommandExec",
                                  conf=delta_merge_enabled_conf)

    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.a == {dest_table}.a " \
                "WHEN MATCHED THEN " \
                "  UPDATE SET {dest_table}.b = {src_table}.b " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT (a, b) VALUES ({src_table}.a, {src_table}.b) " \
                "WHEN NOT MATCHED BY SOURCE AND {dest_table}.b > 0 THEN " \
                "  UPDATE SET {dest_table}.b = 0"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False, enable_deletion_vectors=False,
                         src_table_func=lambda spark: binary_op_df(
                             spark, SetValuesGen(IntegerType(), range(10))),
                         dest_table_func=lambda spark: binary_op_df(
                             spark, SetValuesGen(IntegerType(), range(20, 30))),
                         merge_sql=merge_sql,
                         check_func=checker)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Issue-specific deletion vector fallback coverage for Databricks 17.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_merge_deletion_vector_db173_fallback(spark_tmp_path, spark_tmp_table_factory):
    conf = copy_and_update(
        delta_merge_enabled_conf,
        {"spark.databricks.delta.merge.deletionVectors.persistent": "true"})

    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path, "ExecutedCommandExec",
                                  conf=conf)

    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.a == {dest_table}.a " \
                "WHEN MATCHED THEN DELETE"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False, enable_deletion_vectors=True,
                         src_table_func=lambda spark: unary_op_df(
                             spark, SetValuesGen(IntegerType(), range(10))),
                         dest_table_func=lambda spark: unary_op_df(
                             spark, SetValuesGen(IntegerType(), range(20))),
                         merge_sql=merge_sql,
                         check_func=checker)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Issue-specific schema evolution coverage for Databricks 17.3+")
def test_delta_merge_schema_evolution_db173_smoke(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_table = spark_tmp_table_factory.get()

    def src_table_func(spark):
        return spark.createDataFrame([
            (1, "updated", "new-col"),
            (3, "inserted", "brand-new")
        ], ["a", "b", "c"])

    def dest_table_func(spark):
        return spark.createDataFrame([
            (1, "old"),
            (2, "keep")
        ], ["a", "b"])

    def setup_tables(spark):
        setup_delta_dest_tables(
            spark,
            data_path,
            dest_table_func,
            use_cdf=False,
            enable_deletion_vectors=False)
        src_table_func(spark).createOrReplaceTempView(src_table)

    def do_merge(spark, path):
        src_table_func(spark).createOrReplaceTempView(src_table)
        return spark.sql(
            "MERGE WITH SCHEMA EVOLUTION INTO delta.`{path}` AS dest USING {src_table} AS src "
            "ON dest.a == src.a "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *".format(
                path=path,
                src_table=src_table)).collect()

    def read_func(spark, path):
        return read_delta_path(spark, path).select("a", "b", "c").sort("a")

    with_cpu_session(setup_tables)
    assert_gpu_and_cpu_writes_are_equal_collect(do_merge, read_func, data_path,
                                                conf=delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("merge_sql", [
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'q' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN NOT MATCHED THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN NOT MATCHED AND s.b > 'q' THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'a' AND s.b < 'g' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN MATCHED AND s.b > 'g' AND s.b < 'z' THEN UPDATE SET d.a = s.a / 4, d.b = concat('extra', s.b)" \
    " WHEN NOT MATCHED AND s.b > 'b' AND s.b < 'f' THEN INSERT *" \
    " WHEN NOT MATCHED AND s.b > 'f' AND s.b < 'z' THEN INSERT (b) VALUES ('not here')" ], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, merge_sql, num_slices,
                                           enable_deletion_vector):
    do_test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, enable_deletion_vector,
                                              merge_sql, num_slices, num_slices == 1, delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                                             num_slices, enable_deletion_vector):
    do_test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path,
                                                                spark_tmp_table_factory, use_cdf, enable_deletion_vector,
                                                                num_slices, num_slices == 1,
                                                                delta_merge_enabled_conf,
                                                                expect_write=False)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf, enable_deletion_vector):
    do_test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf, enable_deletion_vector,
                                                delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.xfail(not is_databricks_runtime() and is_before_spark_353(), reason="https://github.com/NVIDIA/spark-rapids/issues/7573")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_merge_dataframe_api(spark_tmp_path, use_cdf, num_slices, enable_deletion_vector):
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/DELTA_DATA"
    dest_table_func = lambda spark: two_col_df(spark, SetValuesGen(IntegerType(), [None] + list(range(100))), string_gen, seed=1, num_slices=num_slices)
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, enable_deletion_vector))
    def do_merge(spark, path):
        # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
        src_df = two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
        dest_table = DeltaTable.forPath(spark, path)
        dest_table.alias("dest").merge(src_df.alias("src"), "dest.a == src.a") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
    assert_gpu_and_cpu_writes_are_equal_collect(do_merge, read_func, data_path, conf=delta_merge_enabled_conf)
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    if num_slices == 1:
        with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
