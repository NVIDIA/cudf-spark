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

import re

import pyspark.sql.functions as f
import pytest

from delta_lake_merge_common import *
from marks import *
from pyspark.sql.types import *
from spark_session import (is_before_spark_320, is_databricks_runtime, spark_version,
                           supports_delta_lake_deletion_vectors, is_before_spark_353,
                           is_spark_400_or_later, is_databricks143,
                           is_databricks173_or_later, is_spark_41x)

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true"})

if is_spark_400_or_later():
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    delta_merge_enabled_conf = copy_and_update(delta_merge_enabled_conf, {"spark.sql.adaptive.enabled": "false"})

delta_merge_no_cpu_bridge_conf = copy_and_update(
    delta_merge_enabled_conf, {"spark.rapids.sql.expression.cpuBridge.enabled": "false"})

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


def _assert_gpu_merge_processor(do_merge, data_path, conf, expect_write=True):
    assert expect_write
    cpu_result = with_cpu_session(lambda spark: do_merge(spark, data_path + "/CPU"), conf=conf)

    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        gpu_result = with_gpu_session(
            lambda spark: do_merge(spark, data_path + "/GPU"), conf=conf)
        captured_plans = callback.getResultsWithTimeout(10000)
    finally:
        callback.endCapture()

    assert_equal(cpu_result, gpu_result)
    # The CPU expression bridge is disabled for this test, so finding the GPU merge processor
    # proves that CheckOverflowInTableWrite and the other merge expressions were replaced on GPU.
    class_name = "GpuRapidsProcessDeltaMergeJoinExec"
    assert any(callback.contains(plan, class_name) for plan in captured_plans), \
        f"{class_name} was not found in the captured MERGE plans"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="CheckOverflowInTableWrite is required on DBR 17.3+")
def test_delta_merge_check_overflow_in_table_write(
        spark_tmp_path, spark_tmp_table_factory):
    def source_df(spark):
        return spark.sql("""
            SELECT timestamp '2024-01-01 00:00:00' AS timestampF,
                   CAST(2 AS INT) AS byteF
        """)

    def target_df(spark):
        return spark.sql("""
            SELECT timestamp '2024-01-01 00:00:00' AS timestampF,
                   CAST(1 AS TINYINT) AS byteF
            UNION ALL
            SELECT timestamp '2024-01-02 00:00:00' AS timestampF,
                   CAST(2 AS TINYINT) AS byteF
        """)

    merge_sql = "MERGE INTO {dest_table} AS target USING {src_table} AS source " \
                "ON target.timestampF = source.timestampF " \
                "WHEN MATCHED THEN UPDATE SET byteF = source.byteF"
    assert_delta_sql_merge_collect(
        spark_tmp_path,
        spark_tmp_table_factory,
        use_cdf=False,
        enable_deletion_vectors=False,
        src_table_func=source_df,
        dest_table_func=target_df,
        merge_sql=merge_sql,
        compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="CheckOverflowInTableWrite is required on DBR 17.3+")
def test_delta_merge_check_overflow_in_table_write_error(
        spark_tmp_path, spark_tmp_table_factory):
    updates_view = spark_tmp_table_factory.get()

    def do_merge(spark):
        gpu_enabled = str(spark.conf.get("spark.rapids.sql.enabled", "false")).lower() == "true"
        target_path = spark_tmp_path + ("/GPU" if gpu_enabled else "/CPU")
        spark.sql("""
            SELECT timestamp '2024-01-01 00:00:00' AS timestampF,
                   CAST(1 AS TINYINT) AS byteF
        """).write.format("delta") \
            .option("delta.enableDeletionVectors", "false") \
            .mode("overwrite") \
            .save(target_path)
        spark.sql("""
            SELECT timestamp '2024-01-01 00:00:00' AS timestampF,
                   CAST(128 AS INT) AS byteF
        """).createOrReplaceTempView(updates_view)
        return spark.sql(f"""
            MERGE INTO delta.`{target_path}` AS target
            USING {updates_view} AS source
            ON target.timestampF = source.timestampF
            WHEN MATCHED THEN UPDATE SET byteF = source.byteF
        """).collect()

    assert_gpu_and_cpu_error(
        do_merge,
        conf=delta_merge_no_cpu_bridge_conf,
        error_message=re.compile(
            r'\[DELTA_CAST_OVERFLOW_IN_TABLE_WRITE\].*"INT".*"TINYINT".*`byteF`',
            re.DOTALL))


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
@pytest.mark.skipif(is_spark_41x(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with OSS Delta 4.1")
@pytest.mark.skipif(is_databricks173_or_later(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with Databricks 17.3+")
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


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not (is_spark_41x() or is_databricks173_or_later()),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with OSS Delta 4.1 "
                           "and Databricks 17.3+")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_not_matched_by_source(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    def src_table_func(spark):
        return spark.createDataFrame([(1, 100), (5, 500)], "a INT, b INT")

    def dest_table_func(spark):
        return spark.createDataFrame(
            [(1, 10), (2, 20), (3, 30), (4, -1)], "a INT, b INT")

    merge_sql = "MERGE INTO {dest_table} AS dest " \
                "USING {src_table} AS src " \
                "ON src.a == dest.a " \
                "WHEN MATCHED THEN UPDATE SET dest.b = src.b " \
                "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b) " \
                "WHEN NOT MATCHED BY SOURCE AND dest.a = 3 THEN DELETE " \
                "WHEN NOT MATCHED BY SOURCE AND dest.b > 0 THEN UPDATE SET dest.b = 0"

    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False, conf=delta_merge_enabled_conf)

    expected = [(1, 100), (2, 0), (4, -1), (5, 500)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: [tuple(row) for row in
                           read_delta_path(spark, data_path + "/" + run).orderBy("a").collect()],
            conf=delta_merge_enabled_conf)
        assert_equal(expected, actual)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not (is_spark_41x() or is_databricks173_or_later()),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with OSS Delta 4.1 "
                           "and Databricks 17.3+")
def test_delta_merge_not_matched_by_source_null_safe_keys(spark_tmp_path, spark_tmp_table_factory):
    # Composite null-safe merge key with an update-only NOT MATCHED BY SOURCE clause, the shape
    # used by SCD type 2 pipelines to expire current rows that disappeared from the source.
    def src_table_func(spark):
        return spark.createDataFrame(
            [(1, None, 100), (2, "x", 200), (5, None, 500)], "k1 INT, k2 STRING, v INT")

    def dest_table_func(spark):
        return spark.createDataFrame(
            [(1, None, 10, True), (2, "x", 20, True), (3, None, 30, True), (4, "y", 40, False)],
            "k1 INT, k2 STRING, v INT, current BOOLEAN")

    merge_sql = "MERGE INTO {dest_table} AS dest " \
                "USING {src_table} AS src " \
                "ON dest.k1 <=> src.k1 AND dest.k2 <=> src.k2 " \
                "WHEN MATCHED AND dest.v <> src.v THEN UPDATE SET dest.v = src.v " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT (k1, k2, v, current) VALUES (src.k1, src.k2, src.v, true) " \
                "WHEN NOT MATCHED BY SOURCE AND dest.current THEN UPDATE SET dest.current = false"

    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=False, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False, conf=delta_merge_enabled_conf)

    expected = [(1, None, 100, True), (2, "x", 200, True), (3, None, 30, False),
                (4, "y", 40, False), (5, None, 500, True)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: [tuple(row) for row in
                           read_delta_path(spark, data_path + "/" + run).orderBy("k1").collect()],
            conf=delta_merge_enabled_conf)
        assert_equal(expected, actual)

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


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with Databricks 17.3+")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_not_matched_by_source_db173(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # Every row path is exercised: matched rows are updated, source-only rows are inserted,
    # target-only rows with b > 0 are updated by the NOT MATCHED BY SOURCE clause and the
    # remaining target-only rows are copied unchanged. The GPU merge processor must be present
    # in the plan, which proves the command did not fall back to the CPU.
    def src_table_func(spark):
        return spark.createDataFrame([(a, a * 10) for a in range(0, 300, 3)], "a INT, b INT")

    def dest_table_func(spark):
        return spark.createDataFrame(
            [(a, -1 if a % 4 == 0 else a) for a in range(0, 200)], "a INT, b INT")

    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.a == {dest_table}.a " \
                "WHEN MATCHED THEN " \
                "  UPDATE SET {dest_table}.b = {src_table}.b " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT (a, b) VALUES ({src_table}.a, {src_table}.b) " \
                "WHEN NOT MATCHED BY SOURCE AND {dest_table}.b > 0 THEN " \
                "  UPDATE SET {dest_table}.b = 0"
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)

    def expected_row(a):
        if a % 3 == 0:
            return (a, a * 10)
        if a % 4 == 0:
            return (a, -1)
        return (a, 0)
    expected = [expected_row(a) for a in range(0, 200)] + \
               [(a, a * 10) for a in range(201, 300, 3)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: [tuple(row) for row in
                           read_delta_path(spark, data_path + "/" + run).orderBy("a").collect()],
            conf=delta_merge_enabled_conf)
        assert_equal(expected, actual)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Per-clause merge metrics are reported by the Databricks 17.3 GPU merge command")
def test_delta_merge_delete_only_duplicate_source_metrics_db173(spark_tmp_path, spark_tmp_table_factory):
    # An unconditional MATCHED DELETE is the only merge that allows several source rows to match
    # the same target row. The delete counters are incremented once per joined pair and then
    # compensated, so both numTargetRowsDeleted and numTargetRowsMatchedDeleted must equal the
    # number of target rows actually deleted, on the CPU and on the GPU.
    def src_table_func(spark):
        return spark.createDataFrame(
            [(a, b) for a in range(0, 50) for b in range(3)], "a INT, b INT")

    def dest_table_func(spark):
        return spark.createDataFrame([(a, -1) for a in range(0, 100)], "a INT, b INT")

    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a " \
                "WHEN MATCHED THEN DELETE"
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=False, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False, conf=delta_merge_enabled_conf)

    def merge_metrics(spark, path):
        row = spark.sql(f"DESCRIBE HISTORY delta.`{path}`") \
            .where("operation = 'MERGE'").orderBy("version", ascending=False).first()
        return {k: int(row["operationMetrics"][k])
                for k in ["numTargetRowsDeleted", "numTargetRowsMatchedDeleted"]}
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(lambda spark: merge_metrics(spark, data_path + "/" + run),
                                  conf=delta_merge_enabled_conf)
        assert actual == {"numTargetRowsDeleted": 50, "numTargetRowsMatchedDeleted": 50}, \
            f"{run}: {actual}"


# Databricks Runtime 16.0+ reports a target row as ambiguously matched only when more than one
# source row satisfies the ON condition AND a WHEN MATCHED clause condition. Source rows that match
# on ON alone take no action: they are not inserted, they do not flag the target row for the
# NOT MATCHED BY SOURCE clauses, and they do not make the row ambiguous. The GPU command has to
# accept the same merges as the CPU, write the same table, and reject the same ambiguous ones.
_dup_match_target_rows = [(1, "a", True), (2, "b", True), (3, "c", True)]
_dup_match_sql_full = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v "
    "WHEN NOT MATCHED THEN INSERT (k, v, cur) VALUES (s.k, s.v, true) "
    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.cur = false")
_dup_match_sql_plain = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v "
    "WHEN NOT MATCHED THEN INSERT (k, v, cur) VALUES (s.k, s.v, true)")
_dup_match_sql_no_matched_clause = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN NOT MATCHED THEN INSERT (k, v, cur) VALUES (s.k, s.v, true) "
    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.cur = false")
_dup_match_sql_two_clauses = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v "
    "WHEN MATCHED AND NOT s.apply THEN DELETE "
    "WHEN NOT MATCHED THEN INSERT (k, v, cur) VALUES (s.k, s.v, true)")
_dup_match_sql_target_condition = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN MATCHED AND t.cur THEN UPDATE SET t.v = s.v "
    "WHEN NOT MATCHED THEN INSERT (k, v, cur) VALUES (s.k, s.v, true)")
_dup_match_sql_conditional_delete = (
    "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
    "WHEN MATCHED AND s.apply THEN DELETE")

_dup_match_accepted_cases = [
    pytest.param([(1, "x", True), (1, "y", False), (4, "d", True)], _dup_match_sql_full,
                 id="one_effective_with_not_matched_by_source"),
    pytest.param([(1, "x", False), (1, "y", False), (4, "d", True)], _dup_match_sql_full,
                 id="none_effective_with_not_matched_by_source"),
    pytest.param([(1, "x", True), (1, "y", True), (4, "d", True)], _dup_match_sql_no_matched_clause,
                 id="no_matched_clause"),
    pytest.param([(1, "x", True), (1, "y", False), (4, "d", True)], _dup_match_sql_plain,
                 id="one_effective_plain"),
    pytest.param([(1, "x", True), (1, "y", False)], _dup_match_sql_conditional_delete,
                 id="conditional_delete_one_effective"),
    pytest.param([(1, "x", False), (1, "y", True), (1, "z", False), (2, "q", False)],
                 _dup_match_sql_full, id="three_matches_one_effective"),
]
_dup_match_rejected_cases = [
    pytest.param([(1, "x", True), (1, "y", True), (4, "d", True)], _dup_match_sql_full,
                 id="both_effective"),
    pytest.param([(1, "x", True), (1, "y", False), (4, "d", True)], _dup_match_sql_two_clauses,
                 id="each_row_takes_a_different_clause"),
    pytest.param([(1, "x", True), (1, "y", True), (4, "d", True)], _dup_match_sql_target_condition,
                 id="target_only_condition"),
]


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Databricks 16.0+ applies WHEN MATCHED conditions when detecting multiple matches")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
@pytest.mark.parametrize("src_rows,merge_sql", _dup_match_accepted_cases)
def test_delta_merge_duplicate_source_rows_matched_conditions_db173(
        spark_tmp_path, spark_tmp_table_factory, src_rows, merge_sql, use_cdf):
    def src_table_func(spark):
        return spark.createDataFrame(src_rows, "k INT, v STRING, apply BOOLEAN")

    def dest_table_func(spark):
        return spark.createDataFrame(_dup_match_target_rows, "k INT, v STRING, cur BOOLEAN")

    # With CDF on, the change rows are compared too: one pre and post image per applied target
    # row and one insert row per source-only row, none for the pairs the window dropped.
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)

    # The row counters per clause must agree too: one action per target row, none for the
    # source rows that matched on ON alone.
    metric_keys = ["numTargetRowsUpdated", "numTargetRowsInserted", "numTargetRowsDeleted",
                   "numTargetRowsMatchedUpdated", "numTargetRowsNotMatchedBySourceUpdated"]

    def merge_metrics(spark, path):
        row = spark.sql(f"DESCRIBE HISTORY delta.`{path}`") \
            .where("operation = 'MERGE'").orderBy("version", ascending=False).first()
        return {k: int(row["operationMetrics"].get(k, 0)) for k in metric_keys}
    data_path = spark_tmp_path + "/DELTA_DATA"
    cpu_metrics, gpu_metrics = [
        with_cpu_session(lambda spark, run=run: merge_metrics(spark, data_path + "/" + run),
                         conf=delta_merge_enabled_conf) for run in ["CPU", "GPU"]]
    assert cpu_metrics == gpu_metrics, f"CPU {cpu_metrics} vs GPU {gpu_metrics}"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Databricks 16.0+ applies WHEN MATCHED conditions when detecting multiple matches")
@pytest.mark.parametrize("src_rows,merge_sql", _dup_match_rejected_cases)
def test_delta_merge_duplicate_source_rows_ambiguous_error_db173(
        spark_tmp_path, spark_tmp_table_factory, src_rows, merge_sql):
    src_table = spark_tmp_table_factory.get()

    def do_merge(spark):
        gpu_enabled = str(spark.conf.get("spark.rapids.sql.enabled", "false")).lower() == "true"
        target_path = spark_tmp_path + ("/GPU" if gpu_enabled else "/CPU")
        spark.createDataFrame(_dup_match_target_rows, "k INT, v STRING, cur BOOLEAN") \
            .write.format("delta") \
            .option("delta.enableDeletionVectors", "false") \
            .mode("overwrite") \
            .save(target_path)
        spark.createDataFrame(src_rows, "k INT, v STRING, apply BOOLEAN") \
            .createOrReplaceTempView(src_table)
        return spark.sql(merge_sql.format(
            dest_table=f"delta.`{target_path}`", src_table=src_table)).collect()

    assert_gpu_and_cpu_error(
        do_merge,
        conf=delta_merge_no_cpu_bridge_conf,
        error_message="DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE")


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Databricks 16.0+ applies WHEN MATCHED conditions when detecting multiple matches")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_duplicate_source_rows_helper_column_names_db173(
        spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # De-duplicating the non-effective duplicate matches attaches helper columns to the joined
    # rows. User columns that carry the helper names must survive, because the clause expressions
    # still reference them: the helper names are generated to be absent from the join.
    def src_table_func(spark):
        return spark.createDataFrame(
            [(1, True, "chosen", 10), (1, False, "ignored", 11), (4, True, "inserted", 40)],
            "k INT, apply BOOLEAN, _duplicate_match_rank_ STRING, _source_row_id_ INT")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "a", 100), (2, "b", 200), (3, "c", 300)],
                                     "k INT, v STRING, _target_row_id_ INT")

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s._duplicate_match_rank_, "
                 "t._target_row_id_ = s._source_row_id_ "
                 "WHEN NOT MATCHED THEN INSERT (k, v, _target_row_id_) "
                 "VALUES (s.k, s._duplicate_match_rank_, s._source_row_id_)")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)
    expected = [(1, "chosen", 10), (2, "b", 200), (3, "c", 300), (4, "inserted", 40)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, data_path + "/" + run).collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"{run}: expected {expected}, got {actual}"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with Databricks 17.3+")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_internal_column_names_db173(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # The command attaches a row id and a file name to the target in findTouchedFiles. User
    # columns carrying those names must survive untouched and stay readable by the clause
    # expressions and conditions. (The presence flags of the join and the two control columns
    # of the processors are generated the same way, but the Databricks CPU command rejects user
    # columns with those names, so a parity test cannot use them; see the GPU-only test below.)
    def src_table_func(spark):
        return spark.createDataFrame([(1, True, 100, "s1"), (4, True, 400, "s4")],
                                     "k INT, apply BOOLEAN, _row_id_ INT, _file_name_ STRING")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "a", 10, "t1"), (2, "b", 20, "t2"), (3, "c", 30, "t3")],
                                     "k INT, v STRING, _row_id_ INT, _file_name_ STRING")

    merge_sql = (
        "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
        "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s._file_name_, t._row_id_ = s._row_id_ "
        "WHEN NOT MATCHED THEN INSERT (k, v, _row_id_, _file_name_) "
        "VALUES (s.k, s._file_name_, s._row_id_, s._file_name_) "
        "WHEN NOT MATCHED BY SOURCE AND t._row_id_ > 25 THEN UPDATE SET t.v = concat(t.v, '-kept')")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)
    expected = [(1, "s1", 100, "t1"), (2, "b", 20, "t2"), (3, "c-kept", 30, "t3"),
                (4, "s4", 400, "s4")]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, data_path + "/" + run).collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"{run}: expected {expected}, got {actual}"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with Databricks 17.3+")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_control_column_names_gpu_db173(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # The processors append the control columns _row_dropped_ and _incr_row_count_ to every
    # output row and read the first one back by position. User columns with those names must
    # neither be mistaken for the control columns nor dropped with them. The Databricks CPU
    # command rejects such columns as ambiguous, so this runs on the GPU only, against
    # spelled-out rows; the GPU processor must be in the plan.
    def src_table_func(spark):
        return spark.createDataFrame([(1, True, False, False), (4, True, True, True)],
                                     "k INT, apply BOOLEAN, _row_dropped_ BOOLEAN, "
                                     "_incr_row_count_ BOOLEAN")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "a", True, True), (2, "b", True, False),
                                      (3, "c", False, True)],
                                     "k INT, v STRING, _row_dropped_ BOOLEAN, "
                                     "_incr_row_count_ BOOLEAN")

    merge_sql = (
        "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
        "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = 'updated', "
        "t._row_dropped_ = s._row_dropped_ "
        "WHEN NOT MATCHED THEN INSERT (k, v, _row_dropped_, _incr_row_count_) "
        "VALUES (s.k, 'inserted', s._row_dropped_, s._incr_row_count_) "
        "WHEN NOT MATCHED BY SOURCE AND t._row_dropped_ THEN UPDATE SET t.v = concat(t.v, '-kept')")
    expected = [(1, "updated", False, True), (2, "b-kept", True, False), (3, "c", False, True),
                (4, "inserted", True, True)]
    expected_changes = {"update_preimage": 2, "update_postimage": 2, "insert": 1}

    def check_func(data_path, do_merge):
        gpu_path = data_path + "/GPU"
        callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        callback.startCapture()
        try:
            with_gpu_session(lambda spark: do_merge(spark, gpu_path),
                             conf=delta_merge_no_cpu_bridge_conf)
            captured_plans = callback.getResultsWithTimeout(10000)
        finally:
            callback.endCapture()
        class_name = "GpuRapidsProcessDeltaMergeJoinExec"
        assert any(callback.contains(plan, class_name) for plan in captured_plans), \
            f"{class_name} was not found in the captured MERGE plans"
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, gpu_path).collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"expected {expected}, got {actual}"
        if use_cdf:
            changes = with_cpu_session(
                lambda spark: [row["_change_type"] for row in
                               read_delta_path_with_cdf(spark, gpu_path).collect()],
                conf=delta_merge_enabled_conf)
            actual_changes = {t: changes.count(t) for t in set(changes)}
            assert expected_changes == actual_changes, \
                f"expected change rows {expected_changes}, got {actual_changes}"

    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                         src_table_func, dest_table_func, merge_sql, check_func)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Per-clause merge metrics are reported by the Databricks 17.3 GPU merge command")
def test_delta_merge_delete_only_duplicate_cdc_internal_column_names_db173(
        spark_tmp_path, spark_tmp_table_factory):
    # An unconditional MATCHED DELETE with duplicate source matches and CDF on de-duplicates the
    # change rows by row ids attached to both sides. User columns carrying those names must
    # survive and stay readable by the insert action.
    def src_table_func(spark):
        return spark.createDataFrame([(1, 10), (1, 11), (4, 40), (4, 41)],
                                     "k INT, _source_row_id_ INT")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "a", 100), (2, "b", 200), (3, "c", 300)],
                                     "k INT, v STRING, _target_row_id_ INT")

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "WHEN MATCHED THEN DELETE "
                 "WHEN NOT MATCHED THEN INSERT (k, v, _target_row_id_) "
                 "VALUES (s.k, 'inserted', s._source_row_id_)")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=True, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)
    expected = [(2, "b", 200), (3, "c", 300), (4, "inserted", 40), (4, "inserted", 41)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, data_path + "/" + run).collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"{run}: expected {expected}, got {actual}"


# A clause condition that evaluates to NULL is false: the row moves on to the next clause or to
# the default action (copy a target row, skip a source row). The GPU merge processor splits each
# batch with the condition and its negation, and a NULL passes neither filter, so without
# nulls-as-false handling such rows silently disappeared. Target rows a = 0..19 have b NULL when
# a % 5 == 0; source rows are the even a = 0..28 with b = a * 10 and a flag that is NULL when
# a % 6 == 0, true when a % 4 == 0, false otherwise.
def _nullable_condition_src(spark):
    def flag(a):
        return None if a % 6 == 0 else a % 4 == 0
    return spark.createDataFrame([(a, a * 10, flag(a)) for a in range(0, 30, 2)],
                                 "a INT, b INT, flag BOOLEAN")


def _nullable_condition_dest(spark):
    return spark.createDataFrame([(a, None if a % 5 == 0 else a) for a in range(0, 20)],
                                 "a INT, b INT")


_nullable_condition_clauses = (
    "WHEN MATCHED AND {src_table}.flag THEN UPDATE SET {dest_table}.b = {src_table}.b "
    "WHEN MATCHED AND NOT {src_table}.flag THEN DELETE "
    "WHEN NOT MATCHED AND {src_table}.flag THEN INSERT (a, b) VALUES ({src_table}.a, {src_table}.b) ")


def _nullable_condition_expected(with_not_matched_by_source):
    rows = []
    for a in range(0, 20):
        b = None if a % 5 == 0 else a
        if a % 2 == 0:  # matched
            if a % 6 == 0:  # flag NULL: neither matched clause applies, copied unchanged
                rows.append((a, b))
            elif a % 4 == 0:  # flag true: updated
                rows.append((a, a * 10))
            # flag false: deleted
        elif with_not_matched_by_source:  # target only: condition b > 0 is NULL when b is NULL
            rows.append((a, None if b is None else 0))
        else:
            rows.append((a, b))
    # source only: a = 20..28, inserted only when the flag is true (NULL is not)
    rows += [(a, a * 10) for a in range(20, 30, 2) if a % 6 != 0 and a % 4 == 0]
    return rows


def _assert_nullable_condition_result(spark_tmp_path, with_not_matched_by_source):
    expected = sorted(_nullable_condition_expected(with_not_matched_by_source))
    data_path = spark_tmp_path + "/DELTA_DATA"
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, data_path + "/" + run).collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"{run}: expected {expected}, got {actual}"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_merge_nullable_matched_conditions(spark_tmp_path, spark_tmp_table_factory):
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a " + \
                _nullable_condition_clauses
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=False, enable_deletion_vectors=False,
        src_table_func=_nullable_condition_src, dest_table_func=_nullable_condition_dest,
        merge_sql=merge_sql, compare_logs=False, conf=delta_merge_enabled_conf)
    _assert_nullable_condition_result(spark_tmp_path, with_not_matched_by_source=False)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not (is_spark_41x() or is_databricks173_or_later()),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with OSS Delta 4.1 "
                           "and Databricks 17.3+")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_merge_nullable_not_matched_by_source_condition(
        spark_tmp_path, spark_tmp_table_factory, use_cdf):
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a " + \
                _nullable_condition_clauses + \
                "WHEN NOT MATCHED BY SOURCE AND {dest_table}.b > 0 THEN UPDATE SET {dest_table}.b = 0"
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=_nullable_condition_src, dest_table_func=_nullable_condition_dest,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_merge_processor,
        conf=delta_merge_no_cpu_bridge_conf)
    _assert_nullable_condition_result(spark_tmp_path, with_not_matched_by_source=True)


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
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="NOT MATCHED BY SOURCE is supported on the GPU with Databricks 17.3+")
def test_delta_merge_not_matched_by_source_schema_evolution_db173(spark_tmp_path, spark_tmp_table_factory):
    # Schema evolution adds a source column to the target. Target-only rows go through the
    # NOT MATCHED BY SOURCE clause and get NULL in the new column, like the copied rows do.
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_table = spark_tmp_table_factory.get()

    def src_table_func(spark):
        return spark.createDataFrame([(1, "updated", "new-col"), (3, "inserted", "brand-new")],
                                     "a INT, b STRING, c STRING")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "old"), (2, "keep"), (4, "stale")], "a INT, b STRING")

    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path, dest_table_func,
                                use_cdf=False, enable_deletion_vectors=False)
        src_table_func(spark).createOrReplaceTempView(src_table)

    def do_merge(spark, path):
        src_table_func(spark).createOrReplaceTempView(src_table)
        return spark.sql(
            "MERGE WITH SCHEMA EVOLUTION INTO delta.`{path}` AS dest USING {src_table} AS src "
            "ON dest.a = src.a "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT * "
            "WHEN NOT MATCHED BY SOURCE AND dest.a > 2 THEN UPDATE SET dest.b = concat(dest.b, '-gone')"
            .format(path=path, src_table=src_table)).collect()

    with_cpu_session(setup_tables)
    _assert_gpu_merge_processor(do_merge, data_path, delta_merge_no_cpu_bridge_conf)
    expected = [(1, "updated", "new-col"), (2, "keep", None), (3, "inserted", "brand-new"),
                (4, "stale-gone", None)]
    for run in ["CPU", "GPU"]:
        actual = with_cpu_session(
            lambda spark: sorted(tuple(row) for row in
                                 read_delta_path(spark, data_path + "/" + run)
                                 .select("a", "b", "c").collect()),
            conf=delta_merge_enabled_conf)
        assert expected == actual, f"{run}: expected {expected}, got {actual}"


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="DBR 17.3 row tracking regression coverage")
@pytest.mark.xfail(strict=True,
                   reason="The GPU merge does not carry the row tracking columns through the "
                          "join, so rewritten rows get fresh row ids while the commit is tagged "
                          "as preserving them; fixed by the next commit")
def test_delta_merge_preserves_row_tracking_db173(spark_tmp_path):
    # Every clause type touches a row-tracked target. The row ids of the rows that existed before
    # the merge must survive on both engines, whether the row is updated by a matched clause, by
    # a not-matched-by-source clause, or copied, and the commit version moves only for the
    # updated rows. The inserted row gets a fresh id that the file layout decides, and the
    # join-based GPU merge lays out files differently from the CPU, so the ids are checked per
    # row rather than by comparing the commit logs as the UPDATE and DELETE tests do.
    conf = copy_and_update(delta_merge_enabled_conf, delta_row_tracking_dml_conf)
    data_path = spark_tmp_path + "/DELTA_DATA"
    with_cpu_session(lambda spark: setup_delta_row_tracking_dest_tables(
        spark, data_path, row_tracking_dml_test_df), conf=conf)
    merge_sql = ("MERGE INTO delta.`{path}` t "
                 "USING (SELECT * FROM VALUES (2, 'B', 'y'), (9, 'I', 'y') AS s(a, b, c)) s "
                 "ON t.a = s.a "
                 "WHEN MATCHED THEN UPDATE SET t.c = s.c "
                 "WHEN NOT MATCHED THEN INSERT * "
                 "WHEN NOT MATCHED BY SOURCE AND t.a = 4 THEN UPDATE SET t.c = 'z'")

    def tracked_rows(spark, path):
        rows = spark.sql("SELECT a, b, c, _metadata.row_id AS row_id, "
                         "_metadata.row_commit_version AS row_commit_version "
                         "FROM delta.`{}`".format(path)).collect()
        return {r["a"]: (r["b"], r["c"], r["row_id"], r["row_commit_version"]) for r in rows}

    data = {}
    for run in ["CPU", "GPU"]:
        path = data_path + "/" + run
        before = with_cpu_session(lambda spark: tracked_rows(spark, path), conf=conf)
        do_merge = lambda spark: spark.sql(merge_sql.format(path=path)).collect()
        if run == "GPU":
            assert_rapids_delta_write(do_merge, conf=conf)
        else:
            with_cpu_session(do_merge, conf=conf)
        after = with_cpu_session(lambda spark: tracked_rows(spark, path), conf=conf)
        assert sorted(after.keys()) == [1, 2, 3, 4, 9], "{}: {}".format(run, after)
        for a in [1, 2, 3, 4]:
            assert after[a][2] == before[a][2], \
                "{}: row id of a={} changed: {} -> {}".format(run, a, before[a], after[a])
        for a in [1, 3]:  # copied unchanged
            assert after[a][3] == before[a][3], \
                "{}: commit version of copied a={} changed: {} -> {}".format(run, a, before[a], after[a])
        for a in [2, 4]:  # updated by the matched and the not-matched-by-source clause
            assert after[a][3] > before[a][3], \
                "{}: commit version of updated a={} did not move: {} -> {}".format(run, a, before[a], after[a])
        assert after[9][2] > max(v[2] for v in before.values()), \
            "{}: inserted row id is not fresh: {}".format(run, after[9])
        data[run] = sorted((a,) + v[:2] for a, v in after.items())
    assert data["CPU"] == data["GPU"], "CPU {} vs GPU {}".format(data["CPU"], data["GPU"])


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
