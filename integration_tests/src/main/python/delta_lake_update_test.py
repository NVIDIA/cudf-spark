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

import pytest

from asserts import assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write, assert_equal, \
    assert_equal_with_local_sort
from data_gen import *
from delta_lake_utils import *
from marks import *
from spark_session import is_before_spark_320, is_databricks_runtime, \
    supports_delta_lake_deletion_vectors, with_cpu_session, with_gpu_session, is_before_spark_353, \
    is_databricks173_or_later

delta_update_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                            {"spark.rapids.sql.command.UpdateCommand": "true",
                                             "spark.rapids.sql.command.UpdateCommandEdge": "true"})

def delta_sql_update_test(spark_tmp_path, use_cdf, dest_table_func, update_sql,
                          check_func, partition_columns=None, enable_deletion_vectors=False):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, enable_deletion_vectors, partition_columns)
    def do_update(spark, path):
        return spark.sql(update_sql.format(path=path))
    with_cpu_session(setup_tables)
    check_func(data_path, do_update)

def assert_delta_sql_update_collect(spark_tmp_path, use_cdf, enable_deletion_vectors, dest_table_func,
                                    update_sql,
                                    partition_columns=None,
                                    conf=delta_update_enabled_conf):
    def read_data(spark, path):
        read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
        df = read_func(spark, path)
        return df.sort(df.columns)
    def checker(data_path, do_update):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        # compare resulting dataframe from the update operation (some older Spark versions return empty here)
        cpu_result = with_cpu_session(lambda spark: do_update(spark, cpu_path).collect(), conf=conf)
        gpu_result = assert_rapids_delta_write(lambda spark: do_update(spark, gpu_path).collect(), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # compare table data results, read both via CPU to make sure GPU write can be read by CPU
        cpu_result = with_cpu_session(lambda spark: read_data(spark, cpu_path).collect(), conf=conf)
        gpu_result = with_cpu_session(lambda spark: read_data(spark, gpu_path).collect(), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # Databricks not guaranteed to write the same number of files due to optimized write when
        # using partitions. Using partition columns involves sorting, and there's no guarantees on
        # the task partitioning due to random sampling.
        if not is_databricks_runtime() and not partition_columns:
            with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    delta_sql_update_test(spark_tmp_path, use_cdf, dest_table_func, update_sql, checker,
                          partition_columns, enable_deletion_vectors)

@allow_non_gpu('ColumnarToRowExec', delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
@pytest.mark.skipif((not is_databricks_runtime()) and is_before_spark_353(),
                    reason="Update with deletion vector is only supported after delta.io 3.0.0")
def test_delta_update_fallback_with_deletion_vectors(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=False, enable_deletion_vectors=True)
    def write_func(spark, path):
        update_sql="UPDATE delta.`{}` SET a = 0".format(path)
        spark.sql(update_sql)
    with_cpu_session(setup_tables)
    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              "ExecutedCommandExec", delta_update_enabled_conf)

fallback_test_params = [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                        {"spark.rapids.sql.format.parquet.write.enabled": "false"},
                        {"spark.rapids.sql.command.UpdateCommand": "false"},
                        ]
if is_before_spark_353():
    # UpdateCommand is disabled by default before Spark 3.5.3
    fallback_test_params.append(delta_writes_enabled_conf)

@allow_non_gpu(delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf", fallback_test_params, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_update_disabled_fallback(spark_tmp_path, disable_conf, enable_deletion_vector):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=False, enable_deletion_vectors=enable_deletion_vector)
    def write_func(spark, path):
        update_sql="UPDATE delta.`{}` SET a = 0".format(path)
        spark.sql(update_sql)
    with_cpu_session(setup_tables)
    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              delta_write_fallback_check, disable_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_update_entire_table(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vector):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    update_sql = "UPDATE delta.`{path}` SET a = 0"
    assert_delta_sql_update_collect(spark_tmp_path, use_cdf, enable_deletion_vector, generate_dest_data,
                                    update_sql, partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [["a"], ["a", "b"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_update_partitions(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vector):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    update_sql = "UPDATE delta.`{path}` SET a = 3 WHERE b < 'c'"
    assert_delta_sql_update_collect(spark_tmp_path, use_cdf, enable_deletion_vector, generate_dest_data,
                                    update_sql, partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, permanent=True, reason='https://github.com/NVIDIA/spark-rapids/issues/9884')
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_update_rows(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vector):
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    update_sql = "UPDATE delta.`{path}` SET c = b WHERE b >= 'd'"
    assert_delta_sql_update_collect(spark_tmp_path, use_cdf, enable_deletion_vector, generate_dest_data,
                                    update_sql, partition_columns)

@allow_non_gpu("HashAggregateExec,ColumnarToRowExec,RapidsDeltaWriteExec,GenerateExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors are new in Spark 3.4.0 / DBR 12.2")
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10025')
def test_delta_update_rows_with_dv(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vectors):
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    update_sql = "UPDATE delta.`{path}` SET c = b WHERE b >= 'd'"
    assert_delta_sql_update_collect(spark_tmp_path, use_cdf, enable_deletion_vectors, generate_dest_data,
                                    update_sql, partition_columns)

@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="DBR 17.3 row tracking regression coverage")
def test_delta_update_preserves_row_tracking_db173(spark_tmp_path):
    conf = copy_and_update(delta_update_enabled_conf, delta_row_tracking_dml_conf)
    assert_delta_row_tracking_dml(
        spark_tmp_path, "UPDATE delta.`{path}` SET c = b WHERE a IN (2, 3)", conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10025')
@pytest.mark.parametrize("enable_deletion_vector", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_update_dataframe_api(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vector):
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/DELTA_DATA"
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path, generate_dest_data, use_cdf, enable_deletion_vector, partition_columns))
    def do_update(spark, path):
        dest_table = DeltaTable.forPath(spark, path)
        dest_table.update(condition="b > 'c'", set={"c": f.col("b"), "a": f.lit(1)})
    read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
    assert_gpu_and_cpu_writes_are_equal_collect(do_update, read_func, data_path, 
                                                conf=delta_update_enabled_conf)
    # Databricks not guaranteed to write the same number of files due to optimized write when
    # using partitions
    if not is_databricks_runtime() or not partition_columns:
        with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


@allow_non_gpu("ExecutedCommandExec,ColumnarToRowExec,DataWritingCommandExec", delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@inject_oom
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="GPU IncrementMetric coverage for Databricks 17.3+")
def test_delta_update_cpu_command_increment_metric_db173(spark_tmp_path):
    # The Databricks UPDATE command counts the rows it updates and copies with
    # ConditionalIncrementMetric in the rewrite job it runs, and the plugin plans that job
    # whenever the command itself stays on the CPU (disabled by conf here, the way any vetoed
    # update runs). The DESCRIBE HISTORY row counts come from those metrics, so they must match
    # the CPU run. inject_oom makes the retry around the GPU row count fire, which must not
    # change the counts.
    conf = copy_and_update(delta_update_enabled_conf,
                           {"spark.rapids.sql.command.UpdateCommand": "false",
                            "spark.rapids.sql.command.UpdateCommandEdge": "false"})
    update_sql = "UPDATE delta.`{path}` SET b = b + 100 WHERE a >= 4"

    def dest_table_func(spark):
        # a = 0..7 in one file: 4..7 are updated, the other 4 rows are copied
        return spark.createDataFrame([(i, i * 10) for i in range(8)], "a INT, b INT").coalesce(1)

    def row_count_metrics(spark, path):
        row = spark.sql(f"DESCRIBE HISTORY delta.`{path}`") \
            .where("operation = 'UPDATE'").orderBy("version", ascending=False).first()
        return {k: int(v) for k, v in row["operationMetrics"].items() if "Rows" in k}

    def checker(data_path, do_update):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        cpu_result = with_cpu_session(lambda spark: do_update(spark, cpu_path).collect(), conf=conf)
        callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        callback.startCapture()
        try:
            gpu_result = with_gpu_session(lambda spark: do_update(spark, gpu_path).collect(), conf=conf)
            captured_plans = callback.getResultsWithTimeout(10000)
        finally:
            callback.endCapture()
        assert_equal(cpu_result, gpu_result)
        assert_equal_with_local_sort(
            with_cpu_session(lambda spark: read_delta_path(spark, cpu_path).collect()),
            with_cpu_session(lambda spark: read_delta_path(spark, gpu_path).collect()))
        cpu_metrics = with_cpu_session(lambda spark: row_count_metrics(spark, cpu_path))
        gpu_metrics = with_cpu_session(lambda spark: row_count_metrics(spark, gpu_path))
        assert cpu_metrics == gpu_metrics, f"CPU {cpu_metrics} vs GPU {gpu_metrics}"
        expected = {"numUpdatedRows": 4, "numCopiedRows": 4}
        assert {k: gpu_metrics.get(k) for k in expected} == expected, gpu_metrics
        plan_strings = [plan.toString() for plan in captured_plans]
        assert any("gpu_conditional_increment_metric" in s for s in plan_strings), \
            "no GPU ConditionalIncrementMetric in the captured UPDATE plans:\n" + "\n".join(plan_strings)

    delta_sql_update_test(spark_tmp_path, use_cdf=False, dest_table_func=dest_table_func,
                          update_sql=update_sql, check_func=checker)
