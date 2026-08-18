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

import pytest

from data_gen import *
from delta_lake_utils import *
from marks import *
from spark_session import with_cpu_session, with_gpu_session, with_spark_session, is_before_spark_353, \
    supports_delta_lake_deletion_vectors, is_databricks_runtime, is_databricks173_or_later
from pyspark.sql.types import IntegerType, StringType

_optimize_conf = copy_and_update(delta_writes_enabled_conf, {
    "spark.rapids.sql.command.OptimizeTableCommand": "true",
    "spark.rapids.sql.command.OptimizeTableCommandEdge": "true",
    "spark.databricks.delta.autoCompact.enabled": "false"
})

_liquid_optimize_dv_conf = copy_and_update(_optimize_conf, {
    "spark.databricks.delta.delete.deletionVectors.persistent": "true"
})

_optimize_deletion_vector_values = [False] if is_databricks173_or_later() else \
    deletion_vector_values_with_xfail_reasons(
        enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042')

_optimize_clustered_deletion_vector_values = deletion_vector_values if is_databricks173_or_later() else \
    deletion_vector_values_with_xfail_reasons(
        enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042')


def _with_gpu_session_no_test(func, conf):
    gpu_conf = copy_and_update(conf, {
        "spark.rapids.sql.enabled": "true",
        "spark.rapids.sql.test.enabled": "false"
    })
    return with_spark_session(func, conf=gpu_conf)


@delta_lake
@allow_non_gpu('ExecutedCommandExec', *delta_meta_allow)
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks173_or_later(),
                    reason="OPTIMIZE table command is supported for Databricks 17.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
def test_delta_optimize_fallback_with_deletion_vectors(spark_tmp_path):
    _assert_optimize_fallback(True, spark_tmp_path)


@delta_lake
@allow_non_gpu('ExecutedCommandExec', *delta_meta_allow)
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Existing-DV OPTIMIZE fallback guard is for Databricks 17.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
def test_delta_optimize_fallback_with_existing_deletion_vectors(spark_tmp_path):
    conf = copy_and_update(_optimize_conf, {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true"
    })
    _assert_optimize_fallback(
        True,
        spark_tmp_path,
        conf=conf,
        post_setup_func=_delete_rows_and_disable_deletion_vectors)


@delta_lake
@allow_non_gpu('ExecutedCommandExec', *delta_meta_allow)
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Partitioned existing-DV OPTIMIZE fallback guard is for Databricks 17.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
def test_delta_optimize_fallback_partitioned_table_with_existing_deletion_vectors(spark_tmp_path):
    conf = copy_and_update(_optimize_conf, {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.sql.adaptive.enabled": "false"
    })
    _assert_optimize_fallback(
        True,
        spark_tmp_path,
        partition_columns=["a"],
        conf=conf,
        post_setup_func=_delete_rows_with_deletion_vectors,
        # Partitioned CPU OPTIMIZE can produce different compacted files and commit
        # metadata across independent tables, so verify fallback and data parity here.
        check_delta_log=False)


def _write_many_small_files(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    if partition_columns and clustering_columns:
        raise ValueError("Only one of partition_columns or clustering_columns can be specified")

    num_slices = 64 if partition_columns is None else 4
    partition_values = range(16) if partition_columns else range(1000)
    df = three_col_df(
        spark,
        SetValuesGen(IntegerType(), partition_values),
        SetValuesGen(StringType(), list("abcdefghij")),
        string_gen,
        num_slices=num_slices)
    if clustering_columns:
        # Note: the SQL below queries the `df` directly without registering it as a temp view first,
        # which is supported in Spark 3.3+ (https://issues.apache.org/jira/browse/SPARK-37516).
        # Starting Spark 4.0, we can use the `clusterBy` DataFrameWriter API instead of SQL.
        spark.sql(f"""
            CREATE TABLE delta.`{path}`
            USING DELTA
            TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(enable_deletion_vectors).lower()}')
            CLUSTER BY ({', '.join(clustering_columns)})
            AS SELECT * FROM {{tmp_df}}
        """, tmp_df=df)
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(partition_columns)
        if supports_delta_lake_deletion_vectors():
            writer = writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        writer.save(path)
    if not clustering_columns:
        # Count the number of files written to ensure we have many small files.
        num_files = spark.read.format("delta").load(path).inputFiles()
        assert len(num_files) > 63, f"Expected more than 63 files, but got {num_files}"


def _write_many_small_row_tracking_files(
        spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    if enable_deletion_vectors:
        raise ValueError("Row-tracking OPTIMIZE coverage must keep deletion vectors disabled")
    if partition_columns or clustering_columns:
        raise ValueError("Row-tracking OPTIMIZE coverage currently uses an unpartitioned table")

    df = three_col_df(
        spark,
        SetValuesGen(IntegerType(), range(1000)),
        SetValuesGen(StringType(), list("abcdefghij")),
        string_gen,
        num_slices=64)
    ddl = schema_to_ddl(spark, df.schema)
    spark.sql(f"""
        CREATE TABLE delta.`{path}` ({ddl})
        USING DELTA
        TBLPROPERTIES (delta.enableRowTracking = true, delta.enableDeletionVectors = false)
    """)
    df.write.format("delta").mode("append").save(path)
    num_files = spark.read.format("delta").load(path).inputFiles()
    assert len(num_files) > 63, f"Expected more than 63 files, but got {num_files}"


def _write_many_small_liquid_files(
        spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    if partition_columns or clustering_columns != ["a"]:
        raise ValueError("Liquid OPTIMIZE GPU rewrite coverage uses CLUSTER BY (a)")

    spark.sql(f"""
        CREATE TABLE delta.`{path}` (a BIGINT, b STRING, c STRING)
        USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(enable_deletion_vectors).lower()}')
        CLUSTER BY (a)
    """)
    data = spark.range(4096).selectExpr(
        "id AS a", "CAST(pmod(id, 17) AS STRING) AS b", "CAST(id * 3 AS STRING) AS c")
    for bucket in range(8):
        data.where(f"pmod(a, 8) = {bucket}").repartition(4) \
            .write.format("delta").mode("append").save(path)

    num_files = spark.read.format("delta").load(path).inputFiles()
    assert len(num_files) > 7, f"Expected more than 7 files, but got {num_files}"


def _write_many_small_liquid_row_tracking_files(
        spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    if enable_deletion_vectors:
        raise ValueError("Liquid row-tracking OPTIMIZE coverage requires deletion vectors disabled")
    if partition_columns or clustering_columns != ["a"]:
        raise ValueError("Liquid row-tracking OPTIMIZE coverage uses CLUSTER BY (a)")

    spark.sql(f"""
        CREATE TABLE delta.`{path}` (a BIGINT, b STRING, c STRING)
        USING DELTA
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'false',
          'delta.enableRowTracking' = 'true')
        CLUSTER BY (a)
    """)
    data = spark.range(4096).selectExpr(
        "id AS a", "CAST(pmod(id, 17) AS STRING) AS b", "CAST(id * 3 AS STRING) AS c")
    for bucket in range(8):
        data.where(f"pmod(a, 8) = {bucket}").repartition(4) \
            .write.format("delta").mode("append").save(path)

    num_files = spark.read.format("delta").load(path).inputFiles()
    assert len(num_files) > 7, f"Expected more than 7 files, but got {num_files}"


def _delete_rows_and_disable_deletion_vectors(spark, path):
    spark.range(10000).selectExpr(
        "CAST(id AS INT) AS a",
        "CAST(id * 2 AS STRING) AS b",
        "CAST(id * 3 AS STRING) AS c"
    ).repartition(16).write.format("delta").mode("overwrite") \
        .option("delta.enableDeletionVectors", "true") \
        .save(path)
    num_deleted = spark.sql(
        f"DELETE FROM delta.`{path}` WHERE a >= 5000 AND a < 5100").collect()[0][0]
    assert num_deleted > 0, "Expected DELETE to create deletion vectors"
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES " +
              "('delta.enableDeletionVectors' = 'false')")


def _delete_rows_with_deletion_vectors(spark, path):
    num_deleted = spark.sql(f"DELETE FROM delta.`{path}` WHERE b = 'a'").collect()[0][0]
    assert num_deleted > 0, "Expected DELETE to create deletion vectors"


def _delete_liquid_rows_with_deletion_vectors(spark, path):
    num_deleted = spark.sql(
        f"DELETE FROM delta.`{path}` WHERE pmod(a, 97) = 0").collect()[0][0]
    assert num_deleted > 0, "Expected DELETE to create deletion vectors"
    dv_adds = spark.read.json(path + "/_delta_log/*.json") \
        .where("add.deletionVector IS NOT NULL").count()
    assert dv_adds > 0, "Expected the liquid table to contain deletion-vector AddFiles"


def _read_sorted(spark, path):
    df = spark.read.format("delta").load(path)
    return df.sort(df.columns)


def _optimize_sql(path):
    return f"OPTIMIZE delta.`{path}`"


def _assert_captured_plan_contains(plan_callback, captured_plans, class_name, description):
    assert len(captured_plans) > 0, f"Did not capture a GPU {description} plan"
    if any(plan_callback.contains(plan, class_name) for plan in captured_plans):
        return
    plan_descriptions = "\n".join(str(plan) for plan in captured_plans)
    assert False, (
        f"{class_name} is not found in any captured GPU {description} plan:\n"
        f"{plan_descriptions}")


def _assert_gpu_optimize_executed(plan_callback, captured_plans, require_gpu_write=True):
    _assert_captured_plan_contains(
        plan_callback, captured_plans, "GpuExecutedCommandExec", "OPTIMIZE")
    if require_gpu_write:
        for class_name in delta_write:
            _assert_captured_plan_contains(
                plan_callback, captured_plans, class_name, "OPTIMIZE write")


def _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns, clustering_columns,
                  conf, post_setup_func=None, write_func=_write_many_small_files):
    def setup_cpu(spark):
        write_func(spark, enable_deletion_vectors, cpu_path, partition_columns, clustering_columns)
        if post_setup_func:
            post_setup_func(spark, cpu_path)
    def setup_gpu(spark):
        write_func(spark, enable_deletion_vectors, gpu_path, partition_columns, clustering_columns)
        if post_setup_func:
            post_setup_func(spark, gpu_path)
    with_cpu_session(setup_cpu, conf)
    with_cpu_session(setup_gpu, conf)


def _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None, clustering_columns=None,
                            conf=_optimize_conf, write_func=_write_many_small_files,
                            require_gpu_write=True, use_gpu_test_mode=True):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"

    _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns, clustering_columns,
                  conf, write_func=write_func)

    # Run OPTIMIZE on each table and verify the returned path matches the target
    cpu_result = with_cpu_session(lambda s: s.sql(_optimize_sql(cpu_path)).collect(), conf=conf)
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        gpu_session = with_gpu_session if use_gpu_test_mode else _with_gpu_session_no_test
        gpu_result = gpu_session(lambda s: s.sql(_optimize_sql(gpu_path)).collect(), conf=conf)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
        _assert_gpu_optimize_executed(
            plan_callback, captured_plans, require_gpu_write=require_gpu_write)
    finally:
        plan_callback.endCapture()
    # Validate the returned path for each run; metrics object is not stable across JVMs
    # Compare only the suffix to avoid scheme differences like file: vs absolute path
    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')

    # Compare table data after optimize (read via CPU)
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=conf)
    assert_equal(cpu_data, gpu_data)

    # Validate the optimize has run on both sides.
    cpu_optimize_count = with_cpu_session(lambda s: s.sql(
        f"DESCRIBE HISTORY delta.`{cpu_path}`").filter("operation = 'OPTIMIZE'").count(), conf=conf)
    gpu_optimize_count = with_cpu_session(lambda s: s.sql(
        f"DESCRIBE HISTORY delta.`{gpu_path}`").filter("operation = 'OPTIMIZE'").count(), conf=conf)
    assert_equal(cpu_optimize_count, 1)
    assert_equal(gpu_optimize_count, 1)

    with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path))


def _assert_optimize_fallback(enable_deletion_vectors, spark_tmp_path, partition_columns=None,
                              clustering_columns=None, conf=_optimize_conf, post_setup_func=None,
                              check_delta_log=True):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE_FALLBACK"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"

    _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns, clustering_columns,
                  conf, post_setup_func)

    cpu_result = with_cpu_session(lambda s: s.sql(_optimize_sql(cpu_path)).collect(), conf=conf)

    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.startCapture()
    try:
        gpu_result = _with_gpu_session_no_test(
            lambda s: s.sql(_optimize_sql(gpu_path)).collect(), conf=conf)
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertCapturedAndGpuFellBack(
            ["ExecutedCommandExec"], 10000)
    finally:
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.endCapture()

    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')

    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=conf)
    assert_equal(cpu_data, gpu_data)

    if check_delta_log:
        with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path))


def _assert_liquid_optimize_gpu_write_parity(
        spark_tmp_path, write_func=_write_many_small_liquid_files, repeat_optimize=False,
        enable_deletion_vectors=False, conf=_optimize_conf, post_setup_func=None):
    data_path = spark_tmp_path + "/DELTA_LIQUID_OPTIMIZE_WRITE"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"
    _setup_tables(
        enable_deletion_vectors, cpu_path, gpu_path, None, ["a"], conf,
        post_setup_func=post_setup_func, write_func=write_func)

    def assert_data_and_log_parity():
        cpu_data = with_cpu_session(lambda spark: _read_sorted(spark, cpu_path).collect(),
                                    conf=conf)
        gpu_data = with_cpu_session(lambda spark: _read_sorted(spark, gpu_path).collect(),
                                    conf=conf)
        assert_equal(cpu_data, gpu_data)
        with_cpu_session(
            lambda spark: assert_gpu_and_cpu_latest_delta_log_equivalent(spark, data_path),
            conf=conf)

    cpu_result = with_cpu_session(
        lambda spark: spark.sql(_optimize_sql(cpu_path)).collect(), conf=conf)
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        gpu_result = _with_gpu_session_no_test(
            lambda spark: spark.sql(_optimize_sql(gpu_path)).collect(), conf=conf)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
        _assert_captured_plan_contains(
            plan_callback, captured_plans, "GpuDataWritingCommandExec", "liquid OPTIMIZE write")
        _assert_captured_plan_contains(
            plan_callback, captured_plans, "GpuWriteFilesExec", "liquid OPTIMIZE write files")
    finally:
        plan_callback.endCapture()

    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')
    assert_data_and_log_parity()

    if repeat_optimize:
        second_cpu_result = with_cpu_session(
            lambda spark: spark.sql(_optimize_sql(cpu_path)).collect(), conf=conf)
        plan_callback.startCapture()
        try:
            second_gpu_result = _with_gpu_session_no_test(
                lambda spark: spark.sql(_optimize_sql(gpu_path)).collect(), conf=conf)
            captured_plans = plan_callback.getResultsWithTimeout(10000)
            _assert_captured_plan_contains(
                plan_callback, captured_plans, "GpuExecutedCommandExec",
                "repeated liquid OPTIMIZE")
        finally:
            plan_callback.endCapture()
        assert str(second_cpu_result[0][0]).rstrip('/').endswith('/CPU')
        assert str(second_gpu_result[0][0]).rstrip('/').endswith('/GPU')
        assert_data_and_log_parity()


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks173_or_later(),
                    reason="OPTIMIZE table command is supported for Databricks 17.3+")
@pytest.mark.parametrize("enable_deletion_vectors", _optimize_deletion_vector_values, ids=idfn)
def test_delta_optimize_unpartitioned_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks173_or_later(),
                    reason="OPTIMIZE table command is supported for Databricks 17.3+")
@pytest.mark.parametrize("enable_deletion_vectors", _optimize_deletion_vector_values, ids=idfn)
def test_delta_optimize_partitioned_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=["a"])


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Row-tracking OPTIMIZE coverage is for Databricks 17.3+")
def test_delta_optimize_row_tracking_table(spark_tmp_path):
    _assert_optimize_parity(
        False,
        spark_tmp_path,
        write_func=_write_many_small_row_tracking_files)


@delta_lake
@allow_non_gpu('ExecutedCommandExec', *delta_meta_allow)
@pytest.mark.skipif(is_before_spark_353(), reason="Liquid clustering requires Delta 3.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks173_or_later(),
                    reason="OPTIMIZE table command is supported for Databricks 17.3+")
@pytest.mark.parametrize("enable_deletion_vectors", _optimize_clustered_deletion_vector_values, ids=idfn)
def test_delta_optimize_clustered_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(
        enable_deletion_vectors,
        spark_tmp_path,
        clustering_columns=["a"],
        require_gpu_write=not is_databricks173_or_later(),
        use_gpu_test_mode=not is_databricks173_or_later())


@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Native liquid OPTIMIZE write coverage is for Databricks 17.3+")
def test_delta_optimize_clustered_table_gpu_write(spark_tmp_path):
    _assert_liquid_optimize_gpu_write_parity(spark_tmp_path)


@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Native liquid DV OPTIMIZE write coverage is for Databricks 17.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Deletion vectors aren't supported")
def test_delta_optimize_clustered_table_gpu_write_with_deletion_vectors(spark_tmp_path):
    _assert_liquid_optimize_gpu_write_parity(
        spark_tmp_path,
        enable_deletion_vectors=True,
        conf=_liquid_optimize_dv_conf,
        post_setup_func=_delete_liquid_rows_with_deletion_vectors)


@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Repeated native liquid OPTIMIZE coverage is for Databricks 17.3+")
def test_delta_optimize_clustered_table_gpu_write_repeated(spark_tmp_path):
    _assert_liquid_optimize_gpu_write_parity(spark_tmp_path, repeat_optimize=True)


@delta_lake
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="Native liquid row-tracking OPTIMIZE coverage is for Databricks 17.3+")
def test_delta_optimize_clustered_row_tracking_table_gpu_write(spark_tmp_path):
    _assert_liquid_optimize_gpu_write_parity(
        spark_tmp_path, write_func=_write_many_small_liquid_row_tracking_files)
