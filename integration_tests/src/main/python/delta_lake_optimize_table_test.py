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


import re

import pytest

from asserts import assert_gpu_and_cpu_error
from data_gen import *
from delta_lake_utils import *
from marks import *
from spark_session import with_cpu_session, with_gpu_session, with_spark_session, is_before_spark_353, \
    supports_delta_lake_deletion_vectors, is_databricks_runtime, is_databricks143, \
    is_databricks173_or_later
from pyspark.sql.types import IntegerType, StringType

_optimize_conf = copy_and_update(delta_writes_enabled_conf, {
    "spark.rapids.sql.command.OptimizeTableCommand": "true",
    "spark.rapids.sql.command.OptimizeTableCommandEdge": "true",
    "spark.databricks.delta.autoCompact.enabled": "false"
})

_native_optimize_write_conf = copy_and_update(delta_writes_enabled_conf, {
    "spark.rapids.sql.command.OptimizeTableCommand": "false",
    "spark.rapids.sql.command.OptimizeTableCommandEdge": "false",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.optimizeWrite.enabled": "false",
    "spark.sql.adaptive.enabled": "false"
})

_native_optimize_dv_write_conf = copy_and_update(_native_optimize_write_conf, {
    "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    # A DV DELETE can force background compaction, overriding the normal auto-compact setting.
    "spark.databricks.delta.delete.enableForceBackgroundAutoCompact": "false"
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


def _write_mixed_timestamp_partition_files(
        spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    if enable_deletion_vectors or partition_columns or clustering_columns:
        raise ValueError("Mixed timestamp partition coverage requires deletion vectors disabled "
                         "and supplies its own timestamp partition column")

    spark.sql(f"""
        CREATE TABLE delta.`{path}` (id LONG, ts TIMESTAMP)
        USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
        PARTITIONED BY (ts)
    """)

    utc_partition_conf = "spark.databricks.delta.write.utcTimestampPartitionValues"
    previous_value = spark.conf.get(utc_partition_conf, None)
    try:
        spark.conf.set(utc_partition_conf, "false")
        spark.sql(f"INSERT INTO delta.`{path}` VALUES (0, '2000-01-01 12:00:00')")
        spark.sql(f"INSERT INTO delta.`{path}` VALUES (1, '2000-01-01 12:00:00')")

        spark.conf.set(utc_partition_conf, "true")
        spark.sql(f"INSERT INTO delta.`{path}` VALUES (2, '2000-01-01T12:00:00.000Z')")
        spark.sql(f"INSERT INTO delta.`{path}` VALUES (3, '2000-01-01T12:00:00.000Z')")
    finally:
        if previous_value is None:
            spark.conf.unset(utc_partition_conf)
        else:
            spark.conf.set(utc_partition_conf, previous_value)

    input_files = spark.read.format("delta").load(path).inputFiles()
    assert len(input_files) == 4, f"Expected 4 input files, but got {input_files}"


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
    dv_adds = spark.read.json(path + "/_delta_log/*.json") \
        .where("add.deletionVector IS NOT NULL").count()
    assert dv_adds > 0, "Expected the table to contain deletion-vector AddFiles"


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


def _optimize_sql(path, zorder_columns=None):
    zorder = f" ZORDER BY ({', '.join(zorder_columns)})" if zorder_columns else ""
    return f"OPTIMIZE delta.`{path}`{zorder}"


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


def _assert_gpu_zorder_clustering(plan_callback, captured_plans, require_shuffle=False,
                                  require_scan=True):
    """The write can be on the GPU while the clustering projection or shuffle is on the CPU."""
    for class_name in (["GpuFileSourceScanExec"] if require_scan else []) + [
            "GpuInterleaveBits", "GpuPartitionerExpr"] + (
            ["GpuShuffleExchangeExec"] if require_shuffle else []):
        _assert_captured_plan_contains(
            plan_callback, captured_plans, class_name, "Z-order clustering")


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


# OPTIMIZE result metrics that do not depend on the (sampled) layout or size of the output files.
_zorder_stable_metric_names = [
    "numFilesAdded", "numFilesRemoved", "filesRemoved", "partitionsOptimized", "zOrderStats",
    "numBins", "numBatches", "totalConsideredFiles", "totalFilesSkipped", "deletionVectorStats"]


_zorder_size_metric_names = {"min", "max", "avg", "totalSize", "size"}


def _assert_metric_equal(cpu, gpu, path, ignored_names=frozenset()):
    # Temporary workaround until https://github.com/NVIDIA/cudf-spark/issues/17018 is resolved.
    # assert_equal does not compare dicts (it sorts the items in place and compares None), so
    # walk them here. Switch back to assert_equal once the issue is fixed.
    if isinstance(cpu, dict):
        assert isinstance(gpu, dict) and cpu.keys() == gpu.keys(), \
            f"CPU ({cpu}) and GPU ({gpu}) metrics have different fields at {path}"
        for key in cpu.keys() - ignored_names:
            _assert_metric_equal(cpu[key], gpu[key], path + [key], ignored_names)
    else:
        assert cpu == gpu, f"CPU ({cpu}) and GPU ({gpu}) metrics are different at {path}"


def _assert_zorder_metrics_equal(cpu_metrics, gpu_metrics, ignore_input_sizes=False):
    """Compare the metrics that do not depend on the layout of the output files.

    The sizes of the added files always depend on which engine wrote them. The sizes of the
    removed (input) files are the same when both tables start from identical files, but not when
    OPTIMIZE runs again on its own output, so a caller can ignore them with ignore_input_sizes.
    """
    cpu_dict = cpu_metrics.asDict(recursive=True)
    gpu_dict = gpu_metrics.asDict(recursive=True)
    for name in _zorder_stable_metric_names:
        if name in cpu_dict:
            ignored = _zorder_size_metric_names if name == "numFilesAdded" or ignore_input_sizes \
                else frozenset()
            _assert_metric_equal(cpu_dict[name], gpu_dict[name], [name], ignored)
    assert cpu_dict["zOrderStats"] is not None, "Expected Z-order stats in the OPTIMIZE metrics"


def _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None, clustering_columns=None,
                            conf=_optimize_conf, write_func=_write_many_small_files,
                            require_gpu_write=True, use_gpu_test_mode=True, check_delta_log=True,
                            zorder_columns=None, post_setup_func=None, require_gpu_clustering=True,
                            require_gpu_scan=True, snapshot_func=None):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"

    _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns, clustering_columns,
                  conf, post_setup_func=post_setup_func, write_func=write_func)
    # Per-table state that OPTIMIZE must preserve, such as row tracking ids. The tables are
    # written independently, so each one is compared only with its own snapshot.
    snapshots = {}
    if snapshot_func:
        for path in [cpu_path, gpu_path]:
            snapshots[path] = with_cpu_session(lambda s: snapshot_func(s, path), conf=conf)

    # Run OPTIMIZE on each table and verify the returned path matches the target
    cpu_result = with_cpu_session(
        lambda s: s.sql(_optimize_sql(cpu_path, zorder_columns)).collect(), conf=conf)
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        gpu_session = with_gpu_session if use_gpu_test_mode else _with_gpu_session_no_test
        gpu_result = gpu_session(
            lambda s: s.sql(_optimize_sql(gpu_path, zorder_columns)).collect(), conf=conf)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
        _assert_gpu_optimize_executed(
            plan_callback, captured_plans, require_gpu_write=require_gpu_write)
        if zorder_columns and require_gpu_clustering:
            _assert_gpu_zorder_clustering(
                plan_callback, captured_plans, require_scan=require_gpu_scan)
    finally:
        plan_callback.endCapture()
    # Validate the returned path for each run; metrics object is not stable across JVMs
    # Compare only the suffix to avoid scheme differences like file: vs absolute path
    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')
    if zorder_columns:
        _assert_zorder_metrics_equal(cpu_result[0][1], gpu_result[0][1])

    # Compare table data after optimize (read via CPU)
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=conf)
    assert_equal(cpu_data, gpu_data)
    for path, before in snapshots.items():
        assert_equal(before, with_cpu_session(lambda s: snapshot_func(s, path), conf=conf))

    # Validate the optimize has run on both sides.
    cpu_optimize_count = with_cpu_session(lambda s: s.sql(
        f"DESCRIBE HISTORY delta.`{cpu_path}`").filter("operation = 'OPTIMIZE'").count(), conf=conf)
    gpu_optimize_count = with_cpu_session(lambda s: s.sql(
        f"DESCRIBE HISTORY delta.`{gpu_path}`").filter("operation = 'OPTIMIZE'").count(), conf=conf)
    assert_equal(cpu_optimize_count, 1)
    assert_equal(gpu_optimize_count, 1)

    if check_delta_log:
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


def _assert_native_optimize_gpu_write_parity(spark_tmp_path, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_NATIVE_OPTIMIZE_WRITE"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"
    conf = _native_optimize_dv_write_conf if enable_deletion_vectors \
        else _native_optimize_write_conf
    post_setup_func = _delete_rows_with_deletion_vectors if enable_deletion_vectors else None
    _setup_tables(
        enable_deletion_vectors, cpu_path, gpu_path, None, None, conf,
        post_setup_func=post_setup_func)
    files_before = {
        path: with_cpu_session(
            lambda spark: len(spark.read.format("delta").load(path).inputFiles()), conf=conf)
        for path in [cpu_path, gpu_path]
    }

    cpu_result = with_cpu_session(
        lambda spark: spark.sql(_optimize_sql(cpu_path)).collect(), conf=conf)
    gpu_result = assert_rapids_delta_write(
        lambda spark: spark.sql(_optimize_sql(gpu_path)).collect(), conf=conf,
        required_gpu_classes=["GpuDataWritingCommandExec", "GpuWriteFilesExec"],
        require_same_plan=True,
        forbidden_cpu_fallback_classes=["DataWritingCommandExec", "WriteFilesExec"])

    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')
    cpu_data = with_cpu_session(lambda spark: _read_sorted(spark, cpu_path).collect(), conf=conf)
    gpu_data = with_cpu_session(lambda spark: _read_sorted(spark, gpu_path).collect(), conf=conf)
    assert_equal(cpu_data, gpu_data)
    for path in [cpu_path, gpu_path]:
        files_after = with_cpu_session(
            lambda spark: len(spark.read.format("delta").load(path).inputFiles()), conf=conf)
        assert files_after < files_before[path], \
            f"OPTIMIZE did not reduce the file count for {path}"
        optimize_count = with_cpu_session(lambda spark: spark.sql(
            f"DESCRIBE HISTORY delta.`{path}`").filter("operation = 'OPTIMIZE'").count(),
            conf=conf)
        assert_equal(optimize_count, 1)
    with_cpu_session(
        lambda spark: assert_gpu_and_cpu_latest_delta_log_equivalent(spark, data_path),
        conf=conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks173_or_later(),
                    reason="OPTIMIZE table command is supported for Databricks 17.3+")
@pytest.mark.parametrize("enable_deletion_vectors", _optimize_deletion_vector_values, ids=idfn)
def test_delta_optimize_unpartitioned_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None)


@allow_non_gpu('ExecutedCommandExec', 'HashAggregateExec', *delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not (is_databricks143() or is_databricks173_or_later()),
                    reason="Native DBR OPTIMIZE write coverage is for DBR 14.3 and 17.3+")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_native_optimize_gpu_write(spark_tmp_path, enable_deletion_vectors):
    _assert_native_optimize_gpu_write_parity(spark_tmp_path, enable_deletion_vectors)


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
@pytest.mark.skipif(not is_oss_delta_lake_43(),
                    reason="Normalized timestamp partition grouping was added in Delta Lake 4.3")
@pytest.mark.parametrize("normalize_partition_values_on_read", [False, True], ids=idfn)
def test_delta_optimize_normalized_timestamp_partitions(
        spark_tmp_path, normalize_partition_values_on_read):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    conf = copy_and_update(_optimize_conf, {
        "spark.sql.session.timeZone": "UTC",
        "spark.databricks.delta.normalizePartitionValuesOnRead":
            str(normalize_partition_values_on_read).lower()
    })

    _assert_optimize_parity(
        False,
        spark_tmp_path,
        conf=conf,
        write_func=_write_mixed_timestamp_partition_files,
        check_delta_log=False)

    expected_file_count = 1 if normalize_partition_values_on_read else 2

    def assert_compacted_file_count(spark, path):
        input_files = spark.read.format("delta").load(path).inputFiles()
        assert len(input_files) == expected_file_count, \
            f"Expected {expected_file_count} compacted files, but got {input_files}"

    with_cpu_session(lambda spark: assert_compacted_file_count(spark, data_path + "/CPU"), conf)
    with_cpu_session(lambda spark: assert_compacted_file_count(spark, data_path + "/GPU"), conf)


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


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_oss_delta_lake_42_or_43(), reason="Requires Delta 4.2+ CDF writes")
def test_delta_optimize_cdf_non_data_changing(spark_tmp_path):
    def write(spark, enable_deletion_vectors, path, partition_columns, clustering_columns):
        gen_df(spark, [("id", long_gen)], length=64, num_slices=4) \
            .write.format("delta").option("delta.enableChangeDataFeed", "true") \
            .option("delta.enableDeletionVectors", "false").save(path)
        assert len(spark.read.format("delta").load(path).inputFiles()) == 4

    conf = copy_and_update(_optimize_conf, {
        "spark.databricks.delta.optimizeWrite.enabled": "false"
    })
    _assert_optimize_parity(False, spark_tmp_path, conf=conf, write_func=write)

    def check(spark):
        cdf_results = []
        for engine in ["CPU", "GPU"]:
            path = spark_tmp_path + "/DELTA_OPTIMIZE/" + engine
            cdf_results.append(assert_delta_non_data_changing_commit(spark, path, {"add", "remove"}))
            initial = spark.read.format("delta").option("versionAsOf", 0).load(path)
            assert_equal(initial.sort("id").collect(), _read_sorted(spark, path).collect())
            assert len(spark.read.format("delta").load(path).inputFiles()) == 1
        assert_equal(*cdf_results)
        assert_gpu_and_cpu_delta_logs_equivalent(spark, spark_tmp_path + "/DELTA_OPTIMIZE")

    with_cpu_session(check, conf=conf)


_zorder_columns = ["a", "b", "c", "d", "e", "f", "g"]

_zorder_gens = [
    ("p", SetValuesGen(IntegerType(), range(4))),
    ("a", int_gen), ("b", long_gen), ("c", string_gen), ("d", date_gen),
    ("e", TimestampGen()), ("f", short_gen), ("g", double_gen)]


def _zorder_writer(table_properties=None, length=2048, num_slices=16):
    """Many small files with mixed Z-order column types, plus a low-cardinality partition column."""
    def write(spark, enable_deletion_vectors, path, partition_columns=None,
              clustering_columns=None):
        writer = gen_df(spark, _zorder_gens, length=length, num_slices=num_slices) \
            .write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(partition_columns)
        if supports_delta_lake_deletion_vectors():
            writer = writer.option(
                "delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        for key, value in (table_properties or {}).items():
            writer = writer.option(key, value)
        writer.save(path)
        if num_slices > 3:
            num_files = spark.read.format("delta").load(path).inputFiles()
            assert len(num_files) > 3, f"Expected more than 3 files, but got {num_files}"
    return write


_write_zorder_files = _zorder_writer()


def _delete_zorder_rows_with_deletion_vectors(spark, path):
    num_deleted = spark.sql(f"DELETE FROM delta.`{path}` WHERE p = 1 OR f % 5 = 0").collect()[0][0]
    assert num_deleted > 0, "Expected DELETE to create deletion vectors"
    dv_adds = spark.read.json(path + "/_delta_log/*.json") \
        .where("add.deletionVector IS NOT NULL").count()
    assert dv_adds > 0, "Expected the table to contain deletion-vector AddFiles"


# The generated dates and timestamps include values before the Gregorian cutover, which Spark 3.x
# refuses to write to Parquet unless the rebase mode is set.
# Delta's Z-order clustering adds a random salt, cast(rand() ... AS tinyint), and the test harness
# turns off GPU float-to-integral casts by default. Without them the clustering projection (and the
# interleave-bits expression in it) silently runs on the CPU.
_zorder_conf = copy_and_update(_optimize_conf, {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.rapids.sql.castFloatToIntegralTypes.enabled": "true",
    "spark.rapids.sql.castFloatToString.enabled": "true"
})

_zorder_dv_conf = copy_and_update(_zorder_conf, {
    "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    "spark.databricks.delta.delete.enableForceBackgroundAutoCompact": "false"
})


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
@pytest.mark.parametrize("partition_columns", [None, ["p"]], ids=idfn)
@pytest.mark.parametrize("zorder_columns", [["a"], ["c", "b"], _zorder_columns], ids=idfn)
def test_delta_optimize_zorder(spark_tmp_path, partition_columns, zorder_columns):
    _assert_optimize_parity(
        False, spark_tmp_path, partition_columns=partition_columns, write_func=_write_zorder_files,
        zorder_columns=zorder_columns, conf=_zorder_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
@pytest.mark.parametrize("partition_columns", [None, ["p"]], ids=idfn)
def test_delta_optimize_zorder_with_deletion_vectors(spark_tmp_path, partition_columns):
    _assert_optimize_parity(
        True, spark_tmp_path, partition_columns=partition_columns, write_func=_write_zorder_files,
        zorder_columns=["a", "c"], conf=_zorder_dv_conf,
        post_setup_func=_delete_zorder_rows_with_deletion_vectors)
    # Z-ORDER rewrites every file, so the OPTIMIZE commit removes DV-bearing files and adds none.
    def check(spark):
        for engine in ["CPU", "GPU"]:
            path = spark_tmp_path + "/DELTA_OPTIMIZE/" + engine
            # Read the log through Spark so this also works on hdfs:// and s3a:// temp paths.
            log = spark.read.text(path + "/_delta_log/*.json") \
                .selectExpr("value", "_metadata.file_name AS file_name") \
                .where("file_name RLIKE '^[0-9]{20}[.]json$'")
            latest = log.selectExpr("max(file_name)").first()[0]
            lines = log.where(f"file_name = '{latest}'")
            def count_with_dv(action):
                return lines.where(
                    f"get_json_object(value, '$.{action}.deletionVector') IS NOT NULL").count()
            assert count_with_dv("remove") > 0, \
                f"Expected OPTIMIZE to remove files with DVs in {path}"
            assert count_with_dv("add") == 0, f"Z-ORDER must not add files with DVs in {path}"
    with_cpu_session(check, conf=_zorder_dv_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_oss_delta_lake_43(),
                    reason="Normalized timestamp partition grouping was added in Delta Lake 4.3")
@pytest.mark.parametrize("normalize_partition_values_on_read", [False, True], ids=idfn)
def test_delta_optimize_zorder_normalized_timestamp_partitions(
        spark_tmp_path, normalize_partition_values_on_read):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    conf = copy_and_update(_zorder_conf, {
        "spark.sql.session.timeZone": "UTC",
        "spark.databricks.delta.normalizePartitionValuesOnRead":
            str(normalize_partition_values_on_read).lower()
    })
    _assert_optimize_parity(
        False, spark_tmp_path, conf=conf, write_func=_write_mixed_timestamp_partition_files,
        zorder_columns=["id"], check_delta_log=False)

    expected_file_count = 1 if normalize_partition_values_on_read else 2

    def assert_compacted_file_count(spark, path):
        input_files = spark.read.format("delta").load(path).inputFiles()
        assert len(input_files) == expected_file_count, \
            f"Expected {expected_file_count} Z-ordered files, but got {input_files}"

    with_cpu_session(lambda spark: assert_compacted_file_count(spark, data_path + "/CPU"), conf)
    with_cpu_session(lambda spark: assert_compacted_file_count(spark, data_path + "/GPU"), conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_partition_predicate(spark_tmp_path):
    # Only the selected partition is rewritten; the other partitions keep their files.
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, ["p"], None, _zorder_conf,
                  write_func=_write_zorder_files)
    sql = lambda path: f"OPTIMIZE delta.`{path}` WHERE p = 1 ZORDER BY (a, c)"
    cpu_metrics = with_cpu_session(lambda s: s.sql(sql(cpu_path)).collect()[0][1], conf=_zorder_conf)
    gpu_metrics = with_gpu_session(lambda s: s.sql(sql(gpu_path)).collect()[0][1], conf=_zorder_conf)
    _assert_zorder_metrics_equal(cpu_metrics, gpu_metrics)
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=_zorder_conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=_zorder_conf)
    assert_equal(cpu_data, gpu_data)
    with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path),
                     conf=_zorder_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_multiple_output_files(spark_tmp_path):
    # A small target file size makes Z-order range-partition into several files. The sampled
    # range boundaries (and so the exact files) may differ from the CPU, so compare the data and
    # require that the output is split into multiple files.
    conf = copy_and_update(_zorder_conf, {
        "spark.databricks.delta.optimize.maxFileSize": str(32 * 1024)})
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, None, None, conf, write_func=_write_zorder_files)
    sql = lambda path: f"OPTIMIZE delta.`{path}` ZORDER BY (a, b)"
    with_cpu_session(lambda s: s.sql(sql(cpu_path)).collect(), conf=conf)
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(lambda s: s.sql(sql(gpu_path)).collect(), conf=conf)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
        _assert_gpu_optimize_executed(plan_callback, captured_plans)
        _assert_gpu_zorder_clustering(plan_callback, captured_plans, require_shuffle=True)
    finally:
        plan_callback.endCapture()
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=conf)
    assert_equal(cpu_data, gpu_data)
    gpu_files = with_cpu_session(
        lambda s: s.read.format("delta").load(gpu_path).inputFiles(), conf=conf)
    assert len(gpu_files) > 1, f"Expected several Z-ordered files, but got {gpu_files}"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
@pytest.mark.parametrize("zorder_column, error_message", [
    ("p", "is a partition column"),
    ("does_not_exist", "does_not_exist")], ids=idfn)
def test_delta_optimize_zorder_invalid_columns(spark_tmp_path, zorder_column, error_message):
    # The GPU command must reject the same Z-order columns the CPU command rejects.
    path = spark_tmp_path + "/DELTA_OPTIMIZE"
    with_cpu_session(lambda s: _write_zorder_files(s, False, path, ["p"]), conf=_zorder_conf)
    assert_gpu_and_cpu_error(
        lambda spark: spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_column})").collect(),
        conf=_zorder_conf,
        error_message=error_message)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_column_without_stats(spark_tmp_path):
    # Only the first column has stats, so Z-ordering on any other column must be rejected as it is
    # on the CPU.
    path = spark_tmp_path + "/DELTA_OPTIMIZE"
    def write(spark):
        gen_df(spark, [("p", int_gen), ("a", int_gen)], length=1024, num_slices=4) \
            .write.format("delta").option("delta.dataSkippingNumIndexedCols", "1").save(path)
    with_cpu_session(write, conf=_zorder_conf)
    assert_gpu_and_cpu_error(
        lambda spark: spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a)").collect(),
        conf=_zorder_conf,
        error_message="Z-Ordering on")


def _write_zorder_type_files(
        spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
    """Many small files with the Z-order column types that _write_zorder_files does not cover."""
    gen_df(spark, [
        ("p", SetValuesGen(IntegerType(), range(4))),
        ("h", boolean_gen), ("i", byte_gen), ("j", float_gen),
        ("k", decimal_gen_32bit), ("l", decimal_gen_64bit)],
        length=2048, num_slices=16).write.format("delta").mode("overwrite").save(path)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
@pytest.mark.parametrize("zorder_columns", [["h"], ["i"], ["j"], ["k"], ["l", "i"]], ids=idfn)
def test_delta_optimize_zorder_more_types(spark_tmp_path, zorder_columns):
    _assert_optimize_parity(
        False, spark_tmp_path, write_func=_write_zorder_type_files, zorder_columns=zorder_columns,
        conf=_zorder_conf)


def _zorder_cpu_gpu(spark_tmp_path, conf, write_func, sql_func, partition_columns=None):
    """Create identical CPU-written tables, then run `sql_func(path)` on the CPU and on the GPU."""
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, partition_columns, None, conf, write_func=write_func)
    cpu_result = with_cpu_session(lambda s: sql_func(s, cpu_path), conf=conf)
    gpu_result = with_gpu_session(lambda s: sql_func(s, gpu_path), conf=conf)
    return data_path, cpu_path, gpu_path, cpu_result, gpu_result


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_repeated(spark_tmp_path):
    # Z-order rewrites every file each time, so a second run is not a no-op and must still match.
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, c)").collect()[0][1]
    data_path, cpu_path, gpu_path, cpu_first, gpu_first = _zorder_cpu_gpu(
        spark_tmp_path, _zorder_conf, _write_zorder_files, sql)
    _assert_zorder_metrics_equal(cpu_first, gpu_first)
    cpu_second = with_cpu_session(lambda s: sql(s, cpu_path), conf=_zorder_conf)
    gpu_second = with_gpu_session(lambda s: sql(s, gpu_path), conf=_zorder_conf)
    # The second run reads files written by each engine, so their sizes differ.
    _assert_zorder_metrics_equal(cpu_second, gpu_second, ignore_input_sizes=True)
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=_zorder_conf)
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=_zorder_conf)
    assert_equal(cpu_data, gpu_data)
    with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path),
                     conf=_zorder_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_empty_table(spark_tmp_path):
    def write(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
        spark.sql(f"CREATE TABLE delta.`{path}` (a INT, b BIGINT) USING DELTA")
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, b)").collect()
    _, cpu_path, gpu_path, _, _ = _zorder_cpu_gpu(spark_tmp_path, _zorder_conf, write, sql)
    for path in [cpu_path, gpu_path]:
        assert with_cpu_session(lambda s: s.sql(f"DESCRIBE HISTORY delta.`{path}`")
                                .filter("operation = 'OPTIMIZE'").count(), conf=_zorder_conf) == 0


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")
def test_delta_optimize_zorder_row_tracking_preserves_row_ids(spark_tmp_path):
    def write(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
        gen_df(spark, [("a", int_gen), ("b", long_gen)], length=1024, num_slices=8) \
            .write.format("delta").option("delta.enableRowTracking", "true").save(path)
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, b)").collect()
    def rows(spark, path):
        return spark.read.format("delta").load(path) \
            .selectExpr("a", "b", "_metadata.row_id AS row_id").orderBy("row_id").collect()
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, None, None, _zorder_conf, write_func=write)
    # The two tables are written independently, so Delta may assign them different base row IDs.
    # Compare each table with its own snapshot, and compare only the data across engines.
    cpu_before = with_cpu_session(lambda s: rows(s, cpu_path), conf=_zorder_conf)
    gpu_before = with_cpu_session(lambda s: rows(s, gpu_path), conf=_zorder_conf)
    with_cpu_session(lambda s: sql(s, cpu_path), conf=_zorder_conf)
    with_gpu_session(lambda s: sql(s, gpu_path), conf=_zorder_conf)
    cpu_after = with_cpu_session(lambda s: rows(s, cpu_path), conf=_zorder_conf)
    gpu_after = with_cpu_session(lambda s: rows(s, gpu_path), conf=_zorder_conf)
    # Row IDs must survive the rewrite on both engines.
    assert_equal(cpu_before, cpu_after)
    assert_equal(gpu_before, gpu_after)
    data = lambda rs: sorted(((r.a is None, r.a), (r.b is None, r.b)) for r in rs)
    assert_equal(data(cpu_after), data(gpu_after))


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_oss_delta_lake_42_or_43(), reason="Requires Delta 4.2+ CDF writes")
def test_delta_optimize_zorder_cdf_non_data_changing(spark_tmp_path):
    def write(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
        gen_df(spark, [("a", long_gen), ("b", int_gen)], length=256, num_slices=4) \
            .write.format("delta").option("delta.enableChangeDataFeed", "true").save(path)
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, b)").collect()
    data_path, cpu_path, gpu_path, _, _ = _zorder_cpu_gpu(
        spark_tmp_path, _zorder_conf, write, sql)
    def check(spark):
        results = [assert_delta_non_data_changing_commit(spark, p, {"add", "remove"})
                   for p in [cpu_path, gpu_path]]
        assert_equal(*results)
        assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path)
    with_cpu_session(check, conf=_zorder_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                    reason="Liquid clustering requires Delta 3.3+")
def test_delta_optimize_zorder_clustered_table_error(spark_tmp_path):
    # ZORDER is not allowed on a liquid-clustered table; the GPU command must fail like the CPU.
    path = spark_tmp_path + "/DELTA_OPTIMIZE"
    with_cpu_session(lambda s: _write_many_small_files(s, False, path, None, ["a"]),
                     conf=_zorder_conf)
    assert_gpu_and_cpu_error(
        lambda spark: spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a)").collect(),
        conf=_zorder_conf,
        error_message="ZORDER")


def _zorder_test(func):
    """Marks shared by the Z-order tests below."""
    for mark in [allow_non_gpu(*delta_meta_allow), delta_lake, ignore_order,
                 pytest.mark.skipif(is_databricks_runtime() or is_before_spark_353(),
                                    reason="GPU Z-order OPTIMIZE is supported on OSS Delta 3.3+")]:
        func = mark(func)
    return func


def _zorder_outcome(spark, path, sql):
    """Returns ("ok", OPTIMIZE metrics) or ("error", the exception's identity, without paths)."""
    try:
        return "ok", spark.sql(sql.format(path=path)).collect()[0][1]
    except Exception as e:
        java_exception = getattr(e, "java_exception", None)
        if java_exception is not None:  # a failure inside a task: compare the root cause
            while java_exception.getCause() is not None:
                java_exception = java_exception.getCause()
            name, error_class, message = (
                java_exception.getClass().getName(), None, str(java_exception.getMessage()))
        else:
            name = type(e).__name__
            error_class = e.getErrorClass() if hasattr(e, "getErrorClass") else None
            message = str(e)
        return "error", (name, error_class,
                         re.sub(r"\S*DELTA_OPTIMIZE/(CPU|GPU)\S*", "<path>", message).split("\n")[0])


def _table_state(spark, path):
    return (spark.sql(f"DESCRIBE HISTORY delta.`{path}`").count(),
            sorted(spark.read.format("delta").load(path).inputFiles()))


def _data_files(spark, path, condition):
    return set(r[0] for r in spark.read.format("delta").load(path).where(condition)
               .selectExpr("input_file_name()").distinct().collect())


def _assert_cpu_gpu_outcomes_match(spark_tmp_path, conf, write_func, sql, partition_columns=None,
                                   expected=None, snapshot_func=None, use_gpu_test_mode=True):
    """Runs `sql` ({path} placeholder) on identical CPU-written tables, once per engine.

    Both engines must either succeed with the same stable metrics and data, or fail with the same
    exception identity; a failure must leave the GPU table unchanged. Returns the paths, the
    outcome and `snapshot_func(spark, gpu_path)` taken before and after the GPU run."""
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, partition_columns, None, conf, write_func=write_func)
    cpu = with_cpu_session(lambda s: _zorder_outcome(s, cpu_path, sql), conf=conf)
    snapshot = snapshot_func or (lambda spark, path: None)
    before, before_snapshot = with_cpu_session(
        lambda s: (_table_state(s, gpu_path), snapshot(s, gpu_path)), conf=conf)
    gpu_session = with_gpu_session if use_gpu_test_mode else _with_gpu_session_no_test
    gpu = gpu_session(lambda s: _zorder_outcome(s, gpu_path, sql), conf=conf)
    after, after_snapshot = with_cpu_session(
        lambda s: (_table_state(s, gpu_path), snapshot(s, gpu_path)), conf=conf)
    assert cpu[0] == gpu[0], f"CPU and GPU disagree on success: CPU {cpu}, GPU {gpu}"
    if expected:
        assert cpu[0] == expected, f"Expected {expected}, got {cpu}"
    if cpu[0] == "ok":
        _assert_zorder_metrics_equal(cpu[1], gpu[1])
        # to_json copes with dotted names and with map columns, which cannot be sorted
        rows = lambda s, path: s.read.format("delta").load(path) \
            .selectExpr("to_json(struct(*)) AS row").sort("row").collect()
        assert_equal(with_cpu_session(lambda s: rows(s, cpu_path), conf=conf),
                     with_cpu_session(lambda s: rows(s, gpu_path), conf=conf))
    else:
        assert cpu[1] == gpu[1], "CPU and GPU failed differently"
        assert before == after, "A failed OPTIMIZE changed the table"
    return cpu_path, gpu_path, cpu[0], before_snapshot, after_snapshot


_zorder_stats_check_conf = "spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled"

_zorder_column_cases = [
    # id, ZORDER BY list, extra conf, number of stats-indexed columns, expected outcome
    ("mixed_valid_invalid", "a, nope", {}, None, "error"),
    ("duplicate", "a, a", {}, None, None),
    ("case_insensitive", "b", {}, None, "ok"),
    ("case_sensitive", "b", {"spark.sql.caseSensitive": "true"}, None, "error"),
    ("quoted_name", "`B`", {}, None, "ok"),
    ("nested_field", "s.x", {}, None, "ok"),
    ("struct", "s", {}, None, None),
    ("array", "arr", {}, None, None),
    ("map", "m", {}, None, None),
    ("beyond_stats_limit", "B", {}, "1", "error"),
    ("stats_check_disabled", "B", {_zorder_stats_check_conf: "false"}, "1", "ok")]


@_zorder_test
@pytest.mark.parametrize("case", _zorder_column_cases, ids=lambda case: case[0])
def test_delta_optimize_zorder_column_resolution(spark_tmp_path, case):
    # Name resolution, stats and type rules come from Delta's validation, so the GPU command must
    # accept and reject exactly what the CPU command does, leaving the table untouched on failure.
    _, zorder_by, extra_conf, indexed_columns, expected = case
    conf = copy_and_update(_zorder_conf, extra_conf)
    def write(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
        writer = gen_df(spark, [
            ("a", int_gen), ("B", long_gen),
            ("s", StructGen([("x", int_gen), ("y", string_gen)])),
            ("arr", ArrayGen(int_gen)), ("m", MapGen(StringGen(nullable=False), int_gen))],
            length=512, num_slices=4).write.format("delta")
        if indexed_columns:
            writer = writer.option("delta.dataSkippingNumIndexedCols", indexed_columns)
        writer.save(path)
    # An array key cannot be range-partitioned on the GPU, so its PartitionerExpr goes through the
    # CPU bridge, which GPU test mode rejects.
    _assert_cpu_gpu_outcomes_match(
        spark_tmp_path, conf, write, f"OPTIMIZE delta.`{{path}}` ZORDER BY ({zorder_by})",
        expected=expected, use_gpu_test_mode=zorder_by != "arr")


_zorder_predicate_cases = [
    # id, partition predicate, expected outcome
    ("no_partition_matches", "p = 99", "ok"),
    ("range", "p >= 1 AND p <= 2", "ok"),
    ("disjunction", "p = 0 OR p = 2", "ok"),
    ("null_partition", "p IS NULL", "ok"),
    ("two_partition_columns", "p = 1 AND q = 0", "ok"),
    ("non_partition_column", "a > 0", None)]


@_zorder_test
@pytest.mark.parametrize("case", _zorder_predicate_cases, ids=lambda case: case[0])
def test_delta_optimize_zorder_partition_predicates(spark_tmp_path, case):
    _, predicate, expected = case
    def write(spark, enable_deletion_vectors, path, partition_columns=None, clustering_columns=None):
        gen_df(spark, [("p", SetValuesGen(IntegerType(), [0, 1, 2, None])), ("a", int_gen),
                       ("b", long_gen), ("c", string_gen)], length=2048, num_slices=16) \
            .selectExpr("p", "a", "b", "c", "CAST(a % 2 AS INT) AS q") \
            .write.format("delta").partitionBy("p", "q").save(path)
    unselected = f"NOT COALESCE({predicate}, false)"
    selected = f"COALESCE({predicate}, false)"
    def snapshot(spark, path):
        return {"unselected": _data_files(spark, path, unselected),
                "selected": _data_files(spark, path, selected)}
    cpu_path, gpu_path, outcome, before, after = _assert_cpu_gpu_outcomes_match(
        spark_tmp_path, _zorder_conf, write,
        f"OPTIMIZE delta.`{{path}}` WHERE {predicate} ZORDER BY (a, c)", expected=expected,
        snapshot_func=snapshot)
    if outcome == "ok":
        assert after["unselected"] == before["unselected"], \
            "Partitions outside the predicate must keep their original files"
        assert not (after["selected"] & before["selected"]), \
            "Every file in the selected partitions must be rewritten"
        with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(
            s, spark_tmp_path + "/DELTA_OPTIMIZE"), conf=_zorder_conf)


def _gpu_optimize_commits(spark, path):
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}`").filter("operation = 'OPTIMIZE'").count()


_zorder_file_selection_cases = [
    # id, rows, input files, extra conf
    ("single_file", 512, 1, {}),
    ("one_row", 1, 1, {}),
    # Compaction would skip files this large; Z-order rewrites them anyway.
    ("files_above_min_size", 2048, 16, {"spark.databricks.delta.optimize.minFileSize": "1"})]


@_zorder_test
@pytest.mark.parametrize("case", _zorder_file_selection_cases, ids=lambda case: case[0])
def test_delta_optimize_zorder_file_selection(spark_tmp_path, case):
    _, rows, files, extra_conf = case
    _assert_optimize_parity(
        False, spark_tmp_path, write_func=_zorder_writer(length=rows, num_slices=files),
        zorder_columns=["a", "c"], conf=copy_and_update(_zorder_conf, extra_conf))


@_zorder_test
def test_delta_optimize_zorder_batches(spark_tmp_path):
    # A tiny batch size splits the partitions into separate jobs and commits.
    conf = copy_and_update(_zorder_conf, {"spark.databricks.delta.optimize.batchSize": "1"})
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, ["p"], None, conf, write_func=_write_zorder_files)
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, c)").collect()[0][1]
    cpu_metrics = with_cpu_session(lambda s: sql(s, cpu_path), conf=conf)
    gpu_metrics = with_gpu_session(lambda s: sql(s, gpu_path), conf=conf)
    _assert_zorder_metrics_equal(cpu_metrics, gpu_metrics)
    cpu_commits = with_cpu_session(lambda s: _gpu_optimize_commits(s, cpu_path), conf=conf)
    gpu_commits = with_cpu_session(lambda s: _gpu_optimize_commits(s, gpu_path), conf=conf)
    assert cpu_commits == gpu_commits, f"CPU made {cpu_commits} commits, GPU made {gpu_commits}"
    with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path),
                     conf=conf)


@_zorder_test
def test_delta_optimize_zorder_repeated_after_changes(spark_tmp_path):
    # New files, new deletion vectors and a different column list must all be picked up by a
    # later Z-order.
    conf = _zorder_dv_conf
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(True, cpu_path, gpu_path, ["p"], None, conf, write_func=_write_zorder_files)
    def mutate(func):
        for path in [cpu_path, gpu_path]:
            with_cpu_session(lambda s: func(s, path), conf=conf)
    def append(spark, path):
        gen_df(spark, _zorder_gens, length=512, num_slices=4) \
            .write.format("delta").mode("append").save(path)
    def delete(spark, path):
        spark.sql(f"DELETE FROM delta.`{path}` WHERE f % 5 = 0")
    def optimize(columns, ignore_input_sizes=True):
        sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({columns})").collect()[0][1]
        _assert_zorder_metrics_equal(with_cpu_session(lambda s: sql(s, cpu_path), conf=conf),
                                     with_gpu_session(lambda s: sql(s, gpu_path), conf=conf),
                                     ignore_input_sizes)
    # Only the first run starts from identical files. Later runs read each engine's own output.
    optimize("a, c", ignore_input_sizes=False)
    mutate(append)
    optimize("a, c")
    mutate(delete)
    optimize("b, c")
    assert_equal(with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect(), conf=conf),
                 with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect(), conf=conf))
    with_cpu_session(lambda s: assert_gpu_and_cpu_latest_delta_log_equivalent(s, data_path),
                     conf=conf)


def _delete_twice(spark, path):
    _delete_zorder_rows_with_deletion_vectors(spark, path)
    spark.sql(f"DELETE FROM delta.`{path}` WHERE f % 7 = 0")


def _delete_then_disable_dvs(spark, path):
    _delete_zorder_rows_with_deletion_vectors(spark, path)
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)")


def _delete_with_persistent_dvs(spark, path):
    # The OPTIMIZE below runs with persistent DV creation turned off in the session.
    spark.conf.set("spark.databricks.delta.delete.deletionVectors.persistent", "true")
    _delete_zorder_rows_with_deletion_vectors(spark, path)


_zorder_dv_state_cases = [
    # id, table properties, setup after the write, extra conf
    ("no_deletion_vectors", {}, None, {}),
    ("repeated_deletes", {}, _delete_twice, {}),
    ("table_dvs_disabled_after_delete", {}, _delete_then_disable_dvs, {}),
    ("session_dvs_disabled", {}, _delete_with_persistent_dvs,
     {"spark.databricks.delta.delete.deletionVectors.persistent": "false"}),
    # The input scan of a row-tracking table (row id columns) is not on the GPU.
    ("row_tracking", {"delta.enableRowTracking": "true"},
     _delete_zorder_rows_with_deletion_vectors, {})]


@_zorder_test
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
@pytest.mark.parametrize("case", _zorder_dv_state_cases, ids=lambda case: case[0])
def test_delta_optimize_zorder_deletion_vector_states(spark_tmp_path, case):
    _, properties, setup, extra_conf = case
    def row_tracking_rows(spark, path):
        return spark.read.format("delta").load(path).selectExpr(
            "a", "c", "_metadata.row_id AS row_id",
            "_metadata.row_commit_version AS row_commit_version").orderBy("row_id").collect()
    _assert_optimize_parity(
        True, spark_tmp_path, partition_columns=["p"], write_func=_zorder_writer(properties),
        zorder_columns=["a", "c"], conf=copy_and_update(_zorder_dv_conf, extra_conf),
        post_setup_func=setup, require_gpu_scan="row_tracking" not in case[0],
        # The order in which partitions get their base row ids is not deterministic.
        check_delta_log="row_tracking" not in case[0],
        snapshot_func=row_tracking_rows if "row_tracking" in case[0] else None)


@_zorder_test
@pytest.mark.parametrize("cdf", [
    False, pytest.param(True, marks=pytest.mark.skipif(
        not is_oss_delta_lake_42_or_43(), reason="Requires Delta 4.2+ CDF writes"))],
    ids=["no_cdf", "cdf"])
def test_delta_optimize_zorder_row_tracking_after_changes(spark_tmp_path, cdf):
    # Row ids and commit versions must survive Z-order on a partitioned table that has several
    # commits, an UPDATE and several output files.
    properties = {"delta.enableRowTracking": "true"}
    if cdf:
        properties["delta.enableChangeDataFeed"] = "true"
    conf = copy_and_update(_zorder_conf, {
        "spark.databricks.delta.optimize.maxFileSize": str(32 * 1024)})
    def mutate(spark, path):
        gen_df(spark, _zorder_gens, length=512, num_slices=4) \
            .write.format("delta").mode("append").save(path)
        spark.sql(f"UPDATE delta.`{path}` SET c = 'updated' WHERE a % 3 = 0")
    def rows(spark, path):
        return spark.read.format("delta").load(path).selectExpr(
            "a", "b", "c", "_metadata.row_id AS row_id",
            "_metadata.row_commit_version AS row_commit_version").orderBy("row_id").collect()
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    _setup_tables(False, cpu_path, gpu_path, ["p"], None, conf,
                  write_func=_zorder_writer(properties), post_setup_func=mutate)
    # The two tables are written independently, so Delta may assign them different base row IDs.
    # Compare each table with its own snapshot, and compare only the data across engines.
    cpu_before = with_cpu_session(lambda s: rows(s, cpu_path), conf=conf)
    gpu_before = with_cpu_session(lambda s: rows(s, gpu_path), conf=conf)
    sql = lambda s, path: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a, c)").collect()
    with_cpu_session(lambda s: sql(s, cpu_path), conf=conf)
    with_gpu_session(lambda s: sql(s, gpu_path), conf=conf)
    cpu_after = with_cpu_session(lambda s: rows(s, cpu_path), conf=conf)
    gpu_after = with_cpu_session(lambda s: rows(s, gpu_path), conf=conf)
    assert_equal(cpu_before, cpu_after)
    assert_equal(gpu_before, gpu_after)
    data = lambda rs: sorted(
        ((r.a is None, r.a), (r.b is None, r.b), (r.c is None, r.c)) for r in rs)
    assert_equal(data(cpu_after), data(gpu_after))


@_zorder_test
@pytest.mark.parametrize("change", ["none", "rename_key_column", "drop_column"])
def test_delta_optimize_zorder_column_mapping(spark_tmp_path, change):
    # With column mapping the logical names differ from the physical Parquet names.
    zorder_columns = ["a", "c"]
    setup = None
    if change == "rename_key_column":
        zorder_columns = ["a_renamed", "c"]
        setup = lambda spark, path: spark.sql(
            f"ALTER TABLE delta.`{path}` RENAME COLUMN a TO a_renamed")
    elif change == "drop_column":
        setup = lambda spark, path: spark.sql(f"ALTER TABLE delta.`{path}` DROP COLUMN g")
    _assert_optimize_parity(
        False, spark_tmp_path, write_func=_zorder_writer({"delta.columnMapping.mode": "name"}),
        zorder_columns=zorder_columns, conf=_zorder_conf, post_setup_func=setup,
        check_delta_log=False)  # physical column names are random for each table


@_zorder_test
def test_delta_optimize_zorder_float_casts_disabled(spark_tmp_path):
    # Without GPU float casts Delta's clustering projection runs on the CPU. The result must still
    # match, with the command and write on the GPU.
    conf = copy_and_update(_zorder_conf, {
        "spark.rapids.sql.castFloatToIntegralTypes.enabled": "false",
        "spark.rapids.sql.castFloatToString.enabled": "false"})
    _assert_optimize_parity(
        False, spark_tmp_path, write_func=_write_zorder_files, zorder_columns=["a", "c"],
        conf=conf, require_gpu_clustering=False)


@_zorder_test
def test_delta_optimize_zorder_clusters_files(spark_tmp_path):
    # Z-order on one column leaves the files with (almost) disjoint, ordered ranges of it.
    conf = copy_and_update(_zorder_conf, {
        "spark.databricks.delta.optimize.maxFileSize": str(32 * 1024)})
    path = spark_tmp_path + "/DELTA_OPTIMIZE/GPU"
    def write(spark):
        gen_df(spark, [("a", IntegerGen(nullable=False)), ("b", long_gen)],
               length=20000, num_slices=16).write.format("delta").save(path)
    def file_ranges(spark):
        return spark.read.format("delta").load(path).selectExpr("input_file_name() AS f", "a") \
            .groupBy("f").agg({"a": "min"}).withColumnRenamed("min(a)", "lo") \
            .join(spark.read.format("delta").load(path).selectExpr("input_file_name() AS f", "a")
                  .groupBy("f").agg({"a": "max"}).withColumnRenamed("max(a)", "hi"), "f") \
            .orderBy("lo").select("lo", "hi").collect()
    with_cpu_session(write, conf=conf)
    before = with_cpu_session(file_ranges, conf=conf)
    with_gpu_session(lambda s: s.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (a)").collect(), conf=conf)
    after = with_cpu_session(file_ranges, conf=conf)
    assert len(after) > 1, f"Expected several output files, got {after}"
    def overlap(ranges):
        span = max(r.hi for r in ranges) - min(r.lo for r in ranges)
        return sum(max(0, prev.hi - nxt.lo) for prev, nxt in zip(ranges, ranges[1:])) / span
    # Rows with equal sampled range ids can land in either neighbouring file, so the boundaries
    # overlap slightly; unclustered files overlap almost completely.
    assert overlap(before) > 1.0, f"The input files were expected to overlap: {before}"
    assert overlap(after) < 0.01, f"Z-ordered file ranges overlap: {after}"
