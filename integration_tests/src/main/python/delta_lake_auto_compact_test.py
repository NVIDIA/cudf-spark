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
from asserts import assert_gpu_and_cpu_writes_are_equal_collect, with_gpu_session
from data_gen import copy_and_update, idfn
from delta_lake_utils import *
from marks import allow_non_gpu, delta_lake, ignore_order
from pyspark.sql.functions import *
from spark_session import is_spark_353_or_later, is_databricks_runtime, \
    is_databricks104_or_later, is_databricks173_or_later, supports_delta_lake_deletion_vectors

_conf = {'spark.rapids.sql.explain': 'ALL',
         'spark.databricks.delta.autoCompact.minNumFiles': 3}  # Num files before compaction.

_auto_compact_min_files_deletion_vector_values = deletion_vector_values \
    if is_databricks173_or_later() else deletion_vector_values_with_xfail_reasons(
        enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12042")


def write_to_delta(enable_deletion_vectors, num_rows=30, is_partitioned=False, num_writes=3):
    """
    Returns bound function that writes to a delta table.
    """

    def write(spark, table_path):
        input_data = spark.range(num_rows)
        input_data = input_data.withColumn("part", expr("id % 3")) if is_partitioned \
            else input_data.repartition(1)
        writer = input_data.write.format("delta").mode("append")
        if supports_delta_lake_deletion_vectors():
            writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        for _ in range(num_writes):
            writer.save(table_path)

    return write


def assert_optimized(spark, table_path):
    """
    Asserts that the table at `table_path` has been optimized.
    """
    from delta.tables import DeltaTable
    input_table = DeltaTable.forPath(spark, table_path)
    table_history = input_table.history()
    assert table_history.select("version", "operation").count() > 0, \
        "Expected at least one version in the table history."
    optimize_op_cnt = table_history.select("version") \
        .where("operation = 'OPTIMIZE'") \
        .count()
    # Check that at least one OPTIMIZE operation exists.
    assert optimize_op_cnt > 0, \
        "Expected at least one OPTIMIZE operation in the table history."


def assert_inline_auto_compaction_used_gpu(captured_plans):
    """
    The triggering append is expected to produce one RapidsDeltaWrite plan. Inline
    auto compaction should produce a second one; otherwise the hook likely fell back.
    """
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    for class_name in delta_write:
        num_delta_write_plans = len([
            plan for plan in captured_plans if plan_callback.contains(plan, class_name)])
        if num_delta_write_plans >= 2:
            continue
        plan_descriptions = "\n".join(str(plan) for plan in captured_plans)
        assert False, (
            f"Expected at least two {class_name} plans, one for the triggering append "
            f"and one for inline auto compaction, but captured {num_delta_write_plans}:\n"
            f"{plan_descriptions}")


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="DBR 17.3 has the GPU inline auto-compaction hook under test")
def test_auto_compact_dbr173_inline_uses_gpu(spark_tmp_path):
    data_path = spark_tmp_path + "/AUTO_COMPACT_DBR173_GPU_INLINE"
    conf_enable_auto_compact = copy_and_update(
        _conf, {'spark.databricks.delta.autoCompact.enabled': 'true'})

    # Write below the threshold first so the captured write below has exactly one
    # user append and, if inline auto compaction runs on GPU, one compaction write.
    writer = write_to_delta(False, num_writes=2)
    with_gpu_session(func=lambda spark: writer(spark, data_path),
                     conf=conf_enable_auto_compact)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(
            func=lambda spark: write_to_delta(False, num_writes=1)(spark, data_path),
            conf=conf_enable_auto_compact)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
        assert_inline_auto_compaction_used_gpu(captured_plans)
    finally:
        plan_callback.endCapture()

    with_cpu_session(lambda spark: assert_optimized(spark, data_path), {})


# Auto-configured RapidsShuffleManager may change row ordering.
@ignore_order(local=True)
@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Apache Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks104_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Databricks 10.4+")
@pytest.mark.parametrize("auto_compact_conf",
                         ["spark.databricks.delta.autoCompact.enabled",
                          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"])
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_auto_compact_basic(spark_tmp_path, auto_compact_conf, enable_deletion_vectors):
    """
    This test checks whether the results of auto compactions on an un-partitioned table
    match, when written via CPU and GPU.
    It also checks that the snapshot metrics (number of files added/removed, etc.)
    match.
    """
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/AUTO_COMPACT_TEST_DATA"

    def read_data(spark, table_path):
        return spark.read.format("delta").load(table_path)

    conf_enable_auto_compact = copy_and_update(_conf, {auto_compact_conf: "true"})

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=write_to_delta(enable_deletion_vectors, is_partitioned=False),
        read_func=read_data,
        base_path=data_path,
        conf=conf_enable_auto_compact)

    def read_metadata(spark, table_path):
        assert_optimized(spark, table_path)
        input_table = DeltaTable.forPath(spark, table_path)
        table_history = input_table.history()
        return table_history.select(
            "version",
            "operation",
            expr("operationMetrics[\"numFiles\"]").alias("numFiles"),
            expr("operationMetrics[\"numRemovedFiles\"]").alias("numRemoved"),
            expr("operationMetrics[\"numAddedFiles\"]").alias("numAdded")
        )

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=lambda spark, table_path: None,  # Already written.
        read_func=read_metadata,
        base_path=data_path,
        conf=conf_enable_auto_compact)


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_oss_delta_lake_43(),
                    reason="Normalized timestamp partition grouping was added in Delta Lake 4.3")
@pytest.mark.parametrize("normalize_partition_values_on_read", [False, True], ids=idfn)
@pytest.mark.parametrize("session_time_zone", ["UTC", "America/Los_Angeles"], ids=idfn)
def test_auto_compact_mixed_timestamp_partition_forms(
        spark_tmp_path, normalize_partition_values_on_read, session_time_zone):
    """
    Logically equal timestamp partition values written in different textual forms are
    compacted into one bin only when `spark.databricks.delta.normalizePartitionValuesOnRead`
    is true, while a logically different value stays in its own bin. Checks that the GPU
    write executed (and, when compaction is expected, that inline auto compaction ran on the
    GPU), and compares CPU and GPU file counts, data and OPTIMIZE metrics. The local form is
    written in the session time zone, which is the main semantic risk of normalization.
    """
    data_path = spark_tmp_path + "/AUTO_COMPACT_TS_PARTITIONS"
    cpu_path, gpu_path = data_path + "/CPU", data_path + "/GPU"
    conf = copy_and_update(_conf, {
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.session.timeZone": session_time_zone,
        "spark.databricks.delta.normalizePartitionValuesOnRead":
            str(normalize_partition_values_on_read).lower()})
    # 12:00 UTC written as a local string and as a UTC string, then a different instant (13:00
    # UTC) that must never join the 12:00 bin, then the local 12:00 form again.
    local_12, local_13 = ("2000-01-01 12:00:00", "2000-01-01 13:00:00") \
        if session_time_zone == "UTC" else ("2000-01-01 04:00:00", "2000-01-01 05:00:00")
    inserts = [(False, local_12), (True, "2000-01-01T12:00:00.000Z"), (False, local_13),
               (False, local_12)]

    def insert(spark, path, start, end):
        for i in range(start, end):
            insert_timestamp_partition_row(spark, path, i, *inserts[i])

    for path in [cpu_path, gpu_path]:
        with_cpu_session(lambda spark: create_timestamp_partitioned_table(spark, path), conf)
    # Write below the threshold first so the captured insert has exactly one user append and,
    # if inline auto compaction runs on the GPU, one compaction write.
    last = len(inserts) - 1
    with_cpu_session(lambda spark: insert(spark, cpu_path, 0, last), conf)
    with_gpu_session(lambda spark: insert(spark, gpu_path, 0, last), conf)
    with_cpu_session(lambda spark: insert(spark, cpu_path, last, last + 1), conf)
    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(lambda spark: insert(spark, gpu_path, last, last + 1), conf)
        captured_plans = plan_callback.getResultsWithTimeout(10000)
    finally:
        plan_callback.endCapture()

    def summarize(spark, path):
        # Raw strings across the whole log history, including files removed by compaction.
        raw_partitions = {r[0] for r in spark.read.json(path + "/_delta_log/*.json")
                          .where("add IS NOT NULL")
                          .selectExpr("add.partitionValues.ts").collect()}
        history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        optimize = history.where("operation = 'OPTIMIZE'").selectExpr(
            "operationMetrics['numAddedFiles']", "operationMetrics['numRemovedFiles']").collect()
        table = spark.read.format("delta").load(path)
        files_per_ts = [r[1] for r in table.select("ts", input_file_name().alias("f"))
                        .groupBy("ts").agg(count_distinct("f")).orderBy("ts").collect()]
        return (len(table.inputFiles()), len(raw_partitions), files_per_ts,
                sorted(tuple(r) for r in optimize),
                sorted((r[0], r[1]) for r in table.selectExpr(
                    "id", "CAST(ts AS LONG)").collect()))

    cpu = with_cpu_session(lambda spark: summarize(spark, cpu_path), conf)
    gpu = with_cpu_session(lambda spark: summarize(spark, gpu_path), conf)
    assert cpu == gpu, f"CPU and GPU differ: {cpu} vs {gpu}"
    num_files, num_raw_partitions, files_per_ts, optimize_ops, rows = cpu
    # Epoch seconds of 2000-01-01 12:00:00 UTC for ids 0, 1 and 3, and 13:00:00 UTC for id 2.
    utc_12, utc_13 = 946728000, 946731600
    assert rows == [(0, utc_12), (1, utc_12), (2, utc_13), (3, utc_12)], \
        f"Unexpected (id, ts) rows: {rows}"
    for class_name in delta_write:
        assert any(plan_callback.contains(plan, class_name) for plan in captured_plans), \
            f"Expected a {class_name} plan for the GPU insert"
    assert num_raw_partitions == 3, \
        "Inputs must use three distinct raw partition strings across the log history"
    if normalize_partition_values_on_read:
        assert optimize_ops, "Expected auto compaction to run"
        # The 12:00 forms are merged into one file; 13:00 keeps its own file.
        assert files_per_ts == [1, 1], f"Unexpected files per partition: {files_per_ts}"
        assert num_files == 2, f"Expected two files, got {num_files}"
        assert_inline_auto_compaction_used_gpu(captured_plans)
    else:
        assert not optimize_ops, "Raw-string partitions must not be auto compacted together"
        assert files_per_ts == [3, 1], f"Unexpected files per partition: {files_per_ts}"
        assert num_files == 4, f"Expected four uncompacted files, got {num_files}"


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Apache Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks104_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Databricks 10.4+")
@pytest.mark.parametrize("auto_compact_conf",
                         ["spark.databricks.delta.autoCompact.enabled",
                          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"])
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_auto_compact_partitioned(spark_tmp_path, auto_compact_conf, enable_deletion_vectors):
    """
    This test checks whether the results of auto compaction on a partitioned table
    match, when written via CPU and GPU.
    Note: The behaviour of compaction itself differs from Databricks, in that
    the plugin enforces `minFiles` restriction uniformly across all partitions.
    Databricks' Delta implementation appears not to.
    """
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/AUTO_COMPACT_TEST_DATA_PARTITIONED"

    def read_data(spark, table_path):
        return spark.read.format("delta").load(table_path).orderBy("id", "part")

    conf_enable_auto_compact = copy_and_update(_conf, {auto_compact_conf: "true"})

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=write_to_delta(enable_deletion_vectors, is_partitioned=True),
        read_func=read_data,
        base_path=data_path,
        conf=conf_enable_auto_compact)

    def read_metadata(spark, table_path):
        assert_optimized(spark, table_path)
        # The snapshots might not look alike, in the partitioned case.
        # Ensure that auto compaction has occurred, even if it's not identical.
        input_table = DeltaTable.forPath(spark, table_path)
        table_history = input_table.history()
        return table_history.select(
            "version",
            "operation",
            expr("operationMetrics[\"numFiles\"] > 0").alias("numFiles_gt_0"),
            expr("operationMetrics[\"numRemovedFiles\"] > 0").alias("numRemoved_gt_0"),
            expr("operationMetrics[\"numAddedFiles\"] > 0").alias("numAdded_gt_0")
        )

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=lambda spark, table_path: None,  # Already written.
        read_func=read_metadata,
        base_path=data_path,
        conf=conf_enable_auto_compact)


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Apache Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks104_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Databricks 10.4+")
@pytest.mark.parametrize("auto_compact_conf",
                         ["spark.databricks.delta.autoCompact.enabled",
                          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"])
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_auto_compact_disabled(spark_tmp_path, auto_compact_conf, enable_deletion_vectors):
    """
    This test verifies that auto-compaction does not run if disabled.
    """
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/AUTO_COMPACT_TEST_CHECK_DISABLED"

    disable_auto_compaction = copy_and_update(_conf, {auto_compact_conf: 'false'})

    writer = write_to_delta(enable_deletion_vectors, num_writes=10)
    with_gpu_session(func=lambda spark: writer(spark, data_path),
                     conf=disable_auto_compaction)

    # 10 writes should correspond to 10 commits.
    # (i.e. there should be no OPTIMIZE commits.)
    def verify_table_history(spark):
        input_table = DeltaTable.forPath(spark, data_path)
        table_history = input_table.history()
        assert table_history.select("version", "operation").count() == 10, \
            "Expected 10 versions, 1 for each WRITE."
        assert table_history.select("version")\
            .where("operation = 'OPTIMIZE'")\
            .count() == 0,\
            "Expected 0 OPTIMIZE operations."

    with_cpu_session(verify_table_history, {})


@delta_lake
# Added 'RapidsDeltaWriteExec', 'CoalesceExec', 'ColumnarToRowExec' to allow_non_gpu
# look at https://github.com/NVIDIA/spark-rapids/issues/12042 for details
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Apache Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks104_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Databricks 10.4+")
@pytest.mark.parametrize("enable_deletion_vectors",
                         _auto_compact_min_files_deletion_vector_values,
                         ids=idfn)
def test_auto_compact_min_num_files(spark_tmp_path, enable_deletion_vectors):
    """
    This test verifies that auto-compaction honours the minNumFiles setting.
    """
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/AUTO_COMPACT_TEST_MIN_FILES"
    enable_auto_compaction_on_5 = {
        'spark.databricks.delta.autoCompact.enabled': 'true',  # Enable auto compaction.
        'spark.databricks.delta.autoCompact.minNumFiles': 5  # Num files before compaction.
    }

    # Minimum number of input files == 5.
    # If 4 files are written, there should be no OPTIMIZE.
    writer = write_to_delta(enable_deletion_vectors, num_writes=4)
    with_gpu_session(func=lambda spark: writer(spark, data_path),
                     conf=enable_auto_compaction_on_5)

    def verify_table_history_before_limit(spark):
        input_table = DeltaTable.forPath(spark, data_path)
        table_history = input_table.history()
        assert table_history.select("version", "operation").count() == 4, \
            "Expected 4 versions, 1 for each WRITE."
        assert table_history.select("version") \
                   .where("operation = 'OPTIMIZE'") \
                   .count() == 0, \
            "Expected 0 OPTIMIZE operations."
    with_cpu_session(verify_table_history_before_limit, {})

    # On the 5th file write, auto-OPTIMIZE should kick in.
    with_gpu_session(func=lambda spark: write_to_delta(enable_deletion_vectors, num_writes=1)(spark, data_path),
                     conf=enable_auto_compaction_on_5)

    def verify_table_history_after_limit(spark):
        input_table = DeltaTable.forPath(spark, data_path)
        table_history = input_table.history()
        assert table_history.select("version", "operation").count() == 6, \
            "Expected 6 versions, i.e. 5 WRITEs + 1 OPTIMIZE."
        assert table_history.select("version") \
                   .where("operation = 'OPTIMIZE'") \
                   .count() == 1, \
            "Expected 1 OPTIMIZE operations."
    with_cpu_session(verify_table_history_after_limit, {})
