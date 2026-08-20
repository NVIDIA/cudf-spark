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

import json
from urllib.parse import unquote, urlparse

import pytest

from asserts import assert_equal
from conftest import is_apache_runtime, spark_jvm
from data_gen import copy_and_update
from delta_lake_utils import (
    delta_meta_allow,
    delta_writes_enabled_conf,
)
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import (
    is_before_spark_353,
    supports_delta_lake_deletion_vectors,
    with_cpu_session,
    with_gpu_session,
    with_spark_session,
)


_reorg_conf = copy_and_update(delta_writes_enabled_conf, {
    "spark.rapids.sql.command.OptimizeTableCommand": "true",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
    "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
    "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
})

_iceberg_compatible_reorg_conf = copy_and_update(_reorg_conf, {
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false",
})

_reorg_metadata_allow = delta_meta_allow + ["ExecutedCommandExec"]


def setup_reorg_table(spark, path, partitioned):
    writer = (spark.range(4096)
              .selectExpr("id", "CAST(id % 4 AS INT) AS p")
              .repartition(8)
              .write
              .format("delta")
              .option("delta.enableDeletionVectors", "true"))
    if partitioned:
        writer = writer.partitionBy("p")
    writer.save(path)
    spark.sql("DELETE FROM delta.`{}` WHERE pmod(id, 17) = 0".format(path)).collect()

    # Add files after the DELETE so PURGE must leave these non-DV files untouched.
    writer = (spark.range(4096, 5120)
              .selectExpr("id", "CAST(id % 4 AS INT) AS p")
              .repartition(4)
              .write
              .format("delta")
              .mode("append"))
    if partitioned:
        writer = writer.partitionBy("p")
    writer.save(path)


def setup_dropped_column_reorg_table(spark, path):
    spark.sql("""
        CREATE TABLE delta.`{}` (id BIGINT, kept STRING, dropped STRING)
        USING DELTA
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'false',
          'delta.columnMapping.mode' = 'name')
        """.format(path))
    (spark.range(1024)
     .selectExpr("id", "CAST(id AS STRING) AS kept", "concat('drop-', id) AS dropped")
     .repartition(4)
     .write
     .format("delta")
     .mode("append")
     .save(path))

    schema_strings = (spark.read.text(path + "/_delta_log/*.json")
                      .selectExpr("get_json_object(value, '$.metaData.schemaString') AS schema")
                      .where("schema IS NOT NULL")
                      .collect())
    assert len(schema_strings) == 1
    fields = json.loads(schema_strings[0][0])["fields"]
    physical_name = next(field["metadata"]["delta.columnMapping.physicalName"]
                         for field in fields if field["name"] == "dropped")
    files_before = active_file_names(spark, path)
    assert physical_name in parquet_field_names(spark, files_before)
    spark.sql("ALTER TABLE delta.`{}` DROP COLUMN dropped".format(path)).collect()
    return physical_name, files_before


def setup_row_tracking_reorg_table(spark, path):
    spark.sql("""
        CREATE TABLE delta.`{}` (id BIGINT, p INT)
        USING DELTA
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true',
          'delta.enableRowTracking' = 'true')
        """.format(path))
    (spark.range(1024)
     .selectExpr("id", "CAST(id % 4 AS INT) AS p")
     .repartition(8)
     .write
     .format("delta")
     .mode("append")
     .save(path))
    spark.sql("DELETE FROM delta.`{}` WHERE pmod(id, 17) = 0".format(path)).collect()


def read_row_tracking_rows(spark, path):
    return spark.sql("""
        SELECT id,
               _metadata.row_id AS row_id,
               _metadata.row_commit_version AS row_commit_version
        FROM delta.`{}`
        ORDER BY id
        """.format(path)).collect()


def create_converted_gpu_reorg_command(spark, path):
    jvm = spark_jvm()
    parsed = spark._jsparkSession.sessionState().sqlParser().parsePlan(reorg_sql(path, False))
    analyzed = spark._jsparkSession.sessionState().analyzer().execute(parsed)
    module = getattr(jvm.org.apache.spark.sql.delta.rapids, "GpuDeltaReorgTableCommand$")
    return getattr(module, "MODULE$").apply(analyzed.target(), analyzed.predicates())


def setup_iceberg_compatible_reorg_table(spark, path):
    spark.sql("""
        CREATE TABLE delta.`{}` (id BIGINT, p INT)
        USING DELTA
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'false',
          'delta.columnMapping.mode' = 'name',
          'delta.enableIcebergCompatV2' = 'true')
        """.format(path))
    (spark.range(256)
     .selectExpr("id", "CAST(id % 4 AS INT) AS p")
     .repartition(4)
     .write
     .format("delta")
     .mode("append")
     .save(path))

    adds = (spark.read.text(path + "/_delta_log/*.json")
            .where("get_json_object(value, '$.add.path') IS NOT NULL"))
    assert adds.where(
        "get_json_object(value, '$.add.tags.ICEBERG_COMPAT_VERSION') = '2'").count() > 0


def assert_table_has_deletion_vectors(spark, path):
    assert (spark.read.text(path + "/_delta_log/*.json")
            .where("get_json_object(value, '$.add.deletionVector') IS NOT NULL")
            .count()) > 0


def assert_table_has_no_deletion_vectors(spark, path):
    assert (spark.read.text(path + "/_delta_log/*.json")
            .where("get_json_object(value, '$.add.deletionVector') IS NOT NULL")
            .count()) == 0


def reorg_sql(path, partitioned):
    predicate = " WHERE p = 0" if partitioned else ""
    return "REORG TABLE delta.`{}`{} APPLY (PURGE)".format(path, predicate)


def assert_gpu_reorg_plans(plan_callback, captured_plans):
    for class_name in ["GpuExecutedCommandExec", "GpuFileSourceScanExec", "RapidsDeltaWrite"]:
        assert any(plan_callback.contains(plan, class_name) for plan in captured_plans), \
            "{} is not found in captured REORG plans:\n{}".format(
                class_name, "\n".join(str(plan) for plan in captured_plans))


def with_gpu_session_no_test(func, conf):
    gpu_conf = copy_and_update(conf, {
        "spark.rapids.sql.enabled": "true",
        "spark.rapids.sql.test.enabled": "false",
    })
    return with_spark_session(func, conf=gpu_conf)


def assert_reorg_fallback(cpu_target, gpu_target, sql_func, read_func):
    with_cpu_session(lambda spark: spark.sql(sql_func(cpu_target)).collect(), conf=_reorg_conf)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session_no_test(
            lambda spark: spark.sql(sql_func(gpu_target)).collect(), conf=_reorg_conf)
        plan_callback.assertCapturedAndGpuFellBack(["ExecutedCommandExec"], 10000)
    finally:
        plan_callback.endCapture()

    cpu_rows = with_cpu_session(
        lambda spark: read_func(spark, cpu_target).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    gpu_rows = with_cpu_session(
        lambda spark: read_func(spark, gpu_target).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    assert_equal(cpu_rows, gpu_rows)


def latest_reorg_version(spark, path):
    rows = (spark.sql("DESCRIBE HISTORY delta.`{}`".format(path))
            .where("operation = 'REORG'")
            .select("version")
            .collect())
    assert rows
    return rows[0][0]


def assert_reorg_adds_have_no_deletion_vectors(spark, path, version):
    log_path = "{}/_delta_log/{:020d}.json".format(path, version)
    adds = spark.read.text(log_path).where("get_json_object(value, '$.add.path') IS NOT NULL")
    assert adds.count() > 0
    assert adds.where(
        "get_json_object(value, '$.add.deletionVector') IS NOT NULL").count() == 0


def assert_reorg_adds_match_partition(spark, path, version, expected_partition):
    log_path = "{}/_delta_log/{:020d}.json".format(path, version)
    partitions = (spark.read.text(log_path)
                  .where("get_json_object(value, '$.add.path') IS NOT NULL")
                  .selectExpr("get_json_object(value, '$.add.partitionValues.p') AS p"))
    assert partitions.count() > 0
    assert partitions.where("p IS NULL OR p != '{}'".format(expected_partition)).count() == 0


def active_file_names(spark, path, predicate=None):
    df = spark.read.format("delta").load(path)
    if predicate is not None:
        df = df.where(predicate)
    return set(row[0] for row in
               df.selectExpr("input_file_name() AS file").distinct().collect())


def spark_readable_file_name(file_name):
    parsed = urlparse(file_name)
    if parsed.scheme == "file" and not parsed.netloc:
        return unquote(parsed.path)
    return file_name


def parquet_field_names(spark, files):
    assert files
    readable_files = sorted(spark_readable_file_name(file_name) for file_name in files)
    return set(spark.read.parquet(*readable_files).schema.fieldNames())


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
@ignore_order(local=True)
@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_purge(spark_tmp_path, partitioned):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_reorg_table(spark, cpu_path, partitioned),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: setup_reorg_table(spark, gpu_path, partitioned),
                     conf=_reorg_conf)
    with_cpu_session(
        lambda spark: (
            assert_table_has_deletion_vectors(spark, cpu_path),
            assert_table_has_deletion_vectors(spark, gpu_path)),
        conf=_reorg_conf)

    selection_predicate = "id < 4096 AND p = 0" if partitioned else "id < 4096"
    gpu_files_before = with_cpu_session(
        lambda spark: active_file_names(spark, gpu_path), conf=_reorg_conf)
    gpu_dv_files_to_purge = with_cpu_session(
        lambda spark: active_file_names(spark, gpu_path, selection_predicate), conf=_reorg_conf)
    gpu_non_dv_files = with_cpu_session(
        lambda spark: active_file_names(spark, gpu_path, "id >= 4096"), conf=_reorg_conf)
    assert gpu_dv_files_to_purge
    assert gpu_non_dv_files
    assert gpu_dv_files_to_purge.isdisjoint(gpu_non_dv_files)

    cpu_result = with_cpu_session(
        lambda spark: spark.sql(reorg_sql(cpu_path, partitioned)).collect(), conf=_reorg_conf)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        gpu_result = with_gpu_session(
            lambda spark: spark.sql(reorg_sql(gpu_path, partitioned)).collect(),
            conf=_reorg_conf)
        assert_gpu_reorg_plans(plan_callback, plan_callback.getResultsWithTimeout(10000))
    finally:
        plan_callback.endCapture()

    assert len(cpu_result) == 1
    assert len(gpu_result) == 1
    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')

    cpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(cpu_path).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    gpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(gpu_path).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    assert_equal(cpu_rows, gpu_rows)

    gpu_files_after = with_cpu_session(
        lambda spark: active_file_names(spark, gpu_path), conf=_reorg_conf)
    assert gpu_dv_files_to_purge.isdisjoint(gpu_files_after)
    assert gpu_non_dv_files.issubset(gpu_files_after)
    assert gpu_files_after - gpu_files_before

    gpu_reorg_version = with_cpu_session(
        lambda spark: latest_reorg_version(spark, gpu_path), conf=_reorg_conf)
    with_cpu_session(
        lambda spark: assert_reorg_adds_have_no_deletion_vectors(
            spark, gpu_path, gpu_reorg_version),
        conf=_reorg_conf)
    if partitioned:
        with_cpu_session(
            lambda spark: assert_reorg_adds_match_partition(
                spark, gpu_path, gpu_reorg_version, "0"),
            conf=_reorg_conf)

    def assert_second_reorg_is_noop(spark):
        before = spark.sql("DESCRIBE HISTORY delta.`{}`".format(gpu_path)).count()
        spark.sql(reorg_sql(gpu_path, partitioned)).collect()
        after = spark.sql("DESCRIBE HISTORY delta.`{}`".format(gpu_path)).count()
        assert before == after

    with_gpu_session(assert_second_reorg_is_noop, conf=_reorg_conf)


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_purge_physically_dropped_column(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_dropped_column_reorg_table(spark, cpu_path),
                     conf=_reorg_conf)
    gpu_physical_name, gpu_files_before = with_cpu_session(
        lambda spark: setup_dropped_column_reorg_table(spark, gpu_path),
        conf=_reorg_conf)
    with_cpu_session(
        lambda spark: (
            assert_table_has_no_deletion_vectors(spark, cpu_path),
            assert_table_has_no_deletion_vectors(spark, gpu_path)),
        conf=_reorg_conf)

    with_cpu_session(
        lambda spark: spark.sql(reorg_sql(cpu_path, False)).collect(), conf=_reorg_conf)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(
            lambda spark: spark.sql(reorg_sql(gpu_path, False)).collect(),
            conf=_reorg_conf)
        assert_gpu_reorg_plans(plan_callback, plan_callback.getResultsWithTimeout(10000))
    finally:
        plan_callback.endCapture()

    cpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(cpu_path).orderBy("id").collect(),
        conf=_reorg_conf)
    gpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(gpu_path).orderBy("id").collect(),
        conf=_reorg_conf)
    assert_equal(cpu_rows, gpu_rows)

    gpu_files_after = with_cpu_session(
        lambda spark: active_file_names(spark, gpu_path), conf=_reorg_conf)
    assert gpu_files_before.isdisjoint(gpu_files_after)
    gpu_fields_after = with_cpu_session(
        lambda spark: parquet_field_names(spark, gpu_files_after), conf=_reorg_conf)
    assert gpu_physical_name not in gpu_fields_after


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_row_tracking_fallback(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_row_tracking_reorg_table(spark, cpu_path),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: setup_row_tracking_reorg_table(spark, gpu_path),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: assert_table_has_deletion_vectors(spark, cpu_path),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: assert_table_has_deletion_vectors(spark, gpu_path),
                     conf=_reorg_conf)

    cpu_before = with_cpu_session(
        lambda spark: read_row_tracking_rows(spark, cpu_path), conf=_reorg_conf)
    gpu_before = with_cpu_session(
        lambda spark: read_row_tracking_rows(spark, gpu_path), conf=_reorg_conf)

    assert_reorg_fallback(
        cpu_path,
        gpu_path,
        lambda path: reorg_sql(path, False),
        lambda spark, path: spark.read.format("delta").load(path))

    cpu_after = with_cpu_session(
        lambda spark: read_row_tracking_rows(spark, cpu_path), conf=_reorg_conf)
    gpu_after = with_cpu_session(
        lambda spark: read_row_tracking_rows(spark, gpu_path), conf=_reorg_conf)
    assert_equal(cpu_before, cpu_after)
    assert_equal(gpu_before, gpu_after)


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_execution_guard_rejects_iceberg(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")

    path = spark_tmp_path + "/TABLE"

    def assert_execution_guard(spark):
        spark.sql("""
            CREATE TABLE delta.`{}` (id BIGINT, p INT)
            USING DELTA
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
            """.format(path))
        gpu_command = create_converted_gpu_reorg_command(spark, path)
        spark.sql("""
            ALTER TABLE delta.`{}` SET TBLPROPERTIES
              ('delta.columnMapping.mode' = 'name',
               'delta.enableIcebergCompatV2' = 'true')
            """.format(path))
        with pytest.raises(Exception, match="does not support Iceberg-compatible tables"):
            gpu_command.run(spark._jsparkSession)

    with_gpu_session_no_test(assert_execution_guard, conf=_iceberg_compatible_reorg_conf)


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_execution_guard_rejects_row_tracking(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")

    path = spark_tmp_path + "/TABLE"

    def assert_execution_guard(spark):
        spark.sql("""
            CREATE TABLE delta.`{}` (id BIGINT, p INT)
            USING DELTA
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
            """.format(path))
        gpu_command = create_converted_gpu_reorg_command(spark, path)
        spark.sql("""
            ALTER TABLE delta.`{}` SET TBLPROPERTIES
              ('delta.enableRowTracking' = 'true')
            """.format(path))
        with pytest.raises(Exception, match="does not support row-tracking tables"):
            gpu_command.run(spark._jsparkSession)

    with_gpu_session_no_test(assert_execution_guard, conf=_reorg_conf)


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
@pytest.mark.skipif(not is_before_spark_353(),
                    reason="This test covers Spark versions where GPU REORG is disabled")
def test_delta_reorg_table_unsupported_version_fallback(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_reorg_table(spark, cpu_path, False), conf=_reorg_conf)
    with_cpu_session(lambda spark: setup_reorg_table(spark, gpu_path, False), conf=_reorg_conf)

    assert_reorg_fallback(
        cpu_path,
        gpu_path,
        lambda path: reorg_sql(path, False),
        lambda spark, path: spark.read.format("delta").load(path))


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_iceberg_compatible_table_fallback(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("Unsupported Spark versions are covered by the version fallback test")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(
        lambda spark: setup_iceberg_compatible_reorg_table(spark, cpu_path),
        conf=_iceberg_compatible_reorg_conf)
    with_cpu_session(
        lambda spark: setup_iceberg_compatible_reorg_table(spark, gpu_path),
        conf=_iceberg_compatible_reorg_conf)

    assert_reorg_fallback(
        cpu_path,
        gpu_path,
        lambda path: reorg_sql(path, False),
        lambda spark, path: spark.read.format("delta").load(path))


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_unsupported_mode_uniform_iceberg_fallback(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("Unsupported Spark versions are covered by the version fallback test")

    path = spark_tmp_path + "/TABLE"

    def setup_table(spark):
        (spark.range(256)
         .selectExpr("id", "CAST(id % 4 AS INT) AS p")
         .repartition(4)
         .write
         .format("delta")
         .save(path))

    with_cpu_session(setup_table, conf=_reorg_conf)

    sql = ("REORG TABLE delta.`{}` APPLY "
           "(UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=3))").format(path)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        def assert_unsupported_version(spark):
            with pytest.raises(Exception, match="COMPAT_VERSION_NOT_SUPPORTED"):
                spark.sql(sql).collect()

        with_gpu_session_no_test(assert_unsupported_version, conf=_reorg_conf)
        plan_callback.assertCapturedAndGpuFellBack(["ExecutedCommandExec"], 10000)
    finally:
        plan_callback.endCapture()
