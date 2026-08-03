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
    is_spark_401_or_later,
    supports_delta_lake_deletion_vectors,
    with_cpu_session,
    with_gpu_session,
)


_reorg_conf = copy_and_update(delta_writes_enabled_conf, {
    "spark.rapids.sql.command.OptimizeTableCommand": "true",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
    "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
    "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
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


def assert_table_has_deletion_vectors(spark, path):
    assert (spark.read.text(path + "/_delta_log/*.json")
            .where("get_json_object(value, '$.add.deletionVector') IS NOT NULL")
            .count()) > 0


def reorg_sql(path, partitioned):
    predicate = " WHERE p = 0" if partitioned else ""
    return "REORG TABLE delta.`{}`{} APPLY (PURGE)".format(path, predicate)


def assert_gpu_reorg_plans(plan_callback, captured_plans):
    for class_name in ["GpuExecutedCommandExec", "GpuFileSourceScanExec", "RapidsDeltaWrite"]:
        assert any(plan_callback.contains(plan, class_name) for plan in captured_plans), \
            "{} is not found in captured REORG plans:\n{}".format(
                class_name, "\n".join(str(plan) for plan in captured_plans))


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


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
@ignore_order(local=True)
@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_purge(spark_tmp_path, partitioned):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353() or is_spark_401_or_later():
        pytest.skip("GPU REORG TABLE is supported with Spark 3.5.3 through 4.0.0")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_reorg_table(spark, cpu_path, partitioned),
                     conf=_reorg_conf)
    with_gpu_session(lambda spark: setup_reorg_table(spark, gpu_path, partitioned),
                     conf=_reorg_conf)
    with_cpu_session(
        lambda spark: (
            assert_table_has_deletion_vectors(spark, cpu_path),
            assert_table_has_deletion_vectors(spark, gpu_path)),
        conf=_reorg_conf)

    with_cpu_session(lambda spark: spark.sql(reorg_sql(cpu_path, partitioned)).collect(),
                     conf=_reorg_conf)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(
            lambda spark: spark.sql(reorg_sql(gpu_path, partitioned)).collect(),
            conf=_reorg_conf)
        assert_gpu_reorg_plans(plan_callback, plan_callback.getResultsWithTimeout(10000))
    finally:
        plan_callback.endCapture()

    cpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(cpu_path).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    gpu_rows = with_cpu_session(
        lambda spark: spark.read.format("delta").load(gpu_path).orderBy("id", "p").collect(),
        conf=_reorg_conf)
    assert_equal(cpu_rows, gpu_rows)

    gpu_reorg_version = with_cpu_session(
        lambda spark: latest_reorg_version(spark, gpu_path), conf=_reorg_conf)
    with_cpu_session(
        lambda spark: assert_reorg_adds_have_no_deletion_vectors(
            spark, gpu_path, gpu_reorg_version),
        conf=_reorg_conf)
    def assert_second_reorg_is_noop(spark):
        before = spark.sql("DESCRIBE HISTORY delta.`{}`".format(gpu_path)).count()
        spark.sql(reorg_sql(gpu_path, partitioned)).collect()
        after = spark.sql("DESCRIBE HISTORY delta.`{}`".format(gpu_path)).count()
        assert before == after

    with_gpu_session(assert_second_reorg_is_noop, conf=_reorg_conf)
