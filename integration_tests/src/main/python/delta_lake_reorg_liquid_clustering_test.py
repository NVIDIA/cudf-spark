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
from delta_lake_reorg_table_test import (
    _reorg_conf,
    _reorg_metadata_allow,
    assert_gpu_reorg_plans,
    assert_reorg_adds_have_no_deletion_vectors,
    assert_table_has_deletion_vectors,
    latest_reorg_version,
    reorg_sql,
)
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import (
    is_before_spark_353,
    supports_delta_lake_deletion_vectors,
    with_cpu_session,
    with_gpu_session,
)


def setup_liquid_clustered_reorg_table(spark, path):
    spark.sql("""
        CREATE TABLE delta.`{}`
        (id BIGINT, p INT)
        USING DELTA
        CLUSTER BY (p)
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """.format(path))
    (spark.range(4096)
     .selectExpr("id", "CAST(id % 4 AS INT) AS p")
     .repartition(8)
     .write
     .format("delta")
     .mode("append")
     .save(path))
    spark.sql("DELETE FROM delta.`{}` WHERE pmod(id, 19) = 0".format(path)).collect()


def clustering_columns(spark, path):
    return spark.sql("DESCRIBE DETAIL delta.`{}`".format(path)).first()["clusteringColumns"]


@ignore_order(local=True)
@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_purge_liquid_clustered(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_liquid_clustered_reorg_table(spark, cpu_path),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: setup_liquid_clustered_reorg_table(spark, gpu_path),
                     conf=_reorg_conf)
    with_cpu_session(
        lambda spark: (
            assert_table_has_deletion_vectors(spark, cpu_path),
            assert_table_has_deletion_vectors(spark, gpu_path)),
        conf=_reorg_conf)

    assert with_cpu_session(lambda spark: clustering_columns(spark, cpu_path),
                            conf=_reorg_conf) == ["p"]
    assert with_cpu_session(lambda spark: clustering_columns(spark, gpu_path),
                            conf=_reorg_conf) == ["p"]

    with_cpu_session(
        lambda spark: spark.sql("REORG TABLE delta.`{}` APPLY (PURGE)".format(cpu_path)).collect(),
        conf=_reorg_conf)

    plan_callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    plan_callback.startCapture()
    try:
        with_gpu_session(
            lambda spark: spark.sql(
                "REORG TABLE delta.`{}` APPLY (PURGE)".format(gpu_path)).collect(),
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

    assert with_cpu_session(lambda spark: clustering_columns(spark, gpu_path),
                            conf=_reorg_conf) == ["p"]
    gpu_reorg_version = with_cpu_session(
        lambda spark: latest_reorg_version(spark, gpu_path), conf=_reorg_conf)
    with_cpu_session(
        lambda spark: assert_reorg_adds_have_no_deletion_vectors(
            spark, gpu_path, gpu_reorg_version),
        conf=_reorg_conf)


@delta_lake
@allow_non_gpu(*_reorg_metadata_allow)
def test_delta_reorg_table_purge_liquid_clustered_predicate_rejected(spark_tmp_path):
    if not is_apache_runtime():
        pytest.skip("GPU REORG TABLE currently supports Apache Delta Lake only")
    if is_before_spark_353():
        pytest.skip("GPU REORG TABLE requires Spark 3.5.3 or later")
    if not supports_delta_lake_deletion_vectors():
        pytest.skip("REORG TABLE PURGE requires deletion vector support")

    cpu_path = spark_tmp_path + "/CPU"
    gpu_path = spark_tmp_path + "/GPU"
    with_cpu_session(lambda spark: setup_liquid_clustered_reorg_table(spark, cpu_path),
                     conf=_reorg_conf)
    with_cpu_session(lambda spark: setup_liquid_clustered_reorg_table(spark, gpu_path),
                     conf=_reorg_conf)

    def assert_predicate_rejected(spark, path):
        with pytest.raises(Exception, match="DELTA_CLUSTERING_WITH_PARTITION_PREDICATE"):
            spark.sql(reorg_sql(path, True)).collect()

    with_cpu_session(lambda spark: assert_predicate_rejected(spark, cpu_path), conf=_reorg_conf)
    with_gpu_session(lambda spark: assert_predicate_rejected(spark, gpu_path), conf=_reorg_conf)

    # A rejected predicate must not silently trigger a full-table rewrite.
    with_cpu_session(
        lambda spark: (
            assert_table_has_deletion_vectors(spark, cpu_path),
            assert_table_has_deletion_vectors(spark, gpu_path)),
        conf=_reorg_conf)
