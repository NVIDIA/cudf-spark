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

from private_optimizer_common import (
    assert_rule_fires,
    assert_rule_skipped,
    private_optimizer_conf,
)
from spark_session import is_databricks_runtime, is_spark_420_or_later


SKEWED_BHJ_MARKER = "coalesced and skewed"
# The DB GpuBroadcastHashJoinExec plan ends with isNullAwareAntiJoin and
# executorBroadcast. This marker proves the shim guard's exact precondition was
# reached rather than accepting any broadcast hash join as a skipped-rule path.
DB_EXECUTOR_BROADCAST_MARKER = "GpuBuildRight, false, true"
# The keyed exchange identifies the explicitly repartitioned streamed input.
# GpuShuffleCoalesce alone is not specific because aggregate stages can add it.
DB_STREAMED_SHUFFLE_MARKER = "GpuColumnarExchange gpuhashpartitioning(key1"


def _skewed_bhj_conf_extra():
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "10m",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
        "spark.sql.shuffle.partitions": "100",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "800",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "800",
        "spark.sql.adaptive.localShuffleReader.enabled": "false",
    }


def _skewed_bhj_confs():
    conf_extra = _skewed_bhj_conf_extra()
    on = private_optimizer_conf(
        {"spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "true"},
        extra_conf=conf_extra)
    off = private_optimizer_conf(
        {"spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "false"},
        extra_conf=conf_extra)
    return on, off


def _skewed_bhj_global_agg(spark):
    spark.range(0, 2000, 1, 10).selectExpr(
        "CASE WHEN id < 1000 THEN 249 ELSE id END AS key2", "id AS value2"
    ).createOrReplaceTempView("skewData2")
    spark.range(0, 1000, 1, 10).selectExpr(
        "CASE WHEN id < 250 THEN 249 WHEN id >= 750 THEN 1000 ELSE id END AS key1",
        "id AS value1"
    ).repartition(100, "key1").createOrReplaceTempView("skewData1")
    return spark.sql(
        "SELECT /*+ BROADCAST(skewData2) */ "
        "count(*) AS cnt, min(value2) AS mn, max(value2) AS mx, sum(value1) AS sm "
        "FROM skewData1 JOIN skewData2 ON key1 = key2")


@pytest.mark.private_optimizer
@pytest.mark.skipif(
    is_databricks_runtime() or is_spark_420_or_later(),
    reason="The positive rule-fire assertion is unsupported on Databricks and Spark 4.2. "
           "Databricks executor-broadcast coverage is in the skipped-path test below; "
           "Spark 4.2 can put the same marker on the off-plan. "
           "See https://github.com/NVIDIA/cudf-spark/issues/15136")
def test_optimize_skewed_bhj_join(spark_tmp_path):
    """OptimizeSkewedBHJJoinRule splits a skewed partition on the streamed side
    of an AQE broadcast hash join. The broadcast hint fixes the build side while
    the explicit key repartition makes the streamed side a materialized shuffle
    stage; small skew thresholds make the key 249 partition eligible to split.
    Marker: the shuffle reader is 'coalesced and skewed'.

    The rule additionally short-circuits in OptimizeSkewedBHJJoinRule.apply when
    AQEUtils.isOptimizeSkewBHJSupported is false, so if a future Spark/runtime
    drops support the rule becomes a no-op and the marker assertion below fails
    loudly rather than passing silently.

    Validated with a small GLOBAL aggregate over the materialized skewed join;
    a GROUP BY on the skew key is intentionally avoided here."""
    on, off = _skewed_bhj_confs()
    assert_rule_fires(_skewed_bhj_global_agg, on, off, marker=SKEWED_BHJ_MARKER,
                      physical=True)


@pytest.mark.private_optimizer
@pytest.mark.skipif(
    not is_databricks_runtime(),
    reason="Databricks-only coverage for executor-broadcast AQE fallback. "
           "Apache runtime coverage is in test_optimize_skewed_bhj_join.")
def test_optimize_skewed_bhj_join_skips_on_databricks_executor_broadcast(spark_tmp_path):
    """Databricks executor-broadcast AQE is expected to skip the streamed-side
    rewrite even when the streamed input is a materialized skewed shuffle stage.
    Without the executor-broadcast shim guard this shape is eligible for the
    rewrite, as verified by the positive Apache test above. The streamed-side
    skew marker must remain absent while CPU and GPU results still match. The
    required markers verify a non-null-aware, executor-broadcast build-right join
    whose streamed input retains the expected GPU hash-partitioning exchange."""
    on, off = _skewed_bhj_confs()
    assert_rule_skipped(_skewed_bhj_global_agg, on, off, marker=SKEWED_BHJ_MARKER,
                        physical=True, required_on_markers=(
                            DB_EXECUTOR_BROADCAST_MARKER,
                            DB_STREAMED_SHUFFLE_MARKER))
