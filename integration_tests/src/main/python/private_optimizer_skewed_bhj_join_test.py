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
    private_optimizer_conf,
)
from spark_session import is_databricks_runtime


@pytest.mark.private_optimizer
@pytest.mark.skipif(
    is_databricks_runtime(),
    reason="Databricks executor-broadcast AQE can put the materialized shuffle on the "
           "BHJ build side; this marker test covers streamed-side skew split. "
           "See https://github.com/NVIDIA/cudf-spark/issues/15136")
def test_optimize_skewed_bhj_join(spark_tmp_path):
    """OptimizeSkewedBHJJoinRule splits a skewed partition on the streamed side
    of an AQE broadcast hash join. Needs a runtime broadcast (static
    autoBroadcastJoinThreshold=-1, adaptive.autoBroadcastJoinThreshold=10m) so
    the streamed side is a materialized shuffle stage, plus small skew
    thresholds. Marker: the shuffle reader is 'coalesced and skewed'.

    The rule additionally short-circuits in OptimizeSkewedBHJJoinRule.apply when
    AQEUtils.isOptimizeSkewBHJSupported is false, so if a future Spark/runtime
    drops support the rule becomes a no-op and the marker assertion below fails
    loudly rather than passing silently.

    Validated with a small GLOBAL aggregate over the materialized skewed join;
    a GROUP BY on the skew key is intentionally avoided here."""
    conf_extra = {
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

    def fn(spark):
        spark.range(0, 2000, 1, 10).selectExpr(
            "CASE WHEN id < 1000 THEN 249 ELSE id END AS key2", "id AS value2"
        ).createOrReplaceTempView("skewData2")
        spark.range(0, 1000, 1, 10).selectExpr(
            "CASE WHEN id < 250 THEN 249 WHEN id >= 750 THEN 1000 ELSE id END AS key1", "id AS value1"
        ).createOrReplaceTempView("skewData1")
        return spark.sql(
            "SELECT count(*) AS cnt, min(value2) AS mn, max(value2) AS mx, sum(value1) AS sm "
            "FROM skewData1 JOIN skewData2 ON key1 = key2")

    on = private_optimizer_conf(
        {"spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "true"}, extra_conf=conf_extra)
    off = private_optimizer_conf(
        {"spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "false"}, extra_conf=conf_extra)
    assert_rule_fires(fn, on, off, marker="coalesced and skewed", physical=True)
