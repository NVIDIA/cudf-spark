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
from spark_session import with_cpu_session


@pytest.mark.private_optimizer
def test_optimize_subquery_shared_scan(spark_tmp_path):
    """OptimizeSubquerySharedScanRule merges scalar subqueries that aggregate
    (no grouping, single agg fn) over the same LogicalRelation into one shared
    scan. Conf is default-on, so the OFF baseline sets it false explicitly.
    Marker: the combined named struct with generated c_0/c_1/c_2 fields."""
    data_path = spark_tmp_path + "/subquery_shared_scan"
    with_cpu_session(lambda s: s.range(1000).selectExpr("id", "id % 100 AS g")
                     .write.mode("overwrite").parquet(data_path))
    sql_text = (
        "SELECT "
        "  (SELECT max(id) FROM t WHERE g = 1) AS a, "
        "  (SELECT min(id) FROM t WHERE g = 2) AS b, "
        "  (SELECT count(*) FROM t WHERE g = 3) AS c"
    )

    def fn(spark):
        spark.read.parquet(data_path).createOrReplaceTempView("t")
        return spark.sql(sql_text)

    # AQE off so the rule runs in the logical optimizer rather than AQE re-opt.
    # Databricks has an overlapping scalar-subquery shared-scan optimizer, so
    # disable it here to keep the ON/OFF marker specific to the RAPIDS rule.
    base = {
        "spark.sql.adaptive.enabled": "false",
        "spark.databricks.optimizer.subquerySharedScan.enabled": "false",
    }
    on = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.optimizeScalarSubquery": "true"}, extra_conf=base)
    off = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.optimizeScalarSubquery": "false"}, extra_conf=base)
    assert_rule_fires(fn, on, off, marker="named_struct(c_0,")
