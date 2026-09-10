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
def test_agg_pushdown_rule(spark_tmp_path):
    """AggPushdownRule pushes a partial aggregate onto the fact side of a
    fact-dim shuffle join. Fires only on a row-counted relation (parquet, not
    Range) where the fact dominates the 0.15 fact/dim ratio and the join is not
    a broadcast join. Marker: a partial 'sum(sum(' over the pushed aggregate."""
    fact_path = spark_tmp_path + "/agg_pushdown_fact"
    dim_path = spark_tmp_path + "/agg_pushdown_dim"
    with_cpu_session(lambda s: s.range(20000)
                     .selectExpr("id AS fact_id", "id % 100 AS dim_fk", "(id % 7 + 1) AS val")
                     .write.mode("overwrite").parquet(fact_path))
    with_cpu_session(lambda s: s.range(100)
                     .selectExpr("id AS dim_id", "concat('d_', id) AS name")
                     .write.mode("overwrite").parquet(dim_path))

    def fn(spark):
        fact = spark.read.parquet(fact_path)
        dim = spark.read.parquet(dim_path)
        joined = fact.join(dim, fact.dim_fk == dim.dim_id, "inner")
        return joined.groupBy("name").agg({"val": "sum", "fact_id": "count"})

    # autoBroadcastJoinThreshold low (positive) keeps the join a shuffle join.
    base = {"spark.sql.autoBroadcastJoinThreshold": "200"}
    on = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.aggPushdownEnabled": "true"}, extra_conf=base)
    off = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.aggPushdownEnabled": "false"}, extra_conf=base)
    assert_rule_fires(fn, on, off, marker="sum(sum(")
