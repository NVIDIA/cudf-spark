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

from marks import approximate_float
from private_optimizer_common import (
    assert_rule_fires,
    private_optimizer_conf,
    require_private_optimizer,
)


@pytest.mark.private_optimizer
@require_private_optimizer
@approximate_float
def test_decompose_stddev_pop():
    """DecomposeStddevPop rewrites stddev_pop(x) into sum(x), sum(x*x),
    count(x) aliased as _stddev_*. variableFloatAgg keeps the rewritten
    Sum<double> on the GPU. Marker: a '_stddev_' intermediate alias."""
    def fn(spark):
        df = spark.range(10000).selectExpr(
            "CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id % 100 AS DOUBLE) END AS x",
            "id % 10 AS g")
        return df.groupBy("g").agg({"x": "stddev_pop"})

    base = {"spark.rapids.sql.variableFloatAgg.enabled": "true"}
    on = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.decomposeStddevPop.enabled": "true"}, extra_conf=base)
    off = private_optimizer_conf(
        {"spark.rapids.sql.optimizer.decomposeStddevPop.enabled": "false"}, extra_conf=base)
    assert_rule_fires(fn, on, off, marker="_stddev_")
