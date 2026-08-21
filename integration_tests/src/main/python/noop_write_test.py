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

import pytest

from conftest import is_databricks_runtime
from data_gen import ansi_enabled_conf
from marks import validate_execs_in_gpu_plan
from spark_session import is_spark_330_or_later, with_gpu_session


@pytest.mark.skipif(
    not is_spark_330_or_later() or is_databricks_runtime(),
    reason="GPU noop writes are supported only on non-Databricks Apache Spark 3.3.0 and later")
@pytest.mark.parametrize("mode", [
    pytest.param("overwrite",
                 marks=validate_execs_in_gpu_plan("GpuNoopOverwriteByExpressionExec")),
    pytest.param("append", marks=validate_execs_in_gpu_plan("GpuNoopAppendDataExec"))
])
def test_noop_write(mode):
    def write_noop(spark):
        spark.range(10).selectExpr(
            "id", "null as n", "named_struct('id', id) as struct_col",
            "array(id) as array_col", "map(id, id) as map_col",
            "cast(null as binary) as binary_col") \
            .write.format("noop").mode(mode).save()

    with_gpu_session(write_noop)


@pytest.mark.skipif(
    not is_spark_330_or_later() or is_databricks_runtime(),
    reason="GPU noop writes are supported only on non-Databricks Apache Spark 3.3.0 and later")
@validate_execs_in_gpu_plan("GpuNoopAppendDataExec")
def test_noop_write_consumes_input():
    def successful_write(spark):
        spark.range(10).selectExpr("id + 5 as v") \
            .write.format("noop").mode("append").save()

    with_gpu_session(successful_write, conf=ansi_enabled_conf)


@pytest.mark.skipif(
    not is_spark_330_or_later() or is_databricks_runtime(),
    reason="GPU noop writes are supported only on non-Databricks Apache Spark 3.3.0 and later")
def test_noop_write_evaluates_input():
    def failing_write(spark):
        # The id == 5 row raises only if the noop command actually evaluates its child plan.
        spark.range(10).selectExpr("id div (id - 5) as v") \
            .write.format("noop").mode("append").save()

    with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
        with_gpu_session(failing_write, conf=ansi_enabled_conf)
