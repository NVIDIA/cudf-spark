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
from marks import validate_execs_in_gpu_plan
from spark_session import is_spark_330_or_later, with_gpu_session


@pytest.mark.skipif(
    not is_spark_330_or_later() or is_databricks_runtime(),
    reason="GPU noop writes are only supported on Apache Spark 3.3.0 and later")
@pytest.mark.parametrize("mode", [
    pytest.param("overwrite",
                 marks=validate_execs_in_gpu_plan("GpuOverwriteByExpressionExec")),
    pytest.param("append", marks=validate_execs_in_gpu_plan("GpuAppendDataExec"))
])
def test_noop_write(mode):
    def write_noop(spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["c1", "c2"])
        df.write.format("noop").mode(mode).save()

    # There is no output so there is nothing to check, except to make sure
    # we did not crash and everything is on the GPU.
    with_gpu_session(write_noop)
