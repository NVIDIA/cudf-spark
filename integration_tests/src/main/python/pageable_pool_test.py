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

from asserts import assert_gpu_and_cpu_are_equal_collect
from spark_session import with_cpu_session


def test_pageable_pool_parquet_query(spark_tmp_path):
    # The pool is initialized at executor startup, so this test needs the setting
    # in the Spark application config (for example, PYSP_TEST_spark_rapids_memory_pageablePool_size=64m).
    pool_size = with_cpu_session(
        lambda spark: spark.sparkContext.getConf().get(
            "spark.rapids.memory.pageablePool.size", "0"))
    if pool_size == "0":
        pytest.skip("requires a pageable pool configured at Spark application startup")

    data_path = spark_tmp_path + "/pageable_pool_parquet"
    with_cpu_session(lambda spark: spark.range(0, 100000, 1, 8).selectExpr(
        "cast(id % 97 as int) AS bucket",
        "concat(cast(id as string), repeat('x', cast(id % 64 as int))) AS payload"
    ).write.parquet(data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .selectExpr("bucket", "length(payload) AS payload_len")
            .where("bucket % 3 != 0")
            .groupBy("bucket")
            .sum("payload_len")
            .orderBy("bucket"))
