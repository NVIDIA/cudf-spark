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
from pyspark.sql.functions import broadcast

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, collect_plan_nodes
from marks import ignore_order, shuffle_test
from spark_init_internal import get_spark_i_know_what_i_am_doing


pytestmark = [shuffle_test]


def _require_ucx():
    startup = get_spark_i_know_what_i_am_doing().sparkContext.getConf()
    if not startup.get('spark.shuffle.manager', '').endswith('RapidsShuffleManager') or \
            startup.get('spark.rapids.shuffle.mode', 'MULTITHREADED').upper() != 'UCX' or \
            startup.get('spark.rapids.shuffle.compression.codec', 'none').lower() != 'none' or \
            startup.get('spark.rapids.shuffle.broadcast.enabled', 'false').lower() != 'true':
        pytest.skip('Requires application startup opt-in and an uncompressed UCX shuffle manager')


def _join(spark, join_type, build_side):
    left = spark.range(128, numPartitions=8).selectExpr(
        'IF(id % 13 = 0, CAST(NULL AS BIGINT), id % 11) AS left_key',
        'id AS left_value', "concat('left-', CAST(id AS STRING)) AS left_payload",
        'CAST(id AS DECIMAL(12, 2)) AS left_decimal')
    right = spark.range(24, numPartitions=4).selectExpr(
        'IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 11) AS right_key',
        'id AS right_value', "concat('right-', CAST(id AS STRING)) AS right_payload")
    if build_side == 'left':
        left = broadcast(left)
    else:
        right = broadcast(right)
    return left.join(right,
        (left.left_key == right.right_key) & (left.left_value > right.right_value), join_type)


def _native_join(plan):
    joins = [node for node in collect_plan_nodes(plan)
             if node.getClass().getSimpleName() == 'GpuShuffleBroadcastHashJoinExec']
    assert len(joins) == 1, f'Expected one executor broadcast join:\n{plan}'
    return joins[0]


_conf = {
    'spark.sql.adaptive.enabled': 'false',
    'spark.sql.exchange.reuse': 'false',
    'spark.rapids.shuffle.broadcast.enabled': 'true',
    'spark.rapids.shuffle.broadcast.maxSize': '1g',
    'spark.rapids.sql.join.hashTable.reuse': 'false',
}


@ignore_order(local=True)
@pytest.mark.parametrize('join_type,build_side', [
    ('inner', 'left'), ('inner', 'right'), ('left_outer', 'right'),
    ('right_outer', 'left'), ('left_semi', 'right'), ('left_anti', 'right')])
def test_executor_broadcast_preserves_null_duplicate_and_residual_semantics(join_type, build_side):
    _require_ucx()

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.metrics().apply('runtimeFallbacks').value() == 0
        assert join.fallbackBroadcast().metrics().apply('dataSize').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, join_type, build_side),
        conf=_conf, gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_rejects_oversized_build_before_executor_assembly():
    _require_ucx()

    def check_fallback(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('runtimeFallbacks').value() == 1
        assert join.metrics().apply('executorBuilds').value() == 0
        assert join.fallbackBroadcast().metrics().apply('dataSize').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.rapids.shuffle.broadcast.maxSize': '1'}),
        gpu_plan_assertion=check_fallback, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_disabled_keeps_standard_broadcast():
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.rapids.shuffle.broadcast.enabled': 'false'}),
        exist_classes='GpuBroadcastHashJoinExec',
        non_exist_classes='GpuShuffleBroadcastHashJoinExec', require_non_empty=True)
