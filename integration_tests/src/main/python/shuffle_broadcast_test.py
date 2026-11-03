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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from marks import ignore_order, shuffle_test
from spark_init_internal import get_spark_i_know_what_i_am_doing


pytestmark = [shuffle_test]


def _require_ucx():
    startup = get_spark_i_know_what_i_am_doing().sparkContext.getConf()
    if not startup.get('spark.shuffle.manager', '').endswith('RapidsShuffleManager') or \
            startup.get('spark.rapids.shuffle.mode', 'MULTITHREADED').upper() != 'UCX' or \
            startup.get('spark.rapids.sql.driverlessBroadcast.enabled', 'false').lower() != 'true':
        pytest.skip('Requires application startup opt-in and a UCX shuffle manager')


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


def _executed_nodes(plan):
    name = plan.getClass().getSimpleName()
    if name == 'AdaptiveSparkPlanExec':
        return _executed_nodes(plan.executedPlan())
    if name.endswith('QueryStageExec'):
        return _executed_nodes(plan.plan())
    nodes = [plan]
    children = plan.children().iterator()
    while children.hasNext():
        nodes.extend(_executed_nodes(children.next()))
    return nodes


def _native_join(plan):
    joins = [node for node in _executed_nodes(plan)
             if node.getClass().getSimpleName() == 'GpuDriverlessBroadcastHashJoinExec']
    assert len(joins) == 1, f'Expected one executor broadcast join:\n{plan}'
    return joins[0]


_conf = {
    'spark.sql.adaptive.enabled': 'false',
    'spark.sql.exchange.reuse': 'false',
    'spark.rapids.sql.driverlessBroadcast.enabled': 'true',
    'spark.rapids.sql.driverlessBroadcast.maxSize': '1g',
    'spark.rapids.sql.join.hashTable.reuse': 'false',
}


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('join_type,build_side', [
    ('inner', 'left'), ('inner', 'right'), ('left_outer', 'right'),
    ('right_outer', 'left'), ('left_semi', 'right'), ('left_anti', 'right')])
def test_executor_broadcast_preserves_null_duplicate_and_residual_semantics(
        join_type, build_side, aqe):
    _require_ucx()

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.metrics().apply('runtimeFallbacks').value() == 0
        exchange = join.fallbackBroadcast()
        assert exchange.metrics().apply('collectTime').value() == 0
        if aqe == 'true':
            assert exchange.metrics().apply('driverlessBuildBytes').value() > 0
        else:
            assert exchange.metrics().apply('dataSize').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, join_type, build_side),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_rejects_oversized_build_before_executor_assembly():
    _require_ucx()

    def check_fallback(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('runtimeFallbacks').value() == 1
        assert join.metrics().apply('executorBuilds').value() == 0
        assert join.fallbackBroadcast().metrics().apply('dataSize').value() > 0
        spark = get_spark_i_know_what_i_am_doing()
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
        store = spark._jsparkSession.sharedState().statusStore()
        metric_id = join.metrics().apply('runtimeFallbacks').id()
        published = []
        executions = store.executionsList().iterator()
        while executions.hasNext():
            values = store.executionMetrics(executions.next().executionId())
            if values.contains(metric_id):
                published.append(values.apply(metric_id))
        assert published == ['1'], f'Fallback count not published to SQL UI: {published}'

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.rapids.sql.driverlessBroadcast.maxSize': '1'}),
        gpu_plan_assertion=check_fallback, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_disabled_keeps_standard_broadcast():
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.rapids.sql.driverlessBroadcast.enabled': 'false'}),
        exist_classes='GpuBroadcastHashJoinExec',
        non_exist_classes='GpuDriverlessBroadcastHashJoinExec', require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_with_infinite_timeout():
    _require_ucx()

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.metrics().apply('runtimeFallbacks').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.sql.broadcastTimeout': '-1'}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_nondeterministic_build_keeps_standard_path():
    _require_ucx()

    def query(spark):
        left = spark.range(128, numPartitions=8).selectExpr('id % 11 AS key', 'id AS probe')
        right = spark.range(24, numPartitions=4).selectExpr('id % 11 AS key', 'rand(17) AS value')
        return left.join(broadcast(right), 'key')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=_conf, exist_classes='GpuBroadcastHashJoinExec',
        non_exist_classes='GpuDriverlessBroadcastHashJoinExec', require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_aqe_oversized_build_uses_real_standard_broadcast():
    _require_ucx()

    def check_fallback(cpu_plan, gpu_plan):
        nodes = _executed_nodes(gpu_plan)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes
                 if node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert exchange.metrics().apply('driverlessStageFallbacks').value() == 1
        assert exchange.metrics().apply('driverlessBuildBytes').value() == 0
        assert exchange.metrics().apply('dataSize').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': 'true',
                           'spark.rapids.sql.driverlessBroadcast.maxSize': '1'}),
        gpu_plan_assertion=check_fallback, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_aqe_converts_an_initial_shuffle_join():
    _require_ucx()

    def query(spark):
        left = spark.range(128, numPartitions=8).selectExpr('id % 11 AS key', 'id AS probe')
        right = spark.range(24, numPartitions=4).selectExpr('id % 11 AS key', 'id AS build')
        return left.join(right, 'key')

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('driverlessBuildBytes').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': 'true',
                                  'spark.sql.autoBroadcastJoinThreshold': '-1',
                                  'spark.sql.adaptive.autoBroadcastJoinThreshold': '1m'}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_aqe_admitted_stage_has_real_payload_for_standard_consumer():
    _require_ucx()

    def query(spark):
        left = spark.range(128, numPartitions=8).selectExpr('id % 11 AS key', 'id AS probe')
        left = left.filter('probe < (SELECT max(id) FROM range(96))')
        right = spark.range(24, numPartitions=4).selectExpr('id % 11 AS key', 'id AS build')
        return left.join(broadcast(right), 'key')

    def check_fallback(cpu_plan, gpu_plan):
        nodes = _executed_nodes(gpu_plan)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes
                 if node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert exchange.metrics().apply('driverlessBuildBytes').value() > 0
        assert exchange.metrics().apply('driverlessStageFallbacks').value() == 1
        assert exchange.metrics().apply('collectTime').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': 'true'}),
        gpu_plan_assertion=check_fallback, require_non_empty=True)
