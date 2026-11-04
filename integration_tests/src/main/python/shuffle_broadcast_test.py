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

import time

import pytest
from pyspark import InheritableThread, TaskContext
from pyspark.sql.functions import broadcast, col, udf
from pyspark.sql.types import LongType

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from marks import allow_non_gpu, ignore_order, shuffle_test
from spark_init_internal import get_spark_i_know_what_i_am_doing
from spark_session import with_cpu_session, with_gpu_session


pytestmark = [shuffle_test]


def _require_ucx():
    startup = get_spark_i_know_what_i_am_doing().sparkContext.getConf()
    if not startup.get('spark.shuffle.manager', '').endswith('RapidsShuffleManager') or \
            startup.get('spark.rapids.shuffle.mode', 'MULTITHREADED').upper() != 'UCX':
        pytest.skip('Requires a UCX shuffle manager')


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


def _published_values(metric):
    spark = get_spark_i_know_what_i_am_doing()
    spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
    store = spark._jsparkSession.sharedState().statusStore()
    published = []
    executions = store.executionsList().iterator()
    while executions.hasNext():
        values = store.executionMetrics(executions.next().executionId())
        if values.contains(metric.id()):
            published.append(values.apply(metric.id()))
    return published


def _assert_published_driver_value(metric):
    spark = get_spark_i_know_what_i_am_doing()
    gateway = spark.sparkContext._gateway
    values = gateway.new_array(spark._jvm.long, 1)
    values[0] = metric.value()
    expected = spark._jvm.org.apache.spark.sql.execution.metric.SQLMetrics.stringValue(
        metric.metricType(), values, gateway.new_array(spark._jvm.long, 0))
    assert _published_values(metric) == [expected]


_conf = {
    'spark.sql.adaptive.enabled': 'false',
    'spark.rapids.sql.driverlessBroadcast.enabled': 'true',
    'spark.rapids.sql.join.hashTable.reuse': 'false',
}


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_executor_broadcast_preserves_nan_signed_zero_decimal_and_timestamp(aqe):
    _require_ucx()

    def query(spark):
        key = "CASE id % 4 WHEN 0 THEN CAST('NaN' AS DOUBLE) " \
              "WHEN 1 THEN CAST('-0.0' AS DOUBLE) WHEN 2 THEN CAST('0.0' AS DOUBLE) " \
              "ELSE CAST(NULL AS DOUBLE) END"
        left = spark.range(16, numPartitions=4).selectExpr(f'{key} AS probe_key', 'id AS probe')
        right = spark.range(8, numPartitions=4).selectExpr(
            f'{key} AS build_key', 'id AS build', 'CAST(id AS DECIMAL(38, 18)) AS measure',
            'timestamp_micros(id - 4) AS instant')
        return left.join(broadcast(right), left.probe_key == right.build_key) \
            .select('probe', 'build', 'measure', 'instant')

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


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
        exchange = join.fallbackBroadcast()
        assert exchange.metrics().apply('collectTime').value() == 0
        if aqe == 'true':
            assert join.metrics().apply('buildStageBytes').value() > 0
            assert join.metrics().apply('buildStageTime').value() > 0
            assert exchange.metrics().apply('driverlessBuildBytes').value() > 0
            if join_type == 'inner' and build_side == 'right':
                _assert_published_driver_value(join.metrics().apply('buildStageBytes'))
                _assert_published_driver_value(join.metrics().apply('buildStageTime'))
                assert _published_values(exchange.metrics().apply('driverlessStageTime'))
        else:
            assert not join.metrics().contains('buildStageTime')
            assert not join.metrics().contains('buildStageBytes')
            assert exchange.metrics().apply('dataSize').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, join_type, build_side),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


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
def test_executor_broadcast_aqe_converts_an_initial_shuffle_join():
    _require_ucx()

    def query(spark):
        left = spark.range(128, numPartitions=8).selectExpr('id % 11 AS key', 'id AS probe')
        right = spark.range(24, numPartitions=4).selectExpr('id % 11 AS key', 'id AS build')
        return left.join(right, 'key')

    def check_native(cpu_plan, gpu_plan):
        assert 'SortMergeJoin' in cpu_plan.initialPlan().toString()
        assert any(node.getClass().getSimpleName() == 'BroadcastHashJoinExec'
                   for node in _executed_nodes(cpu_plan)), cpu_plan.toString()
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('driverlessBuildBytes').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': 'true',
                                  'spark.sql.shuffle.partitions': '4',
                                  'spark.sql.autoBroadcastJoinThreshold': '-1',
                                  'spark.sql.adaptive.autoBroadcastJoinThreshold': '1m'}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_aqe_subquery_is_rejected_before_build_materialization():
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
        assert exchange.metrics().apply('driverlessBuildBytes').value() == 0
        assert exchange.metrics().apply('driverlessStageFallbacks').value() == 0
        assert exchange.metrics().apply('fallbackBroadcastBytes').value() == 0
        assert exchange.metrics().apply('dataSize').value() > 0
        assert exchange.metrics().apply('collectTime').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': 'true'}),
        gpu_plan_assertion=check_fallback, require_non_empty=True)


@ignore_order(local=True)
def test_driverless_broadcast_aqe_dpp_uses_only_standard_build(spark_tmp_path):
    _require_ucx()
    fact_path, dimension_path = spark_tmp_path + '/fact', spark_tmp_path + '/dimension'

    def write(session):
        session.range(128, numPartitions=4).selectExpr('id AS v', 'id % 8 AS k') \
            .write.partitionBy('k').parquet(fact_path)
        session.range(8, numPartitions=2).selectExpr('id AS k', 'id % 3 AS tag') \
            .write.parquet(dimension_path)

    with_cpu_session(write)

    def query(session):
        fact = session.read.parquet(fact_path)
        dimension = session.read.parquet(dimension_path).filter('tag = 1')
        return fact.join(broadcast(dimension), 'k').select('k', 'v')

    expected = sorted((value % 8, value) for value in range(128) if value % 8 % 3 == 1)
    assert with_cpu_session(lambda session: _rows(query(session))) == expected

    def check_standard(cpu_plan, gpu_plan):
        assert 'dynamicpruning' in gpu_plan.toString().lower(), gpu_plan.toString()
        nodes = _executed_nodes(gpu_plan)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert exchange.metrics().apply('driverlessBuildBytes').value() == 0
        assert exchange.metrics().apply('driverlessStageFallbacks').value() == 0
        assert exchange.metrics().apply('dataSize').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.exchange.reuse': 'true',
            'spark.sql.optimizer.dynamicPartitionPruning.enabled': 'true',
            'spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly': 'true'}),
        gpu_plan_assertion=check_standard, require_non_empty=True)


def _lifecycle_query(spark, transform=None):
    probe = spark.range(2048, numPartitions=8).selectExpr('id', 'id % 16 AS k')
    probe = probe.withColumn('v', col('id') if transform is None else transform(col('id')))
    build = spark.range(64, numPartitions=4).selectExpr('id AS b', 'id % 16 AS bk')
    return probe.join(broadcast(build), probe.k == build.bk).select('v', 'b')


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_duplicate_build_respects_exchange_reuse(aqe):
    _require_ucx()

    def query(spark):
        return spark.sql('''
            WITH d AS (
                SELECT IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 11) AS k, id AS b
                FROM range(0, 24, 1, 4)
            )
            SELECT /*+ BROADCAST(d) */ f.id AS p, d.b
            FROM range(0, 128, 1, 8) f JOIN d ON f.id % 11 = d.k
            UNION ALL
            SELECT /*+ BROADCAST(d) */ f.id + 1000 AS p, d.b
            FROM range(0, 128, 1, 8) f JOIN d ON f.id % 11 = d.k
        ''')

    expected = sorted((probe + shift, build) for probe in range(128) for build in range(24)
                      for shift in [0, 1000] if build % 7 != 0 and probe % 11 == build % 11)
    assert with_cpu_session(lambda session: _rows(query(session))) == expected

    def check_reuse(cpu_plan, gpu_plan):
        assert 'ReusedExchange' in gpu_plan.toString(), gpu_plan.toString()
        nodes = _executed_nodes(gpu_plan)
        native = [node for node in nodes if node.getClass().getSimpleName() ==
                  'GpuDriverlessBroadcastHashJoinExec']
        if aqe == 'true':
            assert not native
            joins = [node for node in nodes if node.getClass().getSimpleName() ==
                     'GpuBroadcastHashJoinExec']
            assert len(joins) == 2
            for join in joins:
                exchange = join.broadcastExchange()
                assert exchange.metrics().apply('driverlessBuildBytes').value() == 0
                assert exchange.metrics().apply('driverlessStageFallbacks').value() == 0
                assert exchange.metrics().apply('collectTime').value() > 0
        else:
            assert len(native) == 2
            shuffle_ids = set()
            for join in native:
                build = join.buildPlan()
                if build.getClass().getSimpleName() == 'ReusedExchangeExec':
                    build = build.child()
                shuffle_ids.add(build.shuffleId())
                assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0
            assert len(shuffle_ids) == 1
            builds = sum(join.metrics().apply('executorBuilds').value() for join in native)
            executors = max(1, get_spark_i_know_what_i_am_doing().sparkContext
                            ._jsc.sc().getExecutorMemoryStatus().size() - 1)
            assert 0 < builds <= executors

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe,
                                  'spark.sql.exchange.reuse': 'true'}),
        gpu_plan_assertion=check_reuse, require_non_empty=True)


def _rows(frame):
    return sorted(tuple(row) for row in frame.collect())


@allow_non_gpu('BatchEvalPythonExec', 'PythonUDF')
def test_driverless_broadcast_independent_builds_overlap():
    _require_ucx()

    def query(spark, slow):
        @udf(LongType())
        def payload(value):
            if slow:
                time.sleep(2)
            return value

        probe = spark.range(32, numPartitions=4).selectExpr('id AS probe', 'id % 4 AS k')
        first = spark.range(1, numPartitions=1).selectExpr('id AS k') \
            .withColumn('first', payload(col('k')))
        second = spark.range(1, numPartitions=1).selectExpr('id AS k') \
            .withColumn('second', payload(col('k') + 1))
        return probe.join(broadcast(first), 'k').join(broadcast(second), 'k') \
            .select('probe', 'first', 'second')

    expected = with_cpu_session(lambda session: _rows(query(session, False)))

    def check(session):
        frame = query(session, True)
        rows = list(session.sparkContext._jvm.org.apache.spark.sql.rapids.testing
                    .ExecutorBroadcastFaultSupport.collectWithParallelBuilds(
                        session._jsparkSession, frame._jdf))
        assert sorted(tuple(int(value) for value in row.split(':')) for row in rows) == expected
        joins = [node for node in _executed_nodes(frame._jdf.queryExecution().executedPlan())
                 if node.getClass().getSimpleName() == 'GpuDriverlessBroadcastHashJoinExec']
        assert len(joins) == 2
        assert all(join.metrics().apply('executorBuilds').value() > 0 for join in joins)

    with_gpu_session(check, conf=_conf)


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_node_limit_keeps_standard_path(aqe):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    hosts = spark.sparkContext._jsc.sc().env().blockManager().master().getMemoryStatus() \
        .keysIterator()
    workers = set()
    while hosts.hasNext():
        block_manager = hosts.next()
        if block_manager.executorId() != 'driver':
            workers.add(block_manager.host())
    if len(workers) < 2:
        pytest.skip('Node limit test requires executors on at least two distinct hosts')

    def check_standard(cpu_plan, gpu_plan):
        nodes = _executed_nodes(gpu_plan)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert exchange.metrics().apply('driverlessBuildBytes').value() == 0
        assert exchange.metrics().apply('driverlessStageFallbacks').value() == 0
        assert exchange.metrics().apply('dataSize').value() > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda session: _join(session, 'inner', 'right'),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe,
                           'spark.rapids.sql.driverlessBroadcast.maxNodes': '1'}),
        gpu_plan_assertion=check_standard, require_non_empty=True)


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_unrelated_subquery_does_not_disable_join(aqe):
    _require_ucx()

    def query(spark):
        joined = _lifecycle_query(spark)
        unrelated = spark.range(1).selectExpr(
            '(SELECT max(id) FROM range(3)) AS v', '0L AS b')
        return joined.union(unrelated)

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@allow_non_gpu('BatchEvalPythonExec', 'PythonUDF')
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_recovers_after_probe_task_retry(aqe):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    # A local master without an explicit failure allowance aborts after the first failure.
    if spark.sparkContext.master.startswith('local') and \
            not spark.sparkContext.master.startswith('local['):
        pytest.skip('Task retry requires a master with multiple task attempts')
    if spark.sparkContext.master.startswith('local[') and \
            ',' not in spark.sparkContext.master:
        pytest.skip('Use local[threads,failures] or the shuffle CI standalone cluster')
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    @udf(LongType())
    def fail_once(value):
        if value == 0 and TaskContext.get().attemptNumber() == 0:
            raise RuntimeError('EXPECTED_DRIVERLESS_BROADCAST_PROBE_RETRY')
        return value

    def check(session):
        frame = _lifecycle_query(session, fail_once)
        assert _rows(frame) == expected
        join = _native_join(frame._jdf.queryExecution().executedPlan())
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0
        assert _rows(_lifecycle_query(session)) == expected

    with_gpu_session(check, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}))


@allow_non_gpu('BatchEvalPythonExec', 'PythonUDF')
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('enabled', ['false', 'true'])
def test_driverless_broadcast_cancellation_releases_query_and_allows_next_query(aqe, enabled):
    _require_ucx()
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    @udf(LongType())
    def slow_probe(value):
        if value == 0:
            time.sleep(45)
        return value

    def check(session):
        sc = session.sparkContext
        group = 'driverless-broadcast-cancellation'
        frame = _lifecycle_query(session, slow_probe)
        outcome = []

        def collect():
            sc.setJobGroup(group, 'Cancel broadcast join with an active probe task', True)
            try:
                outcome.append(('success', frame.collect()))
            except Exception as error:
                outcome.append(('failure', str(error)))
            finally:
                sc.setLocalProperty('spark.jobGroup.id', None)
                sc.setLocalProperty('spark.job.interruptOnCancel', None)

        worker = InheritableThread(target=collect, daemon=True)
        worker.start()
        try:
            deadline = time.monotonic() + 40
            reached = False
            while time.monotonic() < deadline:
                plan = frame._jdf.queryExecution().executedPlan()
                joins = [node for node in _executed_nodes(plan)
                         if node.getClass().getSimpleName() == (
                             'GpuDriverlessBroadcastHashJoinExec' if enabled == 'true'
                             else 'GpuBroadcastHashJoinExec')]
                active = sum(info.numRunningTasks()
                             for info in sc._jsc.sc().statusTracker().getExecutorInfos())
                metric = 'executorBuilds' if enabled == 'true' else None
                built = len(joins) == 1 and (
                    joins[0].metrics().apply(metric).value() > 0 if metric else
                    joins[0].broadcastExchange().metrics().apply('dataSize').value() > 0)
                if built and active > 0:
                    reached = True
                    break
                assert worker.is_alive(), outcome
                time.sleep(0.1)
            assert reached, 'Cancellation must reach a built join with an active task'
            sc.cancelJobGroup(group)
            worker.join(30)
            assert not worker.is_alive(), 'Cancellation did not release the query'
            assert len(outcome) == 1 and outcome[0][0] == 'failure', outcome
            message = outcome[0][1].lower()
            assert 'cancel' in message or 'interrupt' in message, message
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if not any(info.numRunningTasks() for info in
                           sc._jsc.sc().statusTracker().getExecutorInfos()):
                    break
                time.sleep(0.1)
            assert not any(info.numRunningTasks() for info in
                           sc._jsc.sc().statusTracker().getExecutorInfos())
            assert _rows(_lifecycle_query(session)) == expected
        finally:
            if worker.is_alive():
                sc.cancelJobGroup(group)
                worker.join(30)

    with_gpu_session(check, conf=dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.rapids.sql.driverlessBroadcast.enabled': enabled}))


def test_driverless_broadcast_missing_build_output_is_recomputed():
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    if executors != 2:
        pytest.skip('Missing-output injection requires the two-executor shuffle CI cluster')
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    def check(session):
        frame = _lifecycle_query(session)
        recovered = list(session.sparkContext._jvm.org.apache.spark.sql.rapids.testing
                         .ExecutorBroadcastFaultSupport.collectAfterRemovingBuildOutputs(
                             session._jsparkSession, frame._jdf,
                             session.sparkContext.defaultParallelism))
        assert sorted(tuple(int(value) for value in row.split(':'))
                      for row in recovered[1:]) == expected
        join = _native_join(frame._jdf.queryExecution().executedPlan())
        assert join.metrics().apply('executorBuilds').value() > 0
        assert join.fallbackBroadcast().metrics().apply('collectTime').value() == 0

    with_gpu_session(check, conf=_conf)


def test_driverless_broadcast_can_be_enabled_after_application_startup():
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    assert spark.sparkContext.getConf().get(
        'spark.rapids.sql.driverlessBroadcast.enabled', 'false').lower() == 'false', \
        'Run shuffle CI with the default startup setting to test first-use initialization'
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))
    for enabled in ['false', 'true', 'false', 'true']:
        def check(session):
            frame = _lifecycle_query(session)
            assert _rows(frame) == expected
            nodes = _executed_nodes(frame._jdf.queryExecution().executedPlan())
            driverless = [node for node in nodes if node.getClass().getSimpleName() ==
                          'GpuDriverlessBroadcastHashJoinExec']
            if enabled == 'true':
                assert len(driverless) == 1
                assert driverless[0].metrics().apply('executorBuilds').value() > 0
                assert driverless[0].fallbackBroadcast().metrics().apply('collectTime').value() == 0
            else:
                assert not driverless
                assert any(node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec'
                           for node in nodes)
        with_gpu_session(check, conf=dict(_conf, **{
            'spark.rapids.sql.driverlessBroadcast.enabled': enabled}))


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_nested_build_retains_standard_path(aqe):
    _require_ucx()

    def query(spark):
        probe = spark.range(32, numPartitions=4).selectExpr('id % 4 AS key', 'id AS probe')
        build = spark.range(8, numPartitions=4).selectExpr('id % 4 AS key', 'array(id, id + 1) AS v')
        return probe.join(broadcast(build), 'key')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        exist_classes='GpuBroadcastHashJoinExec',
        non_exist_classes='GpuDriverlessBroadcastHashJoinExec', require_non_empty=True)


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('join_type', ['inner', 'left_outer', 'left_semi', 'left_anti'])
def test_driverless_broadcast_all_null_build_preserves_results(aqe, join_type):
    _require_ucx()

    def query(spark):
        probe = spark.range(32, numPartitions=4).selectExpr(
            'IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 4) AS pk', 'id AS probe')
        build = spark.range(8, numPartitions=4).selectExpr(
            'IF(id >= 0, CAST(NULL AS BIGINT), id) AS bk', 'id AS build')
        return probe.join(broadcast(build), probe.pk == build.bk, join_type)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}))
