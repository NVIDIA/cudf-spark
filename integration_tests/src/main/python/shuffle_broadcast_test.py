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

import os
import re

import pytest
from pyspark import InheritableThread, TaskContext
from pyspark.sql.functions import broadcast, col, expr, udf
from pyspark.sql.types import LongType

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture, assert_metric_unset_or_zero,
                     collect_plan_nodes, plan_metric)
from marks import allow_non_gpu, ignore_order, shuffle_test
from spark_init_internal import get_spark_i_know_what_i_am_doing
from spark_session import is_spark_400_or_later, with_cpu_session, with_gpu_session


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



def _native_join(plan):
    joins = [node for node in collect_plan_nodes(plan, include_wrapped=True)
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
    formatter = (spark._jvm.org.apache.spark.util.MetricUtils if is_spark_400_or_later()
                 else spark._jvm.org.apache.spark.sql.execution.metric.SQLMetrics)
    expected = formatter.stringValue(
        metric.metricType(), values, gateway.new_array(spark._jvm.long, 0))
    assert _published_values(metric) == [expected]


def _assert_aqe_build_metric_owner(join):
    build = join.buildPlan()
    assert build.getClass().getSimpleName() == 'ReusedExchangeExec'
    producer = join.fallbackBroadcast().admittedDriverlessShuffle().get()
    read_metric = producer.metrics().apply('opTimeNewShuffleRead')
    assert build.child().metrics().apply('opTimeNewShuffleRead').id() == read_metric.id()
    assert join.getDescendantOpTimeMetrics().contains(
        producer.gpuLongMetric('opTimeNewShuffleRead'))
    assert read_metric.value() > 0
    assert _published_values(read_metric), 'Executed build read metric is missing from the SQL UI'


_conf = {
    'spark.sql.adaptive.enabled': 'false',
    'spark.rapids.sql.metrics.level': 'DEBUG',
    'spark.rapids.sql.driverlessBroadcast.enabled': 'true',
    'spark.rapids.sql.join.hashTable.reuse': 'false',
    'spark.sql.shuffle.orderIndependentChecksum.enabled': 'false',
    'spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch': 'false',
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
        assert plan_metric(join, 'executorBuilds') > 0
        assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')

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
        assert plan_metric(join, 'executorBuilds') > 0
        exchange = join.fallbackBroadcast()
        assert_metric_unset_or_zero(exchange, 'collectTime')
        if aqe == 'true':
            assert plan_metric(join, 'buildStageBytes') > 0
            assert plan_metric(join, 'buildStageTime') > 0
            assert plan_metric(exchange, 'driverlessBuildBytes') > 0
            if join_type == 'inner' and build_side == 'right':
                _assert_published_driver_value(join.metrics().apply('buildStageBytes'))
                _assert_published_driver_value(join.metrics().apply('buildStageTime'))
                assert _published_values(exchange.metrics().apply('driverlessStageTime'))
        else:
            assert not join.metrics().contains('buildStageTime')
            assert not join.metrics().contains('buildStageBytes')
            assert_metric_unset_or_zero(exchange, 'dataSize')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, join_type, build_side),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@ignore_order(local=True)
def test_executor_broadcast_disabled_keeps_standard_broadcast():
    def check_standard(cpu_plan, gpu_plan):
        exchanges = [node for node in collect_plan_nodes(gpu_plan, include_wrapped=True)
                     if node.getClass().getSimpleName() == 'GpuBroadcastExchangeExec']
        assert exchanges, gpu_plan.toString()
        for exchange in exchanges:
            assert exchange.metrics().contains('collectTime')
            for name in ('driverlessBuildBytes', 'fallbackBroadcastBytes',
                         'driverlessStageTime', 'driverlessStageFallbacks'):
                assert not exchange.metrics().contains(name), name

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _join(spark, 'inner', 'right'),
        conf=dict(_conf, **{'spark.rapids.sql.driverlessBroadcast.enabled': 'false'}),
        exist_classes='GpuBroadcastHashJoinExec',
        non_exist_classes='GpuDriverlessBroadcastHashJoinExec', require_non_empty=True,
        gpu_plan_assertion=check_standard)


@ignore_order(local=True)
def test_executor_broadcast_with_infinite_timeout():
    _require_ucx()

    def check_native(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert plan_metric(join, 'executorBuilds') > 0

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
                   for node in collect_plan_nodes(cpu_plan, include_wrapped=True)), cpu_plan.toString()
        join = _native_join(gpu_plan)
        assert plan_metric(join, 'executorBuilds') > 0
        assert plan_metric(join.fallbackBroadcast(), 'driverlessBuildBytes') > 0
        assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')

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
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes
                 if node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert plan_metric(exchange, 'driverlessBuildBytes') == 0
        assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
        assert plan_metric(exchange, 'fallbackBroadcastBytes') == 0
        assert plan_metric(exchange, 'dataSize') > 0
        assert plan_metric(exchange, 'collectTime') > 0

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
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert plan_metric(exchange, 'driverlessBuildBytes') == 0
        assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
        assert plan_metric(exchange, 'dataSize') > 0

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
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        native = [node for node in nodes if node.getClass().getSimpleName() ==
                  'GpuDriverlessBroadcastHashJoinExec']
        assert len(native) == 2
        shuffle_ids = set()
        exchanges = {}
        for join in native:
            build = join.buildPlan()
            if build.getClass().getSimpleName() == 'ReusedExchangeExec':
                build = build.child()
            shuffle_ids.add(build.shuffleId())
            exchange = join.fallbackBroadcast()
            exchanges[exchange.id()] = exchange
            assert_metric_unset_or_zero(exchange, 'collectTime')
            assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
        assert len(shuffle_ids) == 1
        if aqe == 'true':
            assert len(exchanges) == 1
            assert plan_metric(next(iter(exchanges.values())), 'driverlessBuildBytes') > 0
        builds = sum(plan_metric(join, 'executorBuilds') for join in native)
        executors = max(1, get_spark_i_know_what_i_am_doing().sparkContext
                        ._jsc.sc().getExecutorMemoryStatus().size() - 1)
        assert 0 < builds <= executors

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe,
                                  'spark.sql.exchange.reuse': 'true'}),
        gpu_plan_assertion=check_reuse, require_non_empty=True)


@ignore_order(local=True)
@allow_non_gpu('BroadcastExchangeExec', 'BroadcastHashJoinExec', 'RLike', 'ProjectExec',
               'RangeExec', 'WholeStageCodegenExec', 'InputAdapter')
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('cpu_build', [False, True])
def test_driverless_broadcast_with_cpu_join_consumers(aqe, cpu_build):
    _require_ucx()

    def query(spark, include_cpu=True):
        sql = """
            WITH d AS (
                SELECT id AS k, id AS b, IF(id % 2 = 0, '0', '1') AS pattern
                FROM range(0, 8, 1, 4)
            )
            SELECT /*+ BROADCAST(d) */ f.id AS p, d.b, d.pattern
            FROM range(0, 128, 1, 8) f JOIN d ON f.id % 8 = d.k
        """
        if include_cpu:
            sql += """
                UNION ALL
                SELECT /*+ BROADCAST(d) */ f.id + 1000 AS p, d.b, d.pattern
                FROM range(0, 128, 1, 8) f LEFT JOIN d
                ON f.id % 8 = d.k AND CAST(f.id % 3 AS STRING) RLIKE d.pattern
            """
        return spark.sql(sql)

    conf = dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.sql.exchange.reuse': 'true',
        'spark.rapids.sql.expression.RLike': 'false'})
    if cpu_build:
        # Keep producer plans on CPU so the CPU exchange preserves the same canonical build.
        conf.update({'spark.rapids.sql.exec.ProjectExec': 'false',
                     'spark.rapids.sql.exec.RangeExec': 'false'})

    def check_eligible_control(cpu_plan, gpu_plan):
        join = _native_join(gpu_plan)
        assert plan_metric(join, 'executorBuilds') > 0
        if aqe == 'true':
            _assert_aqe_build_metric_owner(join)

    expected = with_cpu_session(lambda spark: _rows(query(spark, False)))
    standard_conf = dict(conf, **{'spark.rapids.sql.driverlessBroadcast.enabled': 'false'})
    assert with_gpu_session(lambda spark: _rows(query(spark, False)), conf=standard_conf) == expected
    try:
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: query(spark, False), conf=conf,
            gpu_plan_assertion=check_eligible_control, require_non_empty=True)
    except Exception as error:
        if hasattr(error, 'stackTrace'):
            raise AssertionError(error.stackTrace) from error
        raise

    def check_consumers(cpu_plan, gpu_plan):
        assert 'ReusedExchange' in cpu_plan.toString(), cpu_plan.toString()
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        names = [node.getClass().getSimpleName() for node in nodes]
        assert 'BroadcastHashJoinExec' in names, gpu_plan.toString()
        if cpu_build:
            assert 'GpuBroadcastHashJoinExec' in names, gpu_plan.toString()
            assert 'GpuDriverlessBroadcastHashJoinExec' not in names, gpu_plan.toString()
            exchanges = [node for node in nodes
                         if node.getClass().getSimpleName() == 'GpuBroadcastExchangeExec']
            assert exchanges, gpu_plan.toString()
            for exchange in exchanges:
                assert plan_metric(exchange, 'driverlessBuildBytes') == 0
                assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
                assert plan_metric(exchange, 'dataSize') > 0
        else:
            # GPU conversion separates the CPU and GPU build representations.
            join = _native_join(gpu_plan)
            assert plan_metric(join, 'executorBuilds') > 0
            assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=conf, gpu_plan_assertion=check_consumers, require_non_empty=True)


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('checksum_key', [
    'spark.sql.shuffle.orderIndependentChecksum.enabled',
    'spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch'])
def test_driverless_broadcast_row_checksum_keeps_standard_path(aqe, checksum_key):
    _require_ucx()

    def check_standard(cpu_plan, gpu_plan):
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1, gpu_plan.toString()
        exchange = joins[0].broadcastExchange()
        assert plan_metric(exchange, 'driverlessBuildBytes') == 0
        assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
        assert plan_metric(exchange, 'dataSize') > 0

    conf = dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.sql.shuffle.orderIndependentChecksum.enabled': 'false',
        'spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch': 'false'})
    conf[checksum_key] = 'true'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda session: _join(session, 'inner', 'right'), conf=conf,
        gpu_plan_assertion=check_standard, require_non_empty=True)


def _rows(frame):
    return sorted(tuple(row) for row in frame.collect())


@allow_non_gpu('ProjectExec', 'RangeExec', 'ScalaUDF')
def test_driverless_broadcast_independent_builds_overlap():
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    startup = spark.sparkContext.getConf()
    if spark.sparkContext._jsc.sc().isLocal():
        helper = spark.sparkContext._jvm.org.apache.spark.sql.rapids.testing.ExecutorBroadcastFaultSupport
        slots = helper.localTaskSlots(spark._jsparkSession)
    else:
        cores = spark.sparkContext.defaultParallelism
        configured_max = int(startup.get('spark.cores.max', '0'))
        if configured_max > 0:
            cores = min(cores, configured_max)
        slots = cores // int(startup.get('spark.task.cpus', '1'))
    required = os.environ.get('REQUIRE_DRIVERLESS_PARALLEL_BUILD_TEST') == '1'
    if slots < 2:
        reason = 'Parallel-build overlap requires at least two CPU task slots'
        if required:
            pytest.fail(reason + '; the UCX CI environment must provide them')
        pytest.skip(reason)

    # Executor readiness failures are environment errors, not evidence of serial builds.
    # The Scala helper owns the temporary UDF and bounds its two-build handshake.
    def query(function):
        return f"""SELECT /*+ BROADCAST(a), BROADCAST(b) */ p.probe, a.first, b.second
            FROM (SELECT id AS probe, id % 4 AS k FROM range(0, 32, 1, 4)) p
            JOIN (SELECT id AS k, {function}(id) AS first FROM range(0, 1, 1, 1)) a
              ON p.k = a.k
            JOIN (SELECT id AS k, {function}(id + 1) AS second FROM range(0, 1, 1, 1)) b
              ON p.k = b.k"""

    expected = with_cpu_session(lambda session: _rows(session.sql(query('abs'))))

    def check(session):
        helper = session.sparkContext._jvm.org.apache.spark.sql.rapids.testing.ExecutorBroadcastFaultSupport
        rows = list(helper.collectWithParallelBuilds(
            session._jsparkSession, query('driverless_parallel_build_gate')))
        assert sorted(tuple(int(value) for value in row.split(':')) for row in rows) == expected

    # Keep the blocking handshake on CPUs before the first GPU semaphore acquisition.
    with_gpu_session(check, conf=dict(_conf, **{
        'spark.rapids.sql.exec.RangeExec': 'false',
        'spark.rapids.sql.expression.cpuBridge.enabled': 'false'}))


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_executor_limit_keeps_standard_path(aqe):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    executors = spark.sparkContext._jsc.sc().env().blockManager().master().getMemoryStatus() \
        .keysIterator()
    workers = set()
    while executors.hasNext():
        block_manager = executors.next()
        if block_manager.executorId() != 'driver':
            workers.add(block_manager.executorId())
    if len(workers) < 2:
        pytest.skip('Executor limit test requires at least two executors, including on the same host')

    def check_standard(cpu_plan, gpu_plan):
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1
        exchange = joins[0].broadcastExchange()
        assert plan_metric(exchange, 'driverlessBuildBytes') == 0
        assert plan_metric(exchange, 'driverlessStageFallbacks') == 0
        assert plan_metric(exchange, 'dataSize') > 0

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda session: _join(session, 'inner', 'right'),
        conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe,
                           'spark.rapids.sql.driverlessBroadcast.maxExecutors': '1'}),
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
        assert plan_metric(join, 'executorBuilds') > 0
        assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_native, require_non_empty=True)


@allow_non_gpu('BatchEvalPythonExec', 'PythonUDF')
@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_recovers_after_probe_task_retry(aqe):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    # Local schedulers take their failure allowance from local[threads,failures].
    master = spark.sparkContext.master
    if master.startswith('local') and not master.startswith('local-cluster'):
        match = re.fullmatch(r'local\[[^,\]]+,\s*(\d+)\]', master)
        failures = int(match.group(1)) if match else 1
    else:
        failures = int(spark.sparkContext.getConf().get('spark.task.maxFailures', '4'))
    assert failures >= 2, ('Probe retry requires a Spark application with at least two task '
                           'attempts; set SPARK_TASK_MAXFAILURES=2 before starting the runner '
                           'and use local[threads,2] for a local master')
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    @udf(LongType())
    def fail_once(value):
        if value == 0 and TaskContext.get().attemptNumber() == 0:
            raise RuntimeError('EXPECTED_DRIVERLESS_BROADCAST_PROBE_RETRY')
        return value

    def check(session):
        frame = _lifecycle_query(session, fail_once)
        sc = session.sparkContext
        group = 'driverless-probe-retry-' + aqe
        tracker = sc.statusTracker()
        previous_jobs = set(tracker.getJobIdsForGroup(group))
        previous_group = sc.getLocalProperty('spark.jobGroup.id')
        sc.setLocalProperty('spark.jobGroup.id', group)
        try:
            assert _rows(frame) == expected
        finally:
            sc.setLocalProperty('spark.jobGroup.id', previous_group)
        sc._jsc.sc().listenerBus().waitUntilEmpty(30000)
        current_jobs = set(tracker.getJobIdsForGroup(group)) - previous_jobs
        jobs = [tracker.getJobInfo(job) for job in current_jobs]
        stages = {stage for job in jobs if job for stage in job.stageIds}
        stage_info = [tracker.getStageInfo(stage) for stage in stages]
        assert any(info and info.numFailedTasks > 0 for info in stage_info), \
            'The injected probe failure did not exercise task retry'
        join = _native_join(frame._jdf.queryExecution().executedPlan())
        assert (plan_metric(join, 'executorBuilds') +
                plan_metric(join, 'executorBuildCacheHits')) > 0
        assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')
        assert _rows(_lifecycle_query(session)) == expected

    with_gpu_session(check, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}))


@allow_non_gpu('ProjectExec', 'RangeExec', 'ScalaUDF')
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('enabled', ['false', 'true'])
def test_driverless_broadcast_cancellation_releases_query_and_allows_next_query(aqe, enabled):
    _require_ucx()
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    def check(session):
        sc = session.sparkContext
        group = 'driverless-broadcast-cancellation'
        helper = sc._jvm.org.apache.spark.sql.rapids.testing.ExecutorBroadcastFaultSupport
        probe = helper.cancellationProbe(session._jsparkSession)
        outcome = []
        worker = None
        try:
            frame = _lifecycle_query(session, lambda value: expr(f'{probe.functionName()}(id)'))

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
            assert probe.awaitEntered(60), f'Probe task did not reach cancellation gate: {outcome}'
            plan = frame._jdf.queryExecution().executedPlan()
            joins = [node for node in collect_plan_nodes(plan, include_wrapped=True)
                     if node.getClass().getSimpleName() == (
                         'GpuDriverlessBroadcastHashJoinExec' if enabled == 'true'
                         else 'GpuBroadcastHashJoinExec')]
            assert len(joins) == 1, plan.toString()
            if enabled == 'true':
                # The builder may be the blocked task, whose metrics are not merged yet.
                # A completed cache borrower also proves that the build became usable.
                built = probe.awaitBuildMetric(
                    joins[0].metrics().apply('executorBuilds'),
                    joins[0].metrics().apply('executorBuildCacheHits'), 40)
            else:
                built = probe.awaitMetric(
                    joins[0].broadcastExchange().metrics().apply('dataSize'), 40)
            assert built, 'Cancellation must reach a built join with an active probe task'
            sc.cancelJobGroup(group)
            worker.join(30)
            assert not worker.is_alive(), 'Cancellation did not release the query'
            assert len(outcome) == 1 and outcome[0][0] == 'failure', outcome
            message = outcome[0][1].lower()
            assert 'cancel' in message or 'interrupt' in message, message
            probe.awaitIdle(20)
            assert not any(info.numRunningTasks() for info in
                           sc._jsc.sc().statusTracker().getExecutorInfos())
        finally:
            if worker is not None and worker.is_alive():
                sc.cancelJobGroup(group)
            try:
                probe.close()
            finally:
                if worker is not None and worker.is_alive():
                    worker.join(30)
        assert _rows(_lifecycle_query(session)) == expected

    # The blocking test UDF must run on CPUs, outside CPU bridge GPU expressions.
    with_gpu_session(check, conf=dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.rapids.sql.driverlessBroadcast.enabled': enabled,
        'spark.rapids.sql.exec.RangeExec': 'false',
        'spark.rapids.sql.expression.cpuBridge.enabled': 'false'}))

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
        assert plan_metric(join, 'executorBuilds') > 0
        assert_metric_unset_or_zero(join.fallbackBroadcast(), 'collectTime')

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
            nodes = collect_plan_nodes(frame._jdf.queryExecution().executedPlan(), include_wrapped=True)
            driverless = [node for node in nodes if node.getClass().getSimpleName() ==
                          'GpuDriverlessBroadcastHashJoinExec']
            if enabled == 'true':
                assert len(driverless) == 1
                assert plan_metric(driverless[0], 'executorBuilds') > 0
                assert_metric_unset_or_zero(driverless[0].fallbackBroadcast(), 'collectTime')
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
# Spark 4 AQE can replace a join with no matching keys by an empty result.
@allow_non_gpu('EmptyRelationExec')
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


@ignore_order(local=True)
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('build_has_null', [False, True])
def test_driverless_broadcast_null_aware_anti_join_keeps_standard_path(aqe, build_has_null):
    _require_ucx()

    def query(spark):
        build = ('IF(id = 0, CAST(NULL AS BIGINT), id)' if build_has_null else 'id')
        return spark.sql(f"""
            SELECT IF(id = 0, CAST(NULL AS BIGINT), id) AS key FROM range(8)
            WHERE IF(id = 0, CAST(NULL AS BIGINT), id) NOT IN
                (SELECT {build} FROM range(3))
        """)

    def check_standard(cpu_plan, gpu_plan):
        nodes = collect_plan_nodes(gpu_plan, include_wrapped=True)
        assert not any(node.getClass().getSimpleName() ==
                       'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
        joins = [node for node in nodes if node.getClass().getSimpleName() ==
                 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1 and joins[0].isNullAwareAntiJoin()

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, conf=dict(_conf, **{'spark.sql.adaptive.enabled': aqe}),
        gpu_plan_assertion=check_standard)


def _build_shuffle_id(join):
    admitted = join.fallbackBroadcast().admittedDriverlessShuffle()
    if admitted.isDefined():
        return admitted.get().shuffleId()
    build = join.buildPlan()
    while build.getClass().getSimpleName() == 'ReusedExchangeExec':
        build = build.child()
    return build.shuffleId()


@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('cleanup', ['none', 'sql_end', 'explicit'])
def test_driverless_broadcast_repeated_actions_reuse_or_rebuild_outputs(aqe, cleanup, record_property):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    if cleanup == 'sql_end' and tuple(map(int, spark.version.split('.')[:2])) < (4, 1):
        pytest.skip('SQL-end shuffle cleanup requires Spark 4.1 or later')
    expected = with_cpu_session(lambda session: _rows(_lifecycle_query(session)))

    def check(session):
        helper = session.sparkContext._jvm.org.apache.spark.sql.rapids.testing.ExecutorBroadcastFaultSupport
        frame = _lifecycle_query(session)

        def collect():
            observed = list(helper.collectWithMapAttempts(session._jsparkSession, frame._jdf))
            attempts = set(map(int, observed[0].split(','))) if observed[0] else set()
            rows = sorted(tuple(map(int, row.split(':'))) for row in observed[1:])
            assert rows == expected
            return attempts

        original_attempts = collect()
        assert original_attempts
        first = _native_join(frame._jdf.queryExecution().executedPlan())
        assert plan_metric(first, 'executorBuilds') > 0
        shuffle_id = _build_shuffle_id(first)
        if cleanup == 'explicit':
            helper.removeBuildOutputs(shuffle_id)
        current_attempts = collect()
        record_property('second_action_map_tasks', len(current_attempts))
        second = _native_join(frame._jdf.queryExecution().executedPlan())
        assert_metric_unset_or_zero(second.fallbackBroadcast(), 'collectTime')
        assert _build_shuffle_id(second) == shuffle_id
        if cleanup == 'explicit':
            assert current_attempts and current_attempts.isdisjoint(original_attempts)
            assert plan_metric(second, 'executorBuilds') > 0
        elif cleanup == 'none':
            assert not current_attempts, 'Unchanged source outputs were needlessly recomputed'
        # The SQL-end flag exercises repeated-action compatibility. Spark decides which shuffles
        # it tracks for cleanup; explicit removal above independently proves actual recomputation.
        hits = plan_metric(second, 'executorBuildCacheHits')
        assert hits + plan_metric(second, 'executorBuilds') > 0
        if cleanup == 'none' and session.sparkContext.master.startswith('local['):
            assert plan_metric(second, 'executorBuilds') == 0
            assert hits > 0

    with_gpu_session(check, conf=dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.sql.classic.shuffleDependency.fileCleanup.enabled':
            'true' if cleanup == 'sql_end' else 'false'}))


@pytest.mark.parametrize('aqe', ['false', 'true'])
def test_driverless_broadcast_respects_configured_standard_byte_limit(aqe):
    _require_ucx()
    spark = get_spark_i_know_what_i_am_doing()
    if tuple(map(int, spark.version.split('.')[:2])) < (4, 1):
        pytest.skip('Configurable broadcast byte limit requires Spark 4.1 or later')

    def check(session):
        frame = _lifecycle_query(session)
        with pytest.raises(Exception, match='Cannot broadcast the table'):
            frame.collect()
        nodes = collect_plan_nodes(frame._jdf.queryExecution().executedPlan(), include_wrapped=True)
        exchanges = [node for node in nodes if node.getClass().getSimpleName() ==
                     'GpuBroadcastExchangeExec']
        assert exchanges
        if aqe == 'true':
            assert any(plan_metric(node, 'driverlessStageTime') > 0 for node in exchanges)
            assert all(not node.admittedDriverlessShuffle().isDefined() for node in exchanges)
            for node in exchanges:
                assert_metric_unset_or_zero(node, 'collectTime')
        else:
            assert not any(node.getClass().getSimpleName() ==
                           'GpuDriverlessBroadcastHashJoinExec' for node in nodes)
            assert any(plan_metric(node, 'collectTime') > 0 for node in exchanges)

    with_gpu_session(check, conf=dict(_conf, **{
        'spark.sql.adaptive.enabled': aqe,
        'spark.sql.maxBroadcastTableSize': '128b'}))
