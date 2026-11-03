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

"""GPU broadcast smoke checks; launch with spark-submit and the UCX shuffle manager.

With two executors, checks include remote UCX reads and one cached build per executor.
This is a synthetic correctness check, not the full qualification workload.
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


def plan_nodes(plan):
    yield plan
    children = plan.children().iterator()
    while children.hasNext():
        yield from plan_nodes(children.next())


def query(spark, kind, side, empty_build=False):
    left = spark.range(128, numPartitions=8).selectExpr(
        'IF(id % 13 = 0, CAST(NULL AS BIGINT), id % 11) AS left_key', 'id AS left_value',
        "concat('left-', CAST(id AS STRING)) AS left_payload",
        'CAST(id AS DECIMAL(12, 2)) AS left_decimal')
    right = spark.range(24, numPartitions=4).selectExpr(
        'IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 11) AS right_key', 'id AS right_value',
        "concat('right-', CAST(id AS STRING)) AS right_payload")
    if side == 'left':
        if empty_build:
            left = left.where("sha2(CAST(left_value AS STRING), 256) = repeat('0', 64)")
        left = broadcast(left)
    else:
        if empty_build:
            right = right.where("sha2(CAST(right_value AS STRING), 256) = repeat('0', 64)")
        right = broadcast(right)
    return left.join(right,
        (left.left_key == right.right_key) & (left.left_value > right.right_value), kind)



def check_conservative_admission(spark, expected_builds):
    def growth_query():
        build = spark.range(32768, numPartitions=1).selectExpr('CAST(id % 2 AS TINYINT) AS b')
        null_row = spark.range(1, numPartitions=1).selectExpr('CAST(NULL AS TINYINT) AS b')
        build = build.unionAll(null_row)
        stream = spark.range(3, numPartitions=8).selectExpr(
            'IF(id = 2, CAST(NULL AS TINYINT), CAST(id AS TINYINT)) AS s')
        return stream.join(broadcast(build), stream.s.eqNullSafe(build.b)).select('s', 'b')

    spark.conf.set('spark.rapids.sql.enabled', 'false')
    expected = sorted(repr(row) for row in growth_query().collect())
    assert len(expected) == 32769
    spark.conf.set('spark.rapids.sql.enabled', 'true')
    spark.conf.set('spark.rapids.sql.driverlessBroadcast.enabled', 'true')
    fallback_ceiling = None
    for reject in [False, True]:
        ceiling = str(fallback_ceiling) if reject else '1g'
        spark.conf.set('spark.rapids.sql.driverlessBroadcast.maxSize', ceiling)
        frame = growth_query()
        assert sorted(repr(row) for row in frame.collect()) == expected
        native = [node for node in plan_nodes(frame._jdf.queryExecution().executedPlan())
                  if node.getClass().getSimpleName() == 'GpuDriverlessBroadcastHashJoinExec']
        assert len(native) == 1, frame._jdf.queryExecution().executedPlan().toString()
        builds = native[0].metrics().apply('executorBuilds').value()
        fallbacks = native[0].metrics().apply('runtimeFallbacks').value()
        driver_bytes = native[0].fallbackBroadcast().metrics().apply('dataSize').value()
        packed_bytes = native[0].buildPlan().metrics().apply('dataSize').value()
        if reject:
            assert packed_bytes <= int(ceiling), (packed_bytes, ceiling)
            assert builds == 0 and fallbacks == 1 and driver_bytes > 0
        else:
            assert builds == expected_builds and fallbacks == 0 and driver_bytes == 0
            tracker = spark._jvm.org.apache.spark.SparkEnv.get().mapOutputTracker()
            dependency = native[0].buildPlan().shuffleDependencyColumnar()
            reported = tracker.getStatistics(dependency).bytesByPartitionId()
            fallback_ceiling = max(sum(int(value) for value in reported), packed_bytes)
            assert fallback_ceiling > 0
        print(json.dumps(dict(case='concatenation-bound', ceiling=ceiling, result='PASS',
                              rows=len(expected), packed_bytes=packed_bytes,
                              builds=builds, fallbacks=fallbacks, driver_bytes=driver_bytes)),
              flush=True)

    for payload in ["named_struct('value', id) AS payload", 'CAST(id AS BINARY) AS payload']:
        def unsupported_query():
            build = spark.range(4, numPartitions=1).selectExpr('id AS key', payload)
            stream = spark.range(8, numPartitions=2).selectExpr('id % 4 AS probe')
            return stream.join(broadcast(build), stream.probe == build.key)
        spark.conf.set('spark.rapids.sql.enabled', 'false')
        expected = sorted(repr(row) for row in unsupported_query().collect())
        spark.conf.set('spark.rapids.sql.enabled', 'true')
        spark.conf.set('spark.rapids.sql.driverlessBroadcast.maxSize', '1g')
        frame = unsupported_query()
        assert sorted(repr(row) for row in frame.collect()) == expected
        names = [node.getClass().getSimpleName()
                 for node in plan_nodes(frame._jdf.queryExecution().executedPlan())]
        assert 'GpuDriverlessBroadcastHashJoinExec' not in names, names
        assert 'GpuBroadcastHashJoinExec' in names, names
        print(json.dumps(dict(case='unsupported-build-schema', payload=payload, result='PASS')),
              flush=True)


def main():
    spark = SparkSession.builder.appName('executor-broadcast-smoke').getOrCreate()
    try:
        for key, value in {
            'spark.sql.adaptive.enabled': 'false',
            'spark.sql.exchange.reuse': 'false',
            'spark.rapids.sql.join.hashTable.reuse': 'false',
        }.items():
            spark.conf.set(key, value)
        startup_enabled = spark.sparkContext.getConf().get(
            'spark.rapids.sql.driverlessBroadcast.enabled', 'false').lower() == 'true'
        expected_builds = int(spark.sparkContext.getConf().get('spark.executor.instances', '1'))
        if not startup_enabled:
            spark.conf.set('spark.rapids.sql.enabled', 'false')
            expected = sorted(repr(row) for row in query(spark, 'inner', 'right').collect())
            spark.conf.set('spark.rapids.sql.enabled', 'true')
            for enabled in ['false', 'true']:
                spark.conf.set('spark.rapids.sql.driverlessBroadcast.enabled', enabled)
                frame = query(spark, 'inner', 'right')
                assert sorted(repr(row) for row in frame.collect()) == expected
                plan = frame._jdf.queryExecution().executedPlan()
                names = [node.getClass().getSimpleName() for node in plan_nodes(plan)]
                assert 'GpuDriverlessBroadcastHashJoinExec' not in names, plan.toString()
                assert 'GpuBroadcastHashJoinExec' in names, plan.toString()
            print('EXECUTOR_BROADCAST_DISABLED_APP_PASS', flush=True)
            return
        cases = [('inner', 'left', False), ('inner', 'right', False),
                 ('left_outer', 'right', False), ('right_outer', 'left', False),
                 ('left_semi', 'right', False), ('left_anti', 'right', False),
                 ('inner', 'right', True), ('left_anti', 'right', True)]
        for kind, side, empty_build in cases:
            spark.conf.set('spark.rapids.sql.enabled', 'false')
            expected = sorted(repr(row) for row in query(spark, kind, side, empty_build).collect())
            assert expected or empty_build, (kind, side)
            spark.conf.set('spark.rapids.sql.enabled', 'true')
            modes = [('disabled', '1g'), ('native', '1g')]
            if not empty_build:
                modes.append(('oversized', '1'))
            for mode, ceiling in modes:
                spark.conf.set('spark.rapids.sql.driverlessBroadcast.enabled', str(mode != 'disabled'))
                spark.conf.set('spark.rapids.sql.driverlessBroadcast.maxSize', ceiling)
                frame = query(spark, kind, side, empty_build)
                actual = sorted(repr(row) for row in frame.collect())
                assert actual == expected, (kind, side, mode, len(expected), len(actual))
                plan = frame._jdf.queryExecution().executedPlan()
                native = [node for node in plan_nodes(plan)
                          if node.getClass().getSimpleName() == 'GpuDriverlessBroadcastHashJoinExec']
                builds, fallbacks = 0, 0
                driver_bytes = 0
                remote_build_bytes = 0
                if mode == 'disabled':
                    assert not native, plan.toString()
                    standard = [node for node in plan_nodes(plan)
                                if node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec']
                    assert len(standard) == 1, plan.toString()
                    driver_bytes = standard[0].broadcastExchange().metrics().apply('dataSize').value()
                else:
                    assert len(native) == 1, plan.toString()
                    builds = native[0].metrics().apply('executorBuilds').value()
                    fallbacks = native[0].metrics().apply('runtimeFallbacks').value()
                    driver_bytes = native[0].fallbackBroadcast().metrics().apply('dataSize').value()
                    assert (builds, fallbacks) == \
                        ((expected_builds, 0) if mode == 'native' else (0, 1)), \
                        (kind, side, mode, builds, fallbacks)
                    assert (driver_bytes == 0) if mode == 'native' else (driver_bytes > 0), \
                        (kind, side, mode, driver_bytes)
                    if mode == 'native':
                        build = getattr(native[0], side)()
                        remote_build_bytes = build.metrics().apply('remoteBytesRead').value()
                        if expected_builds > 1 and not empty_build:
                            assert remote_build_bytes > 0, (kind, side, remote_build_bytes)
                print(json.dumps(dict(kind=kind, side=side, empty_build=empty_build,
                                      mode=mode, rows=len(actual),
                                      builds=builds, fallbacks=fallbacks,
                                      remote_build_bytes=remote_build_bytes,
                                      driver_broadcast_bytes=driver_bytes, result='PASS')), flush=True)
        check_conservative_admission(spark, expected_builds)
        print('EXECUTOR_BROADCAST_SMOKE_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
