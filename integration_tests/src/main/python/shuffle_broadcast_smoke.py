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

"""Single-executor smoke check; launch with spark-submit and the UCX shuffle manager.

This does not validate cross-executor UCX transfers or the full qualification workload.
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
        'IF(id % 13 = 0, CAST(NULL AS BIGINT), id % 11) AS left_key', 'id AS left_value')
    right = spark.range(24, numPartitions=4).selectExpr(
        'IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 11) AS right_key', 'id AS right_value')
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


def main():
    spark = SparkSession.builder.appName('executor-broadcast-smoke').getOrCreate()
    try:
        for key, value in {
            'spark.sql.adaptive.enabled': 'false',
            'spark.sql.exchange.reuse': 'false',
            'spark.rapids.sql.join.hashTable.reuse': 'false',
        }.items():
            spark.conf.set(key, value)
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
                spark.conf.set('spark.rapids.shuffle.broadcast.enabled', str(mode != 'disabled'))
                spark.conf.set('spark.rapids.shuffle.broadcast.maxSize', ceiling)
                frame = query(spark, kind, side, empty_build)
                actual = sorted(repr(row) for row in frame.collect())
                assert actual == expected, (kind, side, mode, len(expected), len(actual))
                plan = frame._jdf.queryExecution().executedPlan()
                native = [node for node in plan_nodes(plan)
                          if node.getClass().getSimpleName() == 'GpuShuffleBroadcastHashJoinExec']
                builds, fallbacks = 0, 0
                driver_bytes = 0
                if mode == 'disabled':
                    assert not native, plan.toString()
                    assert any(node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec'
                               for node in plan_nodes(plan)), plan.toString()
                else:
                    assert len(native) == 1, plan.toString()
                    builds = native[0].metrics().apply('executorBuilds').value()
                    fallbacks = native[0].metrics().apply('runtimeFallbacks').value()
                    driver_bytes = native[0].fallbackBroadcast().metrics().apply('dataSize').value()
                    assert (builds, fallbacks) == ((1, 0) if mode == 'native' else (0, 1)), \
                        (kind, side, mode, builds, fallbacks)
                    assert (driver_bytes == 0) if mode == 'native' else (driver_bytes > 0), \
                        (kind, side, mode, driver_bytes)
                print(json.dumps(dict(kind=kind, side=side, empty_build=empty_build,
                                      mode=mode, rows=len(actual),
                                      builds=builds, fallbacks=fallbacks,
                                      driver_broadcast_bytes=driver_bytes, result='PASS')), flush=True)
        print('EXECUTOR_BROADCAST_SMOKE_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
