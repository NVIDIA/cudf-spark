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


def query(spark, kind, side):
    left = spark.range(128, numPartitions=8).selectExpr(
        'IF(id % 13 = 0, CAST(NULL AS BIGINT), id % 11) AS left_key', 'id AS left_value')
    right = spark.range(24, numPartitions=4).selectExpr(
        'IF(id % 7 = 0, CAST(NULL AS BIGINT), id % 11) AS right_key', 'id AS right_value')
    if side == 'left':
        left = broadcast(left)
    else:
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
        cases = [('inner', 'left'), ('inner', 'right'), ('left_outer', 'right'),
                 ('right_outer', 'left'), ('left_semi', 'right'), ('left_anti', 'right')]
        for kind, side in cases:
            spark.conf.set('spark.rapids.sql.enabled', 'false')
            expected = sorted(repr(row) for row in query(spark, kind, side).collect())
            assert expected, (kind, side)
            spark.conf.set('spark.rapids.sql.enabled', 'true')
            for mode, ceiling in [('disabled', '1g'), ('native', '1g'), ('oversized', '1')]:
                spark.conf.set('spark.rapids.shuffle.broadcast.enabled', str(mode != 'disabled'))
                spark.conf.set('spark.rapids.shuffle.broadcast.maxSize', ceiling)
                frame = query(spark, kind, side)
                actual = sorted(repr(row) for row in frame.collect())
                assert actual == expected, (kind, side, mode, len(expected), len(actual))
                plan = frame._jdf.queryExecution().executedPlan()
                native = [node for node in plan_nodes(plan)
                          if node.getClass().getSimpleName() == 'GpuShuffleBroadcastHashJoinExec']
                builds, fallbacks = 0, 0
                if mode == 'disabled':
                    assert not native, plan.toString()
                    assert any(node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec'
                               for node in plan_nodes(plan)), plan.toString()
                else:
                    assert len(native) == 1, plan.toString()
                    builds = native[0].metrics().apply('executorBuilds').value()
                    fallbacks = native[0].metrics().apply('runtimeFallbacks').value()
                    assert (builds, fallbacks) == ((1, 0) if mode == 'native' else (0, 1)), \
                        (kind, side, mode, builds, fallbacks)
                print(json.dumps(dict(kind=kind, side=side, mode=mode, rows=len(actual),
                                      builds=builds, fallbacks=fallbacks, result='PASS')), flush=True)
        print('EXECUTOR_BROADCAST_SMOKE_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
