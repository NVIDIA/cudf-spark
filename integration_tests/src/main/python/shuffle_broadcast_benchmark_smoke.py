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

"""Controlled large-build A/B; this synthetic check is not a TPC-H suite score."""

import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


def nodes(plan):
    yield plan
    children = plan.children().iterator()
    while children.hasNext():
        yield from nodes(children.next())


def digit_sum(count):
    total = 1 if count else 0
    for digits in range(1, len(str(count)) + 1):
        total += max(0, min(count, 10 ** digits) - 10 ** (digits - 1)) * digits
    return total


def main():
    spark = SparkSession.builder.appName('executor-broadcast-large-build-ab').getOrCreate()
    try:
        build_rows = int(os.environ.get('BROADCAST_BENCH_BUILD_ROWS', str(2 ** 23)))
        probe_rows = 2 * build_rows
        expected = (
            probe_rows,
            probe_rows * (probe_rows - 1) // 2 +
            2 * (3 * build_rows * (build_rows - 1) // 2 + 7 * build_rows),
            2 * (digit_sum(build_rows) + 48 * build_rows) + digit_sum(probe_rows),
        )
        for key, value in {
            'spark.sql.adaptive.enabled': 'false',
            'spark.sql.exchange.reuse': 'false',
            'spark.rapids.sql.join.hashTable.reuse': 'false',
            'spark.rapids.shuffle.broadcast.maxSize': '1g',
            'spark.sql.ansi.enabled': 'false',
        }.items():
            spark.conf.set(key, value)
        for iteration in range(2):
            for enabled in (False, True):
                spark.conf.set('spark.rapids.shuffle.broadcast.enabled', str(enabled).lower())
                started = time.monotonic()
                probe = spark.range(probe_rows, numPartitions=32).selectExpr(
                    'id', f'id % {build_rows} AS k')
                build = spark.range(build_rows, numPartitions=16).selectExpr(
                    'id AS bk', 'id * 3 + 7 AS v',
                    "concat(CAST(id AS STRING), repeat('x', 48)) AS payload")
                frame = probe.join(broadcast(build), probe.k == build.bk).selectExpr(
                    'count(*) AS n', 'sum(id + v) AS value_sum',
                    'sum(length(concat(payload, CAST(id AS STRING)))) AS payload_sum')
                actual = tuple(frame.collect()[0])
                elapsed = time.monotonic() - started
                assert actual == expected, (actual, expected)
                name = 'GpuShuffleBroadcastHashJoinExec' if enabled else 'GpuBroadcastHashJoinExec'
                matches = [node for node in nodes(frame._jdf.queryExecution().executedPlan())
                           if node.getClass().getSimpleName() == name]
                assert len(matches) == 1, frame._jdf.queryExecution().executedPlan().toString()
                join = matches[0]
                if enabled:
                    assert join.metrics().apply('runtimeFallbacks').value() == 0
                    driver_bytes = join.fallbackBroadcast().metrics().apply('dataSize').value()
                    builds = join.metrics().apply('executorBuilds').value()
                    shuffle_bytes = join.buildPlan().metrics().apply('dataSize').value()
                    assert driver_bytes == 0 and builds == 2, (driver_bytes, builds)
                    spark.sparkContext._jsc.sc().cleaner().get() \
                        .doCleanupShuffle(join.buildPlan().shuffleId(), True)
                else:
                    driver_bytes = join.broadcastExchange().metrics().apply('dataSize').value()
                    assert driver_bytes > 0
                    builds, shuffle_bytes = 0, 0
                print(json.dumps(dict(iteration=iteration + 1, enabled=enabled,
                                      seconds=elapsed, result='PASS', build_rows=build_rows,
                                      probe_rows=probe_rows, driver_broadcast_bytes=driver_bytes,
                                      executor_builds=builds, build_shuffle_bytes=shuffle_bytes,
                                      actual=actual)), flush=True)
        print('EXECUTOR_BROADCAST_BENCHMARK_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
