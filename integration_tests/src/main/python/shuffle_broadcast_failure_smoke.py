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

"""Distributed retry, cancellation, and explicit shuffle-cleanup checks.

Launch with startup-enabled executor broadcast and two UCX executors. Python UDFs
inject stream-side failures without adding fault-injection hooks to production code.
"""

import json
import os
from pathlib import Path
import time

from pyspark import InheritableThread, TaskContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, udf
from pyspark.sql.types import LongType


def nodes(plan):
    if plan.getClass().getSimpleName() == 'AdaptiveSparkPlanExec':
        yield from nodes(plan.executedPlan())
        return
    if plan.getClass().getSimpleName().endswith('QueryStageExec'):
        yield from nodes(plan.plan())
        return
    yield plan
    children = plan.children().iterator()
    while children.hasNext():
        yield from nodes(children.next())


def query(spark, transform=None):
    stream = spark.range(2048, numPartitions=8).selectExpr('id', 'id % 16 AS k')
    if transform is not None:
        stream = stream.withColumn('v', transform(col('id')))
    else:
        stream = stream.withColumn('v', col('id'))
    build = spark.range(64, numPartitions=4).selectExpr('id AS b', 'id % 16 AS bk')
    return stream.join(broadcast(build), stream.k == build.bk).select('v', 'b')


def native_node(frame):
    plan = frame._jdf.queryExecution().executedPlan()
    matches = [node for node in nodes(plan)
               if node.getClass().getSimpleName() == 'GpuDriverlessBroadcastHashJoinExec']
    assert len(matches) == 1, plan.toString()
    return matches[0]


def cleanup(spark, node):
    shuffle_id = node.buildPlan().shuffleId()
    cleaner = spark.sparkContext._jsc.sc().cleaner()
    assert cleaner.isDefined(), 'A cleaner is required by executor broadcast'
    cleaner.get().doCleanupShuffle(shuffle_id, True)
    return shuffle_id


def running_tasks(spark):
    infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
    return sum(info.numRunningTasks() for info in infos)


def wait_idle(spark):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if running_tasks(spark) == 0:
            return
        time.sleep(0.2)
    raise AssertionError('Executors still report active tasks after query cancellation')


def main():
    spark = SparkSession.builder.appName('executor-broadcast-failures').getOrCreate()
    sc = spark.sparkContext
    root = Path(os.environ['BROADCAST_FAILURE_MARKERS'])
    root.mkdir(parents=True, exist_ok=False)
    try:
        for key, value in {
            'spark.sql.adaptive.enabled': sc.getConf().get('spark.sql.adaptive.enabled', 'false'),
            'spark.sql.exchange.reuse': 'false',
            'spark.rapids.sql.join.hashTable.reuse': 'false',
            'spark.rapids.sql.driverlessBroadcast.enabled': 'true',
            'spark.rapids.sql.driverlessBroadcast.maxSize': '1g',
        }.items():
            spark.conf.set(key, value)
        spark.conf.set('spark.rapids.sql.enabled', 'false')
        expected = sorted(tuple(row) for row in query(spark).collect())
        assert len(expected) == 8192
        spark.conf.set('spark.rapids.sql.enabled', 'true')

        frame = query(spark)
        recovered = list(sc._jvm.org.apache.spark.sql.rapids.testing.ExecutorBroadcastFaultSupport
                         .collectAfterRemovingBuildOutputs(spark._jsparkSession, frame._jdf, 8))
        native = native_node(frame)
        assert sorted(tuple(int(value) for value in row.split(':'))
                      for row in recovered[1:]) == expected
        assert native.metrics().apply('executorBuilds').value() > 0
        assert native.fallbackBroadcast().metrics().apply('collectTime').value() == 0
        print(json.dumps(dict(case='missing-build-output', injection=recovered[0],
                              result='PASS', shuffle=cleanup(spark, native))), flush=True)

        @udf(LongType())
        def fail_once(value):
            context = TaskContext.get()
            if value == 0 and context.attemptNumber() == 0:
                raise RuntimeError('EXPECTED_NATIVE_BROADCAST_STREAM_RETRY')
            return value

        frame = query(spark, fail_once)
        assert sorted(tuple(row) for row in frame.collect()) == expected
        native = native_node(frame)
        assert native.metrics().apply('executorBuilds').value() > 0
        assert native.metrics().apply('runtimeFallbacks').value() == 0
        assert native.fallbackBroadcast().metrics().apply('collectTime').value() == 0
        print(json.dumps(dict(case='stream-task-retry', result='PASS',
                              shuffle=cleanup(spark, native))), flush=True)

        marker = str(root / 'slow-task-entered')

        @udf(LongType())
        def slow_first(value):
            if value == 0:
                Path(marker).write_text('stream task is running\n')
                time.sleep(45)
            return value

        frame = query(spark, slow_first)
        native = None
        outcome = []

        def collect_cancellable():
            sc.setJobGroup('native-broadcast-cancel', 'Cancel an active GPU broadcast join', True)
            try:
                outcome.append(('success', frame.collect()))
            except Exception as error:
                outcome.append(('failure', str(error)))
            finally:
                sc.setLocalProperty('spark.jobGroup.id', None)

        worker = InheritableThread(target=collect_cancellable, daemon=True)
        worker.start()
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if native is None:
                ready = [node for node in nodes(frame._jdf.queryExecution().executedPlan())
                         if node.getClass().getSimpleName() ==
                         'GpuDriverlessBroadcastHashJoinExec']
                if len(ready) == 1:
                    native = ready[0]
            if native is not None and Path(marker).exists() \
                    and native.metrics().apply('executorBuilds').value() > 0 \
                    and running_tasks(spark) > 0:
                break
            assert worker.is_alive(), outcome
            time.sleep(0.1)
        assert Path(marker).exists(), 'The cancellation never reached the running stream task'
        assert native is not None, 'Cancellation never reached a driverless broadcast plan'
        assert native.metrics().apply('executorBuilds').value() > 0, \
            'Cancellation must happen after another task has used the native cache'
        assert running_tasks(spark) > 0, 'Cancellation requires an active executor task'
        sc.cancelJobGroup('native-broadcast-cancel')
        worker.join(30)
        assert not worker.is_alive(), 'The cancelled query did not return within 30 seconds'
        assert len(outcome) == 1 and outcome[0][0] == 'failure', outcome
        message = outcome[0][1].lower()
        assert 'cancel' in message or 'interrupt' in message, message
        wait_idle(spark)
        print(json.dumps(dict(case='query-cancellation', result='PASS',
                              shuffle=cleanup(spark, native))), flush=True)

        for iteration in range(3):
            frame = query(spark)
            assert sorted(tuple(row) for row in frame.collect()) == expected
            native = native_node(frame)
            assert native.metrics().apply('runtimeFallbacks').value() == 0
            assert native.fallbackBroadcast().metrics().apply('collectTime').value() == 0
            print(json.dumps(dict(case='query-after-cleanup', iteration=iteration,
                                  result='PASS', shuffle=cleanup(spark, native))), flush=True)
        expected_executors = int(sc.getConf().get('spark.executor.instances'))
        assert sc._jsc.sc().getExecutorMemoryStatus().size() == expected_executors + 1
        print('EXECUTOR_BROADCAST_FAILURE_SMOKE_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
