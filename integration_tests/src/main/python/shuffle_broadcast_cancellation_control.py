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

"""Run the same cancellation fixture through standard GPU broadcast on an unmodified main."""

import json
import os
from pathlib import Path
import time

from pyspark import InheritableThread
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

from shuffle_broadcast_failure_smoke import nodes, query, running_tasks, wait_idle


def main():
    spark = SparkSession.builder.appName('standard-broadcast-cancellation-control').getOrCreate()
    sc = spark.sparkContext
    root = Path(os.environ['BROADCAST_FAILURE_MARKERS'])
    root.mkdir(parents=True, exist_ok=False)
    try:
        spark.conf.set('spark.sql.adaptive.enabled', 'false')
        spark.conf.set('spark.rapids.sql.join.hashTable.reuse', 'false')
        spark.conf.set('spark.rapids.sql.driverlessBroadcast.enabled', 'false')
        spark.conf.set('spark.rapids.sql.enabled', 'false')
        expected = sorted(tuple(row) for row in query(spark).collect())
        spark.conf.set('spark.rapids.sql.enabled', 'true')
        marker = str(root / 'slow-task-entered')

        @udf(LongType())
        def slow_first(value):
            if value == 0:
                Path(marker).write_text('standard stream task is running\n')
                time.sleep(45)
            return value

        frame = query(spark, slow_first)
        plan = frame._jdf.queryExecution().executedPlan()
        joins = [node for node in nodes(plan)
                 if node.getClass().getSimpleName() == 'GpuBroadcastHashJoinExec']
        assert len(joins) == 1, plan.toString()
        outcome = []

        def collect_cancellable():
            sc.setJobGroup('standard-broadcast-cancel', 'Cancellation control', True)
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
            if Path(marker).exists() and running_tasks(spark) > 0:
                break
            assert worker.is_alive(), outcome
            time.sleep(0.1)
        assert Path(marker).exists() and running_tasks(spark) > 0
        assert joins[0].broadcastExchange().metrics().apply('dataSize').value() > 0
        sc.cancelJobGroup('standard-broadcast-cancel')
        worker.join(30)
        assert not worker.is_alive()
        assert len(outcome) == 1 and outcome[0][0] == 'failure', outcome
        message = outcome[0][1].lower()
        assert 'cancel' in message or 'interrupt' in message, message
        wait_idle(spark)
        assert sorted(tuple(row) for row in query(spark).collect()) == expected
        print(json.dumps(dict(case='standard-broadcast-cancellation-control', result='PASS')),
              flush=True)
        print('STANDARD_BROADCAST_CANCELLATION_CONTROL_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
