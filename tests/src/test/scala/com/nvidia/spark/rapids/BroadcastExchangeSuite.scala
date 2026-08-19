/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.GpuMetric.NUM_OUTPUT_ROWS

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.rapids.execution.GpuBroadcastExchangeExecBase

class BroadcastExchangeSuite extends SparkQueryCompareTestSuite {

  test("SPARK-52962: broadcast exchange should not reset metrics") {
    withGpuSparkSession(spark => {
      val df = spark.range(1).toDF()
      val joinDF = df.join(broadcast(df), "id")
      joinDF.collect()
      
      val broadcasts = PlanUtils.findOperators(
        joinDF.queryExecution.executedPlan,
        _.isInstanceOf[GpuBroadcastExchangeExecBase])
      assert(broadcasts.size == 1, "one and only GpuBroadcastExchangeExec")

      val broadcastExchangeNode = broadcasts.head.asInstanceOf[GpuBroadcastExchangeExecBase]
      val metrics = broadcastExchangeNode.metrics
      assert(metrics(NUM_OUTPUT_ROWS).value == 1)
      broadcastExchangeNode.resetMetrics()
      assert(metrics(NUM_OUTPUT_ROWS).value == 1, "metrics should not be reset")
    })
  }
}
