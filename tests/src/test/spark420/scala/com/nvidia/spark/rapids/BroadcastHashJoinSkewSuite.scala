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

/*** spark-rapids-shim-json-lines
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExecBase
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

/**
 * SPARK-44065 added BroadcastHashJoinExec.isSkewJoin. Verify the GPU conversion
 * preserves the flag and plan-string marker.
 */
class BroadcastHashJoinSkewSuite
    extends SparkQueryCompareTestSuite
    with AdaptiveSparkPlanHelper {

  private def runAdaptiveAndVerifyResult(
      spark: SparkSession, query: String): SparkPlan = {
    val dfAdaptive = spark.sql(query)
    assert(dfAdaptive.queryExecution.executedPlan.toString
      .startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    dfAdaptive.collect()
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
    assert(adaptivePlan.collect { case e: Exchange => e }.isEmpty,
      "The final plan should not contain any Exchange node.")
    adaptivePlan
  }

  test("SPARK-44065: GPU BroadcastHashJoin preserves isSkewJoin in plan display") {
    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, "1000")
      .set(SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key, "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "10")
      .set(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key, "600")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "600")
      .set(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key, "true")

    withGpuSparkSession(spark => {
      spark.range(0, 1000, 1, 10)
        .selectExpr("if(id >= 5, 5, id) as key1", "id as value1")
        .createOrReplaceTempView("skewData1")
      spark.range(0, 5, 1, 10)
        .selectExpr("id as key2", "id as value2")
        .createOrReplaceTempView("smallData")

      val adaptivePlan = runAdaptiveAndVerifyResult(spark,
        """select a.key1, count(*) from skewData1 a join smallData b
          | on a.key1 = b.key2 group by a.key1""".stripMargin)
      val bhjs = collect(adaptivePlan) {
        case j: GpuBroadcastHashJoinExecBase => j
      }
      assert(bhjs.nonEmpty, s"Expected GpuBroadcastHashJoin in plan:\n$adaptivePlan")
      assert(bhjs.forall(_.isSkewJoin),
        s"Expected isSkewJoin=true, got: ${
          bhjs.map(j => j.getClass.getSimpleName -> j.isSkewJoin)}")
      assert(bhjs.forall(_.nodeName.endsWith("(skew=true)")),
        s"Expected (skew=true) in nodeName, got: ${bhjs.map(_.nodeName)}")
    }, conf)
  }
}
