/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.{RangeExec, UnionExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}

class ReusedExchangeFixupSuite extends SparkQueryCompareTestSuite {
  /**
   * Tests reuse exchange fixup. Ideally this would be an integration test, but
   * attempts to reproduce the failure to reuse were not successful at smaller scales.
   */
  private def fixupExchangeReuseTest(spark: SparkSession): Unit = {
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val broadcast1 = BroadcastExchangeExec(
      HashedRelationBroadcastMode(range.output),
      range)
    val join1 = BroadcastHashJoinExec(
      range.output,
      broadcast1.output,
      Inner,
      BuildRight,
      None,
      range,
      broadcast1)

    val broadcast2 = BroadcastExchangeExec(
      HashedRelationBroadcastMode(range.output),
      range)
    val join2 = BroadcastHashJoinExec(
      range.output,
      broadcast2.output,
      Inner,
      BuildRight,
      None,
      range,
      broadcast2)
    val plan = UnionExec(Seq(join1, join2))
    val overrides = GpuOverrides()
    val transitionOverrides = new GpuTransitionOverrides
    var updatedPlan = overrides(plan)
    updatedPlan = transitionOverrides(updatedPlan)
    val reused = updatedPlan.find {
      case _: ReusedExchangeExec => true
      case _ => false
    }
    assert(reused.isDefined)
  }

  test("fixupExchangeReuse") {
    val conf = new SparkConf()
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.exchange.reuse", "true")
        .set(RapidsConf.ENABLE_AQE_EXCHANGE_REUSE_FIXUP.key, "true")
    withGpuSparkSession(fixupExchangeReuseTest, conf)
  }
}
