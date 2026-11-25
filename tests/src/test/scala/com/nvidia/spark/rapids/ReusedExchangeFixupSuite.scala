/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
import org.apache.spark.sql.execution.{RangeExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}
import org.apache.spark.sql.rapids.execution.GpuBroadcastExchangeExec

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

  private def broadcastJoin(range: RangeExec): SparkPlan = {
    val broadcast = BroadcastExchangeExec(HashedRelationBroadcastMode(range.output), range)
    BroadcastHashJoinExec(
      range.output, broadcast.output, Inner, BuildRight, None, range, broadcast)
  }

  private def toGpuPlan(plan: SparkPlan): SparkPlan = {
    new GpuTransitionOverrides()(GpuOverrides()(plan))
  }

  /** The reused exchanges of a plan whose original exchange is not in the same plan. */
  private def reusedFromElsewhere(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    val own = plan.collect { case e: Exchange => e }
    plan.collect { case r: ReusedExchangeExec => r }.filterNot(r => own.exists(_ eq r.child))
  }

  /** A plan must not reuse an exchange that belongs to another plan. */
  private def noReuseAcrossPlansTest(spark: SparkSession): Unit = {
    // One RangeExec, so both plans have the same attribute ids and an equal canonical form, as
    // a query planned twice from the same analyzed plan does.
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val first = toGpuPlan(broadcastJoin(range))
    val second = toGpuPlan(broadcastJoin(range))
    Seq(first, second).foreach { plan =>
      assert(plan.find(_.isInstanceOf[GpuBroadcastExchangeExec]).isDefined,
        s"the broadcast did not run on the GPU:\n$plan")
      assert(reusedFromElsewhere(plan).isEmpty,
        s"the plan reuses an exchange that is not in it:\n$plan")
    }
  }

  private def assertOneReuseInside(plan: SparkPlan): Unit = {
    assert(plan.collect { case r: ReusedExchangeExec => r }.size == 1,
      s"expected one reused exchange:\n$plan")
    assert(reusedFromElsewhere(plan).isEmpty,
      s"the reused exchange's original is not in the plan:\n$plan")
  }

  /** Two equal broadcasts in one plan still end up as one exchange and a reuse of it. */
  private def reuseInsideOnePlanTest(spark: SparkSession): Unit = {
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    assertOneReuseInside(toGpuPlan(UnionExec(Seq(broadcastJoin(range), broadcastJoin(range)))))
  }

  /** An equal plan that is still alive does not stop the reuse inside a later plan. */
  private def reuseInsideLaterPlanTest(spark: SparkSession): Unit = {
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val earlier = toGpuPlan(broadcastJoin(range))
    assertOneReuseInside(toGpuPlan(UnionExec(Seq(broadcastJoin(range), broadcastJoin(range)))))
    // The earlier plan, and with it the exchange cached for it, is alive up to here.
    assert(earlier.find(_.isInstanceOf[GpuBroadcastExchangeExec]).isDefined)
  }

  private val reuseConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.exchange.reuse", "true")
      .set(RapidsConf.ENABLE_AQE_EXCHANGE_REUSE_FIXUP.key, "true")

  test("fixupExchangeReuse leaves exchanges of another plan alone") {
    withGpuSparkSession(noReuseAcrossPlansTest, reuseConf)
  }

  test("fixupExchangeReuse still reuses an exchange inside one plan") {
    withGpuSparkSession(reuseInsideOnePlanTest, reuseConf)
  }

  test("fixupExchangeReuse reuses inside a plan after an equal plan") {
    withGpuSparkSession(reuseInsideLaterPlanTest, reuseConf)
  }
}
