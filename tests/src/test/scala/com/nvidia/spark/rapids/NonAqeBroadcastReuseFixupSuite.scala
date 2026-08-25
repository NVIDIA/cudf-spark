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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.{FilterExec, RangeExec, SparkPlan}
import org.apache.spark.sql.execution.{InSubqueryExec => SparkInSubqueryExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuSubqueryBroadcastExec}

/**
 * Unit coverage for `GpuTransitionOverrides.fixupNonAdaptiveBroadcastReuse`, the non-AQE
 * counterpart of `fixupAdaptiveExchangeReuse` already exercised by `ReusedExchangeFixupSuite`.
 *
 * The non-AQE fixup only fires for a `GpuBroadcastExchangeExec` that lives inside a
 * `GpuSubqueryBroadcastExec` referenced by an `ExecSubqueryExpression` (DPP). A faithful
 * test plan therefore needs to wire up both the main-plan broadcast and a hand-built DPP
 * subquery expression.
 */
class NonAqeBroadcastReuseFixupSuite extends SparkQueryCompareTestSuite {

  private val baseConf: SparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.exchange.reuse", "true")
      .set(RapidsConf.ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP.key, "true")

  private def newRange(): RangeExec = RangeExec(Range(1, 2, 1, Some(1)))

  /** Wrap a plan in `GpuCoalesceBatches` so the signature must `stripGpuCoalesceBatches`. */
  private def coalesce(child: SparkPlan): GpuCoalesceBatches =
    GpuCoalesceBatches(child, TargetSize(1L << 20))

  /**
   * Build a `GpuBroadcastExchangeExec` over `child`. The broadcast mode is keyed on
   * `leafRange.output`, so callers can wrap `child` in `GpuCoalesceBatches` without affecting
   * the mode keys — exactly mirroring real DPP, where the dim-side attributes are pinned by
   * the join's build expression while only the main-plan broadcast's child gets a coalesce
   * wrap from `insertCoalesce` / `optimizeCoalesce`.
   */
  private def newGpuBroadcast(child: SparkPlan, leafRange: RangeExec): GpuBroadcastExchangeExec = {
    val mode = HashedRelationBroadcastMode(leafRange.output)
    GpuBroadcastExchangeExec(mode, child)(BroadcastExchangeExec(mode, child))
  }

  private def newGpuSubqueryBroadcast(
      child: GpuBroadcastExchangeExec,
      leafRange: RangeExec): GpuSubqueryBroadcastExec = {
    val keys = Seq(leafRange.output.head)
    GpuSubqueryBroadcastExec("dpp", Seq(0), keys, child)(modeKeys = Some(keys))
  }

  /**
   * Build a synthetic non-AQE DPP plan that mirrors the actual #14833 structural divergence:
   *
   *   FilterExec(
   *     condition = InSubqueryExec(plan = GpuSubqueryBroadcastExec(child = dppG)),
   *     child     = mainG  // GpuBroadcastExchangeExec(child = GpuCoalesceBatches(range))
   *   )
   *
   * The dim-side `range` is shared between `mainG` and `dppG` so their broadcast modes
   * (keyed on `range.output`) canonicalize identically — matching how real DPP wires both
   * broadcasts off the same logical filter sub-plan. The children diverge structurally
   * only in the `GpuCoalesceBatches` wrap that `insertCoalesce` / `optimizeCoalesce`
   * applies on the main-plan side:
   *   - `mainG.child` = `GpuCoalesceBatches(range)`
   *   - `dppG.child`  = `range`
   *
   * Without the production `stripGpuCoalesceBatches` normalization, the canonical comparison
   * fails because the two children differ by exactly that wrap. With the normalization, both
   * reduce to `range.canonicalized`, the signatures match, and the DPP-side broadcast is
   * rewritten to `ReusedExchangeExec` pointing at `mainG`.
   */
  private def buildDppPlan(): (SparkPlan, GpuBroadcastExchangeExec, GpuBroadcastExchangeExec) = {
    val range = newRange()
    val mainG = newGpuBroadcast(coalesce(range), range)
    val dppG = newGpuBroadcast(range, range)
    val gsb = newGpuSubqueryBroadcast(dppG, range)
    val inSub = SparkInSubqueryExec(range.output.head, gsb, NamedExpression.newExprId)
    (FilterExec(inSub, mainG), mainG, dppG)
  }

  private def reusedExchangeChildren(p: SparkPlan): Seq[SparkPlan] = {
    val collected = scala.collection.mutable.ArrayBuffer.empty[SparkPlan]
    p.foreach { node =>
      node.expressions.foreach(_.foreach {
        case sub: SparkInSubqueryExec =>
          sub.plan match {
            case gsb: GpuSubqueryBroadcastExec =>
              gsb.child match {
                case r: ReusedExchangeExec => collected += r.child
                case _ =>
              }
            case _ =>
          }
        case _ =>
      })
    }
    collected.toSeq
  }

  test("fixupNonAdaptiveBroadcastReuse rewrites matching DPP broadcast to ReusedExchangeExec") {
    withGpuSparkSession(_ => {
      val (plan, mainG, _) = buildDppPlan()
      val updated = new GpuTransitionOverrides().fixupNonAdaptiveBroadcastReuse(plan)

      val reusedChildren = reusedExchangeChildren(updated)
      assert(reusedChildren.size == 1,
        s"expected exactly one ReusedExchangeExec under the GpuSubqueryBroadcastExec, got " +
          s"${reusedChildren.size} in:\n${updated.treeString}")
      assert(reusedChildren.head eq mainG,
        s"expected the ReusedExchangeExec to point at the main-plan broadcast G1, got " +
          s"${reusedChildren.head}")
    }, baseConf)
  }

  test("fixupNonAdaptiveBroadcastReuse leaves plans with no main-plan broadcast unchanged") {
    // No GpuBroadcastExchangeExec in the main plan; just a RangeExec wrapped in a Filter whose
    // condition still references a DPP-side GpuSubqueryBroadcastExec. The early-exit at the
    // `if (mainPlanBroadcasts.isEmpty) return p` line in fixupNonAdaptiveBroadcastReuse should
    // return the input plan unmodified.
    withGpuSparkSession(_ => {
      val range = newRange()
      val dppG = newGpuBroadcast(range, range)
      val gsb = newGpuSubqueryBroadcast(dppG, range)
      val inSub = SparkInSubqueryExec(range.output.head, gsb, NamedExpression.newExprId)
      val plan = FilterExec(inSub, range)

      val updated = new GpuTransitionOverrides().fixupNonAdaptiveBroadcastReuse(plan)
      assert(updated eq plan,
        s"expected the plan to be returned unchanged when mainPlanBroadcasts.isEmpty, got:\n" +
          s"${updated.treeString}")
    }, baseConf)
  }

  test("ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP conf accessor flips with the kill switch") {
    // Scope: this test validates ONLY that the `RapidsConf.isNonAqeBroadcastReuseFixupEnabled`
    // accessor reflects the conf key. The plan-level gate inside `GpuTransitionOverrides.apply`
    // (the `if (rapidsConf.isNonAqeBroadcastReuseFixupEnabled ...)` block, identified by code
    // shape rather than line number so this comment doesn't drift) reads this accessor;
    // exercising the full gate against a real plan needs a GPU-routed end-to-end run and is
    // covered by `RapidsDynamicPartitionPruningV1SuiteAEOff` rather than this unit suite.
    val killSwitchConf = baseConf.clone()
        .set(RapidsConf.ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP.key, "false")
    withGpuSparkSession(spark => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      assert(!rapidsConf.isNonAqeBroadcastReuseFixupEnabled,
        "isNonAqeBroadcastReuseFixupEnabled should be false when the kill switch is set")
    }, killSwitchConf)

    // Use a SparkConf that does NOT set the kill-switch key, so the assertion really
    // exercises the `createWithDefault(true)` default rather than a redundant "true" override.
    val defaultConf = new SparkConf()
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.exchange.reuse", "true")
    withGpuSparkSession(spark => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      assert(rapidsConf.isNonAqeBroadcastReuseFixupEnabled,
        "isNonAqeBroadcastReuseFixupEnabled should default to true (createWithDefault(true))")
    }, defaultConf)
  }
}
