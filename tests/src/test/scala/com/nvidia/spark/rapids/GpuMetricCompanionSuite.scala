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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Regression test for the companion ("op time (excl. SemWait)") accounting on
 * nested `.ns(excludeMetrics)` wraps (NVIDIA/spark-rapids#14901, bug 3).
 *
 * When an outer wrap excludes a descendant, `GpuMetric.deactivateTimer` must
 * subtract the descendant's COMPANION delta (its `op time (excl. SemWait)`)
 * from the outer companion, not the descendant's RAW delta (its `op time`,
 * which still embeds the descendant's own SemWait). Subtracting the raw delta
 * double-counts the descendant's SemWait (once via the outer wrap's own
 * SemWait delta, once embedded in the raw delta), driving the outer companion
 * below its raw value -- and negative once a real SemWait is present.
 */
class GpuMetricCompanionSuite extends AnyFunSuite with BeforeAndAfterEach {

  // Defensive: ensure no ambient TaskContext is registered on this thread (e.g.
  // leaked by another suite in the same forked JVM), so GpuTaskMetrics.get
  // returns a fresh instance with SemWaitTime == 0 and the arithmetic below is
  // fully deterministic (no wall-clock, no real SemWait involved).
  override def beforeEach(): Unit = TrampolineUtil.unsetTaskContext()
  override def afterEach(): Unit = TrampolineUtil.unsetTaskContext()

  test("companion op_time excludes descendant COMPANION delta, not RAW delta (#14901)") {
    // Outer (parent) timing metric with a companion, e.g. a Project.
    val parent = new LocalGpuMetric
    val parentCompanion = new LocalGpuMetric
    parent.companionGpuMetric = Some(parentCompanion)

    // Descendant (child) whose raw op_time (100) embeds 30 of SemWait, so its
    // companion op_time (excl. SemWait) is 70.
    val child = new LocalGpuMetric
    val childCompanion = new LocalGpuMetric
    child.companionGpuMetric = Some(childCompanion)

    // Activate the parent wrap. With no TaskContext on this thread the task
    // SemWait time is 0 and stays 0, so the parent's SemWait delta is 0 and
    // the only difference between fixed and buggy behavior is whether the
    // companion subtracts the child's companion delta (70) or raw delta (100).
    assert(parent.tryActivateTimer(Seq(child)),
      "first activation of an idle timer must succeed")

    // The child wrap runs inside the parent wrap: raw += 100, companion += 70.
    child.add(100L)
    childCompanion.add(70L)

    // Parent wall time = child's 100 + parent's own 10.
    parent.deactivateTimer(110L, Seq(child))

    // Raw op_time: duration - child raw delta = 110 - 100 = 10. (unchanged by the fix)
    assert(parent.value == 10L, s"raw op_time expected 10, got ${parent.value}")

    // Companion op_time (FIX): duration - semWaitDelta(0) - child COMPANION delta(70) = 40.
    // Pre-fix it subtracted the child RAW delta(100) -> 10, which is the bug.
    assert(parentCompanion.value == 40L,
      s"companion op_time must exclude the descendant's COMPANION delta (expected 40), " +
        s"but got ${parentCompanion.value}; a value of 10 is the pre-fix raw-delta subtraction")

    // The companion can never legitimately exceed the raw op_time's wall basis,
    // and must stay non-negative.
    assert(parentCompanion.value >= 0L,
      s"companion op_time must be non-negative, got ${parentCompanion.value}")
  }

  test("companion stays non-negative across a chain of nested excludes (#14901)") {
    // grandparent excludes parent; parent excludes child. Each level embeds
    // SemWait in its raw delta (raw=2*companion here). With the raw-delta
    // subtraction the upper levels' companions are driven down incorrectly.
    val gp = new LocalGpuMetric
    val gpC = new LocalGpuMetric
    gp.companionGpuMetric = Some(gpC)
    val parent = new LocalGpuMetric
    val parentC = new LocalGpuMetric
    parent.companionGpuMetric = Some(parentC)

    assert(gp.tryActivateTimer(Seq(parent)))
    // parent subtree ran: raw += 60, companion += 30
    parent.add(60L)
    parentC.add(30L)
    gp.deactivateTimer(80L, Seq(parent)) // gp wall = 60 (parent) + 20 (own)

    // raw: 80 - 60 = 20
    assert(gp.value == 20L, s"raw expected 20, got ${gp.value}")
    // companion (fix): 80 - 0 - 30 = 50 ; pre-fix: 80 - 0 - 60 = 20
    assert(gpC.value == 50L,
      s"companion must exclude parent COMPANION delta (expected 50), got ${gpC.value}")
    assert(gpC.value >= 0L, s"companion must be non-negative, got ${gpC.value}")
  }

  test("companion exclude falls back to 0 when an excluded metric has no companion (#14901)") {
    // Documents the `getOrElse(0L)` fallback in deactivateTimer. In production
    // this branch does not fire: excludeMetrics are descendant OP_TIME_NEW
    // metrics built from the same allMetrics map as the parent, so whenever the
    // parent has a companion the descendants do too. This test pins the
    // defensive behavior for the (not-expected) case where a child has none:
    // the child's time is NOT subtracted from the parent companion (fallback 0),
    // rather than crashing or mis-subtracting a raw delta.
    val parent = new LocalGpuMetric
    val parentCompanion = new LocalGpuMetric
    parent.companionGpuMetric = Some(parentCompanion)

    val childNoCompanion = new LocalGpuMetric // companionGpuMetric stays None

    assert(parent.tryActivateTimer(Seq(childNoCompanion)))
    childNoCompanion.add(100L)
    parent.deactivateTimer(110L, Seq(childNoCompanion))

    // raw still excludes the child's raw delta.
    assert(parent.value == 10L, s"raw op_time expected 10, got ${parent.value}")
    // companion subtracts 0 for the companion-less child (getOrElse(0L)): 110 - 0 - 0.
    assert(parentCompanion.value == 110L,
      s"companion-less child is not excluded from the parent companion (expected 110), " +
        s"got ${parentCompanion.value}")
  }
}
