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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.TestUtils.withTaskContext
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.GpuTaskMetrics
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext

class GpuMetricTimingSuite extends AnyFunSuite with BeforeAndAfterEach {
  private class TaskMetrics extends GpuTaskMetrics {
    var waitTime = 100L
    var reads = 0

    override def getSemWaitTime(): Long = {
      reads += 1
      waitTime
    }
  }

  override def beforeEach(): Unit = TrampolineUtil.unsetTaskContext()

  override def afterEach(): Unit = {
    try {
      ScalableTaskCompletion.reset()
    } finally {
      TrampolineUtil.unsetTaskContext()
    }
  }

  test("read-only semaphore wait follows task registration, updates and completion") {
    assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
    val context = new MockTaskContext(taskAttemptId = 8101L, partitionId = 0)
    withTaskContext(context, completesTask = true) {
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
      val taskMetrics = new TaskMetrics
      GpuTaskMetrics.registerOnTask(taskMetrics)
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 100L)
      taskMetrics.waitTime = 175L
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 175L)

      // A missing thread-local TaskContext must not expose a previously read task value.
      TrampolineUtil.unsetTaskContext()
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
      TrampolineUtil.setTaskContext(context)
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 175L)

      // Verify completion dispatch before teardown can run pending callbacks.
      context.markTaskComplete()
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
    }
    assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
    // Reusing a task ID after completion must not expose a stale registry entry.
    withTaskContext(new MockTaskContext(taskAttemptId = 8101L, partitionId = 0),
        completesTask = true) {
      assert(GpuTaskMetrics.getCurrentSemWaitTime == 0L)
    }
  }

  test("raw timers exclude child time without reading semaphore wait") {
    withTaskContext(new MockTaskContext(taskAttemptId = 8102L, partitionId = 0),
        completesTask = true) {
      val taskMetrics = new TaskMetrics
      GpuTaskMetrics.registerOnTask(taskMetrics)
      val parent = new LocalGpuMetric
      val child = new LocalGpuMetric
      child.add(10L)
      assert(parent.tryActivateTimer(Seq(child)))
      child.add(40L)
      taskMetrics.waitTime += 7L
      parent.deactivateTimer(60L, Seq(child))
      assert(parent.value == 20L)
      assert(taskMetrics.reads == 0)
    }
  }

  test("companion timers exclude task wait and child companion deltas across activations") {
    withTaskContext(new MockTaskContext(taskAttemptId = 8103L, partitionId = 0),
        completesTask = true) {
      val taskMetrics = new TaskMetrics
      GpuTaskMetrics.registerOnTask(taskMetrics)
      val parent = new LocalGpuMetric
      val companion = new LocalGpuMetric
      parent.companionGpuMetric = Some(companion)
      val child = new LocalGpuMetric
      val childCompanion = new LocalGpuMetric
      child.companionGpuMetric = Some(childCompanion)
      child.add(5L)
      childCompanion.add(3L)

      assert(parent.tryActivateTimer(Seq(child)))
      assert(!parent.tryActivateTimer(Seq(child)))
      assert(taskMetrics.reads == 1)
      child.add(60L)
      childCompanion.add(40L)
      taskMetrics.waitTime += 30L
      parent.deactivateTimer(100L, Seq(child))
      assert(parent.value == 40L)
      assert(companion.value == 30L)

      assert(parent.tryActivateTimer(Seq(child)))
      child.add(10L)
      childCompanion.add(8L)
      taskMetrics.waitTime += 7L
      parent.deactivateTimer(20L, Seq(child))
      assert(parent.value == 50L)
      assert(companion.value == 35L)
      assert(taskMetrics.reads == 4)
    }
  }

  test("timers can be activated again after the timed body throws") {
    for (withCompanion <- Seq(false, true)) {
      val metric = new LocalGpuMetric
      val companion = new LocalGpuMetric
      if (withCompanion) {
        metric.companionGpuMetric = Some(companion)
      }
      val failure = new IllegalStateException("timed body failed")
      assert(intercept[IllegalStateException] {
        metric.ns[Unit](Seq.empty) {
          metric.ns[Unit](Seq.empty) {
            throw failure
          }
        }
      } eq failure)
      val before = metric.value
      val companionBefore = companion.value
      assert(metric.tryActivateTimer(Seq.empty))
      metric.deactivateTimer(10L, Seq.empty)
      assert(metric.value == before + 10L)
      assert(companion.value == companionBefore + (if (withCompanion) 10L else 0L))
    }
  }

  test("primitive snapshots preserve exclusions across sizes, companions and independent timers") {
    val parent = new LocalGpuMetric
    val companion = new LocalGpuMetric
    parent.companionGpuMetric = Some(companion)
    val independent = new LocalGpuMetric
    val children = Vector.fill(4)(new LocalGpuMetric)
    children.zipWithIndex.foreach { case (child, index) =>
      child.add(100L + index)
      if (index % 2 == 0) {
        val childCompanion = new LocalGpuMetric
        childCompanion.add(50L + index)
        child.companionGpuMetric = Some(childCompanion)
      }
    }
    var expectedRaw = 0L
    var expectedCompanion = 0L
    for (size <- Seq(0, 1, 4, 2, 0, 3)) {
      val descendants = children.take(size).toList
      assert(parent.tryActivateTimer(descendants))
      assert(independent.tryActivateTimer(descendants))
      assert(!parent.tryActivateTimer(Seq.empty))
      descendants.foreach { child =>
        child.add(10L)
        child.companionGpuMetric.foreach(_.add(6L))
      }
      independent.deactivateTimer(80L, descendants)
      parent.deactivateTimer(100L, descendants)
      expectedRaw += 100L - size * 10L
      expectedCompanion += 100L - descendants.count(_.companionGpuMetric.isDefined) * 6L
      assert(parent.value == expectedRaw)
      assert(companion.value == expectedCompanion)
    }
    assert(independent.value == 6 * 80L - Seq(0, 1, 4, 2, 0, 3).sum * 10L)
  }

  test("mismatched descendant count fails without leaving the timer active") {
    val metric = new LocalGpuMetric
    val child = new LocalGpuMetric
    assert(metric.tryActivateTimer(Seq(child)))
    intercept[IllegalStateException] {
      metric.deactivateTimer(10L, Seq.empty)
    }
    assert(metric.value == 0L)
    assert(metric.tryActivateTimer(Seq.empty))
    metric.deactivateTimer(20L, Seq.empty)
    assert(metric.value == 20L)
  }

  test("multiple and single timer entry points execute once and retain outer ownership") {
    val outer = new LocalGpuMetric
    val inner = new LocalGpuMetric
    assert(outer.tryActivateTimer(Seq.empty))
    var calls = 0
    assert(GpuMetric.ns(Seq(NoopMetric, outer, inner, inner), Seq.empty) {
      calls += 1
      assert(!inner.tryActivateTimer(Seq.empty))
      42
    } == 42)
    assert(calls == 1)
    assert(outer.value == 0L)
    assert(!outer.tryActivateTimer(Seq.empty))
    outer.deactivateTimer(23L, Seq.empty)
    assert(outer.value == 23L)
    assert(inner.tryActivateTimer(Seq.empty))
    inner.deactivateTimer(0L, Seq.empty)
    assert(GpuMetric.ns(NoopMetric) { calls += 1; 43 } == 43)
    assert(GpuMetric.ns(Seq.empty, Seq.empty) { calls += 1; 44 } == 44)
    assert(GpuMetric.ns(Seq(NoopMetric, NoopMetric), Seq.empty) { calls += 1; 45 } == 45)
    assert(calls == 4)
  }

  test("metric ranges handle empty, single, duplicate and already active timers") {
    val outer = new LocalGpuMetric
    assert(outer.tryActivateTimer(Seq.empty))
    for (metrics <- Seq(Seq.empty, Seq(NoopMetric), Seq(outer), Seq(NoopMetric, outer))) {
      withResource(new MetricRange(metrics)) { range =>
        assert(range.needTracks == metrics.map(_ => false))
      }
      assert(!outer.tryActivateTimer(Seq.empty))
    }
    val inner = new LocalGpuMetric
    withResource(new MetricRange(Seq(NoopMetric, outer, inner, inner))) { range =>
      assert(range.needTracks == Seq(false, false, true, false))
      assert(!inner.tryActivateTimer(Seq.empty))
    }
    assert(inner.tryActivateTimer(Seq.empty))
    inner.deactivateTimer(0L, Seq.empty)
    outer.deactivateTimer(31L, Seq.empty)
    assert(outer.value == 31L)
  }

  test("metric and NVTX ranges release their own timers when the body throws") {
    val outer = new LocalGpuMetric
    val inner = new LocalGpuMetric
    val failure = new IllegalStateException("range body failed")
    val nvtxId = NvtxId("metrics regression", NvtxColor.BLUE, "Test timer ownership")
    assert(outer.tryActivateTimer(Seq.empty))
    val actions: Seq[(() => Unit) => Unit] = Seq(
      body => GpuMetric.ns(Seq(outer, inner, inner), Seq.empty)(body()),
      body => withResource(new MetricRange(Seq(outer, inner)))(_ => body()),
      body => withResource(new NvtxWithMetrics("metrics regression", NvtxColor.BLUE,
        Seq(outer, inner)))(_ => body()),
      body => NvtxIdWithMetrics(nvtxId, outer, inner)(body()),
      body => NvtxIdWithMetrics(nvtxId, inner)(body()),
      body => NvtxIdWithMetrics(nvtxId, NoopMetric)(body()))
    for (action <- actions) {
      var calls = 0
      assert(intercept[IllegalStateException] {
        action(() => { calls += 1; throw failure })
      } eq failure)
      assert(calls == 1)
      assert(!outer.tryActivateTimer(Seq.empty))
      assert(inner.tryActivateTimer(Seq.empty))
      inner.deactivateTimer(0L, Seq.empty)
    }
    outer.deactivateTimer(37L, Seq.empty)
    assert(outer.value == 37L)
  }
}
