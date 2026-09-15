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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito.mockStatic
import org.mockito.invocation.InvocationOnMock
import org.scalactic.Tolerance
import org.scalatest.funsuite.AnyFunSuite

class GpuMemoryEstimatorSuite extends AnyFunSuite with Tolerance {

  /**
   * Per-task `gpuMaxTaskFootprint` values, in bytes, measured over the 64 GPU-touching tasks of a
   * single stage with dynamic concurrency enabled. The distribution is bimodal: 60 light samples
   * against 4 at 5.5-6.6 GiB.
   */
  private val LightMode = 1085796899.0

  private val LightFootprints: Seq[Double] =
    Seq(441925356.0, 585532883.0) ++ Seq.fill(58)(LightMode)

  private val HeavyFootprints: Seq[Double] =
    Seq(5911533958.0, 6181387572.0, 7123115846.0, 7123115846.0)

  private val MeasuredFootprints: Seq[Double] = LightFootprints ++ HeavyFootprints

  /**
   * P95 over `MeasuredFootprints` interpolates three quarters of the way from the smallest to
   * the second-smallest heavy sample, giving 5830.7 MiB.
   */
  private val P95OfMeasured = 6113924168.5

  /**
   * The estimate a stage starts from: `GpuDeviceManager.getMemorySize / concurrentGpuTasks`,
   * here a 23079 MiB RMM pool with `concurrentGpuTasks=4`.
   */
  private val SeedEstimate = 23079L * 1024 * 1024 / 4

  /** Mirrors the percentile hard-coded in `GpuStageMemoryEstimator.estimate`. */
  private val StagePercentile = 0.8

  /** Mirrors the `new StatEstimator(4, ...)` in `GpuStageMemoryEstimator`. */
  private val StageMinEntries = 4

  /**
   * Only the interpolating branch of `percentile` needs slack, and there it is ~1e-6 bytes of
   * double rounding. The estimate is consumed by `GpuSemaphore.memToPermits`, which floors to
   * 32 MiB, so a kilobyte is five orders of magnitude below anything that could change admission.
   */
  private val ToleranceBytes = 1024.0

  private def estimatorOver(samples: Seq[Double]): StatEstimator = {
    val stat = new StatEstimator(StageMinEntries, SeedEstimate.toDouble)
    samples.foreach(stat.add)
    stat
  }

  private def noActiveTasks = ArrayBuffer.empty[Double]

  test("contract: the default value stands in until minEntries real samples arrive") {
    // Because minEntries is 4, four completions stop the seed being padded in at all: however
    // long a stage runs, the estimate is free of its initial guess after the first four tasks.
    Seq(0 -> SeedEstimate.toDouble, 1 -> SeedEstimate.toDouble, 2 -> SeedEstimate.toDouble,
        3 -> SeedEstimate.toDouble, 4 -> LightMode, 8 -> LightMode).foreach {
      case (completedTasks, expected) =>
        val stat = estimatorOver(Seq.fill(completedTasks)(LightMode))
        assert(stat.percentile(StagePercentile, noActiveTasks) == expected,
          s"after $completedTasks completed tasks")
    }
  }

  test("contract: at most 200 samples are retained, oldest evicted first") {
    Seq((200, 1.0, 200.0), (201, 2.0, 201.0), (250, 51.0, 250.0)).foreach {
      case (samplesAdded, oldestRetained, newestRetained) =>
        val stat = estimatorOver((1 to samplesAdded).map(_.toDouble))
        assert(stat.percentile(0.0, noActiveTasks) == oldestRetained,
          s"oldest retained sample after $samplesAdded adds")
        assert(stat.percentile(1.0, noActiveTasks) == newestRetained,
          s"newest retained sample after $samplesAdded adds")
    }
  }

  test("contract: p = 0 and p = 1 return the extremes and p outside [0, 1] is rejected") {
    val stat = estimatorOver(MeasuredFootprints)
    assert(stat.percentile(0.0, noActiveTasks) == MeasuredFootprints.min)
    assert(stat.percentile(1.0, noActiveTasks) == MeasuredFootprints.max)
    assertThrows[IllegalArgumentException](stat.percentile(-0.1, noActiveTasks))
    assertThrows[IllegalArgumentException](stat.percentile(1.1, noActiveTasks))
  }

  test("characterization: the measured footprints hide their heavy mode below P95") {
    val stat = estimatorOver(MeasuredFootprints)
    Seq(StagePercentile -> LightMode, 0.9 -> LightMode, 0.95 -> P95OfMeasured).foreach {
      case (p, expected) =>
        assert(stat.percentile(p, noActiveTasks) === expected +- ToleranceBytes, s"at p = $p")
    }
  }

  test("characterization: no p at or below 60/65 reaches the heavy mode") {
    // pos = p * (n + 1) with 60 light samples out of 64 puts the first heavy sample at rank 61,
    // so no p <= 60/65 = 0.92307692... can reach it. P90 is blind, not just P80; 0.9231 is the
    // first three-decimal p that is not.
    val stat = estimatorOver(MeasuredFootprints)
    val threshold = LightFootprints.size.toDouble / (MeasuredFootprints.size + 1)
    assert(stat.percentile(threshold, noActiveTasks) == LightMode)
    assert(stat.percentile(0.9231, noActiveTasks) > LightMode)
  }

  test("characterization: P80 stays on the light mode while every heavy task is active") {
    // `others` carries the estimates of tasks that have taken the semaphore and not yet
    // finished -- `activeTasks` is cleared at task completion, not at release. pos = 0.8 *
    // (61 + k) has to clear the 60 recorded light samples, so the tail needs k > 14 live tasks
    // above the light mode, and only 4 heavy tasks exist.
    val stat = estimatorOver(LightFootprints)
    val heaviest = HeavyFootprints.max
    assert(stat.percentile(StagePercentile, ArrayBuffer(HeavyFootprints: _*)) == LightMode)
    assert(stat.percentile(StagePercentile, ArrayBuffer.fill(14)(heaviest)) == LightMode)
    assert(stat.percentile(StagePercentile, ArrayBuffer.fill(15)(heaviest)) > LightMode)
  }

  test("contract: allowDynamicUpdate = false pins the task estimate to the seed") {
    val estimator = new GpuTaskMemoryEstimator(stageId = 1, taskId = 1L,
      defaultEstimate = SeedEstimate, allowDynamicUpdate = false)
    estimator.update(timeLost = 0L, memory = HeavyFootprints.max.toLong)
    assert(estimator.estimate() == SeedEstimate)
  }

  test("contract: a task footprint above the seed is adopted immediately, not blended") {
    // This branch never reads the clock, which is why the expectation can be exact.
    val observed = HeavyFootprints.max.toLong
    val estimator = new GpuTaskMemoryEstimator(stageId = 1, taskId = 1L,
      defaultEstimate = SeedEstimate, allowDynamicUpdate = true)
    estimator.update(timeLost = 0L, memory = observed)
    assert(estimator.estimate() == observed)
  }

  test("contract: lost time beyond the elapsed window holds the blend at the seed") {
    // The opposite direction -- a footprint below the seed -- blends over a 100 ms window, and two
    // of its three inputs can still be pinned without advancing the clock: subtracting
    // `totalTimeLost` drives the window position negative, and the lower clamp turns anything
    // negative into 0.0. So the seed is returned exactly, however long this test takes to get here.
    // What that leaves uncovered is the interior of the window and the upper clamp, where the
    // result moves with elapsed time and needs an injectable clock to assert on.
    val estimator = new GpuTaskMemoryEstimator(stageId = 1, taskId = 1L,
      defaultEstimate = SeedEstimate, allowDynamicUpdate = true)
    estimator.update(timeLost = TimeUnit.HOURS.toNanos(1), memory = LightMode.toLong)
    assert(estimator.estimate() == SeedEstimate)
  }

  test("contract: allowDynamicUpdate = false pins the stage estimate to the seed") {
    val stage = new GpuStageMemoryEstimator(stageId = 1, defaultEstimate = SeedEstimate,
      allowDynamicUpdate = false)
    stage.addTaskIfNeeded(1L)
    assert(stage.estimate() == SeedEstimate)
  }

  test("contract: a stage with no observed task footprints estimates the seed") {
    val stage = new GpuStageMemoryEstimator(stageId = 1, defaultEstimate = SeedEstimate,
      allowDynamicUpdate = true)
    assert(stage.estimate() == SeedEstimate)
  }

  /**
   * Drives a real `GpuStageMemoryEstimator` with `RmmSpark`'s two sensors stubbed; unstubbed,
   * `getMaxGpuTaskMemory` returns 0 until RMM is initialized and `taskDone` records nothing. The
   * stub matches the real sensor's values -- the exact allocated byte count, never decreasing --
   * but not its time profile, since it answers a task's final footprint from the first read where
   * a real high-water mark climbs from 0.
   *
   * Mockito instruments the class process-wide but intercepts only on the calling thread, so work
   * handed to another thread would reach the real static. That fails loudly rather than silently:
   * the real sensor returns 0, nothing is recorded, and the assertions below stop holding.
   */
  private def withMockedStage(body: MockedStage => Unit): Unit = {
    val footprints = ArrayBuffer.empty[Long]
    val mocked = mockStatic(classOf[RmmSpark], (invocation: InvocationOnMock) => {
      invocation.getMethod.getName match {
        case "getMaxGpuTaskMemory" =>
          java.lang.Long.valueOf(
            footprints(invocation.getArgument(0).asInstanceOf[java.lang.Long].intValue()))
        case "getTotalBlockedOrLostTime" => java.lang.Long.valueOf(0L)
        case other => throw new IllegalStateException(s"unstubbed RmmSpark.$other")
      }
    })
    try {
      body(new MockedStage(footprints))
    } finally {
      mocked.close()
    }
  }

  private class MockedStage(footprints: ArrayBuffer[Long]) {
    private val estimator =
      new GpuStageMemoryEstimator(stageId = 1, defaultEstimate = SeedEstimate,
        allowDynamicUpdate = true)

    def start(footprint: Double): Long = {
      footprints += footprint.toLong
      val taskId = footprints.length - 1L
      estimator.addTaskIfNeeded(taskId)
      taskId
    }

    def complete(footprint: Double): Unit = estimator.taskDone(start(footprint))

    def estimate(): Long = estimator.estimate()
  }

  test("contract: recorded task footprints replace the stage seed at minEntries") {
    // Pins the `minEntries` that `GpuStageMemoryEstimator` constructs `StatEstimator` with; the
    // next test pins the percentile its `estimate` passes to `percentile`. The mirrored constants
    // above observe neither, and neither test substitutes for the other.
    withMockedStage { stage =>
      assert(stage.estimate() == SeedEstimate)
      (1 to StageMinEntries - 1).foreach { completed =>
        stage.complete(LightMode)
        assert(stage.estimate() == SeedEstimate, s"after $completed completions")
      }
      stage.complete(LightMode)
      assert(stage.estimate() == LightMode.toLong)
    }
  }

  test("contract: a completed task with no recorded footprint contributes no sample") {
    // Pins the `maxMemory > 0` guard in `taskDone`: without it four zero-footprint completions
    // would drag the estimate to 0. A completed task never reaches the 100 ms blend, so this is
    // deterministic -- an *active* zero-footprint task does, which is why that case is absent.
    withMockedStage { stage =>
      (1 to StageMinEntries).foreach(_ => stage.complete(0.0))
      assert(stage.estimate() == SeedEstimate)
    }
  }

  test("characterization: a live heavy task lifts the stage estimate off the light mode") {
    // P80 over four light samples plus one live heavy one interpolates 80 % of the way from the
    // light mode to 6793 MiB. Nothing else asserts that `activeTasks` reaches the percentile.
    withMockedStage { stage =>
      (1 to StageMinEntries).foreach(_ => stage.complete(LightMode))
      assert(stage.estimate() == LightMode.toLong)
      stage.start(HeavyFootprints.max)
      assert(stage.estimate() == 5915652056L)
    }
  }

  test("characterization: the stage estimate holds the light mode with every heavy task live") {
    // The same blind spot through production wiring rather than percentile arithmetic: four heavy
    // tasks are live and the stage still estimates the light mode, since P80 over 60 light samples
    // never reaches them. P80 was a deliberate choice, so this pins the current outcome rather than
    // asserting a bug; any change that widens the estimate while a heavy tail is live moves it up.
    withMockedStage { stage =>
      LightFootprints.foreach(stage.complete)
      HeavyFootprints.foreach(stage.start)
      assert(stage.estimate() == LightMode.toLong)
    }
  }
}
