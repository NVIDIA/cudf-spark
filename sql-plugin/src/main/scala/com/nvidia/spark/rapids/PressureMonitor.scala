/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.lang.management.ManagementFactory

import ai.rapids.cudf.Rmm

import org.apache.spark.internal.Logging

/**
 * Executor-level adaptive back-pressure monitor (Phase 0/1 of the adaptive
 * back-pressure design).
 *
 * A background daemon thread periodically samples two signals:
 *   - the JVM GC time fraction (GC ms per wall ms across all collectors), and
 *   - GPU device-memory utilization (`Rmm.getTotalBytesAllocated` / pool size).
 *
 * From these it maintains a single multiplicative back-pressure `factor` (>= 1.0).
 * [[GpuSemaphore]] multiplies its per-task GPU memory estimate by this factor, so a
 * larger factor makes each task take more permits and therefore reduces the number of
 * tasks allowed on the GPU concurrently. The factor uses AIMD control: it is increased
 * multiplicatively the moment either signal crosses its guard threshold, and decays
 * additively back to 1.0 while healthy.
 *
 * This is deliberately a WAIT-at-acquisition brake: a back-pressured task simply blocks
 * at the semaphore acquire *before* it holds any scarce GPU/host resource, so it cannot
 * deadlock and does no recompute. It throws no exception. The monitor is fully inert
 * (`factor() == 1.0`, no thread started) unless
 * `spark.rapids.adaptive.backpressure.enabled` is set.
 *
 * The GC guard targets the failure mode seen in production GPU runs where a CPU-era
 * `executor.cores` over-subscribes host memory, GC time climbs, and the executor is
 * eventually lost to a GC-overhead heartbeat timeout. Reducing GPU concurrency when GC
 * climbs keeps concurrent host/pinned memory in check adaptively, instead of relying on a
 * statically hand-tuned `concurrentGpuTasks` / `executor.cores`.
 */
object PressureMonitor extends Logging {
  @volatile private var enabled: Boolean = false
  @volatile private var started: Boolean = false
  @volatile private var currentFactor: Double = 1.0

  // Tunables, populated from RapidsConf at initialize().
  private var gcGuard: Double = 0.12
  private var deviceGuard: Double = 0.9
  private var sampleMs: Long = 200L
  private var maxFactor: Double = 8.0
  // Multiplicative increase on a guard trip; additive decay otherwise (AIMD).
  private[rapids] val IncreaseMult: Double = 1.5
  private[rapids] val DecayStep: Double = 0.25

  /**
   * Pure AIMD decision: given the previous back-pressure factor and the latest GC/device
   * signals, return the next factor. The factor increases multiplicatively (capped at
   * `maxFactor`) when either signal is at or above its guard, and decays additively toward a
   * floor of 1.0 otherwise. Extracted as a side-effect-free function so it can be unit tested
   * without a GPU, a sampler thread, or live JVM/GC state.
   */
  private[rapids] def computeNextFactor(
      prev: Double,
      gcFrac: Double,
      devRatio: Double,
      gcGuard: Double,
      deviceGuard: Double,
      increaseMult: Double,
      decayStep: Double,
      maxFactor: Double): Double = {
    val tripped = gcFrac >= gcGuard || devRatio >= deviceGuard
    if (tripped) {
      math.min(maxFactor, prev * increaseMult)
    } else {
      math.max(1.0, prev - decayStep)
    }
  }

  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

  @volatile private var sampler: Thread = _

  /**
   * Initialize the monitor from the executor's RapidsConf. Idempotent; only the first
   * call has any effect. Starts the sampling thread only when the feature is enabled.
   */
  def initialize(conf: RapidsConf): Unit = synchronized {
    if (started) {
      return
    }
    enabled = conf.adaptiveBackpressureEnabled
    if (!enabled) {
      started = true
      return
    }
    gcGuard = conf.adaptiveBackpressureGcGuard
    deviceGuard = conf.adaptiveBackpressureDeviceGuard
    sampleMs = conf.adaptiveBackpressureSampleMs
    maxFactor = conf.adaptiveBackpressureMaxFactor
    val t = new Thread(new SamplerLoop(), "rapids-pressure-monitor")
    t.setDaemon(true)
    sampler = t
    t.start()
    started = true
    logWarning(s"Adaptive back-pressure enabled (gcGuard=$gcGuard, deviceGuard=$deviceGuard, " +
      s"sampleMs=$sampleMs, maxFactor=$maxFactor)")
  }

  /**
   * The current multiplicative back-pressure factor, always >= 1.0. A value of 1.0 means
   * no back-pressure. Returns 1.0 whenever the feature is disabled.
   */
  def factor(): Double = if (enabled) currentFactor else 1.0

  /**
   * Scale a host-side in-flight byte budget down by the current back-pressure factor. When
   * back-pressure is active (factor > 1) the effective budget shrinks, so host-side work
   * (shuffle serialization, async IO) blocks at its existing in-flight gate sooner. This
   * throttles the off-GPU host/pinned memory that the GPU semaphore cannot see -- the actual
   * driver of GC-overhead executor loss under an over-subscribed `executor.cores`. Returns the
   * base unchanged when the feature is disabled or the factor is 1.0. The caller's gate is
   * expected to always admit at least one in-flight item, so this can never deadlock even if
   * the scaled budget falls below a single item's size.
   */
  def scaleHostLimit(base: Long): Long = scaleLimit(base, factor())

  /**
   * Pure host-budget scaling: divide `base` by the back-pressure factor when `f > 1`, with a
   * floor of 1 so the budget never reaches zero. Side-effect free for unit testing.
   */
  private[rapids] def scaleLimit(base: Long, f: Double): Long = {
    if (f > 1.0) math.max(1L, (base / f).toLong) else base
  }

  /** For tests: stop the sampler and reset state. */
  def shutdown(): Unit = synchronized {
    if (sampler != null) {
      sampler.interrupt()
      sampler = null
    }
    enabled = false
    started = false
    currentFactor = 1.0
  }

  private def totalGcMillis(): Long = {
    var sum = 0L
    val it = gcBeans.iterator()
    while (it.hasNext) {
      val t = it.next().getCollectionTime
      if (t > 0) {
        sum += t
      }
    }
    sum
  }

  private def deviceMemRatio(): Double = {
    try {
      val total = GpuDeviceManager.getMemorySize
      if (total > 0) {
        Rmm.getTotalBytesAllocated.toDouble / total.toDouble
      } else {
        0.0
      }
    } catch {
      // Rmm may not be initialized in every context; treat as no device pressure.
      case _: Throwable => 0.0
    }
  }

  private class SamplerLoop extends Runnable {
    override def run(): Unit = {
      var lastGc = totalGcMillis()
      var lastWall = System.nanoTime()
      var keepGoing = true
      while (keepGoing) {
        try {
          Thread.sleep(sampleMs)
        } catch {
          case _: InterruptedException => keepGoing = false
        }
        if (keepGoing) {
          val nowGc = totalGcMillis()
          val nowWall = System.nanoTime()
          val wallMs = (nowWall - lastWall) / 1.0e6
          val gcFrac = if (wallMs > 0) (nowGc - lastGc) / wallMs else 0.0
          lastGc = nowGc
          lastWall = nowWall
          val devRatio = deviceMemRatio()
          val prev = currentFactor
          val next = computeNextFactor(prev, gcFrac, devRatio, gcGuard, deviceGuard,
            IncreaseMult, DecayStep, maxFactor)
          currentFactor = next
          if (next != prev) {
            if (next > prev) {
              logWarning(f"Adaptive back-pressure: gcFrac=$gcFrac%.3f devRatio=$devRatio%.3f " +
                f"factor $prev%.2f -> $next%.2f")
            } else {
              logDebug(f"Adaptive back-pressure relaxing: factor $prev%.2f -> $next%.2f")
            }
          }
        }
      }
    }
  }
}
