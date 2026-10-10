/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.metrics

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
 * GpuBubbleTime, a metric that measures periods when the GPU is underutilized during execution
 * of non-GPU workload. This helps identify CPU or IO bottlenecks preventing full GPU utilization.
 *
 * GpuBubbleTimerManager is designed to work as a singleton across the Spark executor that
 * coordinates multiple GpuBubbleTimer instances. The manager tracks GPU utilization state
 * based on the number of threads waiting for GpuSemaphore.
 *
 * GpuBubbleTimerManager applies the event-driven approach to manage the timers:
 * When there are no waiting threads (indicating GPU underutilization), all registered timers
 * start accumulating time. When threads begin waiting for GPU access (indicating GPU saturation),
 * timers are paused. All status changes are notified to the manager as soon as they occur via
 * addWaiter/removeWaiter calls.
 *
 * The manager ensures thread-safety through synchronized methods when updating timer states, and
 * uses atomic operations to track waiting thread count, providing accurate GPU bubble time metrics
 * with minimal overhead.
 */
class GpuBubbleTimerManager private {

  /**
   * Notify the manager that a thread starts to wait for GpuSemaphore.
   */
  def addWaiter(): Unit = {
    // Pause timers once waiting thread occurs, which indicates that the GPU utilization
    // changes from underutilized to busy.
    if (waitingThreads.incrementAndGet() == 1) {
      traverseTimers(isStart = false)
    }
  }

  /**
   * Notify the manager that a thread is no longer waiting for GpuSemaphore.
   */
  def removeWaiter(): Unit = {
    // Resume timers once there is no more waiting threads, which indicates that
    // the GPU utilization changes from busy to underutilized.
    if (waitingThreads.decrementAndGet() == 0) {
      traverseTimers(isStart = true)
    }
  }

  // Register a new timer to the manager. The timer is activated immediately upon registration.
  // This method is synchronized to ensure handling activeTimers in a thread-safe manner.
  private def registerTimer(timer: GpuBubbleTimer): Unit = synchronized {
    timer.activate(isBusyNow = waitingThreads.get() > 0)
    activeTimers += timer
  }

  // Traverse all active timers to update their status as the update of GPU utilization state.
  // This method is synchronized to ensure handling activeTimers in a thread-safe manner.
  private def traverseTimers(isStart: Boolean): Unit = synchronized {
    if (activeTimers.isEmpty) {
      return
    }
    val toRemove = mutable.ArrayBuffer.empty[GpuBubbleTimer]
    val ts = System.nanoTime()
    // Update all active timers meanwhile remove those have been already closed by callers.
    if (isStart) {
      activeTimers.foreach { timer =>
        if (!timer.startIfActive(ts)) toRemove += timer
      }
    } else {
      activeTimers.foreach { timer =>
        if (!timer.pauseIfActive(ts)) toRemove += timer
      }
    }
    toRemove.foreach(activeTimers.remove)
  }

  private val activeTimers = mutable.HashSet.empty[GpuBubbleTimer]

  private val waitingThreads = new AtomicInteger(0)
}

object GpuBubbleTimerManager {

  def getInstance: GpuBubbleTimerManager = instance

  def newTimer(): GpuBubbleTimer = {
    val timer = new GpuBubbleTimer()
    instance.registerTimer(timer)
    timer
  }

  // Singleton instance that manages GpuBubbleTime tracking across the entire Spark executor.
  private lazy val instance: GpuBubbleTimerManager = new GpuBubbleTimerManager()
}

/**
 * The timer that tracks GPU bubble time for a specific non-GPU workload. This timer is
 * designed to be managed by GpuBubbleTimerManager, which coordinates multiple timers
 * across the Spark executor.
 */
class GpuBubbleTimer {

  private[metrics] def activate(isBusyNow: Boolean): Unit = synchronized {
    require(!active, "Timer is already active")
    require(elapsedNanos == 0L, "Timer was already used")
    active = true
    if (!isBusyNow) {
      startTime = System.nanoTime()
    }
  }

  /**
   * Atomic operation to start the timer if it is active, otherwise return the inactive status.
   * This is an internal method which shall only be called by GpuBubbleTimerManager.
   */
  private[metrics] def startIfActive(ts: Long): Boolean = synchronized {
    if (!active) {
      return false
    }
    if (startTime == 0L) {
      startTime = ts
    }
    true
  }

  /**
   * Atomic operation to pause the timer if it is active, otherwise return the inactive status.
   * This is an internal method which shall only be called by GpuBubbleTimerManager.
   */
  private[metrics] def pauseIfActive(ts: Long): Boolean = synchronized {
    if (!active) {
      return false
    }
    if (startTime > 0L) {
      elapsedNanos += ts - startTime
      startTime = 0L
    }
    true
  }

  /**
   * Close the timer and return the total elapsed bubble time in nanoseconds.
   * After closing, the timer cannot be reused. And the timer will be removed from the manager
   * during the next traversal.
   *
   * @param ts the timestamp when the measured workload finishes
   */
  def close(ts: Long): Long = synchronized {
    require(active, "Timer is not active")
    active = false
    if (startTime > 0L) {
      elapsedNanos += ts - startTime
      startTime = 0L
    }
    // Do not reset elapsedNanos to 0L, so that we can detect if the timer was already used.
    elapsedNanos
  }

  private var active = false

  private var startTime: Long = 0L

  private var elapsedNanos: Long = 0L
}
