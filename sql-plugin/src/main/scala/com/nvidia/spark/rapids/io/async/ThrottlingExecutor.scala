/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async

import java.util.concurrent.{Callable, ExecutorService, Future, FutureTask, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.rapids.{ColumnarWriteTaskStatsTracker, GpuWriteTaskStatsTracker}


/**
 * Stats related classes used by ThrottlingExecutor
 */
case class ThrottlingExecutorStats (
    var numTasksScheduled: Int,
    var accumulatedThrottleTimeNs: Long,
    var minThrottleTimeNs: Long,
    var maxThrottleTimeNs: Long)

/**
 * Only for GpuWriteTaskStatsTracker cases
 */
class StatsUpdaterForWriteFunc(val statsTrackers: Seq[ColumnarWriteTaskStatsTracker]) {
  def func(stats: ThrottlingExecutorStats): Unit = {
    statsTrackers.foreach {
      case gpuStatsTracker: GpuWriteTaskStatsTracker =>
        gpuStatsTracker.setAsyncWriteThrottleTimes(
          stats.numTasksScheduled,
          stats.accumulatedThrottleTimeNs, stats.minThrottleTimeNs, stats.maxThrottleTimeNs)
      case _ =>
    }
  }
}

/**
 * Thin wrapper around an ExecutorService that adds throttling.
 *
 * The given executor is owned by this class and will be shutdown when this class is shutdown.
 */
class ThrottlingExecutor(executor: ExecutorService, throttler: TrafficController,
    updateStats : ThrottlingExecutorStats => Unit) {

  val stats: ThrottlingExecutorStats = ThrottlingExecutorStats(0, 0L, Long.MaxValue, 0L)

  private def blockUntilTaskRunnable(task: Task[_]): Unit = {
    val blockStart = System.nanoTime()
    throttler.blockUntilRunnable(task)
    val blockTimeNs = System.nanoTime() - blockStart
    stats.accumulatedThrottleTimeNs += blockTimeNs
    stats.minThrottleTimeNs = Math.min(stats.minThrottleTimeNs, blockTimeNs)
    stats.maxThrottleTimeNs = Math.max(stats.maxThrottleTimeNs, blockTimeNs)
    stats.numTasksScheduled += 1
    updateStats(stats)
  }

  private class ThrottledFutureTask[T](task: Task[T]) extends FutureTask[T](task) {
    private val started = new AtomicBoolean(false)
    private val admissionReleased = new AtomicBoolean(false)

    private def releaseAdmission(): Unit = {
      if (admissionReleased.compareAndSet(false, true)) {
        throttler.taskCompleted(task)
      }
    }

    override def run(): Unit = {
      started.set(true)
      try {
        super.run()
      } finally {
        releaseAdmission()
      }
    }

    def cancelBeforeRun(): Unit = {
      if (!started.get()) {
        cancel(false)
        if (!started.get()) {
          releaseAdmission()
        }
      }
    }
  }

  def submit[T](callable: Callable[T], hostMemoryBytes: Long): Future[T] = {
    val task = new Task[T](hostMemoryBytes, callable)
    blockUntilTaskRunnable(task)

    val futureTask = new ThrottledFutureTask[T](task)
    try {
      executor.execute(futureTask)
      futureTask
    } catch {
      case e: RejectedExecutionException =>
        // The task was admitted by the TrafficController but never handed to a worker.
        // Canceling completes the FutureTask and releases the admission through done().
        futureTask.cancelBeforeRun()
        throw e
    }
  }

  def shutdownNow(timeout: Long, timeUnit: TimeUnit): Unit = {
    updateStats(stats)
    val pendingTasks = executor.shutdownNow().iterator()
    while (pendingTasks.hasNext) {
      pendingTasks.next() match {
        case task: ThrottledFutureTask[_] =>
          // shutdownNow returns tasks that never started. Cancel them so their
          // TrafficController admission is released through done().
          task.cancelBeforeRun()
        case _ =>
      }
    }
    executor.awaitTermination(timeout, timeUnit)
  }
}
