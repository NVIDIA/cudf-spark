/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.util.PriorityQueue
import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.spark.sql.rapids.GpuTaskMetrics

class PrioritySemaphore[T](val maxPermits: Long, val maxConcurrentGpuTasksLimit: Int)
  (implicit ordering: Ordering[T]) {
  // This lock is used to generate condition variables, which affords us the flexibility to notify
  // specific threads at a time. If we use the regular synchronized pattern, we have to either
  // notify randomly, or if we try creating condition variables not tied to a shared lock, they
  // won't work together properly, and we see things like deadlocks.
  private val lock = new ReentrantLock()
  private var occupiedSlots: Long = 0
  private var currentConcurrentGpuTasksNum: Long = 0

  private case class ThreadInfo(priority: T,
                                condition: Condition,
                                computeNumPermits: () => Long,
                                wasOnGpuBefore: () => Boolean,
                                taskId: Long) {
    var signaled: Boolean = false
    var permitsUsed: Long = 0
  }

  // use task id as tie breaker when priorities are equal (both are 0 because never hold lock)
  private val priorityComp = Ordering.by[ThreadInfo, T](_.priority).reverse.
    thenComparing((a, b) => a.taskId.compareTo(b.taskId))

  // We expect a relatively small number of threads to be contending for this lock at any given
  // time, therefore we are not concerned with the insertion/removal time complexity.
  private val waitingQueue: PriorityQueue[ThreadInfo] =
    new PriorityQueue[ThreadInfo](priorityComp)

  def tryAcquire(numPermits: Long,
                 priority: T,
                 wasOnGpuBefore: () => Boolean,
                 taskAttemptId: Long): Boolean = {
    lock.lock()
    try {
      if (waitingQueue.size() > 0 &&
        priorityComp.compare(
          waitingQueue.peek(),
          ThreadInfo(priority, null, () => numPermits, wasOnGpuBefore, taskAttemptId)
        ) < 0) {
        false
      } else if (!canAcquire(numPermits)) {
        false
      } else {
        commitAcquire(numPermits)
        true
      }
    } finally {
      lock.unlock()
    }
  }

  def acquire(computePermits: () => Long, wasOnGpuBefore: () => Boolean,
              priority: T, taskAttemptId: Long): Long = {
    lock.lock()
    try {
      val numPermitsNow = computePermits()
      if (!tryAcquire(numPermitsNow, priority, wasOnGpuBefore, taskAttemptId)) {
        val condition = lock.newCondition()
        val info = ThreadInfo(priority, condition, computePermits, wasOnGpuBefore, taskAttemptId)
        try {
          waitingQueue.add(info)
          // only count tasks that had held semaphore before,
          // so they're very likely to have remaining data on GPU
          GpuTaskMetrics.get.recordOnGpuTasksWaitingNumber(
            waitingQueue.iterator().asScala.count(_.wasOnGpuBefore()))

          while (!info.signaled) {
            info.condition.await()
          }
          info.permitsUsed
        } catch {
          case e: Exception =>
            waitingQueue.remove(info)
            if (info.signaled) {
              release(info.permitsUsed)
            }
            throw e
        }
      } else {
        numPermitsNow
      }
    } finally {
      lock.unlock()
    }
  }

  private def commitAcquire(numPermits: Long): Unit = {
    occupiedSlots += numPermits
    currentConcurrentGpuTasksNum += 1
    // Report current concurrent tasks to GpuTaskMetrics, let it handle max tracking
    GpuTaskMetrics.get.recordConcurrentGpuTasks(currentConcurrentGpuTasksNum)
  }

  def release(numPermits: Long): Unit = {
    lock.lock()
    try {
      occupiedSlots -= numPermits
      currentConcurrentGpuTasksNum -= 1
      // acquire and wakeup for all threads that now have enough permits
      var done = false
      while (!done && waitingQueue.size() > 0) {
        val nextThread = waitingQueue.peek()
        val threadPermits = nextThread.computeNumPermits()
        if (canAcquire(threadPermits)) {
          val popped = waitingQueue.poll()
          assert(popped eq nextThread)
          commitAcquire(threadPermits)
          nextThread.signaled = true
          nextThread.permitsUsed = threadPermits
          nextThread.condition.signal()
        } else {
          done = true
        }
      }
    } finally {
      lock.unlock()
    }
  }

  private def canAcquire(numPermits: Long): Boolean = {
    val hasPermits = occupiedSlots + numPermits <= maxPermits
    val withinTaskLimit = maxConcurrentGpuTasksLimit <= 0 ||
      currentConcurrentGpuTasksNum < maxConcurrentGpuTasksLimit
    hasPermits && withinTaskLimit
  }
}
