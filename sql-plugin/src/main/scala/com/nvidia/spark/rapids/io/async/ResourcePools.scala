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

package com.nvidia.spark.rapids.io.async

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.bytesToString

// Being thrown when a task requests resources that are not valid or exceed the limits
class InvalidResourceRequest(msg: String) extends RuntimeException(
  s"Invalid resource request: $msg")

// Represents the status of acquiring resources for a task
sealed trait AcquireStatus

case class AcquireSuccessful(elapsedTime: Long) extends AcquireStatus

// AcquireFailed indicates that the task could not be scheduled due to resource constraints
case object AcquireFailed extends AcquireStatus

// AcquireExcepted indicates that an exception occurred while trying to acquire resources
case class AcquireExcepted(exception: Throwable) extends AcquireStatus

/**
 * ResourceManager interface to be implemented for AsyncRunners requiring different kinds of
 * resources.
 *
 * Currently, only HostMemoryManager is implemented, which limits the maximum in-flight host
 * memory bytes. In the future, we can add more.
 */
trait ResourcePool {
  /**
   * Returns true if the task can be accepted, false otherwise.
   * TrafficController will block the task from being scheduled until this method returns true.
   */
  def acquireResource[T](task: AsyncRunner[T], timeout: Long): AcquireStatus

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def releaseResource[T](task: AsyncRunner[T]): Unit
}

/**
 * HostMemoryPool enforces a maximum limit on total host memory bytes that can be held
 * by in-flight tasks simultaneously. It provides blocking resource acquisition with
 * configurable timeout and supports both individual and grouped task allocation patterns.
 *
 * The implementation uses condition variables to efficiently block and wake up waiting
 * tasks when resources become available through task completion and resource release.
 */
class HostMemoryPool(val maxHostMemoryBytes: Long) extends ResourcePool with Logging {

  private val lock = new ReentrantLock()

  private val condition = lock.newCondition()

  // Tracking running AsyncRunners which actually acquires host memory, which is mainly for deadlock
  // prevention for now.
  private val numRunnerInFlight: AtomicLong = new AtomicLong(0L)

  // It is safe to use thread-nonsafe variables because below states are only accessed within
  // the lock guarded critical section.
  private var remaining: Long = maxHostMemoryBytes

  // Only counts for the AsyncRunners which actually acquired host memory.
  private var numRunnerInPool: Long = 0L

  private val tasksInPool = mutable.HashMap[Long, Long]()

  override def acquireResource[T](runner: AsyncRunner[T], timeoutMs: Long): AcquireStatus = {
    // step 1: extract the resource requirements and runner info
    val memoryRequire: Long = extractResource(runner).hostMemoryBytes

    // step 2: try to acquire the resource with blocking and timeout
    // 2.1 If no resource needed, acquire immediately
    if (memoryRequire == 0L) {
      AcquireSuccessful(elapsedTime = 0L)
    }
    // 2.2 The main path for acquiring resource with blocking and timeout
    else {
      var isDone = false
      var isTimeout = false
      val timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
      var waitTimeNs = timeoutNs
      // Enter into the critical section which is guarded by the lock from concurrent access
      lock.lockInterruptibly()
      try {
        // [Deadlock Prevention]
        // Due to the decay release, the virtual memory limit may interact with other dependency
        // mechanisms, such as in a local join. In this scenario, both sides of the join may
        // perform multithreaded scans limited by the HostMemoryPool. The join operator requires
        // outputs from both sides, but one side may occupy all the memory budget, leaving the
        // other side blocked and waiting for memory to be released.
        //
        // [Solution]
        // If there is no runner in flight, run the request runner immediately regardless of
        // the current available resource.
        if (remaining < memoryRequire &&
            numRunnerInFlight.compareAndSet(0L, 1L)) {
          // The remaining resource should be negative here
          remaining -= memoryRequire
          isDone = true
          // Register a post-hook to decrement numRunnerInFlight as soon as the runner is done
          runner.addPostHook(() => numRunnerInFlight.decrementAndGet())
        } else {
          do {
            // If enough resource is available, acquire it and exit the loop.
            if (remaining >= memoryRequire) {
              remaining -= memoryRequire
              isDone = true
              // Update numRunnerInFlight as soon as the runner being permitted to run, meanwhile
              // register a post-hook to decrement it as soon as the runner is done
              numRunnerInFlight.incrementAndGet()
              runner.addPostHook(() => numRunnerInFlight.decrementAndGet())
            } else if (waitTimeNs > 0) {
              waitTimeNs = condition.awaitNanos(waitTimeNs)
            } else {
              isTimeout = true
              logWarning(s"Failed to acquire ${bytesToString(memoryRequire)}, remaining=" +
                  s"${bytesToString(remaining)}, AsyncRunners=$numRunnerInPool, " +
                  s"SparkTasks=${tasksInPool.size}")
            }
          } while (!isDone && !isTimeout)
        }
        if (!isDone) {
          AcquireFailed
        } else {
          // Update nonAtomic states if the resource is acquired successfully
          numRunnerInPool += 1L
          runner.sparkTaskContext.foreach { ctx =>
            val tid = ctx.taskAttemptId()
            val numRunner = tasksInPool.getOrElse(tid, 0L)
            tasksInPool.put(tid, numRunner + 1L)
          }
          // Callback to the runner for post-acquire actions, should keep thread-safe
          runner.onAcquire()
          // Log a warning when the resource is over-committed
          if (remaining < 0) {
            logWarning(
              s"Over-committed HostMemoryPool: exceeded_amount=${bytesToString(-remaining)}, " +
                  s"AsyncRunners=$numRunnerInPool, SparkTasks=${tasksInPool.size}")
          }
          AcquireSuccessful(elapsedTime = timeoutNs - waitTimeNs)
        }
      } catch {
        case ex: Throwable => AcquireExcepted(ex)
      } finally {
        lock.unlock()
      }
    }
  }

  override def releaseResource[T](runner: AsyncRunner[T]): Unit = {
    val toRelease = extractResource(runner).hostMemoryBytes
    if (toRelease > 0) {
      // Enter into the critical section if there is actual resource to release for.
      lock.lock()
      // Update nonAtomic states
      numRunnerInPool -= 1L
      remaining += toRelease // release the memory back to the pool
      runner.sparkTaskContext.foreach { ctx =>
        val tid = ctx.taskAttemptId()
        val runnersForTask = tasksInPool.getOrElse(tid, 0L)
        // It is possible runnersForTask == 0, if some runners were cancelled from caller side
        if (runnersForTask <= 1L) {
          tasksInPool -= tid
          logDebug(s"[LOG POINT] remaining=${bytesToString(remaining)}, " +
              s"AsyncRunners=$numRunnerInPool, SparkTasks=${tasksInPool.size})")
        } else {
          tasksInPool.put(tid, runnersForTask - 1L)
        }
      }
      // Waking up waiters
      condition.signalAll()
      lock.unlock()
    }
    // Callback to the runner for post-release actions, should keep thread-safe
    runner.onRelease()
  }

  override def toString: String = {
    s"HostMemoryPool(maxHostMemoryBytes=${bytesToString(maxHostMemoryBytes)})"
  }

  private def extractResource(task: AsyncRunner[_]): HostResource = {
    task.resource match {
      case r: HostResource => r
      case r => throw new InvalidResourceRequest(
        s"Task ${task.getClass.getName} does not require HostResource, but got $r")
    }
  }
}
