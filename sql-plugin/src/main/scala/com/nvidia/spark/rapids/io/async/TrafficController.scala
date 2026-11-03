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

import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import com.nvidia.spark.rapids.{RapidsConf, TaskRegistryTracker}

/**
 * TrafficController is responsible for blocking tasks from being scheduled when the throttle
 * is exceeded. It also keeps track of the number of tasks that are currently scheduled.
 *
 * This class is thread-safe as it is used by multiple tasks.
 */
class TrafficController protected[rapids] (@GuardedBy("lock") throttle: Throttle) {

  @GuardedBy("lock")
  private var numTasks: Int = 0

  private val lock = new ReentrantLock()
  private val canBeScheduled = lock.newCondition()

  /**
   * Blocks the task from being scheduled until the throttle allows it. If there is no task
   * currently scheduled, the task is scheduled immediately even if the throttle is exceeded.
   */
  def blockUntilRunnable[T](task: Task[T]): Unit = {
    lock.lockInterruptibly()
    try {
      while (numTasks > 0 && !throttle.canAccept(task)) {
        TaskRegistryTracker.withRmmPoolWait {
          canBeScheduled.await()
        }
      }
      numTasks += 1
      throttle.taskScheduled(task)
    } finally {
      lock.unlock()
    }
  }

  def taskCompleted[T](task: Task[T]): Unit = {
    // Not interruptible: this runs in the finally of a task whose thread may have been
    // interrupted, and skipping it would leave the task scheduled for good.
    lock.lock()
    try {
      numTasks -= 1
      throttle.taskCompleted(task)
      canBeScheduled.signal()
    } finally {
      lock.unlock()
    }
  }

  def numScheduledTasks: Int = {
    lock.lockInterruptibly()
    try {
      numTasks
    } finally {
      lock.unlock()
    }
  }
}

object TrafficController {

  @GuardedBy("this")
  private var writeInstance: TrafficController = _

  @GuardedBy("this")
  private var readInstance: TrafficController = _

  /**
   * Initializes the TrafficController. Currently we have two instances, one for
   * write operations and one for read operations.
   *
   * This is called once per executor.
   */
  def initialize(conf: RapidsConf): Unit = synchronized {
    if (writeInstance == null) {
      writeInstance = new TrafficController(
        new HostMemoryThrottle(
          if (conf.asyncWriteMaxInFlightHostMemoryBytes > 0L) {
            conf.asyncWriteMaxInFlightHostMemoryBytes
          } else {
            Long.MaxValue
          }))
    }
    if (readInstance == null) {
      readInstance = new TrafficController(
        new HostMemoryThrottle(
          if (conf.asyncReadMaxInFlightHostMemoryBytes > 0L) {
            conf.asyncReadMaxInFlightHostMemoryBytes
          } else {
            Long.MaxValue
          }))
    }
  }

  def getWriteInstance: TrafficController = synchronized {
    writeInstance
  }

  def getReadInstance: TrafficController = synchronized {
    readInstance
  }

  def shutdown(): Unit = synchronized {
    if (writeInstance != null) {
      writeInstance = null
    }
    if (readInstance != null) {
      readInstance = null
    }
  }
}
