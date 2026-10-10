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

import java.util.concurrent.Callable

/**
 * Simple wrapper around a [[Callable]] that also keeps track of the host memory bytes used by
 * the task.
 *
 * Note: we may want to add more metadata to the task in the future, such as the device memory,
 * as we implement more throttling strategies.
 */
class Task[T](val hostMemoryBytes: Long, callable: Callable[T]) extends Callable[T] {
  override def call(): T = callable.call()
}

/**
 * Throttle interface to be implemented by different throttling strategies.
 *
 * Currently, only HostMemoryThrottle is implemented, which limits the maximum in-flight host
 * memory bytes. In the future, we can add more throttling strategies, such as limiting the
 * device memory usage, the number of tasks, etc.
 */
trait Throttle {

  /**
   * Returns true if the task can be accepted, false otherwise.
   * TrafficController will block the task from being scheduled until this method returns true.
   */
  def canAccept[T](task: Task[T]): Boolean

  /**
   * Callback to be called when a task is scheduled.
   */
  def taskScheduled[T](task: Task[T]): Unit

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def taskCompleted[T](task: Task[T]): Unit
}

/**
 * Throttle implementation that limits the total host memory used by the in-flight tasks.
 */
class HostMemoryThrottle(val maxInFlightHostMemoryBytes: Long) extends Throttle {
  private var totalHostMemoryBytes: Long = 0

  override def canAccept[T](task: Task[T]): Boolean = {
    totalHostMemoryBytes + task.hostMemoryBytes <= maxInFlightHostMemoryBytes
  }

  override def taskScheduled[T](task: Task[T]): Unit = {
    totalHostMemoryBytes += task.hostMemoryBytes
  }

  override def taskCompleted[T](task: Task[T]): Unit = {
    totalHostMemoryBytes -= task.hostMemoryBytes
  }

  def getTotalHostMemoryBytes: Long = totalHostMemoryBytes
}
