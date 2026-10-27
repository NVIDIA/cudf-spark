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

package com.nvidia.spark.rapids.io.async

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
 * ResourcePool interface to be implemented for AsyncRunners requiring different kinds of
 * resources.
 *
 * Currently, only HostMemoryPool is implemented, which limits the maximum in-flight host
 * memory bytes. In the future, we can add more.
 */
trait ResourcePool {
  /**
   * Attempts to acquire the resources required by the task, waiting up to the timeout in
   * milliseconds. Returns [[AcquireSuccessful]] when the resources are acquired,
   * [[AcquireFailed]] when the timeout expires, or [[AcquireExcepted]] when acquisition throws.
   */
  def acquireResource[T](task: AsyncRunner[T], timeout: Long): AcquireStatus

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def releaseResource[T](task: AsyncRunner[T]): Unit
}
