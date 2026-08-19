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

import java.util.concurrent.{Callable, CompletionService, Future, LinkedBlockingQueue, TimeUnit}

/**
 * A CompletionService implementation specialized for ResourceBoundedThreadExecutor that provides
 * resource-aware async task execution and completion handling. This service extends the standard
 * Java CompletionService pattern by accepting only AsyncRunner instances, wrapping them in
 * RapidsFutureTask for resource tracking, and maintaining completion order through a completion
 * queue for efficient result polling.
 *
 * The service is particularly useful for scenarios where multiple async tasks need to be
 * submitted and their results processed as they complete, without blocking on any specific
 * task completion order. This pattern is commonly used in multi-file readers and other
 * batch processing scenarios where work can be parallelized effectively.
 *
 * @param executor the ResourceBoundedThreadExecutor to use for task execution
 * @tparam V the result type of the AsyncRunners
 */
class BoundedCompletionService[V](
    executor: ResourceBoundedThreadExecutor) extends CompletionService[AsyncResult[V]] {

  private val completionQueue = new LinkedBlockingQueue[Future[AsyncResult[V]]]()

  private class CompletionFutureTask(
      task: AsyncRunner[V]) extends RapidsFutureTask[V](task) {

    override def done(): Unit = {
      completionQueue.offer(this)
      super.done()
    }
  }

  override def submit(task: Callable[AsyncResult[V]]): Future[AsyncResult[V]] = {
    task match {
      case runner: AsyncRunner[V] =>
        val futureTask = new CompletionFutureTask(runner)
        executor.submit(futureTask, null.asInstanceOf[AsyncResult[V]])
      case _ =>
        throw new IllegalArgumentException("Task must be an instance of AsyncRunner")
    }
  }

  override def submit(task: Runnable, result: AsyncResult[V]): Future[AsyncResult[V]] = {
    throw new UnsupportedOperationException("Runnable tasks are not supported")
  }

  override def take(): Future[AsyncResult[V]] = completionQueue.take()

  override def poll(): Future[AsyncResult[V]] = completionQueue.poll()

  override def poll(timeout: Long, unit: TimeUnit): Future[AsyncResult[V]] = {
    completionQueue.poll(timeout, unit)
  }
}
