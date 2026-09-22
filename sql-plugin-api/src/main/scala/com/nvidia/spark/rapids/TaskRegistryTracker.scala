/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.jni.RmmSpark
import java.util

import org.apache.spark.TaskContext

/**
 * This handles keeping track of task threads and registering them with RMMSpark
 * as needed. This is here to provide an efficient and lazy way to make sure that
 * we can use the Retry API behind the scenes without having to have callbacks
 * whenever a task starts, or trying to inject code in all of the operators that
 * would first start on the GPU.
 */
object TaskRegistryTracker {
  private val taskToThread = new util.HashMap[Long, ArrayBuffer[Long]]()
  private val registeredThreads = new util.HashSet[Long]()
  private val dedicatedTaskThreads = new util.HashSet[Long]()

  /**
   * Clear the registry. This is used for tests
   */
  def clearRegistry(): Unit = synchronized {
    val copied = new java.util.HashSet(taskToThread.keySet())
    copied.forEach { taskId =>
      taskIsDone(taskId)
    }
  }

  private def taskIsDone(taskId: Long): Unit = synchronized {
    val threads = taskToThread.remove(taskId)
    if (threads != null) {
      threads.foreach { threadId =>
        registeredThreads.remove(threadId)
        dedicatedTaskThreads.remove(threadId)
      }
      RmmSpark.taskDone(taskId)
    }
  }

  /**
   * Called only from `Plugin.onTaskStart` from Spark's thread pool for tasks.
   * This function registers the thread as "dedicated" (not a pool thread)
   */
  def registerDedicatedThreadForRetry(): Unit =
    registerThreadForRetry(addDedicatedThread = true)

  /**
   * Called to register a thread, pool or otherwise, with TaskRegistryTracker.
   * If the thread is a spark task thread, then set `addDedicatedThread` to true.
   * @param addDedicatedThread whether this thread is the spark specific task thread.
   */
  def registerThreadForRetry(addDedicatedThread: Boolean = false): Unit = synchronized {
    val tc = TaskContext.get()
    if (
      tc != null &&
        !RmmSpark.isThreadWorkingOnTaskAsPoolThread // check if the thread is a pool thread
    ) {
      // If we don't have a TaskContext we are either in a test or in some other thread
      // If it is some other thread, then they are responsible to make sure things are
      // registered properly themselves. If it is a test, well you need to update your
      // test code to make this work properly.
      val threadId = RmmSpark.getCurrentThreadId
      val taskId = tc.taskAttemptId()
      if (registeredThreads.add(threadId)) {
        if (addDedicatedThread) {
          RmmSpark.currentThreadIsDedicatedToTask(taskId)
          dedicatedTaskThreads.add(threadId)
        }
        if (!taskToThread.containsKey(taskId)) {
          taskToThread.put(taskId, ArrayBuffer(threadId))
          ScalableTaskCompletion.onTaskCompletion(tc) {
            taskIsDone(taskId)
          }
        } else {
          taskToThread.get(taskId) += threadId
        }
      } else if (addDedicatedThread && dedicatedTaskThreads.add(threadId)) {
        RmmSpark.currentThreadIsDedicatedToTask(taskId)
      }
    }
  }

  /**
   * Returns true only when the current thread was registered as Spark's dedicated task thread.
   */
  def isCurrentThreadRegisteredAsDedicatedTaskThread: Boolean = synchronized {
    dedicatedTaskThreads.contains(RmmSpark.getCurrentThreadId)
  }

  /**
   * Mark this dedicated task thread as waiting on another pool while executing the body.
   *
   * RmmSpark throws if pool-wait APIs are called from a pool thread, so callers that can
   * run from either task or helper threads should go through this guard.
   */
  def withRmmPoolWait[T](body: => T): T = {
    if (isCurrentThreadRegisteredAsDedicatedTaskThread) {
      RmmSpark.waitingOnPool()
      try {
        body
      } finally {
        RmmSpark.doneWaitingOnPool()
      }
    } else {
      body
    }
  }
}
