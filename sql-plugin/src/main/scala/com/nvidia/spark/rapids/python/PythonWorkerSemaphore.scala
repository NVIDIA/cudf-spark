/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.python

import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import scala.annotation.nowarn

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.python.PythonConfEntries.CONCURRENT_PYTHON_WORKERS
import org.apache.commons.lang3.mutable.MutableInt

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging

/*
 * PythonWorkerSemaphore is used to limit the number of Python workers(processes) to be started
 * by an executor.
 *
 * This PythonWorkerSemaphore will not initialize the GPU, different from GpuSemaphore. Since
 * tasks calling the API `acquireIfNecessary` are supposed not to use the GPU directly, but
 * delegate the permits to the Python workers respectively.
 *
 * Call `acquireIfNecessary` or `releaseIfNecessary` directly when needed, since the inner
 * semaphore will be initialized implicitly, but need to call `shutdown` explicitly to release
 * the inner semaphore when no longer needed.
 *
 */
object PythonWorkerSemaphore extends Logging {

  private lazy val rapidsConf = new RapidsConf(SparkEnv.get.conf)
  private lazy val workersPerGpu = rapidsConf.get(CONCURRENT_PYTHON_WORKERS)
  private lazy val enabled = workersPerGpu > 0

  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile
  private var instance: PythonWorkerSemaphore = _

  private def getInstance(): PythonWorkerSemaphore = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          logDebug(s"Initialize the python workers semaphore with number: $workersPerGpu")
          instance = new PythonWorkerSemaphore(workersPerGpu)
        }
      }
    }
    instance
  }

  /*
   * Tasks must call this when they begin to start a Python worker who will use GPU.
   * If the task has not already acquired the GPU semaphore then it is acquired,
   * blocking if necessary.
   * NOTE: A task completion listener will automatically be installed to ensure
   *       the semaphore is always released by the time the task completes.
   */
  def acquireIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.acquireIfNecessary(context)
    }
  }

  /*
   * Tasks must call this when they are finished using the GPU.
   */
  def releaseIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.releaseIfNecessary(context)
    }
  }

  /*
   * Release the inner semaphore.
   * NOTE: This does not wait for active tasks to release!
   */
  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
    }
  }
}

private final class PythonWorkerSemaphore(tasksPerGpu: Int) extends Logging {
  private val semaphore = new Semaphore(tasksPerGpu)
  // Map to track which tasks have acquired the semaphore.
  private val activeTasks = new ConcurrentHashMap[Long, MutableInt]

  def acquireIfNecessary(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    val refs = activeTasks.get(taskAttemptId)
    if (refs == null) {
      // first time this task has been seen
      activeTasks.put(taskAttemptId, new MutableInt(1))
      onTaskCompletion(tc => completeTask(tc))
    } else {
      refs.increment()
    }
    logDebug(s"Task $taskAttemptId acquiring GPU for python worker")
    semaphore.acquire()
  }

  def releaseIfNecessary(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    val refs = activeTasks.get(taskAttemptId)
    if (refs != null &&
        (refs.getValue > 0: @nowarn("msg=getValue in class MutableInt is deprecated"))) {
      logDebug(s"Task $taskAttemptId releasing GPU for python worker")
      (semaphore.release(
        refs.getValue): @nowarn("msg=getValue in class MutableInt is deprecated"))
      refs.setValue(0)
    }
  }

  def completeTask(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    val refs = activeTasks.remove(taskAttemptId)
    if (refs == null) {
      throw new IllegalStateException(s"Completion of unknown task $taskAttemptId")
    }
    if ((refs.getValue > 0: @nowarn("msg=getValue in class MutableInt is deprecated"))) {
      logDebug(s"Task $taskAttemptId releasing all GPU resources for python worker")
      (semaphore.release(
        refs.getValue): @nowarn("msg=getValue in class MutableInt is deprecated"))
    }
  }

  def shutdown(): Unit = {
    if (!activeTasks.isEmpty) {
      logDebug(s"Shutting down Python worker semaphore with ${activeTasks.size} " +
        s"tasks still registered")
    }
  }
}
