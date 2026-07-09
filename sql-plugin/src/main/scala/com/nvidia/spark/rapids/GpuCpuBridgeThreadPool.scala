/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.util.Comparator
import java.util.concurrent.{Callable, FutureTask, PriorityBlockingQueue, RunnableFuture, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import com.nvidia.spark.rapids.jni.{RmmSpark, TaskPriority}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Scheduling priority of a CPU bridge task. Lower priority value runs first; ties break FIFO
 * by submitTime.
 */
private[rapids] trait CpuBridgeTaskPriority {
  def priority: Long
  def submitTime: Long
}

/**
 * A task submitted to the CPU bridge thread pool with priority information.
 */
case class PrioritizedCpuBridgeTask[T](
    task: Callable[T],
    taskContext: TaskContext,
    submitTime: Long = System.nanoTime()) extends Callable[T] with CpuBridgeTaskPriority {
  
  // Higher priority = lower value (processed first)
  def priority: Long = {
    if (taskContext != null) {
      TaskPriority.getTaskPriority(taskContext.taskAttemptId())
    } else {
      Long.MaxValue // Default priority for tasks without context
    }
  }
  
  override def call(): T = {
    if (taskContext != null) {
      // Install the task's TaskContext and attribute GPU work here to the owning task. This is
      // not a retry boundary; retryable failures should propagate to the caller instead of
      // blocking shared pool threads.
      TrampolineUtil.setTaskContext(taskContext)
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        task.call()
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    } else {
      task.call()
    }
  }
}

/**
 * Comparator for prioritizing CPU bridge tasks.
 * Tasks with higher priority (lower priority value) are executed first.
 * If priorities are equal, earlier submitted tasks are processed first.
 */
object CpuBridgeTaskComparator extends Comparator[Runnable] {
  override def compare(r1: Runnable, r2: Runnable): Int = {
    (r1, r2) match {
      case (t1: CpuBridgeTaskPriority, t2: CpuBridgeTaskPriority) =>
        val priorityComparison = java.lang.Long.compare(t1.priority, t2.priority)
        if (priorityComparison != 0) {
          priorityComparison
        } else {
          // If priorities are equal, FIFO order (earlier submitted first)
          java.lang.Long.compare(t1.submitTime, t2.submitTime)
        }
      case (_: CpuBridgeTaskPriority, _) => -1 // Prioritized tasks come first
      case (_, _: CpuBridgeTaskPriority) => 1  // Prioritized tasks come first
      case _ => 0 // Non-prioritized tasks are equal
    }
  }
}

/**
 * A FutureTask that carries the priority of the PrioritizedCpuBridgeTask it wraps, so the
 * priority queue can still order it after submit() turns the task into a future.
 */
private[rapids] class ComparableFutureTask[T](prioritized: PrioritizedCpuBridgeTask[T])
    extends FutureTask[T](prioritized) with CpuBridgeTaskPriority {
  override def priority: Long = prioritized.priority
  override def submitTime: Long = prioritized.submitTime
}

/**
 * Enhanced thread pool for CPU bridge expression evaluation with priority queue,
 * task context propagation, and RmmSpark integration.
 */
object GpuCpuBridgeThreadPool extends Logging {

  // We want a decent number of rows because the code is more efficient
  // with larger batches, even on the CPU
  private val MIN_ROWS_PER_SUBBATCH = 500000  // Min size for each sub-batch
  
  // Lazy initialization of the thread pool
  @volatile private var threadPool: Option[ThreadPoolExecutor] = None
  @volatile private var hasShutdown: Boolean = false
  @volatile private var shutdownHookRegistered: Boolean = false
  private val lock = new Object()
  
  /**
   * Get the thread pool size, either from config override or calculated from task slots.
   */
  private def getThreadPoolSize: Int = {
    val rapidsConf = new RapidsConf(SparkEnv.get.conf)
    
    // Check for explicit override first
    rapidsConf.getCpuBridgeThreadPoolSize match {
      case Some(overrideSize) =>
        logInfo(s"Using CPU bridge thread pool size override: $overrideSize")
        overrideSize
      case None =>
        // Use default calculation based on task slots
        val sparkConf = SparkEnv.get.conf
        // Use the Rapids plugin's method to estimate cores, which considers executor cores
        val estimatedCores = RapidsPluginUtils.estimateCoresOnExec(sparkConf)
        val taskSlots = sparkConf.getInt("spark.task.cpus", 1)
        val maxTasks = estimatedCores / taskSlots

        logDebug(s"Estimated cores: $estimatedCores, task CPUs: $taskSlots, max tasks: $maxTasks")
        if (maxTasks < 1) {
          logWarning(s"Calculated CPU bridge max tasks as $maxTasks from $estimatedCores " +
            s"estimated cores and $taskSlots CPUs per task; using 1 thread")
        }

        // Ensure we have at least 1 thread. The upper bound is naturally limited by
        // maxTasks, which is derived from the estimated cores divided by the CPUs per task.
        Math.max(1, maxTasks)
    }
  }
  
  /**
   * Create a thread factory that properly sets up GPU device and integrates with RmmSpark.
   */
  private def createThreadFactory(): ThreadFactory = {
    val baseFactory: ThreadFactory = (r: Runnable) => {
      val thread = new Thread(r)
      thread.setName(s"gpu-cpu-bridge-worker-${thread.getId}")
      thread.setDaemon(true)
      thread
    }

    GpuDeviceManager.wrapThreadFactory(baseFactory)
  }
  
  /**
   * Get the shared thread pool, creating it if necessary.
   * The pool size is based on configured task slots rather than CPU cores.
   * 
   * @throws IllegalStateException if the thread pool has been shut down
   */
  def getThreadPool: ThreadPoolExecutor = {
    if (hasShutdown) {
      throw new IllegalStateException("Cannot get thread pool after it has been shut down")
    }
    
    threadPool match {
      case Some(pool) => pool
      case None =>
        lock.synchronized {
          if (hasShutdown) {
            throw new IllegalStateException("Cannot get thread pool after it has been shut down")
          }
          
          threadPool match {
            case Some(pool) => pool
            case None =>
              val poolSize = getThreadPoolSize
              
              logDebug(s"Creating CPU bridge thread pool with $poolSize threads")
              
              // Create priority queue for task ordering
              val taskQueue = new PriorityBlockingQueue[Runnable](512, CpuBridgeTaskComparator)
              
              // Create thread pool with priority queue and custom thread factory
              val newPool = new ThreadPoolExecutor(
                poolSize,           // core pool size
                poolSize,           // maximum pool size  
                60L,                // keep alive time
                TimeUnit.SECONDS,   // time unit
                taskQueue,          // work queue with priority
                createThreadFactory() // thread factory with GPU setup
              ) {
                // Wrap prioritized tasks so the queued future keeps its priority.
                override protected def newTaskFor[T](callable: Callable[T]): RunnableFuture[T] =
                  callable match {
                    case p: PrioritizedCpuBridgeTask[_] =>
                      new ComparableFutureTask[T](p.asInstanceOf[PrioritizedCpuBridgeTask[T]])
                    case _ => super.newTaskFor(callable)
                  }
              }
              
              threadPool = Some(newPool)
              
              // Register JVM shutdown hook only once
              if (!shutdownHookRegistered) {
                Runtime.getRuntime.addShutdownHook(new Thread("gpu-cpu-bridge-shutdown") {
                  override def run(): Unit = shutdown()
                })
                shutdownHookRegistered = true
              }
              
              newPool
          }
        }
    }
  }
  
  /**
   * Submit a prioritized task to the thread pool.
   * The task will inherit the current task context and be prioritized appropriately.
   */
  def submitPrioritizedTask[T](task: Callable[T]): java.util.concurrent.Future[T] = {
    val currentTaskContext = TaskContext.get()
    val prioritizedTask = PrioritizedCpuBridgeTask(task, currentTaskContext)
    getThreadPool.submit(prioritizedTask)
  }
  
  /**
   * Determine if a batch should be processed in parallel based on its size.
   */
  def shouldParallelize(numRows: Int): Boolean = {
    numRows >= MIN_ROWS_PER_SUBBATCH
  }
  
  /**
   * Calculate the optimal number of sub-batches for a given row count.
   */
  def getSubBatchCount(numRows: Int): Int = {
    if (!shouldParallelize(numRows)) {
      1
    } else {
      val poolSize = getThreadPool.getCorePoolSize
      val idealSubBatches = Math.ceil(numRows.toDouble / MIN_ROWS_PER_SUBBATCH).toInt
      Math.min(idealSubBatches, poolSize * 2) // Allow up to 2x pool size for better utilization
    }
  }
  
  /**
   * Calculate sub-batch boundaries for splitting a batch.
   * Returns (start, end) pairs for each sub-batch.
   */
  def getSubBatchRanges(numRows: Int): Seq[(Int, Int)] = {
    getSubBatchRanges(numRows, getSubBatchCount(numRows))
  }

  private[rapids] def getSubBatchRanges(
      numRows: Int,
      subBatchCount: Int): Seq[(Int, Int)] = {
    if (subBatchCount <= 1) {
      Seq((0, numRows))
    } else {
      val baseSize = numRows / subBatchCount
      val remainder = numRows % subBatchCount
      
      (0 until subBatchCount).map { i =>
        val start = i * baseSize + Math.min(i, remainder)
        val size = baseSize + (if (i < remainder) 1 else 0)
        val end = start + size
        (start, end)
      }
    }
  }
  
  /**
   * Shutdown the thread pool gracefully.
   * After calling this method, any attempts to get the thread pool will throw an exception.
   */
  def shutdown(): Unit = {
    lock.synchronized {
      if (hasShutdown) {
        return // Already shut down
      }
      
      threadPool.foreach { pool =>
        logInfo("Shutting down CPU bridge thread pool")
        pool.shutdown()
        try {
          if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            logWarning("Thread pool did not terminate gracefully, forcing shutdown")
            pool.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            logWarning("Interrupted while waiting for thread pool shutdown")
            pool.shutdownNow()
        }
      }
      
      threadPool = None
      hasShutdown = true
    }
  }
}
