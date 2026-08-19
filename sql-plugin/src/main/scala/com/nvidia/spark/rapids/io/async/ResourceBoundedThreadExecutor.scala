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

import java.util.concurrent.{BlockingQueue, Callable, Future, FutureTask, PriorityBlockingQueue, RunnableFuture, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.TaskCompletionListener

/**
 * A RapidsFutureTask wrapper is used by ResourceBoundedThreadExecutor to manage the execution of
 * resource-aware asynchronous tasks. It allows the executor to:
 *   - Track whether the task is currently holding a resource.
 *   - [NOT enabled] Adjust the task's scheduling priority dynamically.
 *   - Mark completion and exception state for robust scheduling and error handling.
 *   - Integrate with resource management callbacks for proper resource release.
 *
 * @param runner the AsyncRunner to be executed and tracked
 * @tparam T the result type returned by the AsyncRunner
 */
class RapidsFutureTask[T](val runner: AsyncRunner[T])
    extends FutureTask[AsyncResult[T]](runner) with Logging {

  override def run(): Unit = runner.withStateLock { rr =>
    rr.getState match {
      case Running =>
        // Pass the schedule time to the task metrics builder
        rr.metricsBuilder.setScheduleTimeMs(scheduleTime)
        scheduleTime = 0L
        super.run()
        // Check if the runner completed successfully, since the exception shall be handled
        // quietly by FutureTask and recorded internally by `setException`.
        // Note: `super.isDone` is also true even if the task finished unsuccessfully. Therefore,
        // we need to check `rr.result` as well to determine if the task completed successfully
        if (rr.result.nonEmpty && super.isDone) {
          // runner.call has completed successfully
          rr.setState(Completed)
        } else if (runner.getState.isInstanceOf[ExecFailed]) {
          // runner.call has failed and the exception has been caught by setException
        } else if (isCancelled) {
          // the FutureTask was cancelled during execution by `cancel()`
          rr.setState(Cancelled)
        } else {
          // Failed due to unexpected exceptions
          val ex = new IllegalStateException("runner failed unexpectedly")
          rr.setState(ExecFailed(ex))
        }

      // Throw the ScheduleFailed exception within the scope of `FutureTask.run`, so that
      // the exception can be properly recorded and propagated to the caller of `get()`.
      case ScheduleFailed(ex: Throwable) =>
        // Trick: register a pre-hook to let `AsyncRunner.call` throw the exception
        rr.addPreHook(() => throw ex)
        super.run()

      // Handle the cancelled case as a special kind of ScheduleFailed
      case Cancelled =>
        logWarning(s"Runner being cancelled ahead of execution: $rr")
        rr.addPreHook(() => throw new IllegalStateException("cancelled"))
        super.run()

      // Expected states to bypass the execution
      case Pending =>

      case _ =>
        throw new IllegalStateException(s"should NOT reach this line: $rr")
    }
  }

  override def setException(e: Throwable): Unit = {
    runner.setState(ExecFailed(e))
    super.setException(e)
  }

  override def toString: String = {
    s"RapidsFutureTask(runner=$runner, scheduleTime=${scheduleTime / 1e6}ms)"
  }

  private[async] var scheduleTime: Long = 0L
}

/**
 * Comparator for RapidsFutureTask that orders tasks by priority in descending order.
 * Higher priority tasks (larger priority values) are ordered before lower priority tasks.
 * This enables priority-based scheduling in PriorityBlockingQueue where tasks with
 * higher priority are executed first.
 *
 * @tparam T the result type of the RapidsFutureTask
 */
class RapidsFutureTaskComparator[T] extends java.util.Comparator[RapidsFutureTask[T]] {
  override def compare(o1: RapidsFutureTask[T], o2: RapidsFutureTask[T]): Int = {
    o2.runner.priority.compareTo(o1.runner.priority)
  }
}

/**
 * A thread pool executor that integrates with resource management for executing AsyncRunners.
 *
 * This executor provides resource-aware task scheduling by:
 *   - Acquiring resources before runner execution through the ResourcePool
 *   - Tracking resource usage and releasing resources after task completion
 *   - Supporting priority-based runner ordering via PriorityBlockingQueue
 *   - Managing resource lifecycles including immediate and deferred release strategies
 *
 * AsyncRunners that cannot acquire sufficient resources within timeout are bypassed during
 * execution and re-queued to prevent starvation. The executor works exclusively with AsyncRunner
 * instances and their corresponding RapidsFutureTask wrappers.
 *
 * Resource release timing can be controlled by the implementation of the AsyncRunner.
 * Runners are able to provide callbacks for resource lifecycle management and supports both
 * immediate and deferred resource release patterns based on the corresponding AsyncResult
 * implementation.
 *
 * @param mgr the ResourcePool for managing resource acquisition and release
 * @param timeoutMs timeout in milliseconds for resource acquisition
 * @param corePoolSize the core number of threads in the pool
 * @param maximumPoolSize the maximum number of threads in the pool
 * @param workQueue the queue for holding tasks before execution
 * @param threadFactory the factory for creating new threads
 * @param keepAliveTime the time to keep idle threads alive
 */
class ResourceBoundedThreadExecutor(mgr: ResourcePool,
    timeoutMs: Long,
    corePoolSize: Int,
    maximumPoolSize: Int,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    keepAliveTime: Long = 100L) extends ThreadPoolExecutor(corePoolSize,
  maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, workQueue, threadFactory) with Logging {

  logInfo(s"Creating ResourceBoundedThreadExecutor with resourcePool: ${mgr.toString}, " +
      s"corePoolSize: $corePoolSize, maximumPoolSize: $maximumPoolSize, " +
      s"waitResourceTimeoutMs: $timeoutMs")

  override def submit[T](fn: Callable[T]): Future[T] = {
    fn match {
      case runner: AsyncRunner[_] =>
        super.submit(runner)
      case f =>
        throw new IllegalArgumentException(
          "ResourceBoundedThreadExecutor only accepts AsyncRunner," +
              s" but got: ${f.getClass.getName}")
    }
  }

  // This method is only for the extensions of RapidsFutureTask.
  override def submit[T](r: Runnable, result: T): Future[T] = {
    r match {
      case ft: RapidsFutureTask[_] =>
        super.submit(ft, null.asInstanceOf[T])
      case _ =>
        throw new UnsupportedOperationException("only accepts AsyncRunner or RapidsFutureTask")
    }
  }

  override def submit(r: Runnable): Future[_] = {
    throw new UnsupportedOperationException("only accepts AsyncRunner or RapidsFutureTask")
  }

  override protected def newTaskFor[T](fn: Callable[T]): RunnableFuture[T] = {
    fn match {
      case task: AsyncRunner[_] =>
        new RapidsFutureTask(task)
      case f =>
        throw new RuntimeException(s"Unexpected functor: ${f.getClass.getName}")
    }
  }

  override protected def newTaskFor[T](r: Runnable, result: T): RunnableFuture[T] = {
    r match {
      case futTask: RapidsFutureTask[_] =>
        futTask.asInstanceOf[RunnableFuture[T]]
      case task: AsyncRunner[_] =>
        new RapidsFutureTask(task).asInstanceOf[RunnableFuture[T]]
      case f =>
        throw new RuntimeException(s"Unexpected runnable: ${f.getClass.getName}")
    }
  }

  override def beforeExecute(t: Thread, r: Runnable): Unit = {
    val futTask = parseFutureTask(r)

    futTask.runner.withStateLock { rr =>
      // Check if the Spark task has been interrupted before execution
      rr.sparkTaskContext.foreach {
        case ctx if ctx.isInterrupted() => rr.setState(Cancelled)
        case  _ => // do nothing
      }

      // Check the runner state
      rr.getState match {
        // Cancelled case: Cancelled -> ScheduleFailed
        case Cancelled =>
          rr.setState(ScheduleFailed(new IllegalStateException("cancelled")))
          logWarning(s"Runner being cancelled ahead of execution: $rr")

        // The main path: Init -> Pending -> Running | Pending | ScheduleFailed
        case init: Init =>
          // Update the runner state: Init -> Pending
          rr.setState(Pending)
          // register all the callbacks for the first time
          if (init.firstTime) {
            setupReleaseCallback(futTask)
          }
          // Talk to the ResourcePool to acquire resource for the runner
          val acqStatus = mgr.acquireResource(rr, timeoutMs)
          // Update the runner state based on the acquisition result
          acqStatus match {
            // Proceed to execution: Pending -> Running
            case s: AcquireSuccessful =>
              rr.setState(Running)
              futTask.scheduleTime += s.elapsedTime
            // Fail the scheduling: Pending -> ScheduleFailed
            case AcquireExcepted(ex) =>
              rr.setState(ScheduleFailed(ex))
              logError(s"$ex [$rr]")
            // Bypass the execution: Pending -> Pending
            case AcquireFailed =>
          }

        // Unexpected states
        case _ =>
          // If we throw an exception here, it will crash the ThreadWorker without signaling
          // the caller. So we just mark the state as ScheduleFailed to pass the exception to
          // the caller via FutureTask.get().
          rr.setState(ScheduleFailed(new IllegalStateException("Unexpected state")))
          logError(s"Unexpected state before schedule: $rr")
      }
    }
  }

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    val futTask = parseFutureTask(r)

    futTask.runner.withStateLock { rr =>
      // Throw the unexpected exception if exists, since the FutureTask should have caught
      // and recorded the exception internally.
      if (t != null) {
        if (!rr.getState.isInstanceOf[ExecFailed]) {
          rr.setState(ExecFailed(t))
        }
        // Also try to fail the Spark task which launched this runner.
        rr.sparkTaskContext.foreach { ctx =>
          TrampolineUtil.markTaskFailed(ctx, t)
        }
        // Do not throw non-Fatal error, in case it crashes the entire Spark Executor.
        logError(s"Uncaught exception from $futTask: ${t.getMessage}", t)
      }

      // Post execution state handling
      rr.getState match {
        case Cancelled | // very rare case: cancelled between execution and afterExecute
             ExecFailed(_) => // failed execution (ScheduleFailed should be cast to ExecFailed)
          // release holding resource immediately on exception
          if (rr.isHoldingResource) {
            rr.releaseResourceCallback()
          }

        case Completed => // successful execution
          // release holding resource eagerly if the output does not hold the resource
          rr.result match {
            case None =>
              // Fatal error
              throw new IllegalStateException(s"In Completed State but NO Result: $rr")
            case Some(_: FastReleaseResult[_]) if rr.isHoldingResource =>
              rr.releaseResourceCallback()
            case _ =>
          }

        case Pending => // timeout during resource acquisition
          // Only runners in (`Completed`, `ExecFailed`, `Cancelled`) may hold resource
          require(!rr.isHoldingResource, s"Pending state should NOT hold Resource: $rr")
          // Requeue runners which failed to acquire resource and bypassed the execution.
          futTask.scheduleTime += timeoutMs * 1000000L
          rr.setState(Init(firstTime = false)) // reset to Init state for re-scheduling
          // Re-add the task to the work queue for re-execution
          if (!workQueue.add(futTask)) {
            // Fatal error
            throw new RuntimeException(
              s"Failed to re-add $futTask to the work queue after execution")
          }
          logDebug(s"Re-add timeout runner: $futTask")

        case _: Closed => // Do nothing since the runner has been closed

        case _ => // should NOT reach here
          // Fatal error
          throw new IllegalStateException(s"Unexpected state: $rr")
      }
    }
  }

  private def parseFutureTask(r: Runnable): RapidsFutureTask[_] = {
    r match {
      case fut: RapidsFutureTask[_] => fut
      case _ => throw new RuntimeException(s"Unexpected runnable: ${r.getClass.getName}")
    }
  }

  private def setupReleaseCallback[T](fut: RapidsFutureTask[T]): Unit = {
    // Bind the resource release callback to the AsyncRunner
    fut.runner.releaseResourceCallback = () => {
      require(fut.runner.isHoldingStateLock, "Must hold StateLock to release resource")

      fut.runner match {
        // Check if resource has already been released while waiting for StateLock
        case rr if rr.isHoldingResource =>
          val state = rr.getState
          require(state == Cancelled ||
              state == Completed || state.isInstanceOf[ExecFailed],
            s"Runner $rr is expected to be: Cancelled | Completed | ExecFailed")

          // Modify the status of HostMemoryPool
          mgr.releaseResource(rr)

          // Finalize the runner state
          state match {
            case Completed => // Completed -> Closed
              rr.setState(Closed(None))
            case ExecFailed(ex) => // ExecFailed -> Closed
              rr.setState(Closed(Some(ex)))
            case Cancelled => // Cancelled -> Closed
              rr.setState(Closed(Some(new IllegalStateException("cancelled"))))
            case _ =>
              throw new IllegalStateException(s"Should NOT reach here: $rr")
          }

        case rr => // already released
          logWarning(s"Resource has already been released by other thread: $rr")
      }
    }

    // Register a callback to ensure the AsyncRunner being fully closed when the Spark task
    // completes, regardless of whether the task succeeds or fails. This is a safety net to
    // prevent resource leaks in case that Spark task is killed or fails unexpectedly.
    fut.runner.sparkTaskContext.foreach { ctx =>
      ctx.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          if (context.isInterrupted()) {
            logError(s"[onTaskComp] Handle runner as Task was interrupted: ${fut.runner}")
          }
          // 1. If the runner is still running, we try to cancel it first
          if (fut.runner.getState == Running) {
            fut.cancel(true) // mayInterruptIfRunning = true
          }
          // 2. Mark the runner as Cancelled
          fut.runner.withStateLock { rr =>
            rr.getState match {
              case Init(_) => rr.setState(Cancelled) // Init -> Cancelled
              case Pending => rr.setState(Cancelled) // Pending -> Cancelled
              case Running => rr.setState(Cancelled) // Running -> Cancelled
              case ScheduleFailed(_) => rr.setState(Cancelled) // ScheduleFailed -> Cancelled
              case Completed => rr.setState(Cancelled) // Completed -> Cancelled
              case Cancelled | ExecFailed(_) | Closed(_) => // do nothing
            }
          }
          // 3. If the runner is still holding resource, we release it
          if (fut.runner.isHoldingResource) {
            fut.runner.withStateLock {
              _.releaseResourceCallback()
            }
          }
        }
      })
    }
  }
}

object ResourceBoundedThreadExecutor {
  def apply[T](name: String,
      pool: ResourcePool,
      maxThreadNumber: Int,
      waitResourceTimeoutMs: Long): ResourceBoundedThreadExecutor = {
    val taskQueue = new PriorityBlockingQueue(10000, new RapidsFutureTaskComparator[T])
    val threadFactory: ThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(name)
        .build()

    new ResourceBoundedThreadExecutor(pool,
      waitResourceTimeoutMs,
      corePoolSize = maxThreadNumber,
      maximumPoolSize = maxThreadNumber,
      workQueue = taskQueue.asInstanceOf[BlockingQueue[Runnable]],
      threadFactory = threadFactory)
  }
}
