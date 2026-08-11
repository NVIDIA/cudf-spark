/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.util

import org.apache.spark.TaskContext

/**
 * A handle that can be used to remove a callback if needed
 */
trait TaskCompletionCallbackHandle {
  /**
   * Removes the callback but does not call it if it has not already been called.
   */
  def removeCallback(): Unit

  /**
   * Removes the callback and calls it if it has not been called already.
   */
  def removeAndCall(): Unit
}

/**
 * Provides a task completion listeners in Spark that you can remove if needed to help with scaling.
 * Spark guarantees LIFO order to the callbacks, but we do not. If you need that kind of a
 * guarantee then use the Spark task APIs directly.
 */
object ScalableTaskCompletion {
  // Note: There are three levels of synchronization that can happen here. The order that they are
  // grabbed should always be
  //
  // ScalableTaskCompletion -> TopLevelTaskCompletion -> UserTaskCompletion
  //
  // this is to avoid any potential deadlocks

  /**
   * Wrapper around a user callback function.
   */
  private class UserTaskCompletion(val tc: TaskContext,
      f: Either[() => Unit, TaskContext => Unit])
      extends TaskCompletionCallbackHandle
          with Function[TaskContext, Unit] {
    private var wasCalled = false

    override def removeCallback(): Unit = {
      val topLevel = ScalableTaskCompletion.getTopLevel(tc.taskAttemptId())
      if (topLevel != null) {
        topLevel.remove(this)
      }
    }

    override def apply(tc: TaskContext): Unit = synchronized {
      if (!wasCalled) {
        wasCalled = true
        f match {
          case Left(l) => l()
          case Right(r) => r(tc)
        }
      }
    }

    override def removeAndCall(): Unit = {
      removeCallback()
      apply(tc)
    }
  }

  /**
   * Keeps track of user callbacks for a given task. It is also the callback from
   * Spark itself.
   */
  private class TopLevelTaskCompletion extends Function[TaskContext, Unit] {
    private val pending = new util.HashSet[UserTaskCompletion]()
    private var callbacksDone = false
    private var invokingACallback: Boolean = false

    private def throwIfInCallback(): Unit = {
      if (invokingACallback) {
        throw new IllegalStateException(
          s"Detected a task completion callback attempting " +
          "to add/remove callbacks. This is not supported.")
      }
    }

    private def callAllCallbacks(tc: TaskContext): Throwable = synchronized {
      throwIfInCallback()
      var error: Throwable = null
      pending.forEach { utc =>
        try {
          // this is true while we invoke the callback
          // so we can throw a bette error/stack trace
          // instead of a ConcurrentModificationException
          invokingACallback = true
          if (tc == null) {
            utc(utc.tc)
          } else {
            utc(tc)
          }
        } catch {
          case t: Throwable =>
            if (error == null) {
              error = t
            } else {
              error.addSuppressed(t)
            }
        } finally {
          invokingACallback = false
        }
      }
      pending.clear()
      callbacksDone = true
      error
    }

    override def apply(tc: TaskContext): Unit = {
      var error = callAllCallbacks(tc)
      try {
        ScalableTaskCompletion.removeTopLevel(tc.taskAttemptId())
      } catch {
        case t: Throwable =>
          if (error == null) {
            error = t
          } else {
            error.addSuppressed(t)
          }
      }
      if (error != null) {
        throw error
      }
    }

    def add(u: UserTaskCompletion): Unit = synchronized {
      throwIfInCallback()
      if (callbacksDone) {
        // Added a callback after it was done calling them back already
        u(u.tc)
      } else {
        pending.add(u)
      }
    }

    def remove(u: UserTaskCompletion): Unit = synchronized {
      throwIfInCallback()
      pending.remove(u)
    }

    def removeAllAndShutdown(): Unit = {
      val error = callAllCallbacks(null)
      if (error != null) {
        throw error
      }
    }
  }

  /**
   * Callbacks indexed by the task attempt id.
   */
  private val pendingCallbacks = new util.HashMap[Long, TopLevelTaskCompletion]

  private def getTopLevel(id: Long): TopLevelTaskCompletion = synchronized {
    pendingCallbacks.get(id)
  }

  private def removeTopLevel(id: Long): TopLevelTaskCompletion = synchronized {
    pendingCallbacks.remove(id)
  }

  private def add(ucb: UserTaskCompletion): TaskCompletionCallbackHandle = {
    val tc = ucb.tc
    val id = tc.taskAttemptId()
    synchronized {
      val tl = if (!pendingCallbacks.containsKey(id)) {
        val topLevel = new TopLevelTaskCompletion
        tc.addTaskCompletionListener[Unit](topLevel)
        pendingCallbacks.put(id, topLevel)
        topLevel
      } else {
        pendingCallbacks.get(id)
      }
      tl.add(ucb)
    }

    ucb
  }

  /**
   * When the passed in task context completes, call the given function
   * @return a handle that can be used to remove the callback
   */
  def onTaskCompletion(tc: TaskContext)(f: => Unit): TaskCompletionCallbackHandle =
    add(new UserTaskCompletion(tc, Left(() => f)))

  /**
   * When the passed in task context completes, call the given function.
   * @return a handle that can be used to remove the callback
   */
  def onTaskCompletion(tc: TaskContext, f: TaskContext => Unit): TaskCompletionCallbackHandle = {
    // Note the API here takes both a task context and the function at once. This is a little
    // uglier than what we support for the no argument version. This is because Scala says it
    // is ambiguous to have both here. The other version is more commonly used so it gets the
    // cleaner API.
    add(new UserTaskCompletion(tc, Right(f)))
  }

  /**
   * When the current task completes call the given function.
   * @return a handle that can be used to remove the callback
   */
  def onTaskCompletion(f: => Unit): TaskCompletionCallbackHandle = {
    val tc: TaskContext = TaskContext.get()
    add(new UserTaskCompletion(tc, Left(() => f)))
  }

  /**
   * When the current task completes call the given function
   * @return a handle that can be used to remove the callback
   */
  def onTaskCompletion(f: TaskContext => Unit): TaskCompletionCallbackHandle = {
    val tc: TaskContext = TaskContext.get()
    add(new UserTaskCompletion(tc, Right(f)))
  }

  /**
   * For testing only. This resets the state in case a test was using mocks and we didn't get
   * the callbacks we expected to clean things up. Any callbacks registered will be called, if
   * needed and then removed.
   */
  def reset(): Unit = synchronized {
    pendingCallbacks.values().forEach { topLevel =>
      topLevel.removeAllAndShutdown()
    }
    pendingCallbacks.clear()
  }
}
