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

package org.apache.spark.sql.rapids.metrics.source

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.util.TaskCompletionListener

class MockTaskContextSuite extends AnyFunSuite {

  private def makeListener(action: => Unit): TaskCompletionListener =
    new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = action
    }

  test("pre-completion listeners execute in reverse registration order before completion " +
      "listeners") {
    val events = new ArrayBuffer[String]()
    val context = new MockTaskContext(taskAttemptId = 1L, partitionId = 0)

    context.addTaskCompletionListener(makeListener {
      events += "completion-1"
    })
    context.addTaskPreCompletionListener(makeListener {
      events += "pre-1"
    })
    context.addTaskPreCompletionListener(makeListener {
      events += "pre-2"
      context.addTaskPreCompletionListener(makeListener {
        events += "pre-nested"
      })
    })
    context.addTaskCompletionListener(makeListener {
      events += "completion-2"
    })

    context.markTaskComplete()

    assertResult(List(
      "pre-2",
      "pre-nested",
      "pre-1",
      "completion-1",
      "completion-2"
    ))(events.toList)
  }

  test("pre-completion listener registered after task completion delegates to completion " +
      "listener") {
    val events = new ArrayBuffer[String]()
    val context = new MockTaskContext(taskAttemptId = 2L, partitionId = 0) {
      override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
        events += "registered-completion"
        super.addTaskCompletionListener(listener)
      }
    }

    context.addTaskPreCompletionListener(makeListener {
      events += "pre-before-complete"
      context.addTaskPreCompletionListener(makeListener {
        events += "pre-during-complete"
      })
    })
    assertResult(List.empty)(events.toList)

    context.markTaskComplete()
    assert(context.isCompleted())
    assertResult(List(
      "pre-before-complete",
      "pre-during-complete"
    ))(events.toList)

    events.clear()
    val lateListener = makeListener {
      events += "pre-after-complete"
    }
    context.addTaskPreCompletionListener(lateListener)
    assertResult(List("registered-completion"))(events.toList)
    assert(context.listeners.last eq lateListener)
    context.listeners.last.onTaskCompletion(context)
    assertResult(List("registered-completion", "pre-after-complete"))(events.toList)
  }

  test("throwing pre-completion listener does not prevent remaining pre-completion and " +
      "completion listeners from running") {
    val events = new ArrayBuffer[String]()
    val context = new MockTaskContext(taskAttemptId = 4L, partitionId = 0)
    val failure1 = new RuntimeException("pre-completion listener 2 failed")
    val failure2 = new RuntimeException("completion listener 1 failed")

    context.addTaskCompletionListener(makeListener {
      events += "completion-1"
      throw failure2
    })
    context.addTaskCompletionListener(makeListener {
      events += "completion-2"
    })
    context.addTaskPreCompletionListener(makeListener {
      events += "pre-1"
    })
    context.addTaskPreCompletionListener(makeListener {
      events += "pre-2"
      throw failure1
    })

    val thrown = intercept[RuntimeException](context.markTaskComplete())
    assert(thrown eq failure1)
    assertResult(Seq(failure2))(thrown.getSuppressed.toSeq)
    assert(context.isCompleted())
    assertResult(List(
      "pre-2",
      "pre-1",
      "completion-1",
      "completion-2"
    ))(events.toList)
  }

  test("anonymous MockTaskContext subclass overrides work with pre-completion listeners") {
    val events = new ArrayBuffer[String]()
    val context = new MockTaskContext(taskAttemptId = 3L, partitionId = 0) {
      override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
        events += "custom-completion-registered"
        super.addTaskCompletionListener(listener)
      }
    }

    context.addTaskCompletionListener(makeListener {
      events += "completion"
    })
    context.addTaskPreCompletionListener(makeListener {
      events += "pre-completion"
    })
    context.markTaskComplete()

    assertResult(List(
      "custom-completion-registered",
      "pre-completion",
      "completion"
    ))(events.toList)
  }
}
