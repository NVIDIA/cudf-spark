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

package org.apache.spark.sql.rapids

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.DeterministicLevel

/**
 * Drives the UCX listener with the scheduler events Spark posts for SQL executions, with no
 * SparkContext, and checks what it hands to the reclaim at each execution's end.
 */
class UcxSqlEndListenerSuite extends AnyFunSuite {
  import UcxReclaimEventBuilder._

  /**
   * A listener whose candidates are `candidates`, and which records each hand-over: the ended
   * execution, its shuffles and their watermark.
   */
  private class Harness(candidates: Set[Int]) {
    val handed = new ArrayBuffer[(Long, Seq[Int], Long)]()
    val listener = new UcxSqlEndListener(candidates.contains,
      (executionId, shuffleIds, watermark) => handed += ((executionId, shuffleIds, watermark)))

    /** Submits a determinate map stage of `shuffleId` under `executionId`. */
    def submit(executionId: Long, stageId: Int, shuffleId: Int): Unit =
      listener.onStageSubmitted(stageSubmitted(mapStage(stageId, shuffleId), executionId))
  }

  test("a shuffle counts for an execution only when its map stage is submitted under that " +
      "execution while it runs") {
    val h = new Harness(Set(1, 2, 3, 4, 5))
    // before the execution's start
    h.submit(7L, stageId = 1, shuffleId = 1)
    h.listener.onExecutionStart(7L)
    h.submit(7L, stageId = 2, shuffleId = 2)
    // under an execution that never started, outside SQL, and not a GPU candidate
    h.submit(8L, stageId = 3, shuffleId = 3)
    h.listener.onStageSubmitted(stageSubmittedOutsideSql(mapStage(4, 4)))
    h.submit(7L, stageId = 6, shuffleId = 6)
    h.listener.onStageSubmitted(stageSubmitted(resultStage(9), 7L))
    h.listener.onTaskStart(taskStart(stageId = 2, taskId = 10L))
    h.listener.onExecutionEnd(7L)
    // after the execution's end
    h.submit(7L, stageId = 5, shuffleId = 5)
    h.listener.onExecutionEnd(7L)
    assertResult(Seq((7L, Seq(2), 10L)))(h.handed)
  }

  test("an execution with no candidate shuffle hands nothing to the reclaim") {
    val h = new Harness(Set(1))
    h.listener.onExecutionStart(7L)
    h.submit(7L, stageId = 2, shuffleId = 2)
    h.listener.onExecutionEnd(7L)
    assert(h.handed.isEmpty, h.handed)
  }

  test("an indeterminate map stage, or one that lists no RDD, waits for GC, and the " +
      "determinate shuffles of its execution are reclaimed") {
    val h = new Harness(Set(1, 2, 3))
    h.listener.onExecutionStart(7L)
    h.submit(7L, stageId = 1, shuffleId = 1)
    val indeterminate = Seq(rddInfo(20, DeterministicLevel.INDETERMINATE),
      rddInfo(21, DeterministicLevel.DETERMINATE))
    h.listener.onStageSubmitted(stageSubmitted(mapStage(2, 2, indeterminate), 7L))
    h.listener.onStageSubmitted(stageSubmitted(mapStage(3, 3, Nil), 7L))
    h.listener.onTaskStart(taskStart(stageId = 1, taskId = 4L))
    h.listener.onExecutionEnd(7L)
    assertResult(Seq((7L, Seq(1), 4L)))(h.handed)
  }

  test("the watermark is the largest task id seen before the execution's end") {
    val h = new Harness(Set(1, 2, 3, 4))
    h.listener.onExecutionStart(1L)
    h.submit(1L, stageId = 1, shuffleId = 1)
    Seq(3L, 9L, 5L).foreach(id => h.listener.onTaskStart(taskStart(stageId = 1, taskId = id)))
    h.listener.onExecutionEnd(1L)
    // a later task counts for a later execution, and two that overlap share the task ids
    h.listener.onExecutionStart(2L)
    h.listener.onExecutionStart(3L)
    h.submit(2L, stageId = 2, shuffleId = 2)
    h.submit(3L, stageId = 3, shuffleId = 3)
    h.listener.onTaskStart(taskStart(stageId = 2, taskId = 30L))
    h.listener.onExecutionEnd(2L)
    h.listener.onTaskStart(taskStart(stageId = 3, taskId = 31L))
    h.listener.onExecutionEnd(3L)
    assertResult(Seq((1L, Seq(1), 9L), (2L, Seq(2), 30L), (3L, Seq(3), 31L)))(h.handed)
  }

  test("a shuffle whose map stage reruns under a later execution is handed to each, once") {
    val h = new Harness(Set(1, 2))
    (1L to 2L).foreach { executionId =>
      h.listener.onExecutionStart(executionId)
      // a reused exchange's stage, and a retried attempt, are submitted again
      h.submit(executionId, stageId = 10 + executionId.toInt, shuffleId = 1)
      h.submit(executionId, stageId = 10 + executionId.toInt, shuffleId = 1)
      h.submit(executionId, stageId = 20 + executionId.toInt, shuffleId = 2)
      h.listener.onTaskStart(taskStart(stageId = 1, taskId = executionId * 10))
      h.listener.onExecutionEnd(executionId)
    }
    assertResult(Seq((1L, Seq(1, 2), 10L), (2L, Seq(1, 2), 20L)))(h.handed)
  }

  test("the shutdown drops the shuffles of the executions still running") {
    val h = new Harness(Set(1))
    h.listener.onExecutionStart(7L)
    h.submit(7L, stageId = 1, shuffleId = 1)
    h.listener.shutdown()
    h.listener.onExecutionEnd(7L)
    assert(h.handed.isEmpty, h.handed)
  }
}
