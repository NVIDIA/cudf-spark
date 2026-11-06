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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.LogCaptureUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.Success
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{JobSucceeded, StageInfo}
import org.apache.spark.storage.RDDInfo

/**
 * Drives `UcxReclaimPolicy` with scheduler events built on the test thread, in orders that a
 * live listener bus leaves to timing. No SparkContext, bus or GPU is involved, and the sink
 * records each call into the manager without acting on it.
 */
class UcxReclaimPolicySuite extends AnyFunSuite {
  import UcxReclaimEventBuilder._
  import UcxReclaimPolicySuite._

  /**
   * Execution 1's job 0, up to the submission of its result stage 11. Map stage 10 writes
   * shuffle 0 with task 1 on e1 and completes, and stage 11 reads shuffle 0.
   */
  private def submitResultStage(f: Fixture): StageInfo = {
    val stage10 = mapStage(10, shuffleId = 0, parentIds = Nil)
    val stage11 = resultStage(11, parentIds = Seq(10))
    f.policy.onExecutionStart(1)
    f.policy.onJobStart(jobStart(0, 1, Seq(stage10, stage11)))
    f.policy.onStageSubmitted(stageSubmitted(stage10, 1))
    f.policy.onTaskStart(taskStart(10, 0, 1, "e1"))
    f.policy.onTaskEnd(taskEnd(10, 0, 1, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage10))
    f.policy.onStageSubmitted(stageSubmitted(stage11, 1))
    stage11
  }

  /** Task 2 of result stage 11 runs on e1, and the stage fails. */
  private def failResultStage(f: Fixture, stage11: StageInfo): Unit = {
    f.policy.onTaskStart(taskStart(11, 0, 2, "e1"))
    f.policy.onTaskEnd(taskEnd(11, 0, 2, "e1", taskKilled))
    f.policy.onStageCompleted(stageFailed(stage11))
  }

  /** Each dispatch's shuffles as (shuffle, owner, targets), for a case that leaves W out. */
  private def ownersAndTargets(f: Fixture): Seq[Seq[(Int, Long, Set[String])]] =
    f.sink.dispatches.map(_.map(d => (d.shuffleId, d.owner, d.targets)))

  /**
   * A task of stage 11 starts on e2 after its job failed and before execution 1 ends, with
   * `lateStageAttemptId` in its TaskStart, and a task that never started ends.
   */
  private def assertLateTaskStartHoldsTheShuffle(lateStageAttemptId: Int): Unit = {
    val f = new Fixture(Set(0))
    failResultStage(f, submitResultStage(f))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    f.policy.onTaskStart(taskStart(11, lateStageAttemptId, 3, "e2"))
    f.policy.onTaskEnd(taskEnd(11, 0, 99, "e1", taskKilled))
    f.policy.onExecutionEnd(1)
    assert(f.sink.dispatches.isEmpty, f.sink.calls)

    // past the owner's end plus 10 minutes, whether the deadline check is strict or not
    f.nowMs += TimeUnit.MINUTES.toMillis(10) + 1
    val logged = LogCaptureUtils.captureLogsFrom(Seq(classOf[UcxReclaimPolicy].getName)) {
      f.policy.onExecutorMetricsUpdate(driverMetricsUpdate)
    }
    val warns = logged.filter(_.startsWith(UcxReclaimPolicy.R6_WARN_PREFIX))
    assert(warns.length == 1, logged.mkString("\n"))
    val warn = warns.head
    assert(warn.contains("stage 11"), warn)
    assert(warn.contains("no live attempt"), warn)
    // the running task ids follow the word "task" in the WARN
    assert("""task\D*\b3\b""".r.findFirstIn(warn).isDefined, warn)

    // a TaskEnd carries the task's own attempt, whatever its TaskStart carried
    f.policy.onTaskEnd(taskEnd(11, 0, 3, "e2", taskKilled))
    assertResult(Seq(Seq(ShuffleReclaimDecision(0, 1L, 3L, Set("e1", "e2")))), f.sink.calls)(
      f.sink.dispatches)
  }

  /**
   * Execution 1 runs to its end. Its job 0 has map stage 30, which writes shuffle 0 from
   * `mapRdds`, and result stage 31, which reads it, each with one task on e1. Returns the
   * fixture and the skips recorded by the time stage 30 was submitted.
   */
  private def runR7Case(mapRdds: Seq[RDDInfo]): (Fixture, Seq[Skip]) = {
    val f = new Fixture(Set(0))
    val stage30 = mapStage(30, 0, Nil, mapRdds)
    val stage31 = resultStage(31, parentIds = Seq(30))
    f.policy.onExecutionStart(1)
    f.policy.onJobStart(jobStart(0, 1, Seq(stage30, stage31)))
    f.policy.onStageSubmitted(stageSubmitted(stage30, 1))
    val skipsAtSubmission = f.sink.skips
    f.policy.onTaskStart(taskStart(30, 0, 1, "e1"))
    f.policy.onTaskEnd(taskEnd(30, 0, 1, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage30))
    f.policy.onStageSubmitted(stageSubmitted(stage31, 1))
    f.policy.onTaskStart(taskStart(31, 0, 2, "e1"))
    f.policy.onTaskEnd(taskEnd(31, 0, 2, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage31))
    f.policy.onJobEnd(jobEnd(0, JobSucceeded))
    f.policy.onExecutionEnd(1)
    (f, skipsAtSubmission)
  }

  test("a shuffle is not dispatched at its execution's end while its failed job is active, " +
      "and is dispatched at that job's late JobEnd") {
    val f = new Fixture(_ => true)
    failResultStage(f, submitResultStage(f))
    f.policy.onExecutionEnd(1)
    assert(f.sink.dispatches.isEmpty, f.sink.calls)
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    assertResult(Seq(Seq((0, 1L, Set("e1")))), f.sink.calls)(ownersAndTargets(f))
  }

  test("a shuffle whose failed job ended before its execution is dispatched at the " +
      "execution's end") {
    val f = new Fixture(_ => true)
    failResultStage(f, submitResultStage(f))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    assert(f.sink.dispatches.isEmpty, f.sink.calls)
    f.policy.onExecutionEnd(1)
    assertResult(Seq(Seq((0, 1L, Set("e1")))), f.sink.calls)(ownersAndTargets(f))
  }

  test("a task that starts after its job failed, but before the decision, holds the shuffle " +
      "until it ends, is named by the R6 WARN and becomes a target") {
    assertLateTaskStartHoldsTheShuffle(lateStageAttemptId = 0)
  }

  test("a late task start that carries stage attempt -1, as Spark 3.3.0 to 3.3.2 post it, " +
      "holds the shuffle the same way") {
    assertLateTaskStartHoldsTheShuffle(lateStageAttemptId = -1)
  }

  test("task events after a shuffle's decision change neither its targets nor its watermark, " +
      "and leave no state") {
    val f = new Fixture(Set(0))
    failResultStage(f, submitResultStage(f))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    f.policy.onExecutionEnd(1)
    val decided = Seq(Seq(ShuffleReclaimDecision(0, 1L, 2L, Set("e1"))))
    assertResult(decided, f.sink.calls)(f.sink.dispatches)

    f.policy.onTaskStart(taskStart(11, 0, 3, "e2"))
    f.policy.onTaskEnd(taskEnd(11, 0, 3, "e2", taskKilled))
    // task 3 is above the watermark, so the reclaim keeps whatever it wrote
    assertResult(decided, f.sink.calls)(f.sink.dispatches)
    val held = f.policy.heldState
    assert(held.stages.isEmpty, held)
    assert(held.liveAttempts.isEmpty, held)
    assert(held.runningTasks.isEmpty, held)
  }

  test("an executor that leaves before a shuffle's decision is not one of its targets, and the " +
      "manager hears of its removal") {
    val f = new Fixture(Set(0))
    val stage11 = submitResultStage(f)
    f.policy.onTaskStart(taskStart(11, 0, 2, "e2"))
    // the scheduler's event loop ends a lost executor's tasks after Spark posts its removal
    f.policy.onExecutorRemoved(executorRemoved("e2"))
    f.policy.onTaskEnd(taskEnd(11, 0, 2, "e2", taskKilled))
    f.policy.onTaskStart(taskStart(11, 0, 3, "e1"))
    f.policy.onTaskEnd(taskEnd(11, 0, 3, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage11))
    f.policy.onJobEnd(jobEnd(0, JobSucceeded))
    f.policy.onExecutionEnd(1)
    assertResult(Seq(Seq(ShuffleReclaimDecision(0, 1L, 3L, Set("e1")))), f.sink.calls)(
      f.sink.dispatches)
    assert(f.sink.calls.contains(ExecutorRemoved("e2")), f.sink.calls)
  }

  test("a stage attempt that completes twice leaves no live attempt, so its shuffle is " +
      "dispatched") {
    val f = new Fixture(Set(0))
    val stage11 = submitResultStage(f)
    f.policy.onTaskStart(taskStart(11, 0, 2, "e1"))
    f.policy.onTaskEnd(taskEnd(11, 0, 2, "e1", taskKilled))
    // Spark 4.0.0 and later post a second StageCompleted for an attempt that aborts after
    // repeated fetch failures
    f.policy.onStageCompleted(stageFailed(stage11))
    f.policy.onStageCompleted(stageFailed(stage11))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    f.policy.onExecutionEnd(1)
    assertResult(Seq(Seq(0)), f.sink.calls)(f.sink.dispatches.map(_.map(_.shuffleId)))
    val held = f.policy.heldState
    assert(!held.liveAttempts.exists(_._1 == 11), held)
  }

  test("a stage that was never submitted leaves with its failed job, and a later execution " +
      "owns its ownerless shuffle afresh") {
    val f = new Fixture(Set(0, 1))
    val stage20 = mapStage(20, shuffleId = 1, parentIds = Nil)
    val stage21 = mapStage(21, shuffleId = 0, parentIds = Seq(20))
    val stage22 = resultStage(22, parentIds = Seq(21))
    f.policy.onExecutionStart(1)
    f.policy.onJobStart(jobStart(0, 1, Seq(stage20, stage21, stage22)))
    f.policy.onStageSubmitted(stageSubmitted(stage20, 1))
    f.policy.onTaskStart(taskStart(20, 0, 1, "e1"))
    f.policy.onTaskEnd(taskEnd(20, 0, 1, "e1", taskKilled))
    f.policy.onStageCompleted(stageFailed(stage20))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    f.policy.onExecutionEnd(1)
    assertResult(Seq(Seq(1)), f.sink.calls)(f.sink.dispatches.map(_.map(_.shuffleId)))
    val held = f.policy.heldState
    assert(held.stages.isEmpty, held)
    assert(!held.shuffles.contains(0), held)

    val stage23 = mapStage(23, shuffleId = 0, parentIds = Nil)
    val stage24 = resultStage(24, parentIds = Seq(23))
    f.policy.onExecutionStart(2)
    f.policy.onJobStart(jobStart(1, 2, Seq(stage23, stage24)))
    f.policy.onStageSubmitted(stageSubmitted(stage23, 2))
    f.policy.onTaskStart(taskStart(23, 0, 2, "e1"))
    f.policy.onTaskEnd(taskEnd(23, 0, 2, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage23))
    f.policy.onStageSubmitted(stageSubmitted(stage24, 2))
    f.policy.onTaskStart(taskStart(24, 0, 3, "e1"))
    f.policy.onTaskEnd(taskEnd(24, 0, 3, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage24))
    f.policy.onJobEnd(jobEnd(1, JobSucceeded))
    f.policy.onExecutionEnd(2)
    assertResult(Seq(Seq((1, 1L)), Seq((0, 2L))), f.sink.calls)(
      f.sink.dispatches.map(_.map(d => (d.shuffleId, d.owner))))
  }

  test("R7 skips a shuffle when its map stage is submitted with an indeterminate head RDD, " +
      "and never dispatches it") {
    val (f, skipsAtSubmission) = runR7Case(Seq(rddInfo(30, DeterministicLevel.INDETERMINATE)))
    assertResult(Seq(Skip(0, 1L, ShuffleReclaimRule.R7)), f.sink.calls)(skipsAtSubmission)
    assert(f.sink.dispatches.isEmpty, f.sink.calls)
  }

  test("R7 skips a shuffle whose map stage lists no RDD, and the policy stays enabled") {
    val (f, skipsAtSubmission) = runR7Case(Nil)
    assertResult(Seq(Skip(0, 1L, ShuffleReclaimRule.R7)), f.sink.calls)(skipsAtSubmission)
    assert(f.sink.dispatches.isEmpty, f.sink.calls)
    assert(!f.sink.isDisabled, f.sink.calls)
  }

  test("R7 reads only the head RDD, so a map stage with a determinate head and an " +
      "indeterminate ancestor is dispatched") {
    val (f, _) = runR7Case(Seq(rddInfo(30, DeterministicLevel.DETERMINATE),
      rddInfo(29, DeterministicLevel.INDETERMINATE)))
    assertResult(Seq(Seq(0)), f.sink.calls)(f.sink.dispatches.map(_.map(_.shuffleId)))
  }

  test("a held shuffle that Spark's own cleanup drops, and a later job registers again, is " +
      "left to GC, and no decision covers that job's writers") {
    val candidates = scala.collection.mutable.Set(0)
    val f = new Fixture(candidates)
    val stage11 = submitResultStage(f)
    // task 2 of the reading stage outlives execution 1, so R6 holds shuffle 0 at its end
    f.policy.onTaskStart(taskStart(11, 0, 2, "e1"))
    f.policy.onStageCompleted(stageFailed(stage11))
    f.policy.onJobEnd(jobEnd(0, jobFailed))
    f.policy.onExecutionEnd(1)
    // Spark's cleanup unregisters shuffle 0, and the registry loses it
    candidates -= 0
    // execution 2 runs the same plan, registers shuffle 0 again and reruns its map stage
    val stage12 = mapStage(12, shuffleId = 0, parentIds = Nil)
    val stage13 = resultStage(13, parentIds = Seq(12))
    f.policy.onExecutionStart(2)
    f.policy.onJobStart(jobStart(1, 2, Seq(stage12, stage13)))
    f.policy.onStageSubmitted(stageSubmitted(stage12, 2))
    f.policy.onTaskStart(taskStart(12, 0, 3, "e1"))
    f.policy.onTaskEnd(taskEnd(11, 0, 2, "e1", taskKilled))
    assert(f.sink.dispatches.isEmpty, f.sink.calls)
    assertResult(Seq(Skip(0, 1L, ShuffleReclaimRule.R2)), f.sink.calls)(f.sink.skips)
  }

  test("an indeterminate shuffle that a later job of its execution reads leaves no state") {
    val f = new Fixture(Set(0))
    val stage10 = mapStage(10, 0, Nil, Seq(rddInfo(10, DeterministicLevel.INDETERMINATE)))
    val stage11 = resultStage(11, parentIds = Seq(10))
    f.policy.onExecutionStart(1)
    f.policy.onJobStart(jobStart(0, 1, Seq(stage10)))
    f.policy.onStageSubmitted(stageSubmitted(stage10, 1))
    f.policy.onTaskStart(taskStart(10, 0, 1, "e1"))
    f.policy.onTaskEnd(taskEnd(10, 0, 1, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage10))
    f.policy.onJobEnd(jobEnd(0, JobSucceeded))
    // AQE's next job lists the map stage again, skipped, and submits the stage that reads it
    f.policy.onJobStart(jobStart(1, 1, Seq(stage10, stage11)))
    f.policy.onStageSubmitted(stageSubmitted(stage11, 1))
    f.policy.onTaskStart(taskStart(11, 0, 2, "e1"))
    f.policy.onTaskEnd(taskEnd(11, 0, 2, "e1", Success))
    f.policy.onStageCompleted(stageCompleted(stage11))
    f.policy.onJobEnd(jobEnd(1, JobSucceeded))
    f.policy.onExecutionEnd(1)
    assertResult(Seq(Skip(0, 1L, ShuffleReclaimRule.R7)), f.sink.calls)(f.sink.skips)
    val held = f.policy.heldState
    assert(held.stages.isEmpty && held.shuffles.isEmpty, held)
  }
}

object UcxReclaimPolicySuite {
  /** A call that the policy made into the manager. */
  private sealed trait Call

  private case class Dispatch(
      fenceId: Long,
      shuffles: Seq[ShuffleReclaimDecision],
      decidedAtMs: Long) extends Call

  private case class Skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule)
    extends Call

  private case class ExecutionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int])
    extends Call

  private case class ExecutorRemoved(executorId: String) extends Call

  private case class Release(fenceId: Long, shuffleId: Int) extends Call

  private case class Withdraw(fenceId: Long, shuffleId: Int) extends Call

  private case class Disable(reason: String) extends Call

  /**
   * Records each call the policy makes, in order, and acts on none of them. A recorded
   * `disable` sets the flag that `isDisabled` reads, as the manager's flag would.
   */
  private class RecordingSink extends ShuffleReclaimHandoff {
    val calls = new ArrayBuffer[Call]()

    override def dispatch(
        fenceId: Long,
        shuffles: Seq[ShuffleReclaimDecision],
        decidedAtMs: Long): Unit =
      calls += Dispatch(fenceId, shuffles.toList, decidedAtMs)

    override def skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule): Unit =
      calls += Skip(shuffleId, executionId, rule)

    override def executionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int]): Unit =
      calls += ExecutionClosed(executionId, endedAtMs, ownedIds.toList)

    override def executorRemoved(executorId: String): Unit =
      calls += ExecutorRemoved(executorId)

    override def release(fenceId: Long, shuffleId: Int): Unit =
      calls += Release(fenceId, shuffleId)

    override def withdraw(fenceId: Long, shuffleId: Int): Unit =
      calls += Withdraw(fenceId, shuffleId)

    override def isDisabled: Boolean = calls.exists(_.isInstanceOf[Disable])

    override def disable(reason: String): Unit = calls += Disable(reason)

    /** The shuffles of each dispatch, in dispatch order. */
    def dispatches: Seq[Seq[ShuffleReclaimDecision]] =
      calls.collect { case d: Dispatch => d.shuffles }.toList

    def skips: Seq[Skip] = calls.collect { case s: Skip => s }.toList
  }

  /**
   * A policy over a recording sink, with the event key on, since the policy sends no skip or
   * close with it off. The clock moves only when a test moves it.
   */
  private class Fixture(isCandidate: Int => Boolean) {
    var nowMs = 0L
    val sink = new RecordingSink
    val policy = new UcxReclaimPolicy(sink, () => nowMs, isCandidate, eventLogEnabled = true,
      droppedEvents = () => 0L)
  }
}
