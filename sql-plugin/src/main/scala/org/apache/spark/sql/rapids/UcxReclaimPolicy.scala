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

import java.util.{BitSet, Properties}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerEvent,
  SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerJobEnd,
  SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted,
  SparkListenerTaskEnd, SparkListenerTaskStart, StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.storage.StorageLevel

/**
 * The calls the UCX policy makes into the manager that reclaims UCX shuffles. The policy makes
 * every call on its listener's thread, and none of them waits for an executor.
 */
trait ShuffleReclaimHandoff {
  /**
   * Hands over the shuffles that one event decided to reclaim, under one fence.
   *
   * @param fenceId the fence whose marker follows the driver steps of these shuffles
   * @param shuffles each shuffle decided, with its owner, watermark and targets
   * @param decidedAtMs when the policy decided them
   */
  def dispatch(fenceId: Long, shuffles: Seq[ShuffleReclaimDecision], decidedAtMs: Long): Unit

  /** An owned shuffle that `rule` leaves to GC. */
  def skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule): Unit

  /** An execution that owns a shuffle ended, and it owns exactly `ownedIds`. */
  def executionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int]): Unit

  /** An executor left, so no request waits for it. */
  def executorRemoved(executorId: String): Unit

  /** The fence's marker came with no job that touched the shuffle, so executors may free it. */
  def release(fenceId: Long, shuffleId: Int): Unit

  /** A job touched the shuffle before the fence's marker came, so it waits for GC. */
  def withdraw(fenceId: Long, shuffleId: Int): Unit

  /** Whether the reclaim is disabled for the rest of the application. */
  def isDisabled: Boolean

  /** Disables the reclaim for the rest of the application, and withdraws every fence. */
  def disable(reason: String): Unit
}

/**
 * A shuffle that the UCX policy decided to reclaim.
 *
 * @param shuffleId the shuffle
 * @param owner the SQL execution that owns it
 * @param watermark the largest task id the policy had seen at the decision
 * @param targets the executors that ran a task of a stage that writes or reads the shuffle
 */
case class ShuffleReclaimDecision(
    shuffleId: Int,
    owner: Long,
    watermark: Long,
    targets: Set[String])

/** The rule that leaves an owned shuffle to GC. */
sealed trait ShuffleReclaimRule

object ShuffleReclaimRule {
  /** Another execution, no execution or an ended one touched the shuffle. */
  case object R2 extends ShuffleReclaimRule

  /** The owner succeeded, and nothing it ran read the shuffle. */
  case object R4 extends ShuffleReclaimRule

  /** A stage that reads the shuffle persists an RDD. */
  case object R5 extends ShuffleReclaimRule

  /** Spark marks the shuffle's map stage indeterminate. */
  case object R7 extends ShuffleReclaimRule
}

/**
 * The marker that the reclaim manager posts after the driver steps of one dispatch. Once the
 * policy handles it, the policy has seen every job submitted before those driver steps.
 *
 * @param fenceId the fence the marker ends
 */
case class RapidsShuffleReclaimFence(fenceId: Long) extends SparkListenerEvent {
  // the marker only orders the policy's queue, so it stays out of the event log
  override protected[spark] def logEvent: Boolean = false
}

/**
 * UCX's policy, which hands each GPU shuffle to the reclaim manager once its SQL execution and
 * every task the policy saw for it have ended.
 *
 * The live SQL execution that submits a shuffle's map stage owns it. The policy hands a shuffle
 * over only when no other execution or job touched it, its owner read it or failed, no stage
 * that reads it caches an RDD, and Spark does not mark its map stage indeterminate. Any other
 * shuffle waits for GC. A shuffle handed over stays in flight until its fence's marker, which
 * releases it, or withdraws it when a job listed its map stage first. A dropped event or a
 * handler that throws disables the reclaim for the rest of the application.
 *
 * Comments and DEBUG lines name the rules by number:
 *  - R1, ownership: the live execution that submits a shuffle's map stage owns it.
 *  - R2, sharing: a job of another execution, of an ended one or of none touched it.
 *  - R3, the owner's end: the owner has ended and none of its jobs lists the shuffle.
 *  - R4, consumption: the owner read the shuffle or failed.
 *  - R5, caching: no stage that reads the shuffle persists an RDD.
 *  - R6, quiescence: no stage that writes or reads the shuffle has a live attempt or a task.
 *  - R7, determinism: Spark does not mark the map stage indeterminate.
 *
 * @param handoff the reclaim manager
 * @param clock the current time in milliseconds
 * @param isCandidate whether a shuffle id is a GPU shuffle that the reclaim may free
 * @param eventLogEnabled whether the manager posts an event per SQL execution
 * @param droppedEvents how many events the policy's listener queue has dropped
 */
class UcxReclaimPolicy(
    handoff: ShuffleReclaimHandoff,
    clock: () => Long,
    isCandidate: Int => Boolean,
    eventLogEnabled: Boolean,
    droppedEvents: () => Long) extends ShuffleCleanupPolicy with Logging {

  import UcxReclaimPolicy._

  // set by the thread that stops the SparkContext, after which every handler returns at once
  @volatile private var stopped = false
  // kept by the listener's thread for the stopping thread, which reads each once
  private val undecidedShuffles = new AtomicInteger(0)
  private val liveExecutions = new AtomicInteger(0)

  // The state below changes only on the listener's thread. The state accessor reads it from a
  // test's thread, so both hold this lock, which nothing else takes.
  private val lock = new Object
  // set once the reclaim is disabled, after which the state stays empty
  private var disabled = false
  private val stages = new mutable.HashMap[Int, StageState]()
  private val jobs = new mutable.HashMap[Int, JobState]()
  private val executions = new mutable.HashMap[Long, ExecutionState]()
  private val shuffles = new mutable.HashMap[Int, ShuffleState]()
  // the shuffles left to GC for good, and Spark never reuses a shuffle id, so no bit is cleared
  private val excluded = new BitSet()
  // the shuffles R7 settled, which later jobs list with their map stage skipped. Only a rerun of
  // that stage, through resubmitSettled, settles one again
  private val settledIndeterminate = new BitSet()
  // the largest task id seen, which each decision takes as its watermark
  private var maxTaskId = -1L
  // the executors Spark removed. A TaskStart that Spark posts after its executor's removal must
  // not make it a target again, and Spark never reuses an executor id
  private val removedExecutors = new mutable.HashSet[String]()
  // whether a job listed the map stage of each shuffle in flight since its dispatch
  private val inFlight = new mutable.HashMap[Int, Boolean]()
  // the shuffles of each fence whose marker has not come, in the order the fence ids rise
  private val fences = new mutable.Queue[(Long, List[Int])]()
  private var nextFenceId = 0L
  // the owner's end plus 10 minutes, for each shuffle that waits only for running tasks
  private val r6Deadlines = new mutable.TreeSet[(Long, Int)]()
  // the decisions of the event being handled, which go to the manager under one fence
  private val decisions = new ArrayBuffer[ShuffleReclaimDecision]()

  override def onExecutionStart(executionId: Long): Unit = handle("a SQL execution start") {
    if (!executions.contains(executionId)) {
      executions(executionId) = new ExecutionState()
      liveExecutions.incrementAndGet()
    }
  }

  override def onExecutionEnd(executionId: Long): Unit = handle("a SQL execution end") {
    // Spark posts an End with no start when planning fails, and such an End changes nothing
    executions.get(executionId).filterNot(_.ended).foreach { execution =>
      execution.ended = true
      execution.endedAtMs = clock()
      liveExecutions.decrementAndGet()
      if (eventLogEnabled && execution.owned.nonEmpty) {
        handoff.executionClosed(executionId, execution.endedAtMs, execution.owned.toList)
      }
      execution.undecided.toList.foreach(evaluate)
      dropIfDone(executionId)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = handle("a job start") {
    val jobId = jobStart.jobId
    val execution = liveExecutionOf(jobStart.properties)
    val infos = jobStart.stageInfos
    infos.foreach(_.shuffleDepId.foreach(touchInFlight))
    // the candidate shuffle that each listed stage writes, which also gives each stage's reads,
    // since a job lists every parent of its stages
    val writes = new mutable.HashMap[Int, Int]()
    infos.foreach { info =>
      info.shuffleDepId.foreach { shuffleId =>
        // a held shuffle stays tracked after Spark's own cleanup drops it from the registry,
        // since a later job can register it again and rerun its map stage
        if (!excluded.get(shuffleId) && !settledIndeterminate.get(shuffleId) &&
            (shuffles.contains(shuffleId) || isCandidate(shuffleId))) {
          writes(info.stageId) = shuffleId
        }
      }
    }
    val job = new JobState(execution)
    infos.foreach { info =>
      val written = writes.get(info.stageId)
      val read = info.parentIds.flatMap(writes.get)
      if (written.nonEmpty || read.nonEmpty) {
        val stage = stages.getOrElseUpdate(info.stageId, new StageState(info.stageId))
        stage.jobs += jobId
        job.stages += info.stageId
        written.foreach { shuffleId =>
          stage.writes = Some(shuffleId)
          stateOf(shuffleId).stages += info.stageId
          job.shuffles += shuffleId
        }
        // R5 judges every stage that reads the shuffle, whether Spark runs it or skips it
        val caches = info.rddInfos.exists(_.storageLevel != StorageLevel.NONE)
        read.foreach { shuffleId =>
          stage.reads += shuffleId
          val shuffle = stateOf(shuffleId)
          shuffle.stages += info.stageId
          if (caches) {
            shuffle.cached = true
          }
          job.shuffles += shuffleId
        }
      }
    }
    if (execution.nonEmpty || job.shuffles.nonEmpty) {
      jobs(jobId) = job
      execution.foreach(executionId => executions(executionId).activeJobs += jobId)
    }
    job.shuffles.toList.foreach { shuffleId =>
      shuffles.get(shuffleId).foreach { shuffle =>
        shuffle.jobs += jobId
        if (shuffle.owner.exists(owner => !execution.contains(owner))) {
          // R2: a job of another execution, of an ended one or of none touches the shuffle
          exclude(shuffle, ShuffleReclaimRule.R2)
        } else {
          evaluate(shuffleId)
        }
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
    handle("a stage submission") {
      val info = stageSubmitted.stageInfo
      val execution = liveExecutionOf(stageSubmitted.properties)
      info.shuffleDepId.foreach(touchInFlight)
      stages.get(info.stageId) match {
        case Some(stage) =>
          stage.submitted = true
          stage.liveAttempts += info.attemptNumber()
          stage.reads.toList.foreach { shuffleId =>
            shuffles.get(shuffleId).foreach { shuffle =>
              shuffle.owner.foreach { owner =>
                if (execution.contains(owner)) {
                  // R4: a stage of the owner reads the shuffle
                  shuffle.consumed = true
                } else {
                  exclude(shuffle, ShuffleReclaimRule.R2)
                }
              }
            }
          }
          stage.writes.flatMap(shuffles.get).foreach(submitMapStage(_, execution, info))
        case None =>
          info.shuffleDepId.foreach(resubmitSettled(_, execution, info))
      }
    }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    handle("a stage completion") {
      val info = stageCompleted.stageInfo
      // a second completion of one attempt, which Spark 4 can post, changes nothing
      stages.get(info.stageId).foreach { stage =>
        if (stage.liveAttempts.remove(info.attemptNumber()) && stage.isQuiescent) {
          stage.shuffleIds.foreach(evaluate)
        }
      }
    }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = handle("a task start") {
    val task = taskStart.taskInfo
    maxTaskId = math.max(maxTaskId, task.taskId)
    // Tasks are keyed by stage alone, since Spark 3.3.0 to 3.3.2 give a late TaskStart the
    // stage's latest attempt. A stage the policy does not hold has no undecided shuffle.
    stages.get(taskStart.stageId).foreach { stage =>
      stage.runningTasks += task.taskId
      if (!removedExecutors.contains(task.executorId)) {
        stage.writes.foreach(shuffleId => shuffles(shuffleId).executors += task.executorId)
        stage.reads.foreach(shuffleId => shuffles(shuffleId).executors += task.executorId)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = handle("a task end") {
    // the end of a task that the policy did not see start changes nothing
    Option(taskEnd.taskInfo).foreach { task =>
      stages.get(taskEnd.stageId).foreach { stage =>
        if (stage.runningTasks.remove(task.taskId) && stage.isQuiescent) {
          stage.shuffleIds.foreach(evaluate)
        }
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = handle("a job end") {
    jobs.remove(jobEnd.jobId).foreach { job =>
      val affected = new mutable.LinkedHashSet[Int]()
      job.shuffles.foreach { shuffleId =>
        shuffles.get(shuffleId).foreach(_.jobs -= jobEnd.jobId)
        affected += shuffleId
      }
      job.stages.foreach { stageId =>
        stages.get(stageId).foreach { stage =>
          stage.jobs -= jobEnd.jobId
          // Spark runs tasks only for a stage it submits, so such a stage that no active job
          // lists holds nothing back
          if (!stage.submitted && stage.jobs.isEmpty) {
            stages.remove(stageId)
            stage.shuffleIds.foreach { shuffleId =>
              shuffles.get(shuffleId).foreach(_.stages -= stageId)
              affected += shuffleId
            }
          }
        }
      }
      job.execution.flatMap(executions.get).foreach { execution =>
        execution.activeJobs -= jobEnd.jobId
        if (jobEnd.jobResult != JobSucceeded) {
          execution.failed = true
        }
        // a failure, or the end of the owner's last job, can decide its other shuffles
        if (execution.ended) {
          affected ++= execution.undecided
        }
      }
      affected.foreach(evaluate)
      job.execution.foreach(dropIfDone)
    }
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit =
    handle("a metrics update") {
      val now = clock()
      // the earliest deadline first, so an update costs one comparison until one passes
      while (r6Deadlines.nonEmpty && r6Deadlines.head._1 <= now) {
        val deadline = r6Deadlines.head
        r6Deadlines -= deadline
        shuffles.get(deadline._2).foreach { shuffle =>
          shuffle.r6DeadlineMs = None
          shuffle.r6Warned = true
          warnHeldByTasks(shuffle, now)
        }
      }
    }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val executorId = executorRemoved.executorId
    if (!stopped) {
      lock.synchronized {
        if (isEnabled) {
          guarded("an executor removal") {
            // the manager drops the executor only from requests that exist, so a later decision
            // must not name it either
            removedExecutors += executorId
            shuffles.values.foreach(_.executors -= executorId)
            handoff.executorRemoved(executorId)
          }
        } else {
          // the manager still settles the requests it released before the reclaim was disabled
          handoff.executorRemoved(executorId)
        }
      }
    }
  }

  override def onFence(fenceId: Long): Unit = handle("a reclaim fence marker") {
    val drops = droppedEvents()
    if (drops > 0) {
      // a dropped event can be a job that touched a shuffle in flight
      val queue = ShuffleCleanupListener.QUEUE_NAME
      disable(s"the listener bus queue $queue dropped $drops events, so the policy cannot see " +
        s"every job. Raising spark.scheduler.listenerbus.eventqueue.$queue.capacity avoids this")
    } else {
      // fence ids rise with each dispatch, so a marker also ends an earlier fence whose marker
      // never came
      while (fences.nonEmpty && fences.head._1 <= fenceId) {
        val (fence, shuffleIds) = fences.dequeue()
        shuffleIds.foreach { shuffleId =>
          inFlight.remove(shuffleId).foreach { touched =>
            if (touched) {
              handoff.withdraw(fence, shuffleId)
              // the shuffle waits for GC, and so does a rerun that a later owner wrote
              excluded.set(shuffleId)
              shuffles.get(shuffleId).foreach(exclude(_, ShuffleReclaimRule.R2))
            } else {
              handoff.release(fence, shuffleId)
            }
          }
        }
      }
    }
  }

  override def shutdown(): Unit = {
    // the listener's thread may still be handling an event, so no other state is touched
    stopped = true
    val undecided = undecidedShuffles.get()
    val live = liveExecutions.get()
    if (undecided > 0 || live > 0) {
      logInfo(s"UCX shuffle reclaim stopped. Undecided GPU shuffles, which wait for GC: " +
        s"$undecided. Live SQL executions: $live")
    }
  }

  /**
   * What the policy holds, apart from the shuffles in flight to their fence's marker. Only tests
   * read it, from a thread of their own, which is why the handlers take `lock`.
   */
  private[rapids] def heldState: HeldState = lock.synchronized {
    HeldState(
      stages = stages.keySet.toSet,
      jobs = jobs.keySet.toSet,
      executions = executions.keySet.toSet,
      liveAttempts =
        stages.values.flatMap(stage => stage.liveAttempts.map((stage.stageId, _))).toSet,
      runningTasks = stages.values.filter(_.runningTasks.nonEmpty)
        .map(stage => stage.stageId -> stage.runningTasks.toSet).toMap,
      shuffles = shuffles.keySet.toSet)
  }

  /** Runs a handler's body, unless the policy has stopped or the reclaim is disabled. */
  private def handle(event: String)(body: => Unit): Unit = {
    if (!stopped) {
      lock.synchronized {
        if (isEnabled) {
          guarded(event)(body)
        }
      }
    }
  }

  /**
   * Runs a handler's body, then dispatches what it decided. A handler that throws leaves the
   * state in doubt, so the reclaim fails closed.
   */
  private def guarded(event: String)(body: => Unit): Unit = {
    try {
      body
      dispatchDecided()
    } catch {
      case NonFatal(e) =>
        // the manager's own WARN carries no stack, and none when its thread disabled first
        logWarning(s"The UCX shuffle reclaim policy failed to handle $event", e)
        disable(s"the policy failed to handle $event: $e")
    }
  }

  /** Whether the reclaim is enabled, dropping the state once the manager has disabled it. */
  private def isEnabled: Boolean = {
    if (!disabled && handoff.isDisabled) {
      dropState()
    }
    !disabled
  }

  /** Drops the state, and has the manager disable the reclaim and withdraw every fence. */
  private def disable(reason: String): Unit = {
    dropState()
    handoff.disable(reason)
  }

  private def dropState(): Unit = {
    disabled = true
    stages.clear()
    jobs.clear()
    executions.clear()
    shuffles.clear()
    inFlight.clear()
    fences.clear()
    r6Deadlines.clear()
    decisions.clear()
    undecidedShuffles.set(0)
    liveExecutions.set(0)
  }

  /** The SQL execution that `properties` name, when the policy holds it live. */
  private def liveExecutionOf(properties: Properties): Option[Long] =
    Option(properties).flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(id => Try(id.toLong).toOption)
      .filter(id => executions.get(id).exists(!_.ended))

  private def touchInFlight(shuffleId: Int): Unit = {
    if (inFlight.contains(shuffleId)) {
      inFlight(shuffleId) = true
    }
  }

  private def stateOf(shuffleId: Int): ShuffleState =
    shuffles.getOrElse(shuffleId, {
      val shuffle = new ShuffleState(shuffleId)
      shuffles(shuffleId) = shuffle
      undecidedShuffles.incrementAndGet()
      shuffle
    })

  /** R1 for a held map stage's shuffle as Spark submits the stage, then R7 and R2. */
  private def submitMapStage(
      shuffle: ShuffleState,
      execution: Option[Long],
      info: StageInfo): Unit = {
    shuffle.owner match {
      case Some(owner) =>
        // a retry under the owner changes nothing, and a submission under any other excludes
        if (!execution.contains(owner)) {
          exclude(shuffle, ShuffleReclaimRule.R2)
        }
      case None =>
        execution match {
          case None =>
            // a map stage that runs outside a live execution leaves its shuffle with no owner
            excluded.set(shuffle.shuffleId)
            drop(shuffle)
          case Some(owner) =>
            val ownerState = executions(owner)
            ownerState.owned += shuffle.shuffleId
            if (isIndeterminate(info)) {
              settledIndeterminate.set(shuffle.shuffleId)
              skip(shuffle.shuffleId, owner, ShuffleReclaimRule.R7)
              drop(shuffle)
            } else {
              shuffle.owner = execution
              ownerState.undecided += shuffle.shuffleId
              if (shuffle.jobs.exists(jobId => !jobs(jobId).execution.contains(owner))) {
                exclude(shuffle, ShuffleReclaimRule.R2)
              } else {
                evaluate(shuffle.shuffleId)
              }
            }
        }
    }
  }

  /**
   * A retry of a map stage that the policy no longer holds, because R7 settled its shuffle,
   * settles the shuffle again for the live execution it runs under.
   */
  private def resubmitSettled(shuffleId: Int, execution: Option[Long], info: StageInfo): Unit = {
    execution.foreach { owner =>
      if (!excluded.get(shuffleId) && !shuffles.contains(shuffleId) && isIndeterminate(info) &&
          isCandidate(shuffleId)) {
        executions(owner).owned += shuffleId
        skip(shuffleId, owner, ShuffleReclaimRule.R7)
      }
    }
  }

  /**
   * R7 reads the stage's own RDD, which heads its list, and counts an empty list as
   * indeterminate.
   */
  private def isIndeterminate(info: StageInfo): Boolean =
    !info.rddInfos.headOption.exists(
      _.outputDeterministicLevel != DeterministicLevel.INDETERMINATE)

  /** Hands a shuffle over when every rule holds, or records what it waits for. */
  private def evaluate(shuffleId: Int): Unit = shuffles.get(shuffleId).foreach { shuffle =>
    shuffle.owner match {
      case None =>
        // with no owner and no held stage, the next job that lists the shuffle starts it afresh
        if (shuffle.stages.isEmpty) {
          drop(shuffle)
        }
      case Some(owner) =>
        val execution = executions(owner)
        if (shuffle.cached) {
          exclude(shuffle, ShuffleReclaimRule.R5)
        } else if (execution.ended && shuffle.jobs.isEmpty) {
          if (shuffle.consumed || execution.failed) {
            if (shuffle.stages.forall(stages(_).isQuiescent)) {
              // a shuffle in flight waits for its fence's marker, which withdraws it
              if (!inFlight.contains(shuffleId)) {
                handOver(shuffle, owner)
              }
            } else if (shuffle.r6DeadlineMs.isEmpty && !shuffle.r6Warned) {
              val deadline = execution.endedAtMs + R6_WARN_AFTER_MS
              shuffle.r6DeadlineMs = Some(deadline)
              r6Deadlines += ((deadline, shuffleId))
            }
          } else if (execution.activeJobs.isEmpty) {
            // R4: nothing the owner ran read the shuffle, and none of its jobs can fail now
            exclude(shuffle, ShuffleReclaimRule.R4)
          }
        }
    }
  }

  /** Decides to reclaim a shuffle, with the largest task id seen as its watermark. */
  private def handOver(shuffle: ShuffleState, owner: Long): Unit = {
    decisions += ShuffleReclaimDecision(shuffle.shuffleId, owner, maxTaskId,
      shuffle.executors.toSet)
    drop(shuffle)
  }

  /** Hands the event's decisions to the manager under one fence, and holds each in flight. */
  private def dispatchDecided(): Unit = {
    if (decisions.nonEmpty) {
      val fenceId = nextFenceId
      nextFenceId += 1
      val decided = decisions.toList
      decisions.clear()
      val shuffleIds = decided.map(_.shuffleId)
      shuffleIds.foreach(shuffleId => inFlight(shuffleId) = false)
      fences += ((fenceId, shuffleIds))
      handoff.dispatch(fenceId, decided, clock())
    }
  }

  /** Leaves a shuffle to GC for good, and counts it for its owner under the rule. */
  private def exclude(shuffle: ShuffleState, rule: ShuffleReclaimRule): Unit = {
    excluded.set(shuffle.shuffleId)
    shuffle.owner.foreach(skip(shuffle.shuffleId, _, rule))
    drop(shuffle)
  }

  private def skip(shuffleId: Int, owner: Long, rule: ShuffleReclaimRule): Unit = {
    if (eventLogEnabled) {
      handoff.skip(shuffleId, owner, rule)
    }
  }

  /** Drops a decided shuffle's state, and each stage and execution left holding nothing. */
  private def drop(shuffle: ShuffleState): Unit = {
    val shuffleId = shuffle.shuffleId
    shuffles.remove(shuffleId)
    undecidedShuffles.decrementAndGet()
    shuffle.r6DeadlineMs.foreach(deadline => r6Deadlines -= ((deadline, shuffleId)))
    shuffle.jobs.foreach(jobId => jobs.get(jobId).foreach(_.shuffles -= shuffleId))
    shuffle.stages.foreach { stageId =>
      stages.get(stageId).foreach { stage =>
        stage.writes = stage.writes.filter(_ != shuffleId)
        stage.reads -= shuffleId
        if (stage.writes.isEmpty && stage.reads.isEmpty) {
          stages.remove(stageId)
        }
      }
    }
    shuffle.owner.foreach { owner =>
      executions.get(owner).foreach(_.undecided -= shuffleId)
      dropIfDone(owner)
    }
  }

  /** An execution's entry goes once it has ended with no active job and no undecided shuffle. */
  private def dropIfDone(executionId: Long): Unit = {
    executions.get(executionId).foreach { execution =>
      if (execution.ended && execution.activeJobs.isEmpty && execution.undecided.isEmpty) {
        executions.remove(executionId)
      }
    }
  }

  /** Logs the WARN for a shuffle that running tasks hold, naming each stage that holds it. */
  private def warnHeldByTasks(shuffle: ShuffleState, now: Long): Unit = {
    shuffle.owner.foreach { owner =>
      val holders = shuffle.stages.toList.sorted.map(stages(_)).filterNot(_.isQuiescent)
      if (holders.nonEmpty) {
        val described = holders.map { stage =>
          val attempt = if (stage.liveAttempts.isEmpty) "no live attempt" else "a live attempt"
          val tasks = stage.runningTasks.toList.sorted
          val running = if (tasks.isEmpty) {
            "no running task"
          } else {
            val more = tasks.size - MAX_WARNED_TASK_IDS
            s"${tasks.size} running, task ids: ${tasks.take(MAX_WARNED_TASK_IDS).mkString(", ")}" +
              (if (more > 0) s" and $more more" else "")
          }
          s"stage ${stage.stageId} ($attempt, $running)"
        }
        logWarning(s"$R6_WARN_PREFIX shuffle ${shuffle.shuffleId} of SQL execution $owner, " +
          s"which ended ${now - executions(owner).endedAtMs} ms ago, is still held by " +
          described.mkString(", "))
      }
    }
  }
}

object UcxReclaimPolicy {
  /** The start of the one WARN for a shuffle that running tasks hold for over 10 minutes. */
  private[rapids] val R6_WARN_PREFIX: String = "UCX shuffle reclaim waits on running tasks:"

  /** How long after its owner's end a shuffle that waits only for running tasks gets the WARN. */
  private val R6_WARN_AFTER_MS: Long = TimeUnit.MINUTES.toMillis(10)

  /** How many running task ids the WARN lists for each stage. */
  private val MAX_WARNED_TASK_IDS = 10

  /**
   * What the policy holds, apart from the shuffles in flight to their fence's marker.
   *
   * @param stages the held stage ids
   * @param jobs the active jobs of a live execution, or that list an undecided shuffle's stage
   * @param executions the ids of the executions with an entry
   * @param liveAttempts the stage id and attempt id of each live stage attempt
   * @param runningTasks the running task ids of each held stage
   * @param shuffles the ids of the shuffles with state of their own
   */
  private[rapids] case class HeldState(
      stages: Set[Int],
      jobs: Set[Int],
      executions: Set[Long],
      liveAttempts: Set[(Int, Int)],
      runningTasks: Map[Int, Set[Long]],
      shuffles: Set[Int])

  /** A stage that writes or reads an undecided shuffle. */
  private final class StageState(val stageId: Int) {
    var writes: Option[Int] = None
    val reads = new mutable.HashSet[Int]()
    // the active jobs that list the stage
    val jobs = new mutable.HashSet[Int]()
    var submitted = false
    val liveAttempts = new mutable.HashSet[Int]()
    val runningTasks = new mutable.HashSet[Long]()

    def shuffleIds: List[Int] = writes.toList ++ reads

    def isQuiescent: Boolean = liveAttempts.isEmpty && runningTasks.isEmpty
  }

  /**
   * An active job that belongs to a live SQL execution or lists a stage of an undecided
   * shuffle. Its execution is the one live at its start, whatever happens to it later.
   */
  private final class JobState(val execution: Option[Long]) {
    val stages = new ArrayBuffer[Int]()
    val shuffles = new mutable.HashSet[Int]()
  }

  /** A SQL execution, from its start until it has ended and holds nothing back. */
  private final class ExecutionState {
    var ended = false
    var endedAtMs = 0L
    var failed = false
    val activeJobs = new mutable.HashSet[Int]()
    // every shuffle it owned, and those not decided yet
    val owned = new mutable.LinkedHashSet[Int]()
    val undecided = new mutable.HashSet[Int]()
  }

  /** A candidate shuffle that is not decided yet. */
  private final class ShuffleState(val shuffleId: Int) {
    var owner: Option[Long] = None
    var consumed = false
    var cached = false
    // the held stages that write or read the shuffle, and the active jobs that list them
    val stages = new mutable.HashSet[Int]()
    val jobs = new mutable.HashSet[Int]()
    // the executors that ran a task of those stages
    val executors = new mutable.HashSet[String]()
    var r6DeadlineMs: Option[Long] = None
    var r6Warned = false
  }
}
