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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.ShuffleCleanupManager

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent,
  SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerJobEnd,
  SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted,
  SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}

/**
 * A SparkListener that passes the scheduler and SQL events it receives to the cleanup policy
 * of the shuffle backend in use.
 *
 * Note: This file is placed in org.apache.spark.sql.rapids package to access
 * the private[spark] shuffleDepId field in StageInfo.
 *
 * @param policy the backend's policy, which handles each event on this listener's thread
 */
class ShuffleCleanupListener(policy: ShuffleCleanupPolicy) extends SparkListener {

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = policy.onJobStart(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = policy.onJobEnd(jobEnd)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
    policy.onStageSubmitted(stageSubmitted)

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    policy.onStageCompleted(stageCompleted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
    policy.onTaskStart(taskStart)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = policy.onTaskEnd(taskEnd)

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit =
    policy.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit =
    policy.onExecutorRemoved(executorRemoved)

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    // match by type, since the SQL events' extractors change arity between Spark versions
    event match {
      case e: SparkListenerSQLExecutionStart => policy.onExecutionStart(e.executionId)
      case e: SparkListenerSQLExecutionEnd => policy.onExecutionEnd(e.executionId)
      case e: RapidsShuffleReclaimFence => policy.onFence(e.fenceId)
      case _ => // ignore other events
    }
  }

  /**
   * Called during shutdown to cleanup any remaining execution -> shuffle mappings.
   */
  def shutdown(): Unit = policy.shutdown()
}

object ShuffleCleanupListener {
  /** The listener bus queue that keeps the UCX policy's listener apart from other listeners. */
  val QUEUE_NAME: String = "rapids-shuffle-cleanup"
}

/**
 * A shuffle backend's handling of the events that a `ShuffleCleanupListener` receives, on the
 * listener's thread. A handler that the backend does not override does nothing.
 */
trait ShuffleCleanupPolicy {
  def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

  def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  /** A SQL execution started. */
  def onExecutionStart(executionId: Long): Unit = {}

  /** A SQL execution ended. */
  def onExecutionEnd(executionId: Long): Unit = {}

  /** The marker of a reclaim fence reached the listener. */
  def onFence(fenceId: Long): Unit = {}

  /** Called when the driver plugin shuts down. */
  def shutdown(): Unit
}

/**
 * MULTITHREADED's policy, which triggers shuffle cleanup when a SQL query completes.
 *
 * This policy solves the problem of GC-based shuffle cleanup being unreliable
 * for short-running queries. Instead of waiting for ShuffleDependency objects to be
 * garbage collected, this policy proactively triggers cleanup when a query ends.
 *
 * Using SQL execution completion (not job completion) ensures that:
 * 1. All jobs within the query have completed
 * 2. All shuffle data for this query is no longer needed
 * 3. Cleanup is safe and won't affect running tasks
 *
 * The flow is:
 * 1. On JobStart: Extract shuffle IDs from stageInfos and associate with SQL execution ID
 * 2. On SQLExecutionEnd: Look up all shuffle IDs for this execution and register for cleanup
 * 3. ShuffleCleanupManager handles the RPC to executors for actual cleanup
 */
class MultithreadedSqlEndPolicy extends ShuffleCleanupPolicy with Logging {

  // log settings that name the listener's logger cover these lines too
  override protected def logName: String = classOf[ShuffleCleanupListener].getName

  /**
   * Maps SQL execution ID to the set of shuffle IDs associated with that execution.
   * Shuffle IDs are accumulated from all jobs that belong to this SQL execution.
   */
  private val executionShuffles = new ConcurrentHashMap[Long, mutable.Set[Int]]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // Get the SQL execution ID from job properties
    val executionIdOpt = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(s => scala.util.Try(s.toLong).toOption)

    executionIdOpt.foreach { executionId =>
      // Extract all shuffle IDs from the stages in this job
      // StageInfo.shuffleDepId contains the shuffle ID for ShuffleMapStages
      val shuffleIds = jobStart.stageInfos
        .flatMap(_.shuffleDepId)
        .toSet

      if (shuffleIds.nonEmpty) {
        logDebug(s"Job ${jobStart.jobId} (execution $executionId) started with " +
          s"${shuffleIds.size} shuffles: ${shuffleIds.mkString(", ")}")

        // Accumulate shuffle IDs for this execution
        executionShuffles.compute(executionId, (_, existing) => {
          val set = if (existing == null) mutable.Set[Int]() else existing
          set ++= shuffleIds
          set
        })
      }
    }
  }

  override def onExecutionEnd(executionId: Long): Unit = {
    val shuffleIds = Option(executionShuffles.remove(executionId))
    shuffleIds.foreach { ids =>
      if (ids.nonEmpty) {
        logDebug(s"SQL execution $executionId ended, triggering cleanup for " +
          s"${ids.size} shuffle(s): ${ids.mkString(", ")}")

        val cleanupManager = ShuffleCleanupManager.get
        if (cleanupManager != null) {
          ids.foreach { shuffleId =>
            cleanupManager.registerForCleanup(shuffleId)
          }
        } else {
          logDebug("ShuffleCleanupManager not available, skipping cleanup trigger")
        }
      }
    }
  }

  /**
   * Called during shutdown to cleanup any remaining execution -> shuffle mappings.
   */
  override def shutdown(): Unit = {
    val remainingExecutions = executionShuffles.keySet().asScala.toSeq
    if (remainingExecutions.nonEmpty) {
      logInfo(s"Shutdown: ${remainingExecutions.size} executions still have " +
        s"pending shuffle cleanup")
      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        executionShuffles.asScala.foreach { case (executionId, shuffleIds) =>
          logDebug(s"Shutdown: triggering cleanup for execution $executionId shuffles: " +
            s"${shuffleIds.mkString(", ")}")
          shuffleIds.foreach(cleanupManager.registerForCleanup)
        }
      }
    }
    executionShuffles.clear()
  }
}

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

  override def shutdown(): Unit = {}

  /** What the policy holds, apart from the shuffles in flight to their fence's marker. */
  private[rapids] def heldState: UcxReclaimPolicy.HeldState =
    UcxReclaimPolicy.HeldState(Set.empty, Set.empty, Set.empty, Set.empty, Map.empty, Set.empty)
}

object UcxReclaimPolicy {
  /** The start of the one WARN for a shuffle that running tasks hold for over 10 minutes. */
  private[rapids] val R6_WARN_PREFIX: String = "UCX shuffle reclaim waits on running tasks:"

  /**
   * What the policy holds, apart from the shuffles in flight to their fence's marker.
   *
   * @param stages the held stage ids
   * @param jobs the active job ids
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
}
