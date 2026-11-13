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

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent,
  SparkListenerStageSubmitted, SparkListenerTaskStart, StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}

/**
 * Hands the GPU shuffles that a SQL execution wrote to the UCX reclaim manager once the
 * execution ends, with the largest task id seen by then as their watermark.
 *
 * A shuffle belongs to an execution when its map stage is submitted under that execution while
 * it runs. A shuffle whose map stage Spark marks indeterminate is left to GC, since a rerun of
 * a GPU round-robin shuffle after its reader ran fails the job. The listener keeps no stage, job
 * or task state, and never blocks, since it shares the listener queue with other listeners.
 *
 * A listener sees each TaskStart in task-id order, and before the end of the execution that
 * ran the task, so at an execution's end the watermark covers every writer the execution ran.
 * A writer that reruns a cleared map stage starts later, so its task id is above the watermark.
 *
 * Note: This file is placed in org.apache.spark.sql.rapids package to access
 * the private[spark] shuffleDepId field in StageInfo.
 *
 * @param isCandidate whether a shuffle id is a GPU shuffle that the reclaim may free
 * @param reclaim hands an ended execution's id, its shuffles and their watermark to the manager
 */
class UcxSqlEndListener(
    isCandidate: Int => Boolean,
    reclaim: (Long, Seq[Int], Long) => Unit) extends SparkListener with Logging {

  /** The shuffles of each running SQL execution, from its start to its end. */
  private val executionShuffles = new ConcurrentHashMap[Long, mutable.Set[Int]]()

  /** The largest task id the listener has seen. */
  private var maxTaskId = -1L

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stage = stageSubmitted.stageInfo
    for {
      shuffleId <- stage.shuffleDepId if isCandidate(shuffleId)
      executionId <- UcxSqlEndListener.executionIdOf(stageSubmitted.properties)
      shuffles <- Option(executionShuffles.get(executionId))
    } {
      if (UcxSqlEndListener.isIndeterminate(stage)) {
        logDebug(s"Shuffle $shuffleId of SQL execution $executionId waits for GC, since its " +
          s"map stage ${stage.stageId} is indeterminate")
      } else {
        shuffles += shuffleId
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    maxTaskId = math.max(maxTaskId, taskStart.taskInfo.taskId)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    // match by type, since the SQL events' extractors change arity between Spark versions
    event match {
      case e: SparkListenerSQLExecutionStart => onExecutionStart(e.executionId)
      case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e.executionId)
      case _ => // ignore other events
    }
  }

  /** A SQL execution started. */
  def onExecutionStart(executionId: Long): Unit =
    executionShuffles.putIfAbsent(executionId, mutable.Set[Int]())

  /** A SQL execution ended: hands its shuffles and the watermark to the reclaim manager. */
  def onExecutionEnd(executionId: Long): Unit = {
    Option(executionShuffles.remove(executionId)).filter(_.nonEmpty).foreach { shuffles =>
      val shuffleIds = shuffles.toSeq.sorted
      logDebug(s"SQL execution $executionId ended, reclaiming ${shuffleIds.size} UCX " +
        s"shuffle(s) up to task $maxTaskId: ${shuffleIds.mkString(", ")}")
      reclaim(executionId, shuffleIds, maxTaskId)
    }
  }

  /** Called when the driver plugin shuts down: the running executions' shuffles wait for GC. */
  def shutdown(): Unit = executionShuffles.clear()
}

object UcxSqlEndListener {
  /** The SQL execution that a stage runs under, read from its properties. */
  private def executionIdOf(properties: Properties): Option[Long] =
    Option(properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(s => scala.util.Try(s.toLong).toOption)

  /**
   * Whether Spark marks a map stage's output indeterminate, or lists no RDD to tell. The stage's
   * own RDD heads its RDD list.
   */
  private def isIndeterminate(stage: StageInfo): Boolean =
    stage.rddInfos.headOption.forall(_.outputDeterministicLevel == DeterministicLevel.INDETERMINATE)
}
