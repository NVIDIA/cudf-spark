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
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * A SparkListener that triggers shuffle cleanup when a SQL query completes.
 *
 * This listener solves the problem of GC-based shuffle cleanup being unreliable
 * for short-running queries. Instead of waiting for ShuffleDependency objects to be
 * garbage collected, this listener proactively triggers cleanup when a query ends.
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
 *
 * Note: This file is placed in org.apache.spark.sql.rapids package to access
 * the private[spark] shuffleDepId field in StageInfo.
 */
class ShuffleCleanupListener extends SparkListener with Logging {

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

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionEnd =>
        onSQLExecutionEnd(e)
      case _ => // ignore other events
    }
  }

  private def onSQLExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val shuffleIds = Option(executionShuffles.remove(event.executionId))
    shuffleIds.foreach { ids =>
      if (ids.nonEmpty) {
        logDebug(s"SQL execution ${event.executionId} ended, triggering cleanup for " +
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
  def shutdown(): Unit = {
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
