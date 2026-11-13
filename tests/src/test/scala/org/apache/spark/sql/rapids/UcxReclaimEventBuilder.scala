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

import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerTaskStart, StageInfo,
  TaskInfo, TaskLocality}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.storage.{RDDInfo, StorageLevel}

/**
 * Builds the scheduler events that Spark posts for the jobs of a SQL execution, so a test can
 * hand them to the UCX listener with no SparkContext. Every constructor called here
 * takes the same arguments from Spark 3.3.0 to master.
 */
object UcxReclaimEventBuilder {
  /** An uncached RDD at the deterministic level that a case needs. */
  def rddInfo(rddId: Int, level: DeterministicLevel.Value): RDDInfo =
    new RDDInfo(rddId, s"rdd $rddId", 1, StorageLevel.NONE, isBarrier = false, parentIds = Nil,
      outputDeterministicLevel = level)

  /** A map stage whose own RDD heads `rddInfos`, as Spark lists a stage's RDDs. */
  def mapStage(stageId: Int, shuffleId: Int, rddInfos: Seq[RDDInfo]): StageInfo =
    stageInfo(stageId, Some(shuffleId), rddInfos)

  /** A map stage whose own RDD is determinate. */
  def mapStage(stageId: Int, shuffleId: Int): StageInfo =
    mapStage(stageId, shuffleId, Seq(determinateRdd(stageId)))

  /** A result stage whose own RDD is determinate. */
  def resultStage(stageId: Int): StageInfo =
    stageInfo(stageId, None, Seq(determinateRdd(stageId)))

  def stageSubmitted(stage: StageInfo, executionId: Long): SparkListenerStageSubmitted =
    SparkListenerStageSubmitted(stage, underExecution(executionId))

  /** A stage that a job outside any SQL execution submits. */
  def stageSubmittedOutsideSql(stage: StageInfo): SparkListenerStageSubmitted =
    SparkListenerStageSubmitted(stage, new Properties())

  def taskStart(stageId: Int, taskId: Long): SparkListenerTaskStart =
    SparkListenerTaskStart(stageId, 0, taskInfo(taskId))

  /** The first attempt of a stage with one task. */
  private def stageInfo(stageId: Int, shuffleId: Option[Int], rddInfos: Seq[RDDInfo]): StageInfo =
    new StageInfo(stageId, 0, s"stage $stageId", 1, rddInfos, Nil, "",
      shuffleDepId = shuffleId, resourceProfileId = 0)

  private def determinateRdd(rddId: Int): RDDInfo =
    rddInfo(rddId, DeterministicLevel.DETERMINATE)

  /** The properties of a job or stage that runs under SQL execution `executionId`. */
  private def underExecution(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }

  private def taskInfo(taskId: Long): TaskInfo =
    new TaskInfo(taskId, index = 0, attemptNumber = 0, partitionId = 0, launchTime = 0L,
      executorId = "1", host = "localhost", taskLocality = TaskLocality.PROCESS_LOCAL,
      speculative = false)
}
