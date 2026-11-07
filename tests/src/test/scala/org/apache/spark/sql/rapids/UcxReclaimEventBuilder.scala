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

import org.apache.spark.{TaskEndReason, TaskKilled}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{JobFailed, JobResult, SparkListenerExecutorMetricsUpdate,
  SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd,
  SparkListenerTaskStart, StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.storage.{RDDInfo, StorageLevel}

/**
 * Builds the scheduler events that Spark posts for the jobs of a SQL execution, so a test can
 * hand them to a shuffle cleanup policy with no SparkContext. Every constructor called here
 * takes the same arguments from Spark 3.3.0 to master.
 */
object UcxReclaimEventBuilder {
  /** An uncached RDD at the deterministic level that a case needs. */
  def rddInfo(rddId: Int, level: DeterministicLevel.Value): RDDInfo =
    new RDDInfo(rddId, s"rdd $rddId", 1, StorageLevel.NONE, isBarrier = false, parentIds = Nil,
      outputDeterministicLevel = level)

  /** A map stage whose own RDD heads `rddInfos`, as Spark lists a stage's RDDs. */
  def mapStage(
      stageId: Int,
      shuffleId: Int,
      parentIds: Seq[Int],
      rddInfos: Seq[RDDInfo]): StageInfo =
    stageInfo(stageId, Some(shuffleId), parentIds, rddInfos)

  /** A map stage whose own RDD is determinate. */
  def mapStage(stageId: Int, shuffleId: Int, parentIds: Seq[Int]): StageInfo =
    mapStage(stageId, shuffleId, parentIds, Seq(determinateRdd(stageId)))

  /** A result stage whose own RDD is determinate. */
  def resultStage(stageId: Int, parentIds: Seq[Int]): StageInfo =
    stageInfo(stageId, None, parentIds, Seq(determinateRdd(stageId)))

  /** A job of SQL execution `executionId`, listing every stage it may run, skipped or not. */
  def jobStart(jobId: Int, executionId: Long, stages: Seq[StageInfo]): SparkListenerJobStart =
    SparkListenerJobStart(jobId, 0L, stages, underExecution(executionId))

  def jobEnd(jobId: Int, result: JobResult): SparkListenerJobEnd =
    SparkListenerJobEnd(jobId, 0L, result)

  def jobFailed: JobResult = JobFailed(new Exception("a task of the job failed"))

  def stageSubmitted(stage: StageInfo, executionId: Long): SparkListenerStageSubmitted =
    SparkListenerStageSubmitted(stage, underExecution(executionId))

  def stageCompleted(stage: StageInfo): SparkListenerStageCompleted =
    SparkListenerStageCompleted(stage)

  /** The end of a failed stage attempt, which Spark marks on the attempt's own info. */
  def stageFailed(stage: StageInfo): SparkListenerStageCompleted = {
    stage.stageFailed("a task of the stage failed")
    SparkListenerStageCompleted(stage)
  }

  def taskStart(
      stageId: Int,
      stageAttemptId: Int,
      taskId: Long,
      executorId: String): SparkListenerTaskStart =
    SparkListenerTaskStart(stageId, stageAttemptId, taskInfo(taskId, executorId))

  /**
   * A task's end. The policy reads no task type or metrics, so they are placeholders, as in
   * Spark's own listener tests.
   */
  def taskEnd(
      stageId: Int,
      stageAttemptId: Int,
      taskId: Long,
      executorId: String,
      reason: TaskEndReason): SparkListenerTaskEnd =
    SparkListenerTaskEnd(stageId, stageAttemptId, "taskType", reason,
      taskInfo(taskId, executorId), new ExecutorMetrics(), null)

  def taskKilled: TaskEndReason = TaskKilled("the stage's job ended")

  /** The driver's own heartbeat, which carries no accumulator or executor updates. */
  def driverMetricsUpdate: SparkListenerExecutorMetricsUpdate =
    SparkListenerExecutorMetricsUpdate("driver", Seq.empty)

  def executorRemoved(executorId: String): SparkListenerExecutorRemoved =
    SparkListenerExecutorRemoved(0L, executorId, "the executor was lost")

  /** The first attempt of a stage with one task. */
  private def stageInfo(
      stageId: Int,
      shuffleId: Option[Int],
      parentIds: Seq[Int],
      rddInfos: Seq[RDDInfo]): StageInfo =
    new StageInfo(stageId, 0, s"stage $stageId", 1, rddInfos, parentIds, "",
      shuffleDepId = shuffleId, resourceProfileId = 0)

  private def determinateRdd(rddId: Int): RDDInfo =
    rddInfo(rddId, DeterministicLevel.DETERMINATE)

  /** The properties of a job or stage that runs under SQL execution `executionId`. */
  private def underExecution(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }

  private def taskInfo(taskId: Long, executorId: String): TaskInfo =
    new TaskInfo(taskId, index = 0, attemptNumber = 0, partitionId = 0, launchTime = 0L,
      executorId = executorId, host = "localhost", taskLocality = TaskLocality.PROCESS_LOCAL,
      speculative = false)
}
