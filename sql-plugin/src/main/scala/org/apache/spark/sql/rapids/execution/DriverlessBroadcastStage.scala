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
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.Future

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuMetric, GpuSinglePartitioning, RapidsConf}

import org.apache.spark.{MapOutputStatistics, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ENSURE_REQUIREMENTS}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ThreadUtils

/** AQE materializes the build shuffle, while its broadcast stage carries only a small token. */
private[execution] class DriverlessBroadcastStage(
    exchange: GpuBroadcastExchangeExec, context: SparkContext)
    extends Logging {
  val shuffle: GpuShuffleExchangeExec = GpuShuffleExchangeExec(
    GpuSinglePartitioning, exchange.child, ENSURE_REQUIREMENTS)(GpuSinglePartitioning)

  @volatile private var accepted = false
  private var completedStatistics: Option[MapOutputStatistics] = None

  def isAdmitted: Boolean = accepted
  def mapStatistics: Option[MapOutputStatistics] = completedStatistics

  def materialize(): Option[Broadcast[Any]] = {
    val future = shuffle.mapOutputStatisticsFuture
    val statistics = try {
      Option(ThreadUtils.awaitResult(future,
        DriverlessBroadcastBuildAdmission.waitTimeout(exchange.conf.broadcastTimeout)))
    } finally {
      // The exchange sets this job group before starting its build future.
      if (!future.isCompleted) context.cancelJobGroup(exchange.runId.toString)
    }
    val rows = shuffle.metrics(GpuMetric.NUM_OUTPUT_ROWS).value
    DriverlessBroadcastBuildAdmission.requireSupportedRowCount(rows)
    val uncompressed = shuffle.metrics(GpuShuffleExchangeExecBase.METRIC_DATA_SIZE).value
    // AQE requires materialization and runtime statistics before replanning its consumers.
    // No separate size policy changes the broadcast strategy Spark has already selected.
    exchange.metrics(GpuMetric.NUM_OUTPUT_ROWS).set(rows)
    exchange.metrics("dataSize").set(uncompressed)
    exchange.metrics("driverlessBuildBytes").set(uncompressed)
    val token = context.broadcast[Any](shuffle.shuffleId)
    completedStatistics = statistics
    accepted = true
    logInfo(s"Driverless AQE build ready for exchange ${exchange.id}, " +
      s"shuffle ${shuffle.shuffleId}: rows=$rows, uncompressedBytes=$uncompressed")
    Some(token)
  }

  // A later AQE plan or unsupported consumer may require a real standard broadcast. Its
  // payload must never be confused with the token returned to AQE's stage completion future.
  // An eager exchange lets cancellation access its future without acquiring the
  // standardResult lazy-initialization lock held by a waiting broadcast consumer.
  protected val standard = GpuBroadcastExchangeExec(exchange.mode, exchange.child)(
    exchange.cpuCanonical)

  @volatile private var standardSelected = false

  def activeBroadcastFuture: Option[Future[Broadcast[Any]]] =
    if (standardSelected) Some(standard.relationFuture) else None

  private lazy val standardResult = {
    logInfo(s"Driverless AQE build for exchange ${exchange.id}, shuffle ${shuffle.shuffleId} " +
      "is consumed through standard broadcast")
    exchange.metrics("driverlessStageFallbacks").add(1)
    SQLMetrics.postDriverMetricUpdates(context,
      context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      Seq(exchange.metrics("driverlessStageFallbacks")))
    val result = standard.executeColumnarBroadcast[Any]()
    val updated = Seq(GpuMetric.NUM_OUTPUT_BATCHES, GpuMetric.COLLECT_TIME,
      GpuMetric.BUILD_TIME, "broadcastTime").flatMap { name =>
      exchange.gpuLongMetric(name).set(standard.gpuLongMetric(name).value)
      exchange.metrics.get(name)
    }
    exchange.gpuLongMetric("fallbackBroadcastBytes").set(standard.metrics("dataSize").value)
    SQLMetrics.postDriverMetricUpdates(context,
      context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      updated ++ exchange.metrics.get("fallbackBroadcastBytes"))
    result
  }

  def standardBroadcast(): Broadcast[Any] = {
    // Publish the cancellation owner before starting or waiting for the fallback build.
    standardSelected = true
    standardResult
  }
}

object DriverlessBroadcastStage {
  val eligible = TreeNodeTag[Boolean]("rapids.driverlessBroadcast.aqeBuild")

  /** Tag only exchanges whose original parent was successfully converted to a GPU hash join. */
  def tagEligibleBuilds(original: SparkPlan, converted: SparkPlan): Unit = {
    if (original.conf.adaptiveExecutionEnabled &&
        RapidsConf.DRIVERLESS_BROADCAST_ENABLED.get(original.conf)) {
      original.foreach {
        case build: BroadcastExchangeExec => build.unsetTagValue(eligible)
        case _ =>
      }
      val conf = new RapidsConf(original.conf)
      val eligibility = DriverlessBroadcastEligibility.analyze(converted, conf)
      if (eligibility.reasons.nonEmpty) return
      // Equivalent exchanges may share a token only when every visible consumer can use
      // the GPU build. CPU joins and subqueries still require a real broadcast payload.
      val candidates = converted.collect {
        case join: GpuBroadcastHashJoinExec
            if DriverlessBroadcastEligibility.joinRejections(join).isEmpty =>
          (join.buildSide match {
            case GpuBuildLeft => join.left
            case GpuBuildRight => join.right
          }) match {
            case build: GpuBroadcastExchangeExec
                if eligibility.rejection(join, Some(build)).isEmpty =>
              Some(build.cpuCanonical)
            case _ => None
          }
      }.flatten
      original.foreach {
        case build: BroadcastExchangeExec
            if candidates.exists(_.fastEquals(build.canonicalized)) =>
          build.setTagValue(eligible, true)
        case _ =>
      }
    }
  }
}
