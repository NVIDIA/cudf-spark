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
    val statistics = if (shuffle.inputBatchRDD.getNumPartitions == 0) {
      None
    } else {
      val stage = context.submitMapStage(shuffle.shuffleDependencyColumnar)
      try {
        Some(ThreadUtils.awaitResult(stage,
          DriverlessBroadcastBuildAdmission.waitTimeout(exchange.conf.broadcastTimeout)))
      } finally {
        if (!stage.isCompleted) stage.cancel()
      }
    }
    val rows = shuffle.metrics(GpuMetric.NUM_OUTPUT_ROWS).value
    val uncompressed = shuffle.metrics(GpuShuffleExchangeExecBase.METRIC_DATA_SIZE).value
    val bound = DriverlessBroadcastBuildAdmission.upperBound(
      statistics.map(_.bytesByPartitionId).getOrElse(Array.empty[Long]),
      uncompressed, rows, exchange.schema)
    val limit = RapidsConf.DRIVERLESS_BROADCAST_MAX_SIZE.get(exchange.conf)
    val admitted = DriverlessBroadcastBuildAdmission.accepts(bound, limit)
    val reason = if (admitted) "within build-size limit" else if (bound.isEmpty) {
      "no safe assembled-size bound"
    } else {
      "assembled-size bound exceeds limit"
    }
    logInfo(s"Driverless AQE build admission for exchange ${exchange.id}, " +
      s"shuffle ${shuffle.shuffleId}: rows=$rows, uncompressedBytes=$uncompressed, " +
      s"upperBound=$bound, maxBytes=$limit, admitted=$admitted, reason=$reason")
    if (admitted) {
      // AQE needs real runtime cardinality and bytes before replanning its parent joins.
      exchange.metrics(GpuMetric.NUM_OUTPUT_ROWS).set(rows)
      exchange.metrics("dataSize").set(uncompressed)
      exchange.metrics("driverlessBuildBytes").set(uncompressed)
      val token = context.broadcast[Any](shuffle.shuffleId)
      completedStatistics = statistics
      accepted = true
      Some(token)
    } else {
      exchange.metrics("driverlessStageFallbacks").add(1)
      context.cleaner.foreach(_.doCleanupShuffle(shuffle.shuffleId, blocking = true))
      None
    }
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
    exchange.metrics("fallbackBroadcastBytes").set(standard.metrics("dataSize").value)
    SQLMetrics.postDriverMetricUpdates(context,
      context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      updated :+ exchange.metrics("fallbackBroadcastBytes"))
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
    val conf = new RapidsConf(original.conf)
    if (original.conf.adaptiveExecutionEnabled &&
        RapidsConf.DRIVERLESS_BROADCAST_ENABLED.get(original.conf) &&
        !RapidsConf.HASH_TABLE_REUSE.get(original.conf) &&
        GpuDriverlessBroadcastHashJoinExec.gpuBuildReadRejections(conf).isEmpty) {
      val candidates = converted.collect {
        case join: GpuBroadcastHashJoinExec if !join.isNullAwareAntiJoin && !join.isSkewJoin =>
          (join.buildSide match {
            case GpuBuildLeft => join.left
            case GpuBuildRight => join.right
          }) match {
            case build: GpuBroadcastExchangeExec
                if DriverlessBroadcastBuildAdmission.supportsPlan(build.child) =>
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
