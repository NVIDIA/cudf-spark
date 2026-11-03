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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.closeOnExcept

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Each consumer reads reducer zero, containing all mapper shards, through GPU shuffle.
 * The saved broadcast is not a child: preparing this plan must not collect its payload at
 * the driver. It is executed only if the completed shuffle fails runtime admission.
 */
case class GpuDriverlessBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    @transient fallbackBroadcast: GpuBroadcastExchangeExec)
    extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, false) {

  import GpuMetric._

  override def broadcastExchange: GpuBroadcastExchangeExec = fallbackBroadcast

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, buildSide, condition)

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(UnspecifiedDistribution, UnspecifiedDistribution)

  override lazy val additionalMetrics: Map[String, GpuMetric] = broadcastJoinMetrics ++ Map(
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    "executorBuilds" -> createMetric(ESSENTIAL_LEVEL, "executor broadcast builds"),
    "runtimeFallbacks" -> createMetric(ESSENTIAL_LEVEL, "runtime broadcast fallbacks"),
    // AQE's final plan replaces the broadcast stage. Keep its driver metrics in that plan.
    "buildStageTime" -> fallbackBroadcast.gpuLongMetric("driverlessStageTime"),
    "buildStageBytes" -> fallbackBroadcast.gpuLongMetric("driverlessBuildBytes"))

  override protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val exchange = buildPlan match {
      case shuffle: GpuShuffleExchangeExec => shuffle
      case _ => throw new IllegalStateException("Expected a direct GPU build shuffle")
    }
    val maxBytes = RapidsConf.DRIVERLESS_BROADCAST_MAX_SIZE.get(conf)
    // Spark incorporates successful map-task accumulator updates before completing this future.
    // The writer metric records uncompressed bytes even when map-status sizes are compressed.
    val preparedStatistics = fallbackBroadcast.admittedDriverlessBuild.collect {
      case (prepared, statistics) if prepared.shuffleId == exchange.shuffleId => statistics
    }
    // AQE already completed and admitted this shuffle. Submitting the cached map stage again
    // is unnecessary and can produce duplicate job-end events on Spark 3.5.3.
    val statistics = preparedStatistics.getOrElse {
      if (exchange.inputBatchRDD.getNumPartitions == 0) {
        None
      } else {
        val mapStage = sparkContext.submitMapStage(exchange.shuffleDependencyColumnar)
        try {
          val timeout = DriverlessBroadcastBuildAdmission.waitTimeout(conf.broadcastTimeout)
          Some(ThreadUtils.awaitResult(mapStage, timeout))
        } finally {
          if (!mapStage.isCompleted) {
            mapStage.cancel()
          }
        }
      }
    }
    val rows = exchange.metrics(NUM_OUTPUT_ROWS).value
    val partitionBytes = statistics.map(_.bytesByPartitionId).getOrElse(Array.empty[Long])
    val bytes = partitionBytes.foldLeft(BigInt(0))(_ + _)
    val uncompressedBytes = exchange.metrics(GpuShuffleExchangeExecBase.METRIC_DATA_SIZE).value
    val sizeBound = DriverlessBroadcastBuildAdmission.upperBound(
      partitionBytes, uncompressedBytes, rows, buildPlan.schema)
    val admitted = DriverlessBroadcastBuildAdmission.accepts(sizeBound, maxBytes)
    val reason = if (admitted) "within build-size limit" else if (sizeBound.isEmpty) {
      "no safe assembled-size bound"
    } else {
      "assembled-size bound exceeds limit"
    }
    logInfo(s"Driverless broadcast admission for join $id, shuffle ${exchange.shuffleId}: " +
      s"rows=$rows, bytes=$bytes, " +
      s"uncompressedBytes=$uncompressedBytes, upperBound=$sizeBound, " +
      s"maxBytes=$maxBytes, admitted=$admitted, reason=$reason")
    if (!admitted) {
      gpuLongMetric("runtimeFallbacks") += 1
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
        Seq(metrics("runtimeFallbacks")))
      sparkContext.cleaner.foreach(_.doCleanupShuffle(exchange.shuffleId, blocking = true))
      super.doColumnarBroadcastJoin()
    } else {
      val build = exchange.executeColumnar()
      val stream = streamedPlan.executeColumnar()
      val shuffleId = exchange.shuffleId
      val schema = buildPlan.schema
      val output = buildPlan.output
      val localMetrics = allMetrics
      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val opTime = gpuLongMetric(OP_TIME_LEGACY)
      val streamTime = gpuLongMetric(STREAM_TIME)
      val joinTime = gpuLongMetric(JOIN_TIME)
      val builds = gpuLongMetric("executorBuilds")
      val joinOptions = RapidsConf.getJoinOptions(conf, RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf))
      val rapidsConf = new RapidsConf(conf)
      val codecConfigs = TableCompressionCodec.makeCodecConfig(rapidsConf)
      val maxDecompressBatchMemory = rapidsConf.shuffleCompressionMaxBatchMemory
      new GpuDriverlessBroadcastJoinRDD[ColumnarBatch, ColumnarBatch, ColumnarBatch](
        stream, build, (input, openBuild) => {
          val context = TaskContext.get()
          val streamIter = new CollectTimeIterator(
            NvtxRegistry.BROADCAST_JOIN_STREAM, input, streamTime)
          val built = GpuShuffleEnv.getBroadcastCache.getOrBuild(shuffleId, () => {
            builds += 1
            val batch = GpuDriverlessBroadcastHelper.assemble(
              openBuild(), schema, output, localMetrics, codecConfigs, maxDecompressBatchMemory)
            closeOnExcept(batch)(
              SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
          }, () => GpuSemaphore.releaseIfNecessary(context),
            () => context.killTaskIfInterrupted(),
            () => GpuSemaphore.acquireIfNecessary(context),
            generation = ExecutorBroadcastCache.generation(
              context.stageId(), context.stageAttemptNumber())) { cached =>
            cached.getColumnarBatch()
          }
          doJoin(built, streamIter, joinOptions, numOutputRows, numOutputBatches,
            opTime, joinTime)
        })
    }
  }
}

object GpuDriverlessBroadcastHashJoinExec extends Logging {
  def gpuBuildReadRejections(conf: RapidsConf): Seq[String] = {
    Option(SparkEnv.get).map { env =>
      // The transport is fixed at application startup. The build decoder handles the codec
      // recorded in each batch, and admission uses the writer's uncompressed byte metric.
      val startup = new RapidsConf(env.conf)
      Seq(
        if (!startup.isDriverlessBroadcastEnabled) Some("application-startup opt-in is disabled")
          else None,
        if (!startup.isUCXShuffleManagerMode) Some("UCX shuffle mode is required") else None,
        if (!GpuShuffleEnv.useGPUShuffle(conf)) Some("GPU shuffle is unavailable") else None
      ).flatten
    }.getOrElse(Seq("SparkEnv is not initialized"))
  }

  /** Preserve the standard path for unsupported environments and exchange wrappers. */
  def rewrite(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!RapidsConf.DRIVERLESS_BROADCAST_ENABLED.get(plan.conf)) {
      plan
    } else {
      val reasons = gpuBuildReadRejections(conf) ++ Seq(
        if (SparkSessionUtils.sessionFromPlan(plan).sparkContext.cleaner.isEmpty) {
          Some("Spark context cleaner is disabled")
        } else None,
        if (RapidsConf.HASH_TABLE_REUSE.get(plan.conf)) Some("hash-table reuse is enabled")
          else None,
        if (plan.exists(_.isInstanceOf[ReusedExchangeExec])) {
          Some("plan contains a reused exchange")
        } else None,
        if (plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])))) {
          Some("plan contains a subquery")
        } else None
      ).flatten
      if (reasons.nonEmpty) {
        logInfo(s"Driverless broadcast not selected: ${reasons.mkString("; ")}")
        plan
      } else {
        var sawBroadcastJoin = false
        val rewritten = plan.transformUp {
          case join: GpuBroadcastHashJoinExec =>
            sawBroadcastJoin = true
            val build = join.buildSide match {
              case GpuBuildLeft => join.left
              case GpuBuildRight => join.right
            }
            val broadcast = build match {
              case direct: GpuBroadcastExchangeExec if !plan.conf.adaptiveExecutionEnabled =>
                Some(direct)
              case stage: BroadcastQueryStageExec => stage.plan match {
                case direct: GpuBroadcastExchangeExec
                    if direct.admittedDriverlessShuffle.isDefined => Some(direct)
                case _ => None
              }
              case _ => None
            }
            val rejection = if (join.isNullAwareAntiJoin) {
              Some("null-aware anti join")
            } else if (join.isSkewJoin) {
              Some("skew-optimized broadcast join")
            } else {
              broadcast match {
                case Some(exchange) =>
                  DriverlessBroadcastBuildAdmission.rejectionReason(exchange.child)
                case None => Some(s"no admitted driverless build: ${build.nodeName}")
              }
            }
            rejection match {
              case Some(reason) =>
                logInfo(s"Driverless broadcast not selected for join ${join.id}: $reason")
                join
              case None =>
                val exchange = broadcast.get
                val shuffle = exchange.admittedDriverlessShuffle.getOrElse {
                  GpuShuffleExchangeExec(GpuSinglePartitioning, exchange.child,
                    ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
                }
                val (left, right) = join.buildSide match {
                  case GpuBuildLeft => (shuffle, join.right)
                  case GpuBuildRight => (join.left, shuffle)
                }
                logInfo(s"Driverless broadcast planned for join ${join.id}; " +
                  "execution requires runtime size admission")
                GpuDriverlessBroadcastHashJoinExec(join.leftKeys, join.rightKeys, join.joinType,
                  join.buildSide, join.condition, left, right, exchange)
            }
        }
        if (!sawBroadcastJoin) {
          logInfo("Driverless broadcast not selected: no GPU broadcast hash join in this plan")
        }
        rewritten
      }
    }
  }
}
