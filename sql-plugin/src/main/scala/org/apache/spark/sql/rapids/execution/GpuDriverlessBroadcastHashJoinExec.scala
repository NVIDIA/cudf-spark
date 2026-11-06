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
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ReusedExchangeExec}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Each consumer reads reducer zero, containing all mapper shards, through GPU shuffle.
 * The saved broadcast is not a child: preparing this plan must not collect its payload at
 * the driver. It retains AQE stage metadata; a standard AQE consumer resolves its payload
 * through the original exchange. Materialized AQE shuffles stay behind a reuse wrapper.
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
    "executorBuilds" -> createMetric(ESSENTIAL_LEVEL, "executor broadcast builds")) ++
    (if (conf.adaptiveExecutionEnabled) Map(
      "buildStageTime" -> fallbackBroadcast.gpuLongMetric("driverlessStageTime"),
      "buildStageBytes" -> fallbackBroadcast.gpuLongMetric("driverlessBuildBytes"))
    else Map.empty[String, GpuMetric])

  override protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val exchange = buildPlan match {
      case shuffle: GpuShuffleExchangeExec => shuffle
      case ReusedExchangeExec(_, shuffle: GpuShuffleExchangeExec) => shuffle
      case _ => throw new IllegalStateException("Expected a direct GPU build shuffle")
    }
    // Both upstreams belong to this RDD graph. The scheduler can submit independent map
    // stages together; constructing this plan must not wait for the build map stage.
    val build = buildPlan.executeColumnar()
    val stream = streamedPlan.executeColumnar()
    val shuffleId = exchange.shuffleId
    val rapidsConf = new RapidsConf(conf)
    val maxExecutors = rapidsConf.driverlessBroadcastMaxExecutors
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
    val codecConfigs = TableCompressionCodec.makeCodecConfig(rapidsConf)
    val maxDecompressBatchMemory = rapidsConf.shuffleCompressionMaxBatchMemory
    new GpuDriverlessBroadcastJoinRDD[ColumnarBatch, ColumnarBatch, ColumnarBatch](
      stream, build, (input, openBuild) => {
        val context = TaskContext.get()
        val streamIter = new CollectTimeIterator(
          NvtxRegistry.BROADCAST_JOIN_STREAM, input, streamTime)
        val built = GpuShuffleEnv.getBroadcastCache.getOrBuild[ColumnarBatch](shuffleId, () => {
          DriverlessBroadcastPolicy.checkExecutorLimit(maxExecutors)
          builds += 1
          val batch = GpuDriverlessBroadcastHelper.assemble(
            openBuild(), schema, output, localMetrics, codecConfigs, maxDecompressBatchMemory)
          closeOnExcept(batch)(
            SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }, () => GpuSemaphore.releaseIfNecessary(context),
          () => context.killTaskIfInterrupted(),
          () => GpuSemaphore.acquireIfNecessary(context),
          generation = ExecutorBroadcastCache.generation(
            context.stageId(), context.stageAttemptNumber()))(
          GpuDriverlessBroadcastHelper.borrow, batch => withResource(batch)(_ => ()))
        doJoin(built, streamIter, joinOptions, numOutputRows, numOutputBatches,
          opTime, joinTime)
      })
  }
}

object GpuDriverlessBroadcastHashJoinExec extends Logging {
  /** Preserve the standard path for unsupported environments and exchange wrappers. */
  def rewrite(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!conf.isDriverlessBroadcastEnabled) {
      plan
    } else {
      val eligibility = DriverlessBroadcastEligibility.analyze(plan, conf)
      val reasons = eligibility.reasons
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
                case ReusedExchangeExec(_, direct: GpuBroadcastExchangeExec)
                    if direct.admittedDriverlessShuffle.isDefined => Some(direct)
                case _ => None
              }
              case _ => None
            }
            val rejection = eligibility.rejection(join, broadcast)
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
                // A materialized producer is opaque to later physical rewrites. Reuse keeps its
                // original dependency and metrics while preserving each consumer's attributes.
                val gpuBuild = build match {
                  case stage: BroadcastQueryStageExec =>
                    ReusedExchangeExec(stage.output, shuffle)
                  case _ => shuffle
                }
                val (left, right) = join.buildSide match {
                  case GpuBuildLeft => (gpuBuild, join.right)
                  case GpuBuildRight => (join.left, gpuBuild)
                }
                logInfo(s"Driverless broadcast planned for join ${join.id}")
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
