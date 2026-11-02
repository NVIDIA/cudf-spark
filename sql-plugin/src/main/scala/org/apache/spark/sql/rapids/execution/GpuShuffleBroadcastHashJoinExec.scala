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

import scala.concurrent.duration.Duration

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.closeOnExcept

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ReusedExchangeExec}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Each consumer reads reducer zero, containing all mapper shards, through GPU shuffle.
 * The saved broadcast is not a child: preparing this plan must not collect its payload at
 * the driver. It is executed only if the completed shuffle fails runtime admission.
 */
case class GpuShuffleBroadcastHashJoinExec(
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
    "runtimeFallbacks" -> createMetric(ESSENTIAL_LEVEL, "runtime broadcast fallbacks"))

  override protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val exchange = buildPlan match {
      case shuffle: GpuShuffleExchangeExec => shuffle
      case _ => throw new IllegalStateException("Expected a direct GPU build shuffle")
    }
    val maxBytes = RapidsConf.SHUFFLE_BROADCAST_MAX_SIZE.get(conf)
    // Spark incorporates successful map-task accumulator updates before completing this future.
    // Table compression is excluded during planning, so map sizes cannot hide decompression growth.
    val statistics = if (exchange.inputBatchRDD.getNumPartitions == 0) {
      None
    } else {
      val mapStage = sparkContext.submitMapStage(exchange.shuffleDependencyColumnar)
      try {
        Some(ThreadUtils.awaitResult(mapStage, Duration(conf.broadcastTimeout, "seconds")))
      } finally {
        if (!mapStage.isCompleted) {
          mapStage.cancel()
        }
      }
    }
    val rows = exchange.metrics(NUM_OUTPUT_ROWS).value
    val partitionBytes = statistics.map(_.bytesByPartitionId).getOrElse(Array.empty[Long])
    val bytes = partitionBytes.foldLeft(BigInt(0))(_ + _)
    val admitted = BroadcastBuildAdmission.accepts(partitionBytes, rows, maxBytes)
    logInfo(s"Executor broadcast shuffle ${exchange.shuffleId}: rows=$rows, bytes=$bytes, " +
      s"maxBytes=$maxBytes, admitted=$admitted")
    if (!admitted) {
      gpuLongMetric("runtimeFallbacks") += 1
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
      new GpuShuffleBroadcastJoinRDD[ColumnarBatch, ColumnarBatch, ColumnarBatch](
        stream, build, (input, openBuild) => {
          val context = TaskContext.get()
          val streamIter = new CollectTimeIterator(
            NvtxRegistry.BROADCAST_JOIN_STREAM, input, streamTime)
          val built = GpuShuffleEnv.getBroadcastCache.getOrBuild(shuffleId, () => {
            builds += 1
            val batch = GpuShuffleBroadcastHelper.assemble(
              openBuild(), schema, output, localMetrics)
            closeOnExcept(batch)(
              SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
          }, () => GpuSemaphore.releaseIfNecessary(context),
            () => context.killTaskIfInterrupted(),
            () => GpuSemaphore.acquireIfNecessary(context)) { cached =>
            cached.getColumnarBatch()
          }
          doJoin(built, streamIter, joinOptions, numOutputRows, numOutputBatches,
            opTime, joinTime)
        })
    }
  }
}

object GpuShuffleBroadcastHashJoinExec {
  private def supportsGpuBuildReads(conf: RapidsConf): Boolean = {
    Option(SparkEnv.get).exists { env =>
      // The shuffle transport and codec are fixed when the application starts. A session
      // override must not make compressed map bytes look like uncompressed GPU build bytes.
      val startup = new RapidsConf(env.conf)
      startup.isShuffleBroadcastEnabled && startup.isUCXShuffleManagerMode &&
        startup.shuffleCompressionCodec.equalsIgnoreCase("none") &&
        GpuShuffleEnv.useGPUShuffle(conf)
    }
  }

  /** Preserve the standard path for unsupported environments and exchange wrappers. */
  def rewrite(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!RapidsConf.SHUFFLE_BROADCAST_ENABLED.get(plan.conf) ||
        plan.conf.adaptiveExecutionEnabled || !supportsGpuBuildReads(conf) ||
        SparkSessionUtils.sessionFromPlan(plan).sparkContext.cleaner.isEmpty ||
        RapidsConf.HASH_TABLE_REUSE.get(plan.conf) ||
        plan.exists {
          case _: ReusedExchangeExec => true
          case node => node.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression]))
        }) {
      plan
    } else {
      plan.transformUp {
        case join: GpuBroadcastHashJoinExec if !join.isNullAwareAntiJoin =>
          val build = join.buildSide match {
            case GpuBuildLeft => join.left
            case GpuBuildRight => join.right
          }
          build match {
            case broadcast: GpuBroadcastExchangeExec =>
              val shuffle = GpuShuffleExchangeExec(GpuSinglePartitioning, broadcast.child,
                ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
              val (left, right) = join.buildSide match {
                case GpuBuildLeft => (shuffle, join.right)
                case GpuBuildRight => (join.left, shuffle)
              }
              GpuShuffleBroadcastHashJoinExec(join.leftKeys, join.rightKeys, join.joinType,
                join.buildSide, join.condition, left, right, broadcast)
            case _ => join
          }
      }
    }
  }
}
