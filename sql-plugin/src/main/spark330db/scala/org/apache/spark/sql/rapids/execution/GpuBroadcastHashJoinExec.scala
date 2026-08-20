/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.TaskContext
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ExecutorBroadcastMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBroadcastHashJoinMetaBase(join, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val extractedCondition = GpuHashJoin.extractJoinConditionIfNeeded(
      conditionMeta, join.joinType, left, right)
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSideMeta)
    val joinExec = GpuBroadcastHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      buildSide,
      extractedCondition.joinCondition,
      extractedCondition.left,
      extractedCondition.right,
      join.isNullAwareAntiJoin,
      join.isExecutorBroadcast)
    // For inner joins we can apply a post-join condition for any conditions that cannot be
    // evaluated directly in a mixed join that leverages a cudf AST expression
    val filteredJoinExec =
      extractedCondition.filterCondition.map(c => GpuFilterExec(c, joinExec)()).getOrElse(joinExec)
    extractedCondition.projectIfNeeded(filteredJoinExec)
  }
}

case class GpuBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean,
    executorBroadcast: Boolean)
      extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, isNullAwareAntiJoin) {
  import GpuMetric._

  private type BuildBatchAndStream = (ColumnarBatch, Iterator[ColumnarBatch])

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
  )

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isExecutorBroadcast) {
      buildSide match {
        case GpuBuildLeft =>
          BroadcastDistribution(ExecutorBroadcastMode) :: UnspecifiedDistribution :: Nil
        case GpuBuildRight =>
          UnspecifiedDistribution :: BroadcastDistribution(ExecutorBroadcastMode) :: Nil
      } 
    } else {
      super.requiredChildDistribution
    }
  }

  def isExecutorBroadcast(): Boolean = {
    executorBroadcast
  }

  def shuffleExchange: GpuShuffleExchangeExec = {
    def from(p: ShuffleQueryStageExec): GpuShuffleExchangeExec = p.plan match {
      case g: GpuShuffleExchangeExec => g
      case ReusedExchangeExec(_, g: GpuShuffleExchangeExec) => g
      case _ => throw new IllegalStateException(s"cannot locate GPU shuffle in $p")
    }
    getBroadcastPlan(buildPlan) match {
      case gpu: GpuShuffleExchangeExec => gpu
      case sqse: ShuffleQueryStageExec => from(sqse)
      case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuShuffleExchangeExec]
      case GpuShuffleCoalesceExec(GpuCustomShuffleReaderExec(sqse: ShuffleQueryStageExec, _), _) =>
        from(sqse)
      // A GpuProjectExec for a bridged/split join condition (extractNonAstFromJoinCond) on the
      // executor-broadcast build side is peeled off by getBroadcastPlan, leaving the coalesce
      // directly over the shuffle query stage with no custom shuffle reader in between. The
      // nested-loop-join path already handles this same shape (added with split-condition
      // support in #9702); mirror it here.
      case GpuShuffleCoalesceExec(sqse: ShuffleQueryStageExec, _) => from(sqse)
      case GpuCustomShuffleReaderExec(sqse: ShuffleQueryStageExec, _) => from(sqse)
      case other => throw new IllegalStateException(
        s"cannot locate GPU shuffle exchange in build plan: $other")
    }
  }

  private def getExecutorBuiltBatchAndStreamIter(
      buildRelation: RDD[ColumnarBatch],
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      streamIter: Iterator[ColumnarBatch],
      coalesceMetricsMap: Map[String, GpuMetric],
      postProjectionAndClose: Option[ColumnarBatch => ColumnarBatch]): BuildBatchAndStream = {
    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val metricsMap = allMetrics

    val bufferedStreamIter = new CloseableBufferedIterator(streamIter)
    closeOnExcept(bufferedStreamIter) { _ =>
      NvtxRegistry.JOIN_FIRST_STREAM_BATCH {
        if (bufferedStreamIter.hasNext) {
          bufferedStreamIter.head
        } else {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
        }
      }
      val buildBatch = GpuExecutorBroadcastHelper.getExecutorBroadcastBatch(buildRelation,
          buildSchema, buildOutput, metricsMap, targetSize)
      val projectedBuildBatch = postProjectionAndClose.map(_(buildBatch)).getOrElse(buildBatch)
      (projectedBuildBatch, bufferedStreamIter)
    }
  }

  private[this] def doColumnarExecutorBroadcastJoin(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val joinOptions = RapidsConf.getJoinOptions(conf, targetSize)

    // Get all the broadcast data from the shuffle coalesced into a single partition 
    val partitionSpecs = Seq(CoalescedPartitionSpec(0, shuffleExchange.numPartitions))
    val buildRelation = ShuffleExchangeShim.getShuffleRDD(shuffleExchange, partitionSpecs)
        .asInstanceOf[RDD[ColumnarBatch]]

    val rdd = streamedPlan.executeColumnar()
    val localBuildSchema = getBroadcastPlan(buildPlan).schema
    val localBuildOutput = getBroadcastPlan(buildPlan).output
    val localIsNullAwareAntiJoin = isNullAwareAntiJoin
    val postProjectionAndClose = buildSidePostProjection
    rdd.mapPartitions { it =>
      val (builtBatch, streamIter) =
        getExecutorBuiltBatchAndStreamIter(
          buildRelation,
          localBuildSchema,
          localBuildOutput,
          new CollectTimeIterator(NvtxRegistry.BROADCAST_JOIN_STREAM, it, streamTime),
          allMetrics,
          postProjectionAndClose)
      if (localIsNullAwareAntiJoin) {
        // This is to support the null-aware anti join for the LeftAnti join with
        // BuildRight. See the config "spark.sql.optimizeNullAwareAntiJoin".
        // Spark already executes all the check for the requirements, e.g. join type,
        // build side, keys length == 1. So no need to do it here again.
        // This will cover mainly 3 cases as below, similar as what Spark does.
        if (builtBatch.numRows() == 0) {
          // Build side is empty, return the stream iterator directly.
          withResource(builtBatch)(_ => streamIter)
        } else if (closeOnExcept(builtBatch)(GpuHashJoin.anyNullInKey(_, boundBuildKeys))) {
          // Spark will return an empty iterator if any nulls in the right table
          withResource(builtBatch)(_ => Iterator.empty)
        } else {
          // Nulls will be filtered out
          val nullFilteredStreamIter = streamIter.map { cb =>
            GpuHashJoin.filterNullsWithRetryAndClose(
              SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
              boundStreamKeys)
          }
          doJoin(builtBatch, nullFilteredStreamIter, joinOptions, numOutputRows,
            numOutputBatches, opTime, joinTime)
        }
      } else {
        // builtBatch will be closed in doJoin
        doJoin(builtBatch, streamIter, joinOptions, numOutputRows, numOutputBatches, opTime,
          joinTime)
      }
    }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (isExecutorBroadcast) {
      doColumnarExecutorBroadcastJoin()
    } else {
      doColumnarBroadcastJoin()
    }
  }
}
