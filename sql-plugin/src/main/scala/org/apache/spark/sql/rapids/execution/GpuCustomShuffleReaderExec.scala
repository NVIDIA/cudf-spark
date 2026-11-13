/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION. All rights reserved.
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

import com.nvidia.spark.rapids.{CoalesceGoal, GpuExec, GpuMetric}
import com.nvidia.spark.rapids.shims.{CoalescedHashPartitioningShim, ShimUnaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning,
  RangePartitioning, RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialMapperPartitionSpec,
  PartialReducerPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement.
 */
case class GpuCustomShuffleReaderExec(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec]) extends ShimUnaryExecNode with GpuExec  {
  import GpuMetric._

  /**
   * We intentionally override metrics in this case rather than overriding additionalMetrics so
   * that NUM_OUTPUT_ROWS and NUM_OUTPUT_BATCHES are removed, since this operator does not
   * report any data for those metrics.
   *
   * The Spark version of this operator does not output any metrics.
   */
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    PARTITION_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_PARTITION_SIZE),
    NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
    OP_TIME_NEW_SHUFFLE_READ ->
      createOpTimeMetric(DESCRIPTION_OP_TIME_NEW_SHUFFLE_READ),
  )

  override def getOpTimeNewMetric: Option[GpuMetric] = allMetrics.get(OP_TIME_NEW_SHUFFLE_READ)

  override def output: Seq[Attribute] = child.output
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle reader with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
            partitionSpecs.length) {
      child match {
        case sqse: ShuffleQueryStageExec if sqse.plan.isInstanceOf[ShuffleExchangeLike] =>
          sqse.plan.asInstanceOf[ShuffleExchangeLike].child.outputPartitioning
        case sqse: ShuffleQueryStageExec if sqse.plan.isInstanceOf[ReusedExchangeExec] =>
          val reused = sqse.plan.asInstanceOf[ReusedExchangeExec]
          reused.child match {
            case sel: ShuffleExchangeLike => sel.child.outputPartitioning match {
              case e: Expression => reused.updateAttr(e).asInstanceOf[Partitioning]
              case other => other
            }
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else if (isCoalescedRead) {
      // For coalesced shuffle read, the data distribution is not changed, only the number of
      // partitions is changed.
      child.outputPartitioning match {
        case h: HashPartitioning =>
          CoalescedHashPartitioningShim(h, partitionSpecs)
        case r: RangePartitioning =>
          r.copy(numPartitions = partitionSpecs.length)
        // This can only happen for `REBALANCE_PARTITIONS_BY_NONE`, which uses
        // `RoundRobinPartitioning` but we don't need to retain the number of partitions.
        case r: RoundRobinPartitioning =>
          r.copy(numPartitions = partitionSpecs.length)
        case other @ SinglePartition =>
          throw new IllegalStateException(
            "Unexpected partitioning for coalesced shuffle read: " + other)
        case _ =>
          // Spark plugins may have custom partitioning and may replace this operator
          // during the postStageOptimization phase, so return UnknownPartitioning here
          // rather than throw an exception
          UnknownPartitioning(partitionSpecs.length)
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  override def stringArgs: Iterator[Any] = {
    val desc = if (isLocalReader) {
      "local"
    } else if (hasCoalescedPartition && hasSkewedPartition) {
      "coalesced and skewed"
    } else if (hasCoalescedPartition) {
      "coalesced"
    } else if (hasSkewedPartition) {
      "skewed"
    } else {
      ""
    }
    Iterator(desc)
  }

  def hasCoalescedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[CoalescedPartitionSpec])

  def hasSkewedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialReducerPartitionSpec])

  def isLocalReader: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])

  def isCoalescedRead: Boolean = {
    partitionSpecs.sliding(2).forall {
      // A single partition spec which is `CoalescedPartitionSpec` also means coalesced read.
      case Seq(_: CoalescedPartitionSpec) => true
      case Seq(l: CoalescedPartitionSpec, r: CoalescedPartitionSpec) =>
        l.endReducerIndex <= r.startReducerIndex
      case _ => false
    }
  }

  private var cachedShuffleRDD: RDD[ColumnarBatch] = null

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException()
  }

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          val shuffle = stage.shuffle.asInstanceOf[GpuShuffleExchangeExecBase]
          new ShuffledBatchRDD(
            shuffle.shuffleDependencyColumnar, shuffle.readMetrics ++ metrics,
            partitionSpecs.toArray)
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    }
    cachedShuffleRDD

  }
}
