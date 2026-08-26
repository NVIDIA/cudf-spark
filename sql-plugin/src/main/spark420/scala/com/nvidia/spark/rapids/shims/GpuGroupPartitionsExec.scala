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

/*** spark-rapids-shim-json-lines
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{GroupedPartitionCoalescer,
  GroupPartitionsExec}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGroupPartitionsExecMeta(
    groupPartitions: GroupPartitionsExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[GroupPartitionsExec](groupPartitions, conf, parent, rule) {

  // GroupPartitionsExec passes through its child's output schema, including GPU type conversions.
  override protected val useOutputAttributesOfChild: Boolean = true

  // This plan is transparent to row-to-columnar transitions inserted above its child.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

  private val sortedMergeOrdering: Seq[BaseExprMeta[SortOrder]] =
    if (groupPartitions.enableSortedMerge) {
      groupPartitions.outputOrdering.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    } else {
      Seq.empty
    }

  override val childExprs: Seq[BaseExprMeta[_]] = sortedMergeOrdering

  override def convertToCpu(): SparkPlan = {
    // This is only the safety path when normal GPU compatibility checks reject replacement.
    // Supported ordinary and sorted-merge plans are converted to GpuGroupPartitionsExec.
    // GroupPartitionsExec reads its child's KeyedPartitioning at execution time.
    // If this node cannot be converted to GPU, keep the original CPU subtree so
    // child conversions do not replace the required partitioning.
    groupPartitions
  }

  override def convertToGpu(): GpuExec = {
    val gpuOrdering = sortedMergeOrdering.map(_.convertToGpu().asInstanceOf[SortOrder])
    val groupInfo = GpuGroupPartitionsExecInfo(groupPartitions, gpuOrdering)
    GpuGroupPartitionsExec(
      childPlans.head.convertIfNeeded(),
      groupInfo)
  }
}

case class GpuGroupPartitionsExecInfo(
    outputPartitioning: Partitioning,
    outputOrdering: Seq[SortOrder],
    gpuOutputOrdering: Seq[SortOrder],
    partitionGroups: Seq[Seq[Int]],
    joinKeyPositions: Option[Seq[Int]],
    expectedPartitionKeyCount: Option[Int],
    reducerNames: Option[Seq[String]],
    distributePartitions: Boolean,
    enableSortedMerge: Boolean)

object GpuGroupPartitionsExecInfo {
  // Snapshot the planning metadata needed after the CPU operator has been replaced.
  def apply(
      groupPartitions: GroupPartitionsExec,
      gpuOutputOrdering: Seq[SortOrder]): GpuGroupPartitionsExecInfo = {
    GpuGroupPartitionsExecInfo(
      groupPartitions.outputPartitioning,
      groupPartitions.outputOrdering,
      gpuOutputOrdering,
      groupPartitions.groupedPartitions.map(_._2),
      groupPartitions.joinKeyPositions,
      groupPartitions.expectedPartitionKeys.map(_.size),
      groupPartitions.reducers.map(
        _.map(_.map(_.displayName()).getOrElse("identity"))),
      groupPartitions.distributePartitions,
      groupPartitions.enableSortedMerge)
  }
}

case class GpuGroupPartitionsExec(
    child: SparkPlan,
    @transient groupInfo: GpuGroupPartitionsExecInfo)
    extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  // The ordinary path only changes RDD partition grouping, while the sorted-merge path also
  // produces batches and rows through the out-of-core sorter.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)) ++
      (if (needsSortedMerge) {
        Map(
          NUM_OUTPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
          NUM_OUTPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES),
          OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
          SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME))
      } else {
        Map.empty
      })

  override def output = child.output

  override def outputPartitioning: Partitioning = groupInfo.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = groupInfo.outputOrdering

  // Combining parent partitions can turn one batch per parent task into multiple batches per
  // grouped task. Only TargetSize remains valid because it constrains each batch independently.
  override def outputBatching: CoalesceGoal = {
    val childBatching = GpuExec.outputBatching(child)
    if (needsSortedMerge) {
      // GpuOutOfCoreSortIterator emits multiple target-sized batches but does not advertise a
      // batching guarantee.
      null
    } else if (hasCoalescing) {
      childBatching match {
        case target: TargetSize => target
        case _ => null
      }
    } else {
      childBatching
    }
  }

  // The out-of-core sorter already emits target-sized batches.
  override def coalesceAfter: Boolean = !needsSortedMerge

  def partitionGroups: Seq[Seq[Int]] = groupInfo.partitionGroups

  def enableSortedMerge: Boolean = groupInfo.enableSortedMerge

  def expectedPartitionKeyCount: Option[Int] = groupInfo.expectedPartitionKeyCount

  private def hasCoalescing: Boolean = partitionGroups.exists(_.size > 1)

  // A single-parent group already retains its reported ordering, so it does not need sorting.
  private def needsSortedMerge: Boolean =
    enableSortedMerge && hasCoalescing && groupInfo.gpuOutputOrdering.nonEmpty

  override protected def doCanonicalize(): SparkPlan = {
    val normalizedPartitioning = groupInfo.outputPartitioning match {
      case p: (Partitioning with Expression) =>
        QueryPlan.normalizeExpressions(p, child.output)
      case other => other
    }
    copy(
      child = child.canonicalized,
      groupInfo = groupInfo.copy(
        outputPartitioning = normalizedPartitioning,
        outputOrdering =
          groupInfo.outputOrdering.map(QueryPlan.normalizeExpressions(_, child.output)),
        gpuOutputOrdering =
          groupInfo.gpuOutputOrdering.map(QueryPlan.normalizeExpressions(_, child.output))))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")
  }

  private def sortCoalescedPartitions(
      coalesced: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    // Concatenate each partition group first and then perform a spillable full GPU sort. This
    // is a correctness-first implementation of Spark's row-based k-way merge and reuses the
    // same retry and resource-management path as GpuSortExec.
    val partitionsNeedingSort = partitionGroups.map(_.size > 1).toArray
    val sorter = new GpuSorter(groupInfo.gpuOutputOrdering, output, allMetrics)
    val targetSize = GpuSortExec.targetSize(conf)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val sortTime = gpuLongMetric(SORT_TIME)
    val outputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    coalesced.mapPartitionsWithIndex { case (partitionIndex, batches) =>
      // Only combining multiple independently sorted streams can invalidate global ordering.
      if (partitionsNeedingSort(partitionIndex)) {
        val sorted = GpuOutOfCoreSortIterator(
          batches,
          sorter,
          targetSize,
          opTime,
          sortTime,
          outputBatches,
          outputRows)
        onTaskCompletion(sorted.close())
        sorted
      } else {
        // A single parent partition already satisfies the reported ordering. Empty padded
        // groups also flow through this path without constructing a sorter iterator.
        batches.map { batch =>
          outputBatches += 1
          outputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (partitionGroups.isEmpty) {
      sparkContext.emptyRDD
    } else {
      // Preserve Spark's exact parent-to-output partition mapping without introducing a shuffle.
      val partitionCoalescer = new GroupedPartitionCoalescer(partitionGroups)
      val coalesced = child.executeColumnar().coalesce(
        partitionGroups.size,
        shuffle = false,
        Some(partitionCoalescer))
      if (needsSortedMerge) {
        sortCoalescedPartitions(coalesced)
      } else {
        coalesced
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${planSummaryParts(maxFields).map(" " + _).mkString("")}"
  }

  override def stringArgs: Iterator[Any] = planSummaryParts(Int.MaxValue) ++ loreArgs

  private def planSummaryParts(joinKeyMaxFields: Int): Iterator[String] = {
    val joinKeyStr = groupInfo.joinKeyPositions.map { positions =>
      s"JoinKeyPositions: ${truncatedString(positions, "[", ", ", "]", joinKeyMaxFields)}"
    }.iterator
    val expectedStr = groupInfo.expectedPartitionKeyCount.map { count =>
      s"ExpectedPartitionKeys: $count"
    }
    val reducersStr = groupInfo.reducerNames.map { names =>
      s"Reducers: ${truncatedString(names, "[", ", ", "]", joinKeyMaxFields)}"
    }
    val distributeStr = Iterator(s"DistributePartitions: ${groupInfo.distributePartitions}")
    joinKeyStr ++ expectedStr ++ reducersStr ++ distributeStr
  }
}
