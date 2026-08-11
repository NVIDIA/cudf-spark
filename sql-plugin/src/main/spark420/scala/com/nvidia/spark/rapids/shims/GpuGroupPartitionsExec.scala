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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

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

  override def tagPlanForGpu(): Unit = {
    if (groupPartitions.enableSortedMerge) {
      willNotWorkOnGpu("Sorted-merge GroupPartitionsExec is not supported on GPU")
    }
  }

  override def convertToCpu(): SparkPlan = {
    // GroupPartitionsExec reads its child's KeyedPartitioning at execution time.
    // If this node cannot be converted to GPU, keep the original CPU subtree so
    // child conversions do not replace the required partitioning.
    groupPartitions
  }

  override def convertToGpu(): GpuExec = {
    val groupInfo = GpuGroupPartitionsExecInfo(groupPartitions)
    GpuGroupPartitionsExec(
      childPlans.head.convertIfNeeded(),
      groupInfo)
  }
}

case class GpuGroupPartitionsExecInfo(
    outputPartitioning: Partitioning,
    outputOrdering: Seq[SortOrder],
    partitionGroups: Seq[Seq[Int]],
    joinKeyPositions: Option[Seq[Int]],
    expectedPartitionKeyCount: Option[Int],
    reducerNames: Option[Seq[String]],
    distributePartitions: Boolean)

object GpuGroupPartitionsExecInfo {
  def apply(groupPartitions: GroupPartitionsExec): GpuGroupPartitionsExecInfo = {
    GpuGroupPartitionsExecInfo(
      groupPartitions.outputPartitioning,
      groupPartitions.outputOrdering,
      groupPartitions.groupedPartitions.map(_._2),
      groupPartitions.joinKeyPositions,
      groupPartitions.expectedPartitionKeys.map(_.size),
      groupPartitions.reducers.map(
        _.map(_.map(_.displayName()).getOrElse("identity"))),
      groupPartitions.distributePartitions)
  }
}

case class GpuGroupPartitionsExec(
    child: SparkPlan,
    @transient groupInfo: GpuGroupPartitionsExecInfo)
    extends ShimUnaryExecNode with GpuExec {

  // This operator only changes RDD partition grouping, so it cannot report output row or batch
  // counts directly. It still needs an op-time metric for parent operators' descendant-time
  // accounting.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    GpuMetric.OP_TIME_NEW ->
      createNanoTimingMetric(GpuMetric.MODERATE_LEVEL, GpuMetric.DESCRIPTION_OP_TIME_NEW))

  override def output = child.output

  override def outputPartitioning: Partitioning = groupInfo.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = groupInfo.outputOrdering

  // Combining parent partitions can turn one batch per parent task into multiple batches per
  // grouped task. Only TargetSize remains valid because it constrains each batch independently.
  override def outputBatching: CoalesceGoal = {
    val childBatching = GpuExec.outputBatching(child)
    if (partitionGroups.exists(_.size > 1)) {
      childBatching match {
        case target: TargetSize => target
        case _ => null
      }
    } else {
      childBatching
    }
  }

  override val coalesceAfter: Boolean = true

  def partitionGroups: Seq[Seq[Int]] = groupInfo.partitionGroups

  def expectedPartitionKeyCount: Option[Int] = groupInfo.expectedPartitionKeyCount

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
          groupInfo.outputOrdering.map(QueryPlan.normalizeExpressions(_, child.output))))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (partitionGroups.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val partitionCoalescer = new GroupedPartitionCoalescer(partitionGroups)
      child.executeColumnar().coalesce(
        partitionGroups.size,
        shuffle = false,
        Some(partitionCoalescer))
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
