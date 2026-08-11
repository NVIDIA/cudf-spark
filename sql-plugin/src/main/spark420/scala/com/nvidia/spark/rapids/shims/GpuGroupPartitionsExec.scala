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
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{GroupedPartitionCoalescer,
  GroupPartitionsExec}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GroupPartitionsExecMeta(
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
    groupedPartitions: Seq[(InternalRowComparableWrapper, Seq[Int])],
    isGrouped: Boolean,
    joinKeyPositions: Option[Seq[Int]],
    expectedPartitionKeys: Option[Seq[(InternalRowComparableWrapper, Int)]],
    reducers: Option[Seq[Option[Reducer[_, _]]]],
    distributePartitions: Boolean)

object GpuGroupPartitionsExecInfo {
  def apply(groupPartitions: GroupPartitionsExec): GpuGroupPartitionsExecInfo = {
    GpuGroupPartitionsExecInfo(
      groupPartitions.outputPartitioning,
      groupPartitions.outputOrdering,
      groupPartitions.groupedPartitions,
      groupPartitions.isGrouped,
      groupPartitions.joinKeyPositions,
      groupPartitions.expectedPartitionKeys,
      groupPartitions.reducers,
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

  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  override val coalesceAfter: Boolean = true

  def groupedPartitions: Seq[(InternalRowComparableWrapper, Seq[Int])] =
    groupInfo.groupedPartitions

  def isGrouped: Boolean = groupInfo.isGrouped

  def expectedPartitionKeys: Option[Seq[(InternalRowComparableWrapper, Int)]] =
    groupInfo.expectedPartitionKeys

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (groupedPartitions.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
      child.executeColumnar().coalesce(
        groupedPartitions.size,
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
    val expectedStr =
      groupInfo.expectedPartitionKeys.map(keys => s"ExpectedPartitionKeys: ${keys.size}")
    val reducersStr = groupInfo.reducers.map { values =>
      val names = values.map(_.map(_.displayName()).getOrElse("identity"))
      s"Reducers: ${truncatedString(names, "[", ", ", "]", joinKeyMaxFields)}"
    }
    val distributeStr = Iterator(s"DistributePartitions: ${groupInfo.distributePartitions}")
    joinKeyStr ++ expectedStr ++ reducersStr ++ distributeStr
  }
}
