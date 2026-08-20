/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSeq, CurrentRow, Expression, NamedExpression, RangeFrame, RowFrame, SortOrder, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuWindowExec {
  def isRunningWindow(spec: GpuWindowSpecDefinition): Boolean = spec match {
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
    RowFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding),
    GpuSpecialFrameBoundary(CurrentRow))) => true
    case GpuWindowSpecDefinition(_, _,
    GpuSpecifiedWindowFrame(RowFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding), GpuLiteral(value, _)))
      if value == 0 => true
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
    RangeFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding),
    GpuSpecialFrameBoundary(CurrentRow))) => true
    case GpuWindowSpecDefinition(_, _,
    GpuSpecifiedWindowFrame(RangeFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding), GpuLiteral(value, _)))
      if value == 0 => true
    case _ => false
  }
}

trait GpuWindowBaseExec extends ShimUnaryExecNode with GpuExec {
  val windowOps: Seq[NamedExpression]
  val gpuPartitionSpec: Seq[Expression]
  val gpuOrderSpec: Seq[SortOrder]
  val cpuPartitionSpec: Seq[Expression]
  val cpuOrderSpec: Seq[SortOrder]

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  override def output: Seq[Attribute] = windowOps.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (cpuPartitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(cpuPartitionSpec) :: Nil
  }

  lazy val gpuPartitionOrdering: Seq[SortOrder] = {
    gpuPartitionSpec.map(SortOrder(_, Ascending))
  }

  lazy val cpuPartitionOrdering: Seq[SortOrder] = {
    cpuPartitionSpec.map(SortOrder(_, Ascending))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(cpuPartitionOrdering ++ cpuOrderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")
}

/**
 * An Iterator that performs window operations on the input data. It is required that the input
 * data is batched so all of the data for a given key is in the same batch. The input data must
 * also be sorted by both partition by keys and order by keys.
 */
class GpuWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {

  override def isRunningBatched: Boolean = false

  override def hasNext: Boolean = onDeck.isDefined || input.hasNext

  var onDeck: Option[SpillableColumnarBatch] = None

  override def next(): ColumnarBatch = {
    val cbSpillable = onDeck match {
      case Some(x) =>
        onDeck = None
        x
      case _ =>
        getNext()
    }
    withRetryNoSplit(cbSpillable) { _ =>
      withResource(cbSpillable.getColumnarBatch()) { cb =>
        NvtxIdWithMetrics(NvtxRegistry.WINDOW_EXEC, opTime) {
          val ret = withResource(computeBasicWindow(cb)) { cols =>
            convertToBatch(outputTypes, cols)
          }
          numOutputBatches += 1
          numOutputRows += ret.numRows()
          ret
        }
      }
    }
  }

  def getNext(): SpillableColumnarBatch = {
    SpillableColumnarBatch(input.next(), SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

}

case class GpuWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = if (gpuPartitionSpec.isEmpty) {
    RequireSingleBatch
  } else {
    BatchedByKey(gpuPartitionOrdering)(cpuPartitionOrdering)
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME_LEGACY)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output,
      allMetrics)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec,
      child.output, allMetrics)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output,
      allMetrics)

    child.executeColumnar().mapPartitions { iter =>
      new GpuWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
        output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}
