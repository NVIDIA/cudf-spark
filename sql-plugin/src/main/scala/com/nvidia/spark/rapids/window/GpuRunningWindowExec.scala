/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRestoreOnRetry, withRetry}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, RangeFrame, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.GpuAggregateExpression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * An iterator that can do row based aggregations on running window queries (Unbounded preceding to
 * current row) if and only if the aggregations are instances of GpuBatchedRunningWindowFunction
 * which can fix up the window output when an aggregation is only partly done in one batch of data.
 * Because of this there is no requirement about how the input data is batched, but it  must
 * be sorted by both partitioning and ordering.
 */
class GpuRunningWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends GpuColumnarBatchIterator(true) with BasicWindowCalc {
  import GpuBatchedWindowIteratorUtils._

  override def isRunningBatched: Boolean = true

  // This should only ever be cached in between calls to `hasNext` and `next`. This is just
  // to let us filter out empty batches.
  private val boundOrderColumns = boundOrderSpec.map(_.child)
  private var cachedBatch: Option[SpillableColumnarBatch] = None
  private var lastParts: Array[Scalar] = Array.empty
  private var lastOrder: Array[Scalar] = Array.empty
  private var maybeSplitIter: Iterator[ColumnarBatch] = Iterator.empty

  private def saveLastParts(newLastParts: Array[Scalar]): Unit = {
    lastParts.foreach(_.close())
    lastParts = newLastParts
  }

  private def saveLastOrder(newLastOrder: Array[Scalar]): Unit = {
    lastOrder.foreach(_.close())
    lastOrder = newLastOrder
  }

  override def doClose(): Unit = {
    fixerIndexMap.values.foreach(_.close())
    saveLastParts(Array.empty)
    saveLastOrder(Array.empty)
    cachedBatch.foreach(_.close())
    cachedBatch = None
  }

  private lazy val fixerIndexMap: Map[Int, BatchedRunningWindowFixer] =
    boundWindowOps.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuBatchedRunningWindowWithFixer if f.canFixUp =>
            Some((index, f.newFixer()))
          case GpuAggregateExpression(f: GpuBatchedRunningWindowWithFixer, _, _, _, _)
            if f.canFixUp => Some((index, f.newFixer()))
          case _ => None
        }
      case _ => None
    }.toMap

  private lazy val fixerNeedsOrderMask = fixerIndexMap.values.exists(_.needsOrderMask)

  private def fixUpAll(computedWindows: Array[cudf.ColumnVector],
      fixers: Map[Int, BatchedRunningWindowFixer],
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]]): Array[cudf.ColumnVector] = {
    closeOnExcept(ArrayBuffer[cudf.ColumnVector]()) { newColumns =>
      boundWindowOps.indices.foreach { idx =>
        val column = computedWindows(idx)
        fixers.get(idx) match {
          case Some(fixer) =>
            closeOnExcept(fixer.fixUp(samePartitionMask, sameOrderMask, column)) { finalOutput =>
              newColumns += finalOutput
            }
          case None =>
            newColumns += column.incRefCount()
        }
      }
      newColumns.toArray
    }
  }

  def computeRunningAndClose(sb: SpillableColumnarBatch): Iterator[ColumnarBatch] = {
    val fixers = fixerIndexMap
    withRetry(sb, splitSpillableInHalfByRows) { maybeSplitSb =>
      val numRows = maybeSplitSb.numRows()
      withResource(maybeSplitSb.getColumnarBatch()) { cb =>
        withResource(computeBasicWindow(cb)) { basic =>
          var newOrder: Option[Array[Scalar]] = None
          var newParts: Option[Array[Scalar]] = None
          val fixedUp = try {
            // we backup the fixers state and restore it in the event of a retry
            withRestoreOnRetry(fixers.values.toSeq) {
              withResource(GpuProjectExec.project(cb, boundPartitionSpec)) { parts =>
                val partColumns = GpuColumnVector.extractBases(parts)
                withResourceIfAllowed(arePartsEqual(lastParts, partColumns)) { partsEqual =>
                  val fixedUp = if (fixerNeedsOrderMask) {
                    withResource(GpuProjectExec.project(cb, boundOrderColumns)) { order =>
                      val orderColumns = GpuColumnVector.extractBases(order)
                      // We need to fix up the rows that are part of the same batch as the end of
                      // the last batch
                      withResourceIfAllowed(areOrdersEqual(lastOrder, orderColumns, partsEqual)) {
                        orderEqual =>
                          closeOnExcept(fixUpAll(basic, fixers, partsEqual, Some(orderEqual))) {
                            fixedUp =>
                              newOrder = Some(getScalarRow(numRows - 1, orderColumns))
                              fixedUp
                          }
                      }
                    }
                  } else {
                    // No ordering needed
                    fixUpAll(basic, fixers, partsEqual, None)
                  }
                  newParts = Some(getScalarRow(numRows - 1, partColumns))
                  fixedUp
                }
              }
            }
          } catch {
            case t: Throwable =>
              // avoid leaking unused interim results
              newOrder.foreach(_.foreach(_.close()))
              newParts.foreach(_.foreach(_.close()))
              throw t
          }
          withResource(fixedUp) { _ =>
            (convertToBatch(outputTypes, fixedUp), newParts, newOrder)
          }
        }
      }
    }.map { case (batch, newParts, newOrder) =>
      // this section is outside of the retry logic because the calls to saveLastParts
      // and saveLastOrders can potentially close GPU resources
      newParts.foreach(saveLastParts)
      newOrder.foreach(saveLastOrder)
      batch
    }
  }

  private def cacheBatchIfNeeded(): Unit = {
    while (cachedBatch.isEmpty && input.hasNext) {
      closeOnExcept(input.next()) { cb =>
        if (cb.numRows() > 0) {
          cachedBatch = Some(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        } else {
          cb.close()
        }
      }
    }
  }

  def readNextInputBatch(): SpillableColumnarBatch = {
    cacheBatchIfNeeded()
    val ret = cachedBatch.getOrElse {
      throw new NoSuchElementException()
    }
    cachedBatch = None
    ret
  }

  override def hasNext: Boolean = maybeSplitIter.hasNext || {
    cacheBatchIfNeeded()
    cachedBatch.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!maybeSplitIter.hasNext) {
      maybeSplitIter = computeRunningAndClose(readNextInputBatch())
      // maybeSplitIter is not empty here
    }
    NvtxIdWithMetrics(NvtxRegistry.RUNNING_WINDOW, opTime) {
      val ret = maybeSplitIter.next()
      numOutputBatches += 1
      numOutputRows += ret.numRows()
      ret
    }
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries, but only for window operations that are unbounded preceding
 * to current row (Running Window). This works because a small amount of data can be saved
 * from a previous batch and used to update the current batch.
 */
case class GpuRunningWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = {
    val isRangeFrame = windowOps.exists {
      case GpuAlias(
      GpuWindowExpression(
      _, GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(RangeFrame, _, _))),
      _) => true
      case _ => false
    }
    if (!isRangeFrame) {
      return null // NO batching restrictions on ROW frames.
    }
    if (gpuPartitionSpec.isEmpty) {
      // If unpartitioned, batch on the order-by column.
      BatchedByKey(gpuOrderSpec)(cpuOrderSpec)
    } else {
      // If partitioned, batch on partition-columns + order-by columns.
      BatchedByKey(gpuPartitionOrdering ++ gpuOrderSpec)(cpuPartitionOrdering ++ cpuOrderSpec)
    }
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
      new GpuRunningWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
        output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}