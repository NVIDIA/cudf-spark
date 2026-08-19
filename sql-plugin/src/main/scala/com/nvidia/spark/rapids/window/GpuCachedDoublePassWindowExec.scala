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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.GpuAggregateExpression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch


class FixerPair(op: GpuUnboundToUnboundWindowWithFixer) extends AutoCloseable {
  var fixing: BatchedUnboundedToUnboundedWindowFixer = op.newUnboundedToUnboundedFixer
  var collecting: BatchedUnboundedToUnboundedWindowFixer = op.newUnboundedToUnboundedFixer

  def updateState(scalar: Scalar): Unit = {
    collecting.updateState(scalar)
  }

  def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      column: cudf.ColumnVector): cudf.ColumnVector =
    fixing.fixUp(samePartitionMask, column)

  def swap(): Unit = {
    val tmp = fixing
    tmp.reset()
    fixing = collecting
    collecting = tmp
  }

  override def close(): Unit = {
    fixing.close()
    collecting.close()
  }
}

/**
 * An iterator that can do aggregations on window queries that need a small amount of
 * information from all of the batches to update the result in a second pass. It does this by
 * having the aggregations be instances of GpuUnboundToUnboundWindowWithFixer
 * which can fix up the window output for unbounded to unbounded windows.
 * Because of this there is no requirement about how the input data is batched, but it  must
 * be sorted by both partitioning and ordering.
 */
class GpuCachedDoublePassWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {
  import GpuBatchedWindowIteratorUtils._

  override def isRunningBatched: Boolean = true

  // firstPassIter returns tuples where the first element is the result of calling
  // computeBasicWindow on a batch, and the second element is a projection of boundPartitionSpec
  // against the batch
  private var firstPassIter: Option[Iterator[(Array[cudf.ColumnVector], ColumnarBatch)]] = None
  private var postProcessedIter: Option[Iterator[ColumnarBatch]] = None
  private var readyForPostProcessing = mutable.Queue[SpillableColumnarBatch]()
  private var firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()
  // This should only ever be cached in between calls to `hasNext` and `next`.
  // This is just to let us filter out empty batches.
  private var waitingForFirstPass: Option[ColumnarBatch] = None
  private var lastPartsCaching: Array[Scalar] = Array.empty
  private var lastPartsProcessing: Array[Scalar] = Array.empty
  private var isClosed: Boolean = false

  onTaskCompletion(close())

  private def saveLastPartsCaching(newLastParts: Array[Scalar]): Unit = {
    lastPartsCaching.foreach(_.close())
    lastPartsCaching = newLastParts
  }

  def close(): Unit = {
    if (!isClosed) {
      isClosed = true

      fixerIndexMap.values.foreach(_.close())

      saveLastPartsCaching(Array.empty)

      lastPartsProcessing.foreach(_.close())
      lastPartsProcessing = Array.empty

      firstPassProcessed.foreach(_.close())
      firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()

      readyForPostProcessing.foreach(_.close())
      readyForPostProcessing = mutable.Queue[SpillableColumnarBatch]()

      waitingForFirstPass.foreach(_.close())
      waitingForFirstPass = None

      firstPassIter = None
      postProcessedIter = None
    }
  }

  private lazy val fixerIndexMap: Map[Int, FixerPair] =
    boundWindowOps.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuUnboundToUnboundWindowWithFixer =>
            Some((index, new FixerPair(f)))
          case GpuAggregateExpression(f: GpuUnboundToUnboundWindowWithFixer, _, _, _, _) =>
            Some((index, new FixerPair(f)))
          case _ => None
        }
      case _ => None
    }.toMap

  // Do any post processing fixup for the batch before it is sent out the door
  def postProcess(cb: ColumnarBatch): ColumnarBatch = {
    val computedWindows = GpuColumnVector.extractBases(cb)
    withResource(GpuProjectExec.project(cb, boundPartitionSpec)) { parts =>
      val partColumns = GpuColumnVector.extractBases(parts)
      withResourceIfAllowed(arePartsEqual(lastPartsProcessing, partColumns)) { samePartitionMask =>
        withResource(ArrayBuffer[cudf.ColumnVector]()) { newColumns =>
          boundWindowOps.indices.foreach { idx =>
            val column = computedWindows(idx)
            fixerIndexMap.get(idx) match {
              case Some(fixer) =>
                closeOnExcept(fixer.fixUp(samePartitionMask, column)) { finalOutput =>
                  newColumns += finalOutput
                }
              case None =>
                newColumns += column.incRefCount()
            }
          }
          makeBatch(newColumns.toSeq)
        }
      }
    }
  }

  def makeBatch(columns: Seq[cudf.ColumnVector]): ColumnarBatch = {
    withResource(new cudf.Table(columns: _*)) { table =>
      GpuColumnVector.from(table, outputTypes)
    }
  }

  def swapFirstPassIsReadyForPost(): Unit = {
    // Swap the caching so it is ready to be used for updating
    fixerIndexMap.values.foreach(_.swap())

    // Swap the parts so we know what mask to use for updating
    lastPartsProcessing.foreach(_.close())
    lastPartsProcessing = lastPartsCaching
    lastPartsCaching = Array.empty

    // Swap the queues so we are ready to dequeue the data
    // Before we swap this must be empty, or we are dropping data...
    assert(readyForPostProcessing.isEmpty)
    readyForPostProcessing = firstPassProcessed
    firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()
  }

  // The last batch was already processed so everything in processed needs to be moved to
  // readyForPostProcessing
  def lastBatch(): Unit = swapFirstPassIsReadyForPost()

  private def cacheInFixers(computedWindows: Array[cudf.ColumnVector],
      fixers: Map[Int, FixerPair],
      rowIndex: Int): Unit =
    fixers.foreach {
      case (columnIndex, fixer) =>
        val column = computedWindows(columnIndex)
        withResource(column.getScalarElement(rowIndex)) { scalar =>
          fixer.updateState(scalar)
        }
    }

  def saveBatchForPostProcessing(batch: ColumnarBatch): Unit = {
    firstPassProcessed += SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
  }

  def saveBatchForPostProcessing(basic: Array[cudf.ColumnVector]): Unit = {
    closeOnExcept(makeBatch(basic)) { batch =>
      saveBatchForPostProcessing(batch)
    }
  }

  // Compute the window operation and cache/update caches as needed.
  // This method takes ownership of cb
  def firstPassComputeAndCache(cb: ColumnarBatch): Unit = {
    val fixers = fixerIndexMap
    val numRows = cb.numRows()

    val sp = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)

    val (basic, parts) = firstPassIter match {
      case Some(it) if it.hasNext => it.next()
      case _ =>
        firstPassIter = Some(withRetry(sp, splitSpillableInHalfByRows) { sp =>
          withResource(sp.getColumnarBatch()) { batch =>
            closeOnExcept(computeBasicWindow(batch)) { basic =>
              (basic, GpuProjectExec.project(batch, boundPartitionSpec))
            }
          }
        })
        firstPassIter.get.next()
    }

    withResource(basic) { _ =>
      withResource(parts) { _ =>
        val partColumns = GpuColumnVector.extractBases(parts)

        val firstLastEqual = areRowPartsEqual(lastPartsCaching, partColumns, Seq(0, numRows - 1))
        val firstEqual = firstLastEqual(0)
        val lastEqual = firstLastEqual(1)
        if (firstEqual) {
          // This batch is a continuation of the previous batch so we need to update the
          // fixer with info from it.
          // This assumes that the window is unbounded to unbounded. We will need to update
          // APIs in the future and rename things if we want to support more than this.
          cacheInFixers(basic, fixers, 0)
        }

        // If the last part entry in this batch does not match the last entry in the previous batch
        // then we need to start post-processing the batches.
        if (!lastEqual) {
          // We swap the fixers and queues so we are ready to start on the next partition by group
          swapFirstPassIsReadyForPost()
          // Collect/Cache the needed info from the end of this batch
          cacheInFixers(basic, fixers, numRows - 1)
          saveLastPartsCaching(getScalarRow(numRows - 1, partColumns))

          if (firstEqual) {
            // Process the batch now, but it will only be for the first part of the batch
            // the last part may need to be fixed again, so put it into the queue for
            // when the next round finishes.
            val processedBatch = withResource(makeBatch(basic)) { basicBatch =>
              postProcess(basicBatch)
            }
            closeOnExcept(processedBatch) { processedBatch =>
              saveBatchForPostProcessing(processedBatch)
            }
          } else {
            // We split on a partition boundary, so just need to save it
            saveBatchForPostProcessing(basic)
          }
        } else {
          // No need to save the parts, it was equal...
          saveBatchForPostProcessing(basic)
        }
      }
    }
  }

  private def cacheBatchIfNeeded(): Unit = {
    while (waitingForFirstPass.isEmpty && input.hasNext) {
      closeOnExcept(input.next()) { cb =>
        if (cb.numRows() > 0) {
          waitingForFirstPass = Some(cb)
        } else {
          cb.close()
        }
      }
    }
  }

  override def hasNext: Boolean = {
    if (readyForPostProcessing.nonEmpty || firstPassProcessed.nonEmpty) {
      true
    } else {
      cacheBatchIfNeeded()
      waitingForFirstPass.isDefined
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    postProcessedIter match {
      case Some(it) if it.hasNext =>
        it.next()
      case _ =>
        postProcessedIter = Some(withRetry(getReadyForPostProcessing(),
          splitSpillableInHalfByRows) { sb =>
          withResource(sb.getColumnarBatch()) { cb =>
            val ret = NvtxIdWithMetrics(NvtxRegistry.DOUBLE_BATCHED_WINDOW_POST, opTime) {
              postProcess(cb)
            }
            numOutputBatches += 1
            numOutputRows += ret.numRows()
            ret
          }
        })
        postProcessedIter.get.next()
    }
  }

  /**
   * Get the next batch that is ready for post-processing.
   */
  private def getReadyForPostProcessing(): SpillableColumnarBatch = {
    while (readyForPostProcessing.isEmpty) {
      // Keep reading and processing data until we have something to output
      cacheBatchIfNeeded()
      if (waitingForFirstPass.isEmpty) {
        lastBatch()
      } else {
        val cb = waitingForFirstPass.get
        waitingForFirstPass = None
        NvtxIdWithMetrics(NvtxRegistry.DOUBLE_BATCHED_WINDOW_PRE, opTime) {
          // firstPassComputeAndCache takes ownership of the batch passed to it
          firstPassComputeAndCache(cb)
        }
      }
    }
    readyForPostProcessing.dequeue()
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries. This is similar to GpuRunningWindowExec, but for operations
 * that need a small amount of information from all of the batches associated with a partition
 * instead of just the previous batch. It does this by processing a batch, collecting and
 * updating a small cache of information about the last partition in the batch, and then putting
 * that batch into a form that would let it be spilled if needed. A batch is released when the
 * last partition key in the batch is fully processed. Before it is released it will be updated
 * to include any information needed from the cached data.
 *
 * Currently this only works for unbounded to unbounded windows, but could be extended to more.
 */
case class GpuCachedDoublePassWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

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
      new GpuCachedDoublePassWindowIterator(iter, boundWindowOps, boundPartitionSpec,
        boundOrderSpec, output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}