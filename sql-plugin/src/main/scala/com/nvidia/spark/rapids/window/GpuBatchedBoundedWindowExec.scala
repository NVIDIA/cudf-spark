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

package com.nvidia.spark.rapids.window

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBatchedBoundedWindowIterator(
  input: Iterator[ColumnarBatch],
  override val boundWindowOps: Seq[GpuExpression],
  override val boundPartitionSpec: Seq[GpuExpression],
  override val boundOrderSpec: Seq[SortOrder],
  val outputTypes: Array[DataType],
  minPreceding: Int,
  maxFollowing: Int,
  numOutputBatches: GpuMetric,
  numOutputRows: GpuMetric,
  opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc with Logging {

  override def isRunningBatched: Boolean = false  // Not "Running Window" optimized.
                                                  // This is strictly for batching.

  override def hasNext: Boolean = numUnprocessedInCache > 0 || input.hasNext

  // For processing with the next batch.
  private var cached: Option[SpillableColumnarBatch] = None

  private var numUnprocessedInCache: Int = 0  // numRows at the bottom not processed completely.
  private var numPrecedingRowsAdded: Int = 0  // numRows at the top, added for preceding context.

  // Register handler to clean up cache when task completes.
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      clearCached()
    }
  }

  // Caches input column schema on first read.
  private var inputTypes: Option[Array[DataType]] = None

  // Clears the cache after consumption.
  private[rapids] def clearCached(): Unit = {
    cached.foreach(_.close())
    cached = None
  }

  protected final def hasCache: Boolean = cached.isDefined // for unit test

  protected def getNextInputBatchWithRetry: SpillableColumnarBatch = {
    // Either cached has unprocessed rows, or input.hasNext().
    if (input.hasNext) {
      val freshSCB = closeOnExcept(input.next()) { freshCB =>
        if (inputTypes.isEmpty) {
          // initializes input data-types if necessary.
          inputTypes = Some(GpuColumnVector.extractTypes(freshCB))
        }
        SpillableColumnarBatch(freshCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
      if (hasCache) {
        // Cached input AND new input rows exist.  Return concat-ed rows.
        // The two input batches will be closed by `withRetryNoSplit`.
        val concatedTbl = withRetryNoSplit(Seq(cached.get, freshSCB)) { toConcat =>
          val cbs = toConcat.safeMap(_.getColumnarBatch())
          val tbls = withResource(cbs)(_ => cbs.safeMap(GpuColumnVector.from))
          withResource(tbls)(_ => Table.concatenate(tbls: _*))
        }
        withResource(concatedTbl) { _ =>
          cached = None
          closeOnExcept(GpuColumnVector.from(concatedTbl, inputTypes.get)) { concatedCB =>
            SpillableColumnarBatch(concatedCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          }
        }
      } else { // no cache, return the input batch directly
        freshSCB
      }
    } else { // No fresh input available. Return cached input.
      val cachedSCB = cached.get
      cached = None
      cachedSCB
    }
  }

  /**
   * Helper to trim specified number of rows off the top and bottom,
   * of all specified columns.
   */
  protected def trimWithRetry(scb: SpillableColumnarBatch, offTheTop: Int,
      offTheBottom: Int): ColumnarBatch = {
    val batchRowsNum = scb.numRows()
    if ((offTheTop + offTheBottom) > batchRowsNum) {
      throw new IllegalArgumentException(s"Cannot trim batch of size ${batchRowsNum} by " +
        s"$offTheTop rows at the top, and $offTheBottom rows at the bottom.")
    }

    withRetryNoSplit[ColumnarBatch] {
      withResource(scb.getColumnarBatch()) { cb =>
        val types = GpuColumnVector.extractTypes(cb)
        val baseCols = GpuColumnVector.extractBases(cb)
        withResource(baseCols.safeMap(_.subVector(offTheTop, batchRowsNum - offTheBottom))) {
          convertToBatch(types, _)
        }
      }
    }
  }

  /**
   * Compute the window aggregations on the input batch with retry support.
   * It does not close the input batch.
   */
  protected def computeWindowWithRetry(
      scb: SpillableColumnarBatch): SpillableColumnarBatch = {
    val outputCB = withRetryNoSplit[ColumnarBatch] {
      val outputCols = withResource(scb.getColumnarBatch())(computeBasicWindow)
      withResource(outputCols)(convertToBatch(outputTypes, _))
    }
    SpillableColumnarBatch(outputCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  private def resetInputCache(newCache: Option[ColumnarBatch],
                              newPrecedingAdded: Int): Unit= {
    clearCached()
    cached = newCache.map(
      SpillableColumnarBatch(_, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    )
    numPrecedingRowsAdded = newPrecedingAdded
  }

  override def next(): ColumnarBatch = {
    var outputBatch: ColumnarBatch = null
    while (outputBatch == null && hasNext) {
      withResource(getNextInputBatchWithRetry) { inputSCB =>
        val inputRowCount = inputSCB.numRows()
        val noMoreInput = !input.hasNext
        numUnprocessedInCache = if (noMoreInput) {
          // If there are no more input rows expected,
          // this is the last output batch.
          // Consider all rows in the batch as processed.
          0
        } else {
          // More input rows expected. The last `maxFollowing` rows can't be finalized.
          // Cannot exceed `inputRowCount`.
          if (maxFollowing < 0) { // E.g. LAG(3) => [ preceding=-3, following=-3 ]
            // -ve following => No need to wait for more following rows.
            // All "following" context is already available in the current batch.
            0
          } else {
            maxFollowing min inputRowCount
          }
        }

        if (numPrecedingRowsAdded + numUnprocessedInCache >= inputRowCount) {
          // No point calling windowing kernel: the results will simply be ignored.
          logWarning("Not enough rows! Cannot output a batch.")
        } else {
          NvtxIdWithMetrics(NvtxRegistry.WINDOW_EXEC, opTime) {
            outputBatch = withResource(computeWindowWithRetry(inputSCB)) {
              trimWithRetry(_, numPrecedingRowsAdded, numUnprocessedInCache)
            }
          }
        }

        // Compute new cache using current input.
        numPrecedingRowsAdded = if (minPreceding > 0) { // E.g. LEAD(3) => [prec=3, foll=3]
          // preceding > 0 => No "preceding" rows need be carried forward.
          // Only the rows that need to be recomputed.
          0
        } else {
          Math.abs(minPreceding) min (inputRowCount - numUnprocessedInCache)
        }

        if (!noMoreInput) { // cache is needed only when have more input data
          val newCached = trimWithRetry(inputSCB,
            inputRowCount - (numPrecedingRowsAdded + numUnprocessedInCache),
            0)
          resetInputCache(Some(newCached), numPrecedingRowsAdded)
        }
      }
    }
    numOutputBatches += 1
    numOutputRows += outputBatch.numRows()
    outputBatch
  }
}

/// Window Exec used exclusively for batching bounded window functions.
class GpuBatchedBoundedWindowExec(
    override val windowOps: Seq[NamedExpression],
    override val gpuPartitionSpec: Seq[Expression],
    override val gpuOrderSpec: Seq[SortOrder],
    override val child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder],
    minPreceding: Integer,
    maxFollowing: Integer
) extends GpuWindowExec(windowOps,
                        gpuPartitionSpec,
                        gpuOrderSpec,
                        child)(cpuPartitionSpec, cpuOrderSpec) {

  override def otherCopyArgs: Seq[AnyRef] =
    cpuPartitionSpec :: cpuOrderSpec :: minPreceding :: maxFollowing :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq.fill(children.size)(null)

  override def outputBatching: CoalesceGoal = null

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
      new GpuBatchedBoundedWindowIterator(iter, boundWindowOps, boundPartitionSpec,
        boundOrderSpec, output.map(_.dataType).toArray, minPreceding, maxFollowing,
        numOutputBatches, numOutputRows, opTime)
    }
  }
}