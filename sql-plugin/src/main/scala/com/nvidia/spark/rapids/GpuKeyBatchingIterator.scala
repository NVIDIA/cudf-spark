/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Given a stream of data that is sorted by a set of keys, split the data so each batch output
 * contains all of the keys for a given key set. This tries to get the batch sizes close to the
 * target size. It assumes that the input batches will already be close to that size and does
 * not try to split them too much further.
 */
class GpuKeyBatchingIterator(
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    types: Array[DataType],
    targetSizeBytes: Long,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    concatTime: GpuMetric,
    opTime: GpuMetric)
    extends Iterator[ColumnarBatch] {
  private val pending = mutable.Queue[SpillableColumnarBatch]()
  private var pendingSize: Long = 0

  onTaskCompletion(close())

  def close(): Unit = {
    pending.foreach(_.close())
    pending.clear()
    pendingSize = 0
  }

  override def hasNext: Boolean = pending.nonEmpty || iter.hasNext

  private def getKeyCutoff(cb: ColumnarBatch): Int = {
    val candidates = withResource(sorter.appendProjectedColumns(cb)) { appended =>
      withResource(GpuColumnVector.from(appended)) { table =>
        // With data skew we don't want to include too much more in a batch than is necessary
        // so we are going to search up to 16 evenly spaced locations in the batch, including the
        // last entry. From that we will see what cutoff works and is closest to the size we want.
        // The 16 is an arbitrary number. It was picked because hopefully it is small enough that
        // the indexes will all fit in the CPU cache at once so we can process the data in an
        // efficient way.
        val maxRow = cb.numRows() - 1
        require(maxRow >= 0)
        val rowsPerBatch = Math.max(1, Math.ceil(cb.numRows()/16.0).toInt)
        val probePoints = (1 to 16).map(idx => Math.min(maxRow, idx * rowsPerBatch)).distinct
        val searchTab = withResource(ColumnVector.fromInts(probePoints: _*)) { gatherMap =>
          table.gather(gatherMap)
        }
        val cutoffVec = withResource(searchTab) { searchTab =>
          sorter.lowerBound(table, searchTab)
        }
        val cutoffCandidates = new Array[Int](cutoffVec.getRowCount.toInt)
        withResource(cutoffVec) { cutoffVec =>
          withResource(cutoffVec.copyToHost()) { vecHost =>
            cutoffCandidates.indices.foreach { idx =>
              cutoffCandidates(idx) = vecHost.getInt(idx)
            }
          }
        }
        cutoffCandidates.distinct
      }
    }
    // The goal is to find the candidate that is closest to the target size without going over,
    // excluding 0, which is a special case because we cannot slice there without more information.
    // If we have to go over, then we want to find the smallest cutoff that is over the target.
    if (candidates.length == 1) {
      // No point checking more there is only one cutoff
      candidates.head
    } else {
      // Just use an estimate for the row size.
      val averageRowSize = Math.max(1.0,
        GpuColumnVector.getTotalDeviceMemoryUsed(cb).toDouble / cb.numRows())
      val leftForTarget = targetSizeBytes - pendingSize
      val approximateRows = leftForTarget / averageRowSize
      val willFit = candidates.filter(f => f > 0 && f <= approximateRows)
      if (willFit.nonEmpty) {
        willFit.max
      } else {
        val overButValid = candidates.filter(f => f > 0)
        if (overButValid.nonEmpty) {
          overButValid.min
        } else {
          // This should have been covered above with only a single candidate left, but
          // just in case...
          0
        }
      }
    }
  }

  private def concatPending(last: Option[SpillableColumnarBatch] = None): ColumnarBatch = {
    val spillableBuffers = new ArrayBuffer[SpillableColumnarBatch]()
    spillableBuffers.appendAll(pending)
    pending.clear()
    pendingSize = 0
    spillableBuffers.appendAll(last)
    RmmRapidsRetryIterator.withRetryNoSplit(spillableBuffers.toSeq) { attempt =>
      NvtxIdWithMetrics(NvtxRegistry.CONCAT_PENDING, concatTime) {
        withResource(mutable.ArrayBuffer[Table]()) { toConcat =>
          attempt.foreach { spillable =>
            withResource(spillable.getColumnarBatch()) { cb =>
              toConcat.append(GpuColumnVector.from(cb))
            }
          }

          if (toConcat.length > 1) {
            withResource(Table.concatenate(toConcat.toSeq: _*)) { concated =>
              GpuColumnVector.from(concated, types)
            }
          } else if (toConcat.nonEmpty) {
            GpuColumnVector.from(toConcat.head, types)
          } else {
            // We got nothing but have to do something
            GpuColumnVector.emptyBatchFromTypes(types)
          }
        }
      }
    }
  }

  private[this] def processAndCloseFinalBatch(cb: ColumnarBatch): ColumnarBatch = {
    val scb = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    // concatPending will close scb
    concatPending(Some(scb))
  }

  /**
   * Split cb at a key boundary trying to get the output buffers to be close to
   * the target batch size. Part of cb might be cached to be returned later on.
   * @param cb the batch to split. This will be closed when the call completes.
   * @return a Columnar batch to return from next or None if there was no place to
   *         the batch could be split.
   */
  private[this] def splitAndCloseBatch(cb: ColumnarBatch): Option[ColumnarBatch] = {
    val cutoff = closeOnExcept(cb)(getKeyCutoff)
    if (cutoff <= 0) {
      val cbSize = GpuColumnVector.getTotalDeviceMemoryUsed(cb)
      // Everything is for a single key, so save it away and try the next batch...
      pending +=
          SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      pendingSize += cbSize
      None
    } else {
      val table = withResource(cb)(GpuColumnVector.from)
      val tables = withResource(table) { table =>
        table.contiguousSplit(cutoff)
      }
      val (firstSpill, secondSpill) = closeOnExcept(tables) { tables =>
        assert(tables.length == 2)
        val tmp = tables.zipWithIndex.safeMap { case (t, ix) =>
          tables(ix) = null
          SpillableColumnarBatch(t, types, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        (tmp(0), tmp(1))
      }
      val ret = closeOnExcept(secondSpill) { secondSpill =>
        concatPending(Some(firstSpill))
      }
      closeOnExcept(ret) { ret =>
        val savedSize = secondSpill.sizeInBytes
        pending += secondSpill
        pendingSize += savedSize
        Some(ret)
      }
    }
  }

  override def next(): ColumnarBatch = {
    var ret: Option[ColumnarBatch] = None
    while (ret.isEmpty && iter.hasNext) {
      val cb = iter.next()
      numInputBatches += 1
      val numRows = cb.numRows()
      if (numRows <= 0) {
        cb.close()
      } else {
        numInputRows += numRows
        withResource(new MetricRange(opTime)) { _ =>
          if (GpuColumnVector.isTaggedAsFinalBatch(cb)) {
            // No need to do a split on the final row and create extra work this is the last batch
            ret = Some(processAndCloseFinalBatch(cb))
          } else {
            ret = splitAndCloseBatch(cb)
          }
        }
      }
    }
    val finalRet = ret.getOrElse {
      withResource(new MetricRange(opTime)) { _ =>
        // At the end of the iterator, nothing more to process
        concatPending()
      }
    }
    numOutputRows += finalRet.numRows()
    numOutputBatches += 1
    finalRet
  }
}

object GpuKeyBatchingIterator {
  def makeFunc(unboundOrderSpec: Seq[SortOrder],
      schema: Array[Attribute],
      targetSizeBytes: Long,
      numInputRows: GpuMetric,
      numInputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      concatTime: GpuMetric,
      opTime: GpuMetric,
      metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] => GpuKeyBatchingIterator = {
    val sorter = new GpuSorter(unboundOrderSpec, schema, metrics)
    val types = schema.map(_.dataType)
    def makeIter(iter: Iterator[ColumnarBatch]): GpuKeyBatchingIterator = {
      new GpuKeyBatchingIterator(iter, sorter, types, targetSizeBytes,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches,
        concatTime, opTime)
    }
    makeIter
  }
}
