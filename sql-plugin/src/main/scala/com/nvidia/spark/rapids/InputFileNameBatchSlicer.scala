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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object InputFileNameBatchSlicer {
  // Safety margin below the 2 GiB cudf string limit. The limit applies to the character data;
  // offsets and validity live in separate buffers, so this is only a cushion.
  //
  // Limitation: the cap only guarantees that the bare input_file_name() column fits. An expression
  // that builds a longer string from the name, such as concat(input_file_name(), 'x') or
  // upper(...), can still overflow. At ~16M rows per slice, concatenating a 1 byte literal adds
  // ~16 MB, far more than this headroom. Delta MERGE uses the bare expression, which is the
  // case this stopgap targets.
  private[rapids] val HEADROOM_BYTES = 1L << 20

  /**
   * The most rows a batch may have so that `input_file_name()` on a path of `pathBytes` UTF-8
   * bytes still fits in one cudf string column (whose data is capped at Int.MaxValue bytes).
   * Always at least 1 so a path that is itself huge still makes progress.
   */
  def maxRowsPerBatch(pathBytes: Int, limitBytes: Long = Int.MaxValue): Int = {
    val rows = (limitBytes - HEADROOM_BYTES) / math.max(1L, pathBytes.toLong)
    math.max(1L, math.min(rows, Int.MaxValue.toLong)).toInt
  }

  /**
   * Slice the row range `[start, end)` out of `batch`. The slices are zero-copy views that share
   * the source buffers, so the source stays resident while any slice is alive. A zero column
   * batch only has a row count, so it naturally yields a zero column batch of `end - start` rows.
   */
  def slice(batch: ColumnarBatch, start: Int, end: Int): ColumnarBatch = {
    closeOnExcept(new Array[ColumnVector](batch.numCols())) { cols =>
      val bases = GpuColumnVector.extractBases(batch)
      cols.indices.foreach { i =>
        cols(i) = GpuColumnVector.from(bases(i).subVector(start, end), batch.column(i).dataType())
      }
      new ColumnarBatch(cols, end - start)
    }
  }
}

/**
 * Splits scan batches that are too big for `input_file_name()` to materialize as a single cudf
 * string column. The path is read (via `pathBytes`) right after the source batch is pulled, and
 * while slices of that batch are pending the source iterator is not touched, so the reader cannot
 * advance to another file and change the path under us.
 *
 * @param batches   the scan output, expected to hold one file per batch
 * @param pathBytes UTF-8 byte length of the file the most recently returned batch came from
 * @param limitBytes max string bytes one batch may need for the file name column
 */
class InputFileNameSlicingIterator(
    batches: Iterator[ColumnarBatch],
    pathBytes: () => Int,
    limitBytes: Long = Int.MaxValue) extends Iterator[ColumnarBatch] {
  private var pending: SpillableColumnarBatch = _
  private var offset = 0
  private var maxRows = 0

  // There is no task context when run outside of a Spark task (e.g. unit tests).
  Option(TaskContext.get()).foreach(tc => onTaskCompletion(tc)(close()))

  def close(): Unit = {
    if (pending != null) {
      pending.close()
      pending = null
    }
  }

  override def hasNext: Boolean = pending != null || batches.hasNext

  override def next(): ColumnarBatch = {
    if (pending == null) {
      val batch = batches.next()
      val rows = InputFileNameBatchSlicer.maxRowsPerBatch(pathBytes(), limitBytes)
      if (batch.numRows() <= rows) {
        batch
      } else {
        maxRows = rows
        offset = 0
        pending = closeOnExcept(batch) { _ =>
          SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        nextSlice()
      }
    } else {
      nextSlice()
    }
  }

  private def nextSlice(): ColumnarBatch = {
    val start = offset
    val end = math.min(start.toLong + maxRows, pending.numRows().toLong).toInt
    val result = RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch] {
      withResource(pending.getColumnarBatch()) { b =>
        InputFileNameBatchSlicer.slice(b, start, end)
      }
    }
    offset = end
    if (offset >= pending.numRows()) {
      close()
    }
    result
  }
}
