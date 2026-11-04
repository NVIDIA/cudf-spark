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

import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}

/**
 * Streams the all-null batches a reader produces when none of the requested columns exist in
 * the file(s), so nothing is decoded. Each batch is capped like a decoded batch, so a split
 * with a huge row count is never materialized in one piece. Batches are built lazily, so
 * nothing is held if the consumer stops early.
 */
object GpuNullBatchIterator {
  // Row count used to measure the per-row size of a schema; large enough that validity buffer
  // padding is negligible. GpuBatchUtils.estimateRowCount is not used because it requires a
  // non-zero row count and returns 0 for a schema with no columns.
  private val PROBE_ROWS = 1 << 20

  /**
   * The maximum rows of a batch with the given schema: the lesser of `maxReadBatchSizeRows`
   * and the rows that fit in `targetBatchSizeBytes`. Always at least 1.
   */
  def maxRows(
      outputSchema: StructType,
      maxReadBatchSizeRows: Int,
      targetBatchSizeBytes: Long): Int = {
    val probeBytes = GpuBatchUtils.estimateGpuMemory(outputSchema, PROBE_ROWS)
    val rowsForBytes = if (probeBytes == 0) {
      Int.MaxValue.toLong
    } else {
      (targetBatchSizeBytes.toDouble / probeBytes * PROBE_ROWS).toLong
    }
    math.max(1L, math.min(maxReadBatchSizeRows.toLong, rowsForBytes)).toInt
  }

  /** Batches of `readSchema` null columns totalling `totalRows` rows, each of <= `maxRows`. */
  def apply(readSchema: StructType, totalRows: Long, maxRows: Int): Iterator[ColumnarBatch] =
    new Iterator[ColumnarBatch] {
      private var remaining = totalRows

      override def hasNext: Boolean = remaining > 0

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        val rows = math.min(remaining, maxRows.toLong).toInt
        remaining -= rows
        // Someone is going to process this data, even if it is just a row count, and partition
        // columns may be added to it on the GPU.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        if (readSchema.isEmpty) {
          new ColumnarBatch(Array.empty, rows)
        } else {
          RmmRapidsRetryIterator.withRetryNoSplit {
            val nullColumns = readSchema.fields.safeMap(f =>
              GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
            new ColumnarBatch(nullColumns, rows)
          }
        }
      }
    }
}
