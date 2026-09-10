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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.shims.parquet.GpuParquetUtilsShims
import org.apache.parquet.hadoop.metadata.BlockMetaData

object RapidsDeletionVectorRowCountUtils {
  def getRowGroupMetadata(blocks: collection.Seq[BlockMetaData]): (Array[Long], Array[Int]) = {
    val rowGroupOffsets = blocks.map(GpuParquetUtilsShims.getRowIndexOffset)
    if (rowGroupOffsets.exists(_ < 0)) {
      throw new IllegalStateException("Found invalid row group offset")
    }
    val rowGroupNumRows = blocks.map(_.getRowCount)
    if (rowGroupNumRows.exists(numRows => !numRows.isValidInt)) {
      throw new IllegalStateException("Found invalid row group num rows")
    }
    (rowGroupOffsets.toArray, rowGroupNumRows.map(_.toInt).toArray)
  }

  /**
   * Computes the number of bitmap-marked rows within the given row ranges.
   *
   * The caller supplies bitmap iteration so OSS and Databricks shims can adapt
   * their distinct RoaringBitmapArray types without coupling this shared source
   * set to either implementation.
   */
  def countMarkedRows(
      bitmapCardinality: Long,
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int])(
      foreachMarkedRow: (Long => Unit) => Unit): Long = {
    if (bitmapCardinality == 0) {
      0L
    } else {
      var count = 0L
      val rowRanges = rowGroupOffsets.zip(rowGroupNumRows)
      // Computes the number of marked rows by iterating only over the set bits
      // in the bitmap (marked row indices) and checking which row group each
      // belongs to. This is O(marked_rows * num_row_groups) instead of
      // O(total_rows). The former is usually smaller than the latter.
      // cuDF added a dedicated API in https://github.com/rapidsai/cudf/pull/21963.
      // Track the Spark RAPIDS follow-up in
      // https://github.com/NVIDIA/spark-rapids/issues/14628.
      foreachMarkedRow { markedIndex: Long =>
        rowRanges.find { case (offset, numRows) =>
          markedIndex >= offset && markedIndex < offset + numRows
        }.foreach { _ =>
          // If the marked index falls within this row group, count it.
          count += 1L
        }
      }
      count
    }
  }

  def computeNumRowsAlive(
      totalNumRows: Long,
      bitmapCardinality: Long,
      chunkedBlocks: collection.Seq[BlockMetaData])(
      foreachDeletedRow: (Long => Unit) => Unit): Int = {
    val (rowGroupOffsets, rowGroupNumRows) = getRowGroupMetadata(chunkedBlocks)
    val numDeletedRows = countMarkedRows(bitmapCardinality, rowGroupOffsets, rowGroupNumRows)(
      foreachDeletedRow)

    require(numDeletedRows <= totalNumRows,
      s"Deleted row count in selected row groups ($numDeletedRows) exceeds selected " +
        s"row-group row count ($totalNumRows)")
    Math.toIntExact(totalNumRows - numDeletedRows)
  }
}
