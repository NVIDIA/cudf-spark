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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.parquet

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}

object GpuParquetUtilsShims {

  /**
   * Sets the given row index offset on the BlockMetaData. No-op for Spark 3.3.x.
   */
  def setRowIndexOffset(block: BlockMetaData, offset: Long): Unit = {}

  /**
   * Gets the row index offset from the BlockMetaData. Not available in Spark 3.3.x.
   */
  def getRowIndexOffset(block: BlockMetaData): Long = {
    throw new UnsupportedOperationException("Row index offsets are not available in Spark 3.3.x")
  }

  /**
   * Build a new BlockMetaData from an existing one, but with a new set of column chunks metadata.
   *
   * @param existingMetadata the existing BlockMetaData to copy row index offset from
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  def newBlockMeta(
      existingMetadata: BlockMetaData,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(existingMetadata.getRowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }
}
