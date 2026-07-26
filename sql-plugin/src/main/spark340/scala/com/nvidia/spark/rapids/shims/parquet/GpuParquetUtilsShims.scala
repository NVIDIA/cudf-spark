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
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims.parquet

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}

object GpuParquetUtilsShims {

  /**
   * Sets the given row index offset on the BlockMetaData.
   */
  def setRowIndexOffset(block: BlockMetaData, offset: Long): Unit = {
    block.setRowIndexOffset(offset)
  }

  /**
   * Gets the row index offset from the BlockMetaData.
   */
  def getRowIndexOffset(block: BlockMetaData): Long = block.getRowIndexOffset

  /**
   * Build a new BlockMetaData from an existing one, but with a new set of column chunks metadata.
   *
   * @param existingMetadata the existing BlockMetaData to copy row count and row index offset from
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  def newBlockMeta(
      existingMetadata: BlockMetaData,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(existingMetadata.getRowCount)
    block.setRowIndexOffset(existingMetadata.getRowIndexOffset)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }
}
