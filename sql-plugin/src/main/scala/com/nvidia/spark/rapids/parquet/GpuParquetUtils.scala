/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.shims.parquet.GpuParquetUtilsShims
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging

object GpuParquetUtils extends Logging {
  /**
   * Trim block metadata to contain only the column chunks that occur in the specified schema.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param readSchema the clipped schema with physical column names
   * @param blocks the block metadata from the original Parquet file
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def clipBlocksToSchema(
      readSchema: MessageType,
      blocks: java.util.List[BlockMetaData]): Seq[BlockMetaData] = {
    // Schema clipping has already resolved names and IDs, preserving physical names.
    // Match exact components: case-folding can retain excluded fields, and joining with
    // dots would conflate a flat "a.b" with the struct field a.b.
    val pathSet = readSchema.getPaths.asScala.map(path => ColumnPath.get(path: _*)).toSet
    blocks.asScala.toSeq.map { oldBlock =>
      //noinspection ScalaDeprecation
      val newColumns = oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath))
      newBlockMeta(oldBlock, newColumns.toSeq)
    }
  }

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
    GpuParquetUtilsShims.newBlockMeta(existingMetadata, columns)
  }
}
