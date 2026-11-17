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

import java.util.{Arrays, HashMap, Locale}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.shims.parquet.GpuParquetUtilsShims
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging

object GpuParquetUtils extends Logging {
  // Below this, cache setup can cost more than normalizing the repeated paths.
  private val MinRowGroupsForPathCache = 4

  /**
   * Trim block metadata to contain only the column chunks that occur in the specified schema.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param readSchema the schema to preserve
   * @param blocks the block metadata from the original Parquet file
   * @param isCaseSensitive indicate if it is case sensitive
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def clipBlocksToSchema(
      readSchema: MessageType,
      blocks: java.util.List[BlockMetaData],
      isCaseSensitive: Boolean): Seq[BlockMetaData] = {
    val columnPaths = readSchema.getPaths.asScala.map(path => ColumnPath.get(path: _*))
    // Keep path components distinct: a flat column "a.b" is not the struct field a.b.
    val matchesPath: ColumnPath => Boolean = if (isCaseSensitive) {
      val pathSet = columnPaths.toSet
      pathSet.contains
    } else {
      def normalizePath(path: ColumnPath): java.util.List[String] = {
        // Value keys avoid interning a new ColumnPath for every normalized chunk path.
        Arrays.asList(path.toArray.map(_.toLowerCase(Locale.ROOT)): _*)
      }
      val pathSet = columnPaths.map(normalizePath).toSet
      val matches: ColumnPath => Boolean = path => pathSet.contains(normalizePath(path))
      if (blocks.size() >= MinRowGroupsForPathCache) {
        // Row groups normally reuse column paths. Cache only within this clipping call to
        // avoid normalizing every repeated chunk without retaining paths globally.
        val matchesByPath = new HashMap[ColumnPath, java.lang.Boolean]()
        path => {
          val cached = matchesByPath.get(path)
          if (cached != null) {
            cached.booleanValue()
          } else {
            val matched = matches(path)
            matchesByPath.put(path, java.lang.Boolean.valueOf(matched))
            matched
          }
        }
      } else {
        matches
      }
    }
    blocks.asScala.toSeq.map { oldBlock =>
      //noinspection ScalaDeprecation
      val newColumns = oldBlock.getColumns.asScala.filter(c => matchesPath(c.getPath))
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
