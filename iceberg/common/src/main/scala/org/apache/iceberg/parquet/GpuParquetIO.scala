/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.parquet

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.ShimUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.mapping.NameMapping
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile => ShadedInputFile}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType => ShadedGroupType,
  MessageType => ShadedMessageType, OriginalType, Type => ShadedType}

object GpuParquetIO {
  /**
   * Retain a physical carrier for deletion-vector reads with no projected data columns.
   * Only its row count is used. Preserve repetition, field IDs and LIST/MAP shapes, but use
   * ordinary groups for logical leaves such as Variant so no value reconstruction is needed.
   *
   * @param fileSchema physical Parquet schema with field IDs already assigned
   * @return a nonempty physical projection whose temporary column is discarded after reading
   */
  def projectRowCountSchema(fileSchema: ShadedMessageType): ShadedMessageType = {
    def project(field: ShadedType, mapEntries: Boolean = false): ShadedType = {
      if (field.isPrimitive) {
        field
      } else {
        val group = field.asGroupType()
        val fields = group.getFields.asScala
        val children = group.getOriginalType match {
          case OriginalType.MAP => fields.map(project(_, mapEntries = true))
          case OriginalType.MAP_KEY_VALUE if fields.size == 1 =>
            fields.map(project(_, mapEntries = true))
          case OriginalType.MAP_KEY_VALUE => fields.map(project(_))
          case _ if mapEntries => fields.map(project(_))
          case _ => fields.take(1).map(project(_))
        }
        // Preserve LIST/MAP annotations, but use an ordinary group for VARIANT. Its first
        // physical child (normally metadata) needs no reconstruction, even for shredded input.
        val projected = if (group.getOriginalType == null) {
          new ShadedGroupType(group.getRepetition, group.getName, children.asJava)
        } else {
          group.withNewFields(children.asJava)
        }
        Option(group.getId).map(id => projected.withId(id.intValue())).getOrElse(projected)
      }
    }

    new ShadedMessageType(fileSchema.getName, project(fileSchema.getType(0)))
  }

  /**
   * Assign field IDs where needed and prune a Parquet schema to the required Iceberg schema.
   * Execution uses this projection for files with IDs, a name mapping, or fallback IDs.
   * Prepare the schema after ID assignment and before pruning it to the requested columns.
   *
   * @param fileSchema physical Parquet schema
   * @param requiredSchema Iceberg schema to project
   * @param nameMapping optional mapping used when the file schema has no field IDs
   * @param prepareSchema validate and normalize requested fields before Iceberg pruning
   * @return the prepared schema with field IDs and the pruned schema, in that order
   */
  def projectAndPruneSchema(
      fileSchema: ShadedMessageType,
      requiredSchema: Schema,
      nameMapping: Option[NameMapping],
      prepareSchema: ShadedMessageType => ShadedMessageType = schema => schema):
      (ShadedMessageType, ShadedMessageType) = {
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      val prepared = prepareSchema(fileSchema)
      (prepared, ParquetSchemaUtil.pruneColumns(prepared, requiredSchema))
    } else if (nameMapping.isDefined) {
      val typeWithIds = prepareSchema(
        ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping.get))
      (typeWithIds, ParquetSchemaUtil.pruneColumns(typeWithIds, requiredSchema))
    } else {
      val typeWithIds = prepareSchema(ParquetSchemaUtil.addFallbackIds(fileSchema))
      (typeWithIds, ParquetSchemaUtil.pruneColumnsFallback(typeWithIds, requiredSchema))
    }
  }

  def file(file: InputFile): ShadedInputFile = {
    GpuParquetIOAccess.file(file)
  }

  /**
   * Open a shaded `ParquetFileReader`. Footer caching is version-dependent and resolved via
   * `IcebergShimUtils.openParquetReader`: 1.10.x overrides it to cache via `FileCache`, while
   * 1.6.x / 1.9.x inherit the no-cache default (their shaded parquet has no way to inject a
   * pre-parsed footer).
   */
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    ShimUtils.openParquetReader(inputFile, filePath, options, metrics)
  }
}
