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

package com.nvidia.spark.rapids.iceberg.parquet

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.MetadataColumns
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.shaded.org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{
  MessageType => ShadedMessageType, Types => ShadedTypes}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{
  PrimitiveTypeName => ShadedPrimitiveTypeName}
import org.apache.iceberg.shaded.org.apache.parquet.schema.Type.{Repetition => ShadedRepetition}

object GpuIcebergParquetReaderUtils {
  private val READ_PROPERTIES_TO_REMOVE = Set(
    "parquet.read.filter",
    "parquet.private.read.filter.predicate",
    "parquet.read.support.class")

  /**
   * Adds the leading file-global row index emitted by the cuDF deletion-vector reader to the
   * schema consumed by the Iceberg post-processor.
   */
  private[iceberg] def withNativeRowIndex(
      fileReadSchema: ShadedMessageType): ShadedMessageType = {
    val rowPosition = ShadedTypes
      .primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.REQUIRED)
      .id(MetadataColumns.ROW_POSITION.fieldId())
      .named(MetadataColumns.ROW_POSITION.name())
    new ShadedMessageType(
      fileReadSchema.getName,
      (rowPosition +: fileReadSchema.getFields.asScala).asJava)
  }

  def buildReaderOptions(file: InputFile, split: Option[(Long, Long)])
  : ParquetReadOptions = {
    var optionsBuilder: ParquetReadOptions.Builder = null
    file match {
      case hadoop: HadoopInputFile =>
        // remove read properties already set that may conflict with this read
        val conf = new Configuration(hadoop.getConf)
        for (property <- READ_PROPERTIES_TO_REMOVE) {
          conf.unset(property)
        }
        optionsBuilder = HadoopReadOptions.builder(conf)
      case _ =>
        optionsBuilder = ParquetReadOptions.builder()
    }
    split.foreach { case (start, length) =>
      optionsBuilder = optionsBuilder.withRange(start, start + length)
    }
    optionsBuilder.build
  }
}
