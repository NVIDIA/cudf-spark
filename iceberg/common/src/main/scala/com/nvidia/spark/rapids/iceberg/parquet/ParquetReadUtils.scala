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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.Collections
import java.util.function.IntPredicate

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.iceberg.ShimUtils
import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.{Binder, Expression}
import org.apache.iceberg.parquet.{ParquetBloomRowGroupFilter, ParquetDictionaryRowGroupFilter, ParquetMetricsRowGroupFilter, ParquetSchemaUtil}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType

/** Compatibility implementations used by Iceberg shims before 1.12. */
object ParquetReadUtils {
  def pruneColumns(
      fileSchema: MessageType,
      requiredSchema: Schema,
      useFallbackIds: Boolean): MessageType = {
    val projected = if (useFallbackIds) {
      ParquetSchemaUtil.pruneColumnsFallback(fileSchema, requiredSchema)
    } else {
      ParquetSchemaUtil.pruneColumns(fileSchema, requiredSchema)
    }
    ParquetSchemaProjection.preserveStructValidity(fileSchema, projected, requiredSchema)
  }

  def rowGroupFilter(
      reader: ParquetFileReader,
      requiredSchema: Schema,
      fileSchema: MessageType,
      filter: Expression,
      caseSensitive: Boolean): IntPredicate = {
    // Older filters treat absent columns as null, ignoring initial defaults. Spark must
    // evaluate these residuals after defaults are materialized, so retain every row group.
    if (hasMissingDefaultFilter(fileSchema, requiredSchema, filter, caseSensitive)) {
      (_: Int) => true
    } else {
      val stats = new ParquetMetricsRowGroupFilter(requiredSchema, filter, caseSensitive)
      val dictionary = new ParquetDictionaryRowGroupFilter(requiredSchema, filter, caseSensitive)
      val bloom = new ParquetBloomRowGroupFilter(requiredSchema, filter, caseSensitive)
      val blocks = reader.getRowGroups
      val shouldRead: IntPredicate = (index: Int) => {
        val block = blocks.get(index)
        stats.shouldRead(fileSchema, block) &&
          dictionary.shouldRead(fileSchema, block, reader.getDictionaryReader(block)) &&
          bloom.shouldRead(fileSchema, block, reader.getBloomFilterDataReader(block))
      }
      shouldRead
    }
  }

  private[iceberg] def hasMissingDefaultFilter(
      fileSchema: MessageType,
      requiredSchema: Schema,
      filter: Expression,
      caseSensitive: Boolean): Boolean = {
    val defaultFieldIds = Binder.boundReferences(
      requiredSchema.asStruct(), Collections.singletonList(filter), caseSensitive)
      .asScala.filter(id => ShimUtils.hasInitialDefault(requiredSchema.findField(id)))

    if (defaultFieldIds.isEmpty) {
      false
    } else {
      val fileFieldIds = PhysicalParquetSchemaVisitor.fieldIds(fileSchema)
      defaultFieldIds.exists(id => !fileFieldIds.contains(id.intValue()))
    }
  }
}
