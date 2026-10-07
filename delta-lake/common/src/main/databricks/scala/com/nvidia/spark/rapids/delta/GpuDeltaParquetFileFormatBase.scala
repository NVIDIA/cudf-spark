/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaColumnMappingMode, NameMapping, NoMapping}
import com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils
import com.nvidia.spark.rapids.{GpuMetric, GpuReadParquetFileFormat, ThreadPoolConfBuilder}
import  com.nvidia.spark.rapids.parquet.GpuParquetMultiFilePartitionReaderFactory
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

object GpuDeltaParquetFileFormatBase {
  private val GPU_ROW_INDEX_METADATA_KEY = "rapids.delta.internalRowIndex"

  def markGpuRowIndexColumn(field: StructField): StructField = field.copy(
    metadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putBoolean(GPU_ROW_INDEX_METADATA_KEY, value = true)
      .build())

  val GPU_ROW_INDEX_STRUCT_FIELD: StructField = markGpuRowIndexColumn(StructField(
    "_tmp_metadata_row_index",
    LongType,
    nullable = false,
    metadata = Metadata.empty))

  private[delta] def isGpuRowIndexColumn(field: StructField): Boolean =
    field.metadata.contains(GPU_ROW_INDEX_METADATA_KEY) &&
      field.metadata.getBoolean(GPU_ROW_INDEX_METADATA_KEY)

  private[delta] def findGpuRowIndexColumn(schema: StructType): Int =
    schema.fields.indexWhere(isGpuRowIndexColumn)
}

abstract class GpuDeltaParquetFileFormatBase extends GpuReadParquetFileFormat {
  val columnMappingMode: DeltaColumnMappingMode
  val referenceSchema: StructType

  def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .build())
      }
    } else {
      schema
    }
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val poolConfBuilder = ThreadPoolConfBuilder(fileScan.rapidsConf)
    GpuParquetMultiFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      prepareSchema(fileScan.requiredSchema),
      prepareSchema(fileScan.readPartitionSchema),
      pushedFilters,
      fileScan.rapidsConf,
      poolConfBuilder,
      fileScan.allMetrics,
      fileScan.queryUsesInputFile)
  }

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {
    super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      prepareSchema(dataSchema),
      prepareSchema(partitionSchema),
      prepareSchema(requiredSchema),
      filters,
      options,
      hadoopConf,
      metrics)
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }
}
