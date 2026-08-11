/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta24x

import java.net.URI

import com.nvidia.spark.rapids.{GpuMetric, RapidsConf}
import com.nvidia.spark.rapids.delta.{GpuDeltaParquetFileFormat, RoaringBitmapWrapper}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.addMetadataColumnToIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, IdMapping}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.DeletionVectorDescriptorWithFilterType
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuDelta24xParquetFileFormat(
    metadata: Metadata,
    isSplittable: Boolean,
    disablePushDown: Boolean,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends GpuDeltaParquetFileFormat {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = isSplittable

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
    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      if (disablePushDown) Seq.empty else filters,
      options,
      hadoopConf,
      metrics)

    val delVecs = broadcastDvMap
    val maxDelVecScatterBatchSize = RapidsConf
      .DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE
      .get(sparkSession.sessionState.conf)
    val delVecScatterTimeMetric = metrics(GpuMetric.DELETION_VECTOR_SCATTER_TIME)
    val delVecSizeMetric = metrics(GpuMetric.DELETION_VECTOR_SIZE)


    (file: PartitionedFile) => {
      val input = dataReader(file)
      val dv = delVecs.flatMap(_.value.get(new URI(file.filePath.toString())))
        .map { dv =>
          delVecSizeMetric += dv.descriptor.inlineData.length
          RoaringBitmapWrapper.deserializeFromBytes(dv.descriptor.inlineData).inner
        }
      addMetadataColumnToIterator(prepareSchema(requiredSchema),
        dv,
        input.asInstanceOf[Iterator[ColumnarBatch]],
        maxDelVecScatterBatchSize,
        delVecScatterTimeMetric)
        .asInstanceOf[Iterator[InternalRow]]
    }
  }

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: GpuDelta24xParquetFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
          ff.referenceSchema == referenceSchema &&
          ff.isSplittable == isSplittable
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()
}
