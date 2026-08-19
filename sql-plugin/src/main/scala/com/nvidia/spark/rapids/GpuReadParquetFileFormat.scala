/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.parquet.{GpuParquetMultiFilePartitionReaderFactory, GpuParquetPartitionReaderFactory, GpuParquetPartitionReaderFactoryBase, GpuParquetScan}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading Parquet files with the GPU.
 */
class GpuReadParquetFileFormat extends ParquetFileFormat with GpuReadFileFormatWithMetrics {

  /**
   * Create a partition reader factory for the per-file reader.
   */
  def createPartitionReaderFactory(sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Seq[Filter],
      rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      options: Map[String, String]) : GpuParquetPartitionReaderFactoryBase = {
    GpuParquetPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      partitionSchema,
      filters.toArray,
      rapidsConf,
      metrics,
      options)
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
    val sqlConf = sparkSession.sessionState.conf
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val factory = createPartitionReaderFactory(
      sqlConf,
      broadcastedHadoopConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      filters,
      new RapidsConf(sqlConf),
      metrics,
      options)
    PartitionReaderIterator.buildReader(factory)
  }

  override def isPerFileReadEnabled(conf: RapidsConf): Boolean = conf.isParquetPerFileReadEnabled

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val poolConf = ThreadPoolConfBuilder(fileScan.rapidsConf)
    GpuParquetMultiFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      fileScan.relation.dataSchema,
      fileScan.requiredSchema,
      fileScan.readPartitionSchema,
      pushedFilters,
      fileScan.rapidsConf,
      poolConf,
      fileScan.allMetrics,
      fileScan.queryUsesInputFile)
  }
}

object GpuReadParquetFileFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    val session = SparkSessionUtils.sessionFromPlan(fsse)
    GpuParquetScan.tagSupport(session, fsse.requiredSchema, meta)
  }
}
