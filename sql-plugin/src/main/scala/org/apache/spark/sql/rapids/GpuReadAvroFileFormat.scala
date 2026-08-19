/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuMetric, GpuReadFileFormatWithMetrics, PartitionReaderIterator, RapidsConf, SparkPlanMeta, ThreadPoolConfBuilder}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AvroFileFormat, AvroOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading Avro files with the GPU.
 */
class GpuReadAvroFileFormat extends AvroFileFormat with GpuReadFileFormatWithMetrics {

  @scala.annotation.nowarn(
    "msg=value ignoreExtension in class AvroOptions is deprecated*"
  )
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

    val factory = GpuAvroPartitionReaderFactory(
      sqlConf,
      new RapidsConf(sqlConf),
      broadcastedHadoopConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      new AvroOptions(options, hadoopConf),
      metrics,
      options)
    PartitionReaderIterator.buildReader(factory)
  }

  override def isPerFileReadEnabled(conf: RapidsConf): Boolean = conf.isAvroPerFileReadEnabled

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val poolConfBuilder = ThreadPoolConfBuilder(fileScan.rapidsConf)
    GpuAvroMultiFilePartitionReaderFactory(
      fileScan.relation.sparkSession.sessionState.conf,
      fileScan.rapidsConf,
      broadcastedConf,
      fileScan.relation.dataSchema,
      fileScan.requiredSchema,
      fileScan.readPartitionSchema,
      new AvroOptions(fileScan.relation.options, broadcastedConf.value.value),
      fileScan.allMetrics,
      pushedFilters,
      poolConfBuilder,
      fileScan.queryUsesInputFile)
  }
}

object GpuReadAvroFileFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    GpuAvroScan.tagSupport(
      SparkSessionUtils.sessionFromPlan(fsse),
      fsse.requiredSchema,
      fsse.relation.options,
      meta
    )
  }
}
