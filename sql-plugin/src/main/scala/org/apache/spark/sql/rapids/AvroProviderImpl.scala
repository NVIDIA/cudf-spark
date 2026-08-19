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

import com.nvidia.spark.rapids._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.avro.{AvroFileFormat, AvroOptions}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.SerializableConfiguration

class AvroProviderImpl extends AvroProvider {

  /** If the file format is supported as an external source */
  def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = {
    format == classOf[AvroFileFormat]
  }

  def isPerFileReadEnabledForFormat(format: FileFormat, conf: RapidsConf): Boolean = {
    conf.isAvroPerFileReadEnabled
  }

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    GpuReadAvroFileFormat.tagSupport(meta)
  }

  /**
   * Get a read file format for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def getReadFileFormat(format: FileFormat): FileFormat = {
    require(isSupportedFormat(format.getClass), s"unexpected format: $format")
    new GpuReadAvroFileFormat
  }

  /**
   * Create a multi-file reader factory for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def createMultiFileReaderFactory(
      format: FileFormat,
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

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    Seq(
      GpuOverrides.scan[AvroScan](
        "Avro parsing",
        (a, conf, p, r) => new ScanMeta[AvroScan](a, conf, p, r) {
          override def tagSelfForGpu(): Unit = GpuAvroScan.tagSupport(this)

          override def convertToGpu(): GpuScan =
            GpuAvroScan(a.sparkSession,
              a.fileIndex,
              a.dataSchema,
              a.readDataSchema,
              a.readPartitionSchema,
              a.options,
              a.pushedFilters,
              this.conf,
              a.partitionFilters,
              a.dataFilters)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }
}
