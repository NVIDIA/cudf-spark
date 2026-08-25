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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.sequencefile.GpuSequenceFilePartitionReaderFactory
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class GpuReadSequenceFileBinaryFormat extends SequenceFileBinaryFileFormat
    with GpuReadFileFormatWithMetrics {

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric]): PartitionedFile => Iterator[InternalRow] = {
    throw new IllegalStateException("SequenceFile binary requires the multi-file reader")
  }

  override def isPerFileReadEnabled(conf: RapidsConf): Boolean = false

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    SequenceFileBinaryFileFormat.validateDataSchema(fileScan.relation.dataSchema)
    SequenceFileBinaryFileFormat.validateRequiredSchema(fileScan.requiredSchema)
    GpuSequenceFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      fileScan.requiredSchema,
      fileScan.readPartitionSchema,
      fileScan.rapidsConf,
      ThreadPoolConfBuilder(fileScan.rapidsConf),
      fileScan.allMetrics,
      fileScan.queryUsesInputFile)
  }
}

object GpuReadSequenceFileBinaryFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    meta.mustBeReplaced("SequenceFile binary has no CPU reader")
    val scan = meta.wrapped
    try {
      SequenceFileBinaryFileFormat.validateDataSchema(scan.relation.dataSchema)
      SequenceFileBinaryFileFormat.validateRequiredSchema(scan.requiredSchema)
    } catch {
      case e: IllegalArgumentException => meta.willNotWorkOnGpu(e.getMessage)
    }
    if (scan.relation.partitionSchema.nonEmpty) {
      meta.willNotWorkOnGpu("SequenceFile binary does not support partition columns")
    }
    val options = scan.relation.options
    if (SequenceFileBinaryFileFormat.booleanOption(
        options, "ignoreMissingFiles", scan.conf.ignoreMissingFiles) ||
        SequenceFileBinaryFileFormat.booleanOption(
          options, "ignoreCorruptFiles", scan.conf.ignoreCorruptFiles)) {
      meta.willNotWorkOnGpu(
        "SequenceFile binary does not support ignoreMissingFiles or ignoreCorruptFiles")
    }
  }
}
