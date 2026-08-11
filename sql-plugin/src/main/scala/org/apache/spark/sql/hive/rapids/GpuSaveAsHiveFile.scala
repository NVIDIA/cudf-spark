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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.rapids.{ColumnarFileFormat, GpuDataWritingCommand}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.hive.execution.SaveAsHiveFile
import org.apache.spark.sql.rapids.GpuFileFormatWriter
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

// Base trait from which all hive insert statement physical execution extends.
private[hive] trait GpuSaveAsHiveFile extends GpuDataWritingCommand with SaveAsHiveFile {

  // TODO(future): Examine compressions options.
  // - Apache Spark 3.1-3 has code to examine Hadoop compression settings
  //   (and takes a FileSinkDesc instead of FileFormat).
  // - Apache Spark 3.4 has removed all that logic.
  // - GPU Hive text writer does not support compression for output.
  protected def gpuSaveAsHiveFile(sparkSession: SparkSession,
      plan: SparkPlan,
      hadoopConf: Configuration,
      fileFormat: ColumnarFileFormat,
      outputLocation: String,
      forceHiveHashForBucketing: Boolean,
      baseDebugOutputPath: Option[String],
      customPartitionLocations: Map[TablePartitionSpec,String] = Map.empty,
      partitionAttributes: Seq[Attribute] = Nil,
      bucketSpec: Option[BucketSpec] = None,
      options: Map[String, String] = Map.empty): Set[String] = {

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputLocation)

    GpuFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = fileFormat,
      committer = committer,
      outputSpec =
        FileFormatWriter.OutputSpec(outputLocation, customPartitionLocations, outputColumns),
      hadoopConf = hadoopConf,
      partitionColumns = partitionAttributes,
      bucketSpec = bucketSpec,
      statsTrackers = Seq(gpuWriteJobStatsTracker(hadoopConf)),
      options = options,
      useStableSort = false,                  // TODO: Fetch from RapidsConf.
      forceHiveHashForBucketing = forceHiveHashForBucketing,
      concurrentWriterPartitionFlushSize = 0L, // TODO: Fetch from RapidsConf.
      baseDebugOutputPath = baseDebugOutputPath
    )
  }
}
