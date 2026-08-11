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

package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.RapidsConf
import org.apache.iceberg.{Schema, SchemaParser}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}
import org.apache.spark.util.SerializableConfiguration

class GpuSparkInputPartition(val cpuPartition: InputPartition,
    rapidsConf: RapidsConf,
    val hadoopConf: Broadcast[SerializableConfiguration],
    val expectedSchemaStr: String) extends
  InputPartition with HasPartitionKey with Serializable {

  private val cpuPartitionWithKey: HasPartitionKey = cpuPartition match {
    case withKey: HasPartitionKey => withKey
    case other =>
      throw new IllegalArgumentException(
        s"Iceberg input partition ${other.getClass.getName} does not provide a partition key")
  }

  val maxReadBatchSizeRows: Int = rapidsConf.maxReadBatchSizeRows
  val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes
  val gpuTargetBatchSizeBytes: Long = rapidsConf.gpuTargetBatchSizeBytes
  val maxGpuColumnSizeBytes: Long = rapidsConf.maxGpuColumnSizeBytes
  val chunkedReaderEnabled: Boolean = rapidsConf.chunkedReaderEnabled
  val parquetDebugDumpPrefix: Option[String] = rapidsConf.parquetDebugDumpPrefix
  val parquetDebugDumpAlways: Boolean = rapidsConf.parquetDebugDumpAlways

  val maxChunkedReaderMemoryUsageSizeBytes: Long = {
    if (rapidsConf.limitChunkedReaderMemoryUsage) {
      val limitRatio = rapidsConf.chunkedReaderMemoryUsageRatio
      (limitRatio * rapidsConf.gpuTargetBatchSizeBytes).toLong
    } else {
      0L
    }
  }

  val multiThreadReadNumThreads: Int = rapidsConf.multiThreadReadNumThreads
  val maxNumParquetFilesParallel: Int = rapidsConf.maxNumParquetFilesParallel


  override def preferredLocations(): Array[String] = cpuPartition.preferredLocations()
  override def partitionKey(): InternalRow = cpuPartitionWithKey.partitionKey()

  @transient lazy val expectedSchema: Schema = {
    SchemaParser.fromJson(expectedSchemaStr)
  }
}
