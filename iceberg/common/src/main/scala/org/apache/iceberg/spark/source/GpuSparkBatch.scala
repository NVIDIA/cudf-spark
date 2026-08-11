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

import java.util.Objects

import org.apache.iceberg.SchemaParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.util.SerializableConfiguration

class GpuSparkBatch(
    val cpuBatch: Batch,
    val parentScan: GpuSparkScan,
) extends Batch  {
  override def createReaderFactory(): PartitionReaderFactory = {
    new GpuReaderFactory(
      parentScan.metrics,
      parentScan.rapidsConf,
      parentScan.queryUsesInputFile)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val expectedSchema = GpuSparkScanAccess.expectedSchema(cpuBatch)
    val expectedSchemaString = SchemaParser.toJson(expectedSchema)

    val sparkContext = SparkSession.getActiveSession.get.sparkContext
    val hadoopConf = sparkContext.broadcast(
      new SerializableConfiguration(sparkContext.hadoopConfiguration))

    cpuBatch.planInputPartitions().map { partition =>
      new GpuSparkInputPartition(partition,
        parentScan.rapidsConf,
        hadoopConf,
        expectedSchemaString)
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuBatch, parentScan)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkBatch =>
        this.cpuBatch == that.cpuBatch && this.parentScan == that.parentScan
      case _ => false
    }
  }
}
