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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.filecache.FileCacheLocalityManager
import com.nvidia.spark.rapids.iceberg.ShimUtils.locationOf
import org.apache.iceberg.SchemaParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.util.SerializableConfiguration

object GpuSparkBatch {
  private val LocalHost = "localhost"
  private val NumPreferredLocations = 3
}

class GpuSparkBatch(
    val cpuBatch: Batch,
    val parentScan: GpuSparkScan,
) extends Batch  {
  import GpuSparkBatch._

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
      val fileCacheLocations = preferredLocationsFromFileCache(partition)
      val preferredLocations = if (fileCacheLocations.nonEmpty) {
        fileCacheLocations
      } else {
        partition.preferredLocations()
      }
      new GpuSparkInputPartition(partition,
        parentScan.rapidsConf,
        hadoopConf,
        expectedSchemaString,
        preferredLocations)
    }
  }

  private def preferredLocationsFromFileCache(partition: InputPartition): Array[String] = {
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    GpuSparkScanAccess.taskGroup(partition)
      .tasks()
      .asScala
      .map(_.asFileScanTask())
      .foreach { task =>
        addFileCacheLocations(hostToNumBytes, locationOf(task.file()), task.length())
      }

    hostToNumBytes.toSeq.sortBy {
      case (_, numBytes) => numBytes
    }.reverse.take(NumPreferredLocations).map {
      case (host, _) => host
    }.toArray
  }

  private def addFileCacheLocations(
      hostToNumBytes: mutable.HashMap[String, Long],
      file: String,
      bytes: Long): Unit = {
    FileCacheLocalityManager.get.getLocations(file).filter(_ != LocalHost).foreach { host =>
      hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + bytes
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
