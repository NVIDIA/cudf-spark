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
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.perf.ScanSplitHeuristic
import org.apache.iceberg.SchemaParser

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.util.SerializableConfiguration

class GpuSparkBatch(
    val cpuBatch: Batch,
    val parentScan: GpuSparkScan,
) extends Batch  {
  override def createReaderFactory(): PartitionReaderFactory = {
    new GpuReaderFactory(
      parentScan.metrics,
      parentScan.rapidsConf,
      parentScan.queryUsesInputFile,
      parentScan.splitTotals)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val expectedSchema = GpuSparkScanAccess.expectedSchema(cpuBatch)
    val expectedSchemaString = SchemaParser.toJson(expectedSchema)

    // Spark can plan a scan from a background thread where the thread-local active
    // SparkSession is not set. The application SparkContext is not thread-local.
    val sparkContext = SparkContext.getOrCreate()
    val hadoopConf = sparkContext.broadcast(
      new SerializableConfiguration(sparkContext.hadoopConfiguration))

    // The same key and size are used to decide and to record.
    val key = parentScan.historyKey()
    lazy val listedBytes =
      GpuSparkScanAccess.dataBytes(GpuSparkScanAccess.tasks(parentScan.cpuScan))
    val cpuPartitions = key.flatMap { k =>
      GpuSparkBatch.regroup(parentScan.cpuScan, cpuBatch, listedBytes, (listed, icebergSplit) =>
        // Planning can run without an active session.
        SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
          .fold(icebergSplit) { spark =>
            ScanSplitHeuristic.learnedFileSplit(Some(k), spark, listed, icebergSplit)
          })
    }.getOrElse(cpuBatch.planInputPartitions())
    val partitions: Array[InputPartition] = cpuPartitions.map { partition =>
      new GpuSparkInputPartition(partition,
        parentScan.rapidsConf,
        hadoopConf,
        expectedSchemaString)
    }
    parentScan.registerPlanning(partitions.length, listedBytes, key)
    partitions
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

object GpuSparkBatch extends Logging {

  private val warnedNotRegrouped = new AtomicBoolean(false)

  /**
   * The scan's tasks regrouped by `decide(listedBytes, icebergSplit)`, or None to keep Iceberg's
   * groups. Scans with delete files are skipped: the GPU reader disables coalescing for any
   * partition that has them.
   */
  def regroup(
      cpuScan: Scan,
      cpuBatch: Batch,
      listedBytes: => Long,
      decide: (Long, Long) => Long): Option[Array[InputPartition]] = {
    try {
      if (!GpuSparkScanAccess.isBatchQueryScan(cpuScan) ||
          GpuSparkScanAccess.isMetadataScan(cpuScan) ||
          !GpuSparkScanAccess.groupingKeyType(cpuScan).fields().isEmpty ||
          GpuSparkScanAccess.hasExplicitSplitSize(cpuScan) ||
          GpuSparkScanAccess.hasDeletes(GpuSparkScanAccess.tasks(cpuScan))) {
        None
      } else {
        val icebergSplit = GpuSparkScanAccess.splitSize(cpuScan)
        val split = decide(listedBytes, icebergSplit)
        if (split == icebergSplit) {
          None
        } else {
          val partitions = GpuSparkScanAccess.planInputPartitions(cpuScan, cpuBatch, split)
          logInfo(s"scan.split: table=${GpuSparkScanAccess.table(cpuScan).name()} regrouped " +
            s"into ${partitions.length} partitions, split=$split")
          Some(partitions)
        }
      }
    } catch {
      case NonFatal(e) =>
        if (warnedNotRegrouped.compareAndSet(false, true)) {
          logWarning("Iceberg scan not regrouped, keeping Iceberg's groups; later failures " +
            "are logged at debug level", e)
        } else {
          logDebug(s"Iceberg scan not regrouped, keeping Iceberg's groups: $e")
        }
        None
    }
  }
}
