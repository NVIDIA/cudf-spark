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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{CombineConf, GpuMetric, MultiFileReaderUtils, RapidsConf, ThreadPoolConfBuilder}
import com.nvidia.spark.rapids.iceberg.ShimUtils.locationOf
import com.nvidia.spark.rapids.iceberg.parquet.{
  MultiFile,
  MultiThread,
  SingleFile,
  ThreadConf
}
import org.apache.iceberg.{FileFormat, MetadataColumns}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuReaderFactory(private val metrics: Map[String, GpuMetric],
    @transient rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean) extends PartitionReaderFactory {

  private val allCloudSchemes = rapidsConf.getCloudSchemes.toSet
  private val isParquetPerFileReadEnabled = rapidsConf.isParquetPerFileReadEnabled
  private val canUseParquetMultiThread = rapidsConf.isParquetMultiThreadReadEnabled
  // Here ignores the "ignoreCorruptFiles" comparing to the code in
  // "GpuParquetMultiFilePartitionReaderFactory", since "ignoreCorruptFiles" is
  // not honored by Iceberg.
  private val canUseParquetCoalescing = rapidsConf.isParquetCoalesceFileReadEnabled

  private val poolConfBuilder = ThreadPoolConfBuilder(rapidsConf)
  private val combineThresholdSize = rapidsConf.getMultithreadedCombineThreshold
  private val combineWaitTime = rapidsConf.getMultithreadedCombineWaitTime

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("GpuReaderFactory does not support createReader()")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case gpuPartition: GpuSparkInputPartition =>
        val threadConf = calcThreadConf(gpuPartition)
        new GpuIcebergPartitionReader(gpuPartition, threadConf, metrics)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported partition type: ${partition.getClass}")
    }
  }

  override def supportColumnarReads(partition: InputPartition) = true

  private def calcThreadConf(partition: GpuSparkInputPartition): ThreadConf = {
    val scans = GpuSparkScanAccess
      .taskGroup(partition.cpuPartition)
      .tasks
      .asScala
      .map(_.asFileScanTask())

    val hasNoDeletes = scans.forall(_.deletes.isEmpty)
    val hasFilePathMetadata =
      partition.expectedSchema.findField(MetadataColumns.FILE_PATH.fieldId()) != null
    val hasRowPositionMetadata =
      partition.expectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null

    val allParquet = scans.forall(_.file.format == FileFormat.PARQUET)

    if (allParquet) {
      if (isParquetPerFileReadEnabled) {
        // If per-file read is enabled, we can only use single threaded reading.
        return SingleFile
      }

      val canUseMultiThread = canUseParquetMultiThread
      // `_pos` must be file-global. The coalescing reader's parent
      // (MultiFileCoalescingPartitionReaderBase.populateCurrentBlockChunk) can merge blocks
      // from multiple Iceberg splits of the same physical Parquet file into one chunk and
      // finalize the whole chunk with the first split's per-file post-processor, which
      // would emit wrong `_pos` for any rows past the first split. Route `_pos`-projecting
      // scans to the multi-thread/single-file readers instead — those finalize batches per
      // `IcebergPartitionedFile`, so each split's own post-processor handles its own rows.
      val canUseCoalescing = canUseParquetCoalescing && hasNoDeletes && !queryUsesInputFile &&
        !hasRowPositionMetadata

      val files = scans.map(s => locationOf(s.file)).toArray

      val useMultiThread = MultiFileReaderUtils.useMultiThreadReader(canUseCoalescing,
        canUseMultiThread, files, allCloudSchemes)

      if (useMultiThread) {
        // Delete filtering is still file-specific for the multi-thread reader, so any delete file
        // must keep combining off.
        val disableCombining =
          queryUsesInputFile || hasFilePathMetadata || hasRowPositionMetadata ||
            !hasNoDeletes
        MultiThread(poolConfBuilder, partition.maxNumParquetFilesParallel,
          CombineConf(combineThresholdSize, combineWaitTime),
          disableCombining,
          hasFilePathMetadata,
          hasRowPositionMetadata)
      } else {
        MultiFile(poolConfBuilder, hasFilePathMetadata, hasRowPositionMetadata)
      }
    } else {
      throw new UnsupportedOperationException("Currently only parquet format is supported")
    }
  }
}
