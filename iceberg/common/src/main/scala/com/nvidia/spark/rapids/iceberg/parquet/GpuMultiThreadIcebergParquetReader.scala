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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.HostMemoryBuffersWithMetaDataBase
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, HostMemoryBuffersWithMetaData, MultiFileCloudParquetPartitionReader}

import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiThreadIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val deleteFilterProvider: IcebergPartitionedFile => Option[GpuDeleteFilter],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {
  private val pathToFile = files.groupBy(_.urlEncodedPath).mapValues(_.toSeq)
  private val postProcessors: ConcurrentMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor]
  = new ConcurrentHashMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor](files.size)


  private var inited = false
  private lazy val reader = createParquetReader()

  override def close(): Unit = {
    if (inited) {
      withResource(reader) { _ => }
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = reader.get()

  private def findIcebergFile(f: PartitionedFile): IcebergPartitionedFile = {
    val path = f.filePath.toString()
    val icebergFiles = pathToFile(path).filter(p => p.isSame(f))
    require(icebergFiles.length == 1, s"Expected 1 iceberg partition file, but found " +
      s"${icebergFiles.length} for $f")
    icebergFiles.head
  }

  private def createParquetReader() = {
    val sparkPartitionedFiles = files.map(_.sparkPartitionedFile).toArray
    val multiThreadConf = conf.threadConf.asInstanceOf[MultiThread]

    inited = true
    new MultiFileCloudParquetPartitionReader(
      rapidsFileIO,
      conf.conf,
      sparkPartitionedFiles,
      this.filterBlock,
      conf.caseSensitive,
      conf.parquetDebugDumpPrefix,
      conf.parquetDebugDumpAlways,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      conf.targetBatchSizeBytes,
      conf.maxGpuColumnSizeBytes,
      conf.useChunkedReader,
      conf.maxChunkedReaderMemoryUsageSizeBytes,
      CpuCompressionConfig.disabled(),
      conf.metrics,
      new StructType(), // iceberg handles partition value by itself
      multiThreadConf.poolConfBuilder.build(),
      multiThreadConf.maxNumFilesProcessed,
      false, // ignoreMissingFiles
      false, // ignoreCorruptFiles
      false, // useFieldId
      multiThreadConf.disableCombining,
      multiThreadConf.disableCombining,
      multiThreadConf.combineConf) {

      override def checkIfNeedToSplit(current: HostMemoryBuffersWithMetaData,
          next: HostMemoryBuffersWithMetaData): Boolean = {
        if (current.partitionedFile.filePath == next.partitionedFile.filePath) return false
        if (super.checkIfNeedToSplit(current, next)) return true
        if (multiThreadConf.disableCombining) return true
        val curFile = findIcebergFile(current.partitionedFile)
        val nextFile = findIcebergFile(next.partitionedFile)
        val curProcessor = postProcessors.get(curFile)
        val nextProcessor = postProcessors.get(nextFile)
        if (curProcessor == null || nextProcessor == null) return true
        !curProcessor.compatibleForCombining(nextProcessor)
      }

      override def readBatches(
          fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
        val icebergFile = findIcebergFile(fileBufsAndMeta.partitionedFile)
        val postProcessor = postProcessors.get(icebergFile)
        require(postProcessor != null,
          s"Iceberg parquet partition file post processor does not exist for $icebergFile")

        val postProcessed = super.readBatches(fileBufsAndMeta).map { batch =>
          // Empty row-count batches are materialized with the Parquet read schema, so they still
          // need Iceberg post-processing to match the expected output schema.
          postProcessor.process(batch)
        }
        deleteFilterProvider(icebergFile)
          .map(_.filterAndDelete(postProcessed))
          .getOrElse(postProcessed)
      }
    }
  }

  private def filterBlock(f: PartitionedFile) = {
    val icebergFile = findIcebergFile(f)
    val deleteFilter = deleteFilterProvider(icebergFile)

    val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

    val (filteredParquet, shadedFileReadSchema) =
      super.filterParquetBlocks(icebergFile, requiredSchema)

    val postProcessor = new GpuParquetReaderPostProcessor(
      filteredParquet,
      constantsProvider(icebergFile),
      requiredSchema,
      shadedFileReadSchema,
      conf.metrics)

    val old = postProcessors.put(icebergFile, postProcessor)
    require(old == null, "Iceberg parquet partition file post processor already exists!")
    filteredParquet
  }
}