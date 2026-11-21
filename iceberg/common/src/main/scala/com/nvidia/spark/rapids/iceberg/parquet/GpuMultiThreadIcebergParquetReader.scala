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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.IcebergDeletionVector
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, HostMemoryBuffersWithMetaData,
  HostMemoryEmptyMetaData, MultiFileCloudParquetPartitionReader, ParquetDataBlock}
import org.apache.iceberg.spark.SparkSchemaUtil

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiThreadIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val deleteFilterProvider: IcebergPartitionedFile => Option[GpuDeleteFilter],
    val deletionVectorProvider: IcebergPartitionedFile => Option[IcebergDeletionVector],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {
  private val pathToFile = files.groupBy(_.urlEncodedPath).mapValues(_.toSeq)
  private val postProcessors: ConcurrentMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor]
  = new ConcurrentHashMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor](files.size)
  private val deletionVectors: ConcurrentMap[IcebergPartitionedFile, IcebergDeletionVector] =
    new ConcurrentHashMap[IcebergPartitionedFile, IcebergDeletionVector](files.size)
  // Shared SQL metrics are not thread-safe. Reader threads accumulate elapsed times atomically
  // here so that only the task thread updates the published metrics.
  private val pendingTimers = new ConcurrentHashMap[String, AtomicLong]()

  private var inited = false
  // Visible for testing
  private[iceberg] lazy val reader = createParquetReader()

  override def close(): Unit = {
    try {
      if (inited) {
        withResource(reader) { _ => }
      }
    } finally {
      flushTimeMetrics()
      deletionVectors.values().asScala.toSeq.safeClose()
    }
  }

  override def hasNext: Boolean = {
    try {
      reader.next()
    } finally {
      flushTimeMetrics()
    }
  }

  override def next(): ColumnarBatch = reader.get()

  override protected def addTimeByMetric(metricName: String, value: Long): Unit = {
    pendingTimers.computeIfAbsent(metricName, _ => new AtomicLong()).addAndGet(value)
  }

  // Publish completed file timings on the task thread, including files with no surviving rows.
  private def flushTimeMetrics(): Unit = {
    pendingTimers.asScala.foreach { case (metricName, time) =>
      // Atomically take the current total; later additions remain for the next flush.
      val value = time.getAndSet(0L)
      if (value != 0L) {
        super.addTimeByMetric(metricName, value)
      }
    }
  }

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
    val readerConf = conf

    inited = true
    new MultiFileCloudParquetPartitionReader(
      rapidsFileIO,
      conf.conf,
      sparkPartitionedFiles,
      this.filterBlock,
      true, // Iceberg has already resolved the physical schema by field ID.
      conf.parquetDebugDumpPrefix,
      conf.parquetDebugDumpAlways,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      conf.targetBatchSizeBytes,
      conf.maxGpuColumnSizeBytes,
      conf.useChunkedReader,
      conf.maxChunkedReaderMemoryUsageSizeBytes,
      conf.skipReadEstimate,
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

      private lazy val outputSchema = SparkSchemaUtil.convert(readerConf.expectedSchema)
      private lazy val copyOutputRow = UnsafeProjection.create(outputSchema)
      private lazy val metadataBatchRows = GpuNullBatchIterator.maxRows(outputSchema,
        readerConf.maxBatchSizeRows, math.min(readerConf.maxBatchSizeBytes,
          readerConf.targetBatchSizeBytes))
      private val combineMetadata = !multiThreadConf.disableCombining &&
        multiThreadConf.combineConf.combineThresholdSize > 0

      private def metadataRow(meta: HostMemoryEmptyMetaData): Option[InternalRow] = {
        if (!combineMetadata || meta.numRows == 0) {
          None
        } else {
          val file = findIcebergFile(meta.partitionedFile)
          Option(postProcessors.get(file))
            .filter(_ => !deletionVectors.containsKey(file) && deleteFilterProvider(file).isEmpty)
            .flatMap(_.metadataOnlyOutputRow)
        }
      }

      override def checkIfNeedToSplit(current: HostMemoryBuffersWithMetaData,
          next: HostMemoryBuffersWithMetaData): Boolean = {
        if (current.partitionedFile.filePath == next.partitionedFile.filePath) return false
        if (super.checkIfNeedToSplit(current, next)) return true
        checkIfNeedToSplitIcebergFiles(current.partitionedFile, next.partitionedFile)
      }

      override protected def checkIfNeedToSplitEmpty(current: HostMemoryEmptyMetaData,
          next: HostMemoryEmptyMetaData): Boolean = {
        (metadataRow(current), metadataRow(next)) match {
          case (Some(_), Some(_)) => false
          case (None, None) => checkIfNeedToSplitIcebergFiles(current.partitionedFile,
            next.partitionedFile)
          case _ => true
        }
      }

      override protected def newCombinedHMEmptyMetadata(empty: CombinedEmptyMeta,
          selected: CombinedMeta): HostMemoryEmptyMetaData = {
        if (metadataRow(empty.metaForEmpty).isEmpty) {
          super.newCombinedHMEmptyMetadata(empty, selected)
        } else {
          // Every positive-row split passed checkIfNeedToSplitEmpty. Save its exact footer
          // count and its projected values in the same order as the selected splits.
          val runs = ArrayBuffer.empty[(Long, InternalRow)]
          empty.emptyMetas.iterator.filter(_.numRows > 0).foreach { meta =>
            val row = copyOutputRow(metadataRow(meta).get)
            // UnsafeRow compares the encoded values, including the sign bit of nested floats.
            if (runs.nonEmpty && runs.last._2 == row) {
              runs(runs.length - 1) = (runs.last._1 + meta.numRows, runs.last._2)
            } else {
              runs += ((meta.numRows, row.copy()))
            }
          }
          if (runs.length > 1) {
            readerConf.metrics.get(GpuMetric.NUM_MIXED_CONSTANT_METADATA_GROUPS).foreach(_ += 1)
          }
          super.newCombinedHMEmptyMetadata(empty, selected.copy(allPartValues = runs.toArray))
        }
      }

      private def checkIfNeedToSplitIcebergFiles(current: PartitionedFile,
          next: PartitionedFile): Boolean = {
        val curFile = findIcebergFile(current)
        val nextFile = findIcebergFile(next)
        val curProcessor = postProcessors.get(curFile)
        val nextProcessor = postProcessors.get(nextFile)
        if (curProcessor == null || nextProcessor == null) return true
        if (deletionVectors.containsKey(curFile) || deletionVectors.containsKey(nextFile)) {
          return true
        }
        !curProcessor.compatibleForCombining(nextProcessor)
      }

      override protected def readBufferToBatches(
          buffer: HostMemoryBuffersWithMetaData): Iterator[ColumnarBatch] = {
        val icebergFile = findIcebergFile(buffer.partitionedFile)
        Option(deletionVectors.get(icebergFile)).map { deletionVector =>
          val hmbAndInfo = buffer.memBuffersAndSizes.head
          val hostBuffers = hmbAndInfo.hmbs
          val blocks = hmbAndInfo.blockMeta.map(_.asInstanceOf[ParquetDataBlock].dataBlock)
          val parseOptions = closeOnExcept(hostBuffers) { _ =>
            getParquetOptions(buffer.readSchema, buffer.clippedSchema, useFieldId = false)
          }
          val columnTypes = LongType +: buffer.readSchema.fields.map(_.dataType)

          RmmRapidsRetryIterator.withRetryNoSplit(hostBuffers.toSeq) { spillableBuffers =>
            val hostBufs = spillableBuffers.safeMap(_.getDataHostBuffer()).toArray
            closeOnExcept(hostBufs) { _ =>
              GpuSemaphore.acquireIfNecessary(TaskContext.get())
            }
            val producer = GpuIcebergDeletionVector.makeProducer(
              GpuMultiThreadIcebergParquetReader.this.conf.useChunkedReader,
              GpuMultiThreadIcebergParquetReader.this.conf.maxChunkedReaderMemoryUsageSizeBytes,
              GpuMultiThreadIcebergParquetReader.this.conf.conf,
              GpuMultiThreadIcebergParquetReader.this.conf.targetBatchSizeBytes,
              parseOptions, hostBufs, GpuMultiThreadIcebergParquetReader.this.conf.metrics,
              buffer.dateRebaseMode, buffer.timestampRebaseMode,
              true, // The deletion-vector producer also evolves the physical schema.
              useFieldId = false, buffer.readSchema, buffer.clippedSchema,
              Array(buffer.partitionedFile),
              GpuMultiThreadIcebergParquetReader.this.conf.parquetDebugDumpPrefix,
              GpuMultiThreadIcebergParquetReader.this.conf.parquetDebugDumpAlways,
              deletionVector, blocks)
            CachedGpuBatchIterator(producer, columnTypes)
          }
        }.getOrElse(super.readBufferToBatches(buffer))
      }

      override def readBatches(
          fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
        val icebergFile = findIcebergFile(fileBufsAndMeta.partitionedFile)
        val postProcessor = postProcessors.get(icebergFile)
        require(postProcessor != null,
          s"Iceberg parquet partition file post processor does not exist for $icebergFile")

        fileBufsAndMeta match {
          case empty: HostMemoryEmptyMetaData if metadataRow(empty).isDefined =>
            withResource(empty) { _ =>
              val (rows, values) = empty.allPartValues.getOrElse(
                Array((empty.numRows, postProcessor.metadataOnlyOutputRow.get))).unzip
              val counts = GpuNullBatchIterator(new StructType, empty.numRows, metadataBatchRows)
              val batches = new GpuColumnarBatchWithPartitionValuesIterator(counts, values,
                rows, outputSchema, readerConf.maxGpuColumnSizeBytes)
              new Iterator[ColumnarBatch] {
                override def hasNext: Boolean = batches.hasNext
                override def next(): ColumnarBatch =
                  postProcessor.materializeMetadataOnlyBatch(batches.next())
              }
            }
          case _ =>
            val postProcessed = super.readBatches(fileBufsAndMeta).map(postProcessor.process)
            deleteFilterProvider(icebergFile)
              .map(_.filterAndDelete(postProcessed))
              .getOrElse(postProcessed)
        }
      }
    }
  }

  private def filterBlock(f: PartitionedFile) = {
    val icebergFile = findIcebergFile(f)
    val deleteFilter = deleteFilterProvider(icebergFile)
    val deletionVector = deletionVectorProvider(icebergFile)
    closeOnExcept(deletionVector) { _ =>
      val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

      val (filteredParquet, shadedFileReadSchema) =
        super.filterParquetBlocks(icebergFile, requiredSchema, deletionVector.isDefined)

      val postProcessor = new GpuParquetReaderPostProcessor(
        filteredParquet,
        constantsProvider(icebergFile),
        requiredSchema,
        shadedFileReadSchema,
        conf.metrics)

      deletionVector.foreach { dv =>
        val oldVector = deletionVectors.put(icebergFile, dv)
        require(oldVector == null,
          "Iceberg parquet partition file deletion vector already exists!")
      }
      // Publish the post-processor last. Its presence tells the combiner that the DV state for
      // this file is final and can be checked without racing filterBlock initialization.
      val oldProcessor = postProcessors.put(icebergFile, postProcessor)
      require(oldProcessor == null,
        "Iceberg parquet partition file post processor already exists!")
      filteredParquet
    }
  }
}
