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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Iterator => JIterator, List => JList, Map => JMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{
  CachedGpuBatchIterator,
  GpuBatchUtils,
  GpuSemaphore,
  RmmRapidsRetryIterator,
  SingleGpuColumnarBatchIterator,
  SpillableHostBuffer
}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric.{
  BUFFER_TIME,
  FILECACHE_DATA_RANGE_HITS,
  FILECACHE_DATA_RANGE_HITS_SIZE,
  FILECACHE_DATA_RANGE_MISSES,
  FILECACHE_DATA_RANGE_MISSES_SIZE,
  FILECACHE_DATA_RANGE_READ_TIME,
  FILTER_TIME,
  ICEBERG_STAGED_COMBINE_TIME,
  ICEBERG_STAGED_DISK_BYTES,
  ICEBERG_STAGED_DISK_SUBTASK_COUNT,
  ICEBERG_STAGED_FOOTER_TIME,
  ICEBERG_STAGED_FOOTER_WAIT_TIME,
  ICEBERG_STAGED_IO_TIME,
  ICEBERG_STAGED_MATERIALIZATION_TIME,
  ICEBERG_STAGED_RESULT_WAIT_TIME,
  ICEBERG_STAGED_WAIT_TIME
}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_BATCHING_PRIORITY
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.parquet.staged._
import com.nvidia.spark.rapids.parquet.{
  CpuCompressionConfig,
  MakeParquetTableProducer,
  ParquetPartitionReaderBase
}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iceberg adapter for the staged Parquet partition reader.
 *
 * Most of the new pipeline is Java. This small Scala boundary intentionally remains beside the
 * existing Iceberg reader because it invokes Scala-native RAPIDS decode and post-processing APIs.
 * Footer loading/filtering and synthetic-file finalization run in the CPU pool, planning runs on
 * the Spark task thread, each source file is read independently in the I/O pool, and this adapter's
 * decode method is called only by the Spark task thread.
 */
class GpuStagedIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf,
    cpuThreads: Int,
    ioThreads: Int) extends GpuIcebergParquetReader {

  private val multiThreadConf = conf.threadConf.asInstanceOf[MultiThread]
  private val expectedSparkSchema = SparkSchemaUtil.convert(conf.expectedSchema)
  private val closed = new AtomicBoolean()
  private val stagedReaderRef = new AtomicReference[
    StagedParquetPartitionReader[IcebergPartitionedFile, IcebergFooterContext]]()
  private lazy val stagedReader = {
    val reader = createReader()
    stagedReaderRef.set(reader)
    // A task-completion close can race lazy construction. Whichever side observes the reader owns
    // the one close; a close that happened before publication is honored here.
    if (closed.get() && stagedReaderRef.compareAndSet(reader, null)) {
      reader.close()
    }
    reader
  }

  override def hasNext: Boolean = stagedReader.hasNext

  override def next(): ColumnarBatch = stagedReader.next()

  override def close(): Unit = {
    closed.set(true)
    Option(stagedReaderRef.getAndSet(null)).foreach(_.close())
  }

  private def createReader()
  : StagedParquetPartitionReader[IcebergPartitionedFile, IcebergFooterContext] = {
    val stagedFiles: JList[StagedScanFile[IcebergPartitionedFile]] =
      files.zipWithIndex.map { case (file, ordinal) =>
        val source = new StagedFileSource(
          ordinal,
          file.path,
          file.sparkPartitionedFile,
          () => rapidsFileIO.newInputFile(file.file.getDelegate.location()))
        new StagedScanFile(source, file)
      }.asJava

    val targetParquetBytes = if (multiThreadConf.disableCombining) {
      0L
    } else {
      multiThreadConf.combineConf.combineThresholdSize
    }
    val scratchBytes = conf.conf.getInt(
      "parquet.read.allocation.size", 8 * 1024 * 1024)
    new StagedParquetPartitionReader(
      stagedFiles,
      new IcebergAdapter,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      targetParquetBytes,
      cpuThreads,
      ioThreads,
      scratchBytes,
      TaskContext.get())
  }

  /**
   * Format-specific state and operations kept at the edge of the format-neutral Java pipeline.
   * Footer contexts are immutable after construction; only their post-processor is stateful, and
   * it is called exclusively from the task thread. The outer reader has already rejected `_pos`,
   * whose row-order state is incompatible with completion-order decode in this first version.
   */
  private class IcebergAdapter
      extends StagedScanAdapter[IcebergPartitionedFile, IcebergFooterContext] {

    override def readAndFilterFooter(file: StagedScanFile[IcebergPartitionedFile])
    : FooterResult[IcebergFooterContext] = {
      val icebergFile = file.getFormatFile
      val (parquetInfo, shadedFileReadSchema) =
        filterParquetBlocks(icebergFile, conf.expectedSchema)
      val postProcessor = new GpuParquetReaderPostProcessor(
        parquetInfo,
        constantsProvider(icebergFile),
        conf.expectedSchema,
        shadedFileReadSchema,
        conf.metrics)
      val context = new IcebergFooterContext(
        icebergFile, parquetInfo, shadedFileReadSchema, postProcessor)

      new FooterResult(
        file.getSource,
        parquetInfo.blocks.toList.asJava,
        parquetInfo.schema,
        parquetInfo.readSchema,
        parquetInfo.blocksFirstRowIndices.map(Long.box).toList.asJava,
        parquetInfo.dateRebaseMode,
        parquetInfo.timestampRebaseMode,
        parquetInfo.hasInt96Timestamps,
        context)
    }

    override def canCombine(
        left: FooterResult[IcebergFooterContext],
        right: FooterResult[IcebergFooterContext]): Boolean = {
      left.getClippedSchema == right.getClippedSchema &&
        left.getReadSchema == right.getReadSchema &&
        left.getDateRebaseMode == right.getDateRebaseMode &&
        left.getTimestampRebaseMode == right.getTimestampRebaseMode &&
        left.hasInt96Timestamps() == right.hasInt96Timestamps() &&
        left.getContext.getPostProcessor.compatibleForCombining(
          right.getContext.getPostProcessor)
    }

    override def estimateGpuBytes(
        footer: FooterResult[IcebergFooterContext],
        rowCount: Long): Long = {
      GpuBatchUtils.estimateGpuMemory(expectedSparkSchema, rowCount)
    }

    override def onFooterCompleted(
        footer: FooterResult[IcebergFooterContext],
        footerNanos: Long): Unit = {
      conf.metrics.get(FILTER_TIME).foreach(_ += footerNanos)
      conf.metrics.get(ICEBERG_STAGED_FOOTER_TIME).foreach(_ += footerNanos)
    }

    override def onSubtaskCompleted(
        subtask: ReadSubtask[IcebergFooterContext],
        stats: SubtaskStats): Unit = {
      conf.metrics.get(BUFFER_TIME).foreach(_ += stats.getIoNanos + stats.getCombineNanos)
      conf.metrics.get(ICEBERG_STAGED_IO_TIME).foreach(_ += stats.getIoNanos)
      conf.metrics.get(ICEBERG_STAGED_COMBINE_TIME).foreach(_ += stats.getCombineNanos)
      conf.metrics.get(FILECACHE_DATA_RANGE_HITS).foreach(_ += stats.getCacheHitCount)
      conf.metrics.get(FILECACHE_DATA_RANGE_HITS_SIZE).foreach(_ += stats.getCacheHitBytes)
      conf.metrics.get(FILECACHE_DATA_RANGE_MISSES).foreach(_ += stats.getCacheMissCount)
      conf.metrics.get(FILECACHE_DATA_RANGE_MISSES_SIZE).foreach(_ += stats.getCacheMissBytes)
      conf.metrics.get(FILECACHE_DATA_RANGE_READ_TIME).foreach(_ += stats.getCacheReadNanos)
      conf.metrics.get("readBufferSize").foreach(_ += subtask.getLayout.getDataSizeBytes)
      if (stats.getBackingStore == StagedParquetOutput.BackingStore.LOCAL_FILE) {
        conf.metrics.get(ICEBERG_STAGED_DISK_SUBTASK_COUNT).foreach(_ += 1L)
        conf.metrics.get(ICEBERG_STAGED_DISK_BYTES).foreach(_ += stats.getStagedBytes)
      }
    }

    override def onMaterializationCompleted(
        subtask: ReadSubtask[IcebergFooterContext],
        materializationNanos: Long): Unit = {
      conf.metrics.get(ICEBERG_STAGED_MATERIALIZATION_TIME).foreach(_ += materializationNanos)
    }

    override def onTaskWait(waitNanos: Long): Unit = {
      conf.metrics.get(ICEBERG_STAGED_WAIT_TIME).foreach(_ += waitNanos)
    }

    override def onFooterWait(waitNanos: Long): Unit = {
      onTaskWait(waitNanos)
      conf.metrics.get(ICEBERG_STAGED_FOOTER_WAIT_TIME).foreach(_ += waitNanos)
    }

    override def onResultWait(waitNanos: Long): Unit = {
      onTaskWait(waitNanos)
      conf.metrics.get(ICEBERG_STAGED_RESULT_WAIT_TIME).foreach(_ += waitNanos)
    }

    override def decodeAndPostProcess(
        subtask: ReadSubtask[IcebergFooterContext],
        parquetData: HostMemoryBuffer): JIterator[ColumnarBatch] = {
      val firstFooter = subtask.getSegments.get(0).getFooter
      val context = firstFooter.getContext
      val readSchema = firstFooter.getReadSchema
      val clippedSchema = firstFooter.getClippedSchema

      if (clippedSchema.getFieldCount == 0) {
        parquetData.close()
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val emptyInput = new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector],
          Math.toIntExact(subtask.getRowCount))
        val processed = context.getPostProcessor.process(emptyInput)
        return new CloseableJavaBatchIterator(
          new SingleGpuColumnarBatchIterator(processed))
      }

      // Make the materialized input spillable before entering the retry block. Each retry obtains
      // a fresh owning HMB reference; MakeParquetTableProducer consumes that reference, while the
      // spillable remains available to rematerialize another attempt. CachedGpuBatchIterator
      // eagerly drains the producer, so returned batches no longer depend on staged host storage.
      val stagedInput = SpillableHostBuffer(
        parquetData, parquetData.getLength, ACTIVE_BATCHING_PRIORITY)
      val decoded = withResource(stagedInput) { input =>
        val parseOptions = stagedParquetOptions(readSchema, clippedSchema)
        val splits = subtask.getSegments.asScala
          .map(_.getFooter.getSource.getPartitionedFile)
          .toArray
        RmmRapidsRetryIterator.withRetryNoSplit[Iterator[ColumnarBatch]] {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val attempt = input.getDataHostBuffer
          val producer = MakeParquetTableProducer(
            conf.useChunkedReader,
            conf.maxChunkedReaderMemoryUsageSizeBytes,
            conf.conf,
            conf.targetBatchSizeBytes,
            parseOptions,
            Array(attempt),
            conf.metrics,
            firstFooter.getDateRebaseMode,
            firstFooter.getTimestampRebaseMode,
            firstFooter.hasInt96Timestamps(),
            conf.caseSensitive,
            useFieldId = false,
            readSchema,
            clippedSchema,
            splits,
            conf.parquetDebugDumpPrefix,
            conf.parquetDebugDumpAlways)
          CachedGpuBatchIterator(producer, readSchema.fields.map(_.dataType))
        }
      }
      new PostProcessingJavaBatchIterator(decoded, context.getPostProcessor)
    }

    override def closeContext(context: IcebergFooterContext): Unit = {}

    override def close(): Unit = {}
  }

  /**
   * Build cuDF Parquet options through the shared plugin column-selection implementation.
   *
   * Package visibility lets the initialization-order regression test exercise the same lazy
   * helper used by the decode path without allocating a GPU buffer.
   */
  private[parquet] def stagedParquetOptions(
      readSchema: StructType,
      clippedSchema: MessageType) = {
    DecodeSupport.parquetOptions(readSchema, clippedSchema)
  }

  /** Reuses the plugin's Parquet column-name selection without duplicating field-ID logic. */
  private object DecodeSupport extends ParquetPartitionReaderBase {
    // These must be methods, not eager vals. ParquetPartitionReaderBase initializes
    // copyBufferSize in its trait constructor by calling conf; an override val would still have
    // its JVM default value (null) at that point in Scala 2.12.
    override def fileIO = rapidsFileIO
    override def conf = GpuStagedIcebergParquetReader.this.conf.conf
    override def execMetrics = GpuStagedIcebergParquetReader.this.conf.metrics
    override def isSchemaCaseSensitive = GpuStagedIcebergParquetReader.this.conf.caseSensitive
    override def compressCfg = CpuCompressionConfig.disabled()

    def parquetOptions(readSchema: StructType, clippedSchema: MessageType) = {
      getParquetOptions(readSchema, clippedSchema, useFieldId = false)
    }
  }

  /** Java-facing wrapper that preserves the RAPIDS iterator's close semantics. */
  private class CloseableJavaBatchIterator(
      protected val owner: Iterator[ColumnarBatch])
      extends JIterator[ColumnarBatch] with AutoCloseable {
    private var closed = false

    override def hasNext: Boolean = {
      val more = owner.hasNext
      if (!more) {
        close()
      }
      more
    }

    override def next(): ColumnarBatch = owner.next()

    override def close(): Unit = {
      if (!closed) {
        owner match {
          case resource: AutoCloseable => resource.close()
          case _ =>
        }
        closed = true
      }
    }
  }

  /** Applies one task-confined Iceberg post-processor and closes pending GPU batches on failure. */
  private class PostProcessingJavaBatchIterator(
      decoded: Iterator[ColumnarBatch],
      postProcessor: GpuParquetReaderPostProcessor)
      extends CloseableJavaBatchIterator(decoded) {

    override def next(): ColumnarBatch = {
      try {
        postProcessor.process(owner.next())
      } catch {
        case error: Throwable =>
          close()
          throw error
      }
    }
  }
}
