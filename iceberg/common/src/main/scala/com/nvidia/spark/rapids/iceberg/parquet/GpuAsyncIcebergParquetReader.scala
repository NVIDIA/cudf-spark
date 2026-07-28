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

import java.util.{Iterator => JIterator, Map => JMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{
  CachedGpuBatchIterator,
  GpuSemaphore,
  RmmRapidsRetryIterator,
  SingleGpuColumnarBatchIterator
}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuMetric.{
  BUFFER_TIME,
  FILECACHE_DATA_RANGE_HITS,
  FILECACHE_DATA_RANGE_HITS_SIZE,
  FILECACHE_DATA_RANGE_MISSES,
  FILECACHE_DATA_RANGE_MISSES_SIZE,
  FILECACHE_DATA_RANGE_READ_TIME,
  FILTER_TIME
}
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.parquet.async._
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.parquet.{
  CpuCompressionConfig,
  MakeParquetTableProducer,
  ParquetPartitionReaderBase
}
import com.nvidia.spark.rapids.reader.{Decoder, UnifiedReader}
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iceberg callbacks and GPU decode for the asynchronous Parquet partition reader.
 *
 * The Java reader owns scheduling, I/O, and output lifetime. This small callback boundary remains
 * because footer filtering and GPU decode use existing Scala APIs. File jobs are submitted in
 * partition order; combine mode admits their completed fragments in completion order, matching
 * the existing multithreaded Iceberg reader.
 */
class GpuAsyncIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf,
    workerThreads: Int) extends GpuIcebergParquetReader {

  private val multiThreadConf = conf.threadConf.asInstanceOf[MultiThread]
  private val closed = new AtomicBoolean()
  private val readerRef =
    new AtomicReference[
      UnifiedReader[IcebergPartitionedFile, FooterResult, FileFragment, ParquetDecodeInput]]()
  private lazy val asyncReader = {
    val reader = createReader()
    readerRef.set(reader)
    // A task-completion close can race lazy construction. Whichever side observes the reader owns
    // the one close; a close that happened before publication is honored here.
    if (closed.get() && readerRef.compareAndSet(reader, null)) {
      reader.close()
    }
    reader
  }

  override def hasNext: Boolean = asyncReader.hasNext

  override def next(): ColumnarBatch = asyncReader.next()

  override def close(): Unit = {
    closed.set(true)
    Option(readerRef.getAndSet(null)).foreach(_.close())
  }

  private def createReader()
  : UnifiedReader[IcebergPartitionedFile, FooterResult, FileFragment, ParquetDecodeInput] = {
    val combineThreshold = if (multiThreadConf.disableCombining) {
      0L
    } else {
      multiThreadConf.combineConf.combineThresholdSize
    }
    val combineWaitMs = if (multiThreadConf.disableCombining) {
      0L
    } else {
      multiThreadConf.combineConf.combineWaitTime.toLong
    }
    val executor = ParquetReaderThreadPool.getOrCreate(workerThreads).executor()
    val adapter = new IcebergAdapter
    val planner = new ParquetReadPlanner(
      new StableGreedyReadPlanner(
        conf.maxBatchSizeRows,
        conf.maxBatchSizeBytes,
        combineThreshold),
      new ParquetCombiner,
      executor,
      combineThreshold > 0L,
      combineWaitMs,
      closed)
    new UnifiedReader(
      files.asJava,
      new ParquetReadOps(adapter, TaskContext.get(), closed),
      planner,
      new Decoder[ParquetDecodeInput] {
        override def decode(input: ParquetDecodeInput): JIterator[ColumnarBatch] = {
          adapter.onSubtaskCompleted(input.getPlan, input.getStats)
          adapter.decodeAndPostProcess(input.getPlan, input)
        }
      },
      executor)
  }

  /** Scala operations invoked by the Iceberg-specific Java pipeline. */
  private class IcebergAdapter extends ParquetReaderAdapter {

    override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult = {
      val icebergFile = file
      val (parquetInfo, shadedFileReadSchema) =
        filterParquetBlocks(icebergFile, conf.expectedSchema)
      val postProcessor = new GpuParquetReaderPostProcessor(
        parquetInfo,
        constantsProvider(icebergFile),
        conf.expectedSchema,
        shadedFileReadSchema,
        conf.metrics)
      new FooterResult(
        file,
        parquetInfo.blocks.toList.asJava,
        parquetInfo.schema,
        parquetInfo.readSchema,
        parquetInfo.dateRebaseMode,
        parquetInfo.timestampRebaseMode,
        parquetInfo.hasInt96Timestamps,
        postProcessor)
    }

    override def openInputFile(file: IcebergPartitionedFile): RapidsInputFile = {
      rapidsFileIO.newInputFile(file.file.getDelegate.location())
    }

    override def onFooterCompleted(footerNanos: Long): Unit = {
      conf.metrics.get(FILTER_TIME).foreach(_ += footerNanos)
    }

    override def onSubtaskCompleted(
        subtask: ReadSubtask,
        stats: SubtaskStats): Unit = {
      conf.metrics.get(FILECACHE_DATA_RANGE_HITS).foreach(_ += stats.getCacheHitCount)
      conf.metrics.get(FILECACHE_DATA_RANGE_HITS_SIZE).foreach(_ += stats.getCacheHitBytes)
      conf.metrics.get(FILECACHE_DATA_RANGE_MISSES).foreach(_ += stats.getCacheMissCount)
      conf.metrics.get(FILECACHE_DATA_RANGE_MISSES_SIZE).foreach(_ += stats.getCacheMissBytes)
      conf.metrics.get(FILECACHE_DATA_RANGE_READ_TIME).foreach(_ += stats.getCacheReadNanos)
      conf.metrics.get("readBufferSize").foreach(_ += subtask.getDataSizeBytes)
    }

    override def onResultWait(waitNanos: Long): Unit = {
      // Match the standard multithreaded reader's critical-path semantics. Per-file worker I/O
      // spans overlap; BUFFER_TIME records how long the Spark task was actually blocked waiting
      // for a file result.
      conf.metrics.get(BUFFER_TIME).foreach(_ += waitNanos)
    }

    override def decodeAndPostProcess(
        subtask: ReadSubtask,
        parquetInput: ParquetDecodeInput): JIterator[ColumnarBatch] = {
      val firstFooter = subtask.getFileSlices.get(0).getFooter
      val postProcessor = firstFooter.getPostProcessor
      val readSchema = firstFooter.getReadSchema
      val clippedSchema = firstFooter.getClippedSchema

      if (clippedSchema.getFieldCount == 0) {
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val emptyInput = new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector],
          Math.toIntExact(subtask.getRowCount))
        val processed = postProcessor.process(emptyInput)
        return new CloseableJavaBatchIterator(
          new SingleGpuColumnarBatchIterator(processed))
      }

      val parseOptions = asyncParquetOptions(readSchema, clippedSchema)
      val splits = subtask.getFileSlices.asScala
        .map(_.getFooter.getFile.sparkPartitionedFile)
        .toArray
      val decoded = RmmRapidsRetryIterator.withRetryNoSplit[Iterator[ColumnarBatch]] {
        val attempt = parquetInput.materialize()
        // A retry receives fresh owning header/footer buffers and fresh owning references to the
        // fragment slices. MakeParquetTableProducer consumes them once invoked; closeOnExcept
        // covers failures before that ownership transfer. CachedGpuBatchIterator eagerly drains
        // the producer, so returned batches no longer depend on asynchronous host storage.
        closeOnExcept(attempt) { hostBuffers =>
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val producer = MakeParquetTableProducer(
            conf.useChunkedReader,
            conf.maxChunkedReaderMemoryUsageSizeBytes,
            conf.conf,
            conf.targetBatchSizeBytes,
            parseOptions,
            hostBuffers,
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
      new PostProcessingJavaBatchIterator(decoded, postProcessor)
    }
  }

  /**
   * Build cuDF Parquet options through the shared plugin column-selection implementation.
   *
   * Package visibility lets the initialization-order regression test exercise the same lazy
   * helper used by the decode path without allocating a GPU buffer.
   */
  private[parquet] def asyncParquetOptions(
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
    override def conf = GpuAsyncIcebergParquetReader.this.conf.conf
    override def execMetrics = GpuAsyncIcebergParquetReader.this.conf.metrics
    override def isSchemaCaseSensitive = GpuAsyncIcebergParquetReader.this.conf.caseSensitive
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
