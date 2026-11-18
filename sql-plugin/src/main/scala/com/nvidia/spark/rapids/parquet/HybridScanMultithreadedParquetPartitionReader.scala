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

package com.nvidia.spark.rapids.parquet

import java.io.FileNotFoundException
import java.net.URI

import ai.rapids.cudf.{HostMemoryBuffer, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.{BUFFER_TIME, FILTER_TIME, HYBRID_SCAN_FOOTER_PARSE_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.io.async.{AsyncMetrics, AsyncRunner, DecayReleaseResult, MemoryBoundedAsyncRunner}
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Reads the files of a `MULTITHREADED` scan with the libcudf hybrid scan reader. For each file a
 * pool thread parses the footer, prunes the split's row groups on statistics with the pushed
 * filter and fetches the surviving column chunks into host memory. The task thread then prunes
 * those with the filter's dictionaries, which needs the GPU a pool thread may not touch, and
 * decodes what survives into one batch per file.
 *
 * A standalone reader rather than a branch of the parquet-mr cloud reader, because Delta and
 * Iceberg customize that reader's decode and a hybrid branch inside it would bypass them.
 */
private[parquet] class HybridScanMultithreadedParquetPartitionReader(
    fileIO: RapidsFileIO,
    conf: Configuration,
    files: Array[PartitionedFile],
    context: HybridScanContext,
    readFooter: (PartitionedFile, Configuration) => HostMemoryBuffer,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    ignoreMissingFiles: Boolean,
    keepReadsInOrder: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, poolConf, maxNumFileProcessed,
    /* filters */ null, context.execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    keepReadsInOrder = keepReadsInOrder)
  with MultiFileReaderFunctions {

  private val parquetOptions =
    HybridScanPipeline.parquetOptions(context.readDataSchema.fieldNames)
  private val pipeline = new HybridScanPipeline(context)
  private val filterTime = context.execMetrics.getOrElse(FILTER_TIME, NoopMetric)
  private val bufferTime = context.execMetrics.getOrElse(BUFFER_TIME, NoopMetric)

  /** A pool thread's result, with its partial metric counts for the task thread to add. */
  private trait HybridResult extends HostMemoryBuffersWithMetaDataBase {
    var runnerCounts: Map[String, Long] = Map.empty

    override def addRunnerMetrics(metrics: Map[String, GpuMetric]): Unit = {
      super.addRunnerMetrics(metrics)
      runnerCounts.foreach { case (name, count) => metrics.get(name).foreach(_ += count) }
    }
  }

  /**
   * A file's fetched column chunks and its footer, handed from the pool thread to the task thread,
   * which re-parses the footer since the pool's reader cannot cross threads. Both are spillable so
   * the pool's memory budget covers them. The chunks are one buffer in `memBuffersAndSizes`, each
   * at its offset and size. `numRows` in `memBuffersAndSizes` is the surviving row groups' row
   * count, so the framework does not mistake it for an empty result.
   */
  private case class HybridFileResult(
      override val partitionedFile: PartitionedFile,
      footer: SpillableHostBuffer,
      rowGroups: Array[Int],
      chunkOffsets: Array[Long],
      chunkSizes: Array[Long],
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long)
    extends HybridResult {
    /** The single spillable buffer holding every column chunk. */
    def chunkBuffer: SpillableHostBuffer = memBuffersAndSizes.head.hmbs.head

    // SpillableHostBuffer.close is idempotent, so this stays idempotent like the base close.
    override def close(): Unit = {
      footer.close()
      super.close()
    }
  }

  /** A file with nothing to decode, which the framework skips since it reports no rows. */
  private case class EmptyHybridFileResult(
      override val partitionedFile: PartitionedFile,
      override val bytesRead: Long)
    extends HybridResult {
    override val memBuffersAndSizes: Array[SingleHMBAndMeta] = Array(SingleHMBAndMeta.empty())
  }

  override final def getFileFormatShortName: String = "Parquet"

  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(file, conf, tc)
  }

  override def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
      Iterator[ColumnarBatch] = fileBufsAndMeta match {
    case result: HybridFileResult =>
      // The whole file decodes eagerly into one batch, so its host buffers are closed here.
      currentFileHostBuffers = None
      val table = withResource(result)(decodeResult)
      val colTypes = context.readDataSchema.fields.map(_.dataType)
      val batchIter: Iterator[ColumnarBatch] = table match {
        case Some(t) => Iterator.single(withResource(t)(GpuColumnVector.from(_, colTypes)))
        case None => Iterator.empty
      }
      batchIter.flatMap { batch =>
        BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
          result.partitionedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
      }
    case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
  }

  private def hybridSplit(
      file: PartitionedFile,
      metrics: Map[String, GpuMetric]): HybridScanSplit = {
    val filePath = new Path(new URI(file.filePath.toString()))
    val rangeReader = HybridScanFileByteSource.createRangeReader(filePath, metrics)
    HybridScanSplit(new HybridScanFileByteSource(fileIO, filePath, metrics, rangeReader),
      file.start, file.length)
  }

  private class ReadBatchRunner(
      file: PartitionedFile,
      conf: Configuration,
      taskContext: TaskContext) extends MemoryBoundedAsyncRunner[BufferInfo] with Logging {

    override def sparkTaskContext: Option[TaskContext] = Some(taskContext)

    // The fetched size is not known before planning, so budget the split's raw size.
    override val requiredMemoryBytes: Long = file.length

    // The result holds its buffers until the task thread decodes them, so the budget is released
    // by callback rather than as soon as the result is handed over.
    override protected def buildResult(resultData: BufferInfo,
        metrics: AsyncMetrics): RunnerResult = {
      val releaseCallback: () => Unit = () => {
        if (this.isHoldingResource) {
          this.withStateLock { _ =>
            this.releaseResourceCallback()
          }
        }
      }
      new DecayReleaseResult[BufferInfo](resultData, metrics, releaseCallback)
    }

    override def callImpl(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.filePath}", e)
          EmptyHybridFileResult(file, 0)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val filterStartTime = System.nanoTime()
      val footer = readFooter(file, conf)
      val footerTime = System.nanoTime() - filterStartTime
      val bufferStartTime = System.nanoTime()
      // Counted locally and added on the task thread, since the pool threads reading the task's
      // other files would race on the scan's metrics.
      val runnerMetrics: Map[String, GpuMetric] =
        (context.execMetrics.keySet + FILTER_TIME).map(_ -> new LocalGpuMetric).toMap
      // The footer parse and row group selection are filter work although they run inside the
      // fetch, so the base reader splits the task's wait the way it does for parquet-mr.
      val selectTime = runnerMetrics(FILTER_TIME)
      val result: HybridResult = withResource(footer) { _ =>
        // A closed reader discards this result, so neither fetch for it nor hand it buffers.
        val planned = if (isDone) None else planAndFetchForPool(footer, runnerMetrics)
        val bytesRead = fileSystemBytesRead() - startingBytesRead
        planned match {
          case None => EmptyHybridFileResult(file, bytesRead)
          case Some((fetched, numRows)) =>
            withResource(fetched) { _ =>
              if (isDone) {
                EmptyHybridFileResult(file, bytesRead)
              } else {
                toSpillableResult(footer, fetched, numRows, bytesRead)
              }
            }
        }
      }
      result.setExecutionTime(footerTime + selectTime.value,
        System.nanoTime() - bufferStartTime - selectTime.value)
      // The base reader times filter and buffer from the task's wait instead.
      result.runnerCounts = runnerMetrics.collect {
        case (name, metric) if name != FILTER_TIME && name != BUFFER_TIME && metric.value != 0 =>
          name -> metric.value
      }
      result
    }

    /**
     * Prune the split's row groups on statistics with the pushed filter and fetch the surviving
     * column chunks. Returns them with the surviving row count, taken while the reader is open,
     * or None when nothing survived. The pushed filter and the dictionary compare may still drop
     * rows at decode, so the count is an upper bound. Everything is counted into `metrics`, the
     * footer parse and row group selection both as `FILTER_TIME`.
     */
    private def planAndFetchForPool(
        footer: HostMemoryBuffer,
        metrics: Map[String, GpuMetric]): Option[(FetchedColumnChunks, Long)] = {
      val filePipeline = new HybridScanPipeline(context.copy(execMetrics = metrics))
      val parsed = metrics(FILTER_TIME).ns {
        HybridScanPipeline.openReader(footer, parquetOptions,
          metrics.getOrElse(HYBRID_SCAN_FOOTER_PARSE_TIME, NoopMetric))
      }
      withResource(parsed) { reader =>
        withResource(context.scanFilters.ast.map(HybridScanPipeline.compileFilter)) { ast =>
          val split = hybridSplit(file, metrics)
          withResource(split.source) { _ =>
            filePipeline.planAndFetch(split, reader, ast, allowDictionaryPruning = false)
              .map { fetched =>
                closeOnExcept(fetched)(f => (f, reader.totalRowsInRowGroups(f.rowGroups)))
              }
          }
        }
      }
    }

    /**
     * Wrap the buffer the fetched chunks slice and the footer as spillables for the task thread.
     * The buffer rather than each chunk is the spillable, as a spilled slice frees nothing while
     * its siblings hold the buffer. Each spillable owns an incremented reference; the caller
     * closes the originals.
     */
    private def toSpillableResult(
        footer: HostMemoryBuffer,
        fetched: FetchedColumnChunks,
        numRows: Long,
        bytesRead: Long): HybridFileResult = {
      val buffer = fetched.backing.getOrElse(
        throw new IllegalStateException("the fetched chunks do not slice one buffer"))
      val chunkOffsets = fetched.hostChunks.map(_.getAddress - buffer.getAddress)
      val chunkSizes = fetched.hostChunks.map(_.getLength)
      footer.incRefCount()
      closeOnExcept(SpillableHostBuffer(footer, footer.getLength,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)) { spillFooter =>
        buffer.incRefCount()
        closeOnExcept(SpillableHostBuffer(buffer, buffer.getLength,
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)) { spillBuffer =>
          val bufferAndMeta =
            SingleHMBAndMeta(Array(spillBuffer), buffer.getLength, numRows, Seq.empty)
          HybridFileResult(file, spillFooter, fetched.rowGroups, chunkOffsets, chunkSizes,
            Array(bufferAndMeta), bytesRead)
        }
      }
    }
  }

  /** Decode a pool thread's result on the task thread. The result stays the caller's to close. */
  private def decodeResult(result: HybridFileResult): Option[Table] = {
    val spillBuffer = result.chunkBuffer
    // While waiting for the task, the footer and chunks may have been moved to disk to free host
    // memory. Reading them back allocates host memory again, which can fail while it is still
    // short, so retry: a failed attempt frees what it read back and waits for memory. Timed as
    // buffer time, which the base reader's wait did not cover.
    val (footer, buffer) = bufferTime.ns {
      withRetryNoSplit {
        closeOnExcept(result.footer.getDataHostBuffer()) { f =>
          (f, spillBuffer.getDataHostBuffer())
        }
      }
    }
    withResource(footer) { _ =>
      val fetched = withResource(buffer) { _ =>
        val hostChunks = result.chunkOffsets.indices.toArray.safeMap { i =>
          buffer.slice(result.chunkOffsets(i), result.chunkSizes(i))
        }
        closeOnExcept(hostChunks) { _ =>
          new FetchedColumnChunks(result.rowGroups, hostChunks)
        }
      }
      decodeFetched(footer, fetched)
    }
  }

  /**
   * The task phase, the counterpart to a pool thread's `HybridScanPipeline.planAndFetch`: parse
   * the footer into a fresh reader, since the plan phase's reader is on another thread, prune the
   * fetched chunks with the dictionaries the pool could not compare, and decode what survives.
   * Takes `fetched`; the footer stays the caller's to close.
   */
  private def decodeFetched(
      footer: HostMemoryBuffer,
      fetched: FetchedColumnChunks): Option[Table] = {
    val ast = closeOnExcept(fetched) { _ =>
      context.scanFilters.ast.map(HybridScanPipeline.compileFilter)
    }
    withResource(ast) { _ =>
      // A second parse of this footer, added to the pool thread's parse time.
      val parsed = closeOnExcept(fetched)(_ => HybridScanPipeline.openReader(footer,
        parquetOptions,
        context.execMetrics.getOrElse(HYBRID_SCAN_FOOTER_PARSE_TIME, NoopMetric)))
      withResource(parsed) { reader =>
        // After the wait the base reader times, so this is filter time it never counted.
        val pruned = filterTime.ns {
          pipeline.pruneFetchedWithDictionaries(reader, ast, fetched)
        }
        val table = pruned.flatMap(pipeline.materializeFetched(reader, ast, _))
        if (table.isEmpty) {
          // Nothing goes downstream, so don't hold the GPU while the next file is fetched.
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        }
        table
      }
    }
  }
}
