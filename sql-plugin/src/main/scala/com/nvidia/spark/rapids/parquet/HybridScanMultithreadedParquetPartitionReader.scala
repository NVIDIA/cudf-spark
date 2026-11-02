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

import ai.rapids.cudf.{ByteRange, HostMemoryBuffer, HybridScanReader, ParquetOptions, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
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
    config: HybridScanConfig,
    readDataSchema: StructType,
    parquetOptions: ParquetOptions,
    readFooter: (PartitionedFile, Configuration) => HostMemoryBuffer,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    ignoreMissingFiles: Boolean,
    keepReadsInOrder: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, poolConf, maxNumFileProcessed, null,
    execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes, keepReadsInOrder = keepReadsInOrder)
  with MultiFileReaderFunctions with HybridScanBase {

  /**
   * A file's fetched column chunks and its footer, handed from the pool thread to the task thread,
   * which re-parses the footer since the pool's reader cannot cross threads. Both are spillable so
   * the pool's memory budget covers them. `numRows` in `memBuffersAndSizes` is the surviving row
   * groups' row count, so the framework does not mistake it for an empty result.
   */
  private case class HybridFileResult(
      override val partitionedFile: PartitionedFile,
      footer: SpillableHostBuffer,
      rowGroups: Array[Int],
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long)
    extends HostMemoryBuffersWithMetaDataBase {
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
    extends HostMemoryBuffersWithMetaDataBase {
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
      val colTypes = readDataSchema.fields.map(_.dataType)
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

  private def hybridFile(file: PartitionedFile): HybridScanFile = {
    val filePath = new Path(new URI(file.filePath.toString()))
    val source = new HybridScanFileByteSource(fileIO, filePath, execMetrics)
    HybridScanFile(source, file.start, file.length, parquetOptions, readDataSchema,
      config.filters, config.filterKeepThreshold, execMetrics)
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
      val filterTime = System.nanoTime() - filterStartTime
      val bufferStartTime = System.nanoTime()
      val result = withResource(footer) { _ =>
        val planned = planAndFetchForPool(footer)
        val bytesRead = fileSystemBytesRead() - startingBytesRead
        planned match {
          case None => EmptyHybridFileResult(file, bytesRead)
          case Some((fetched, numRows)) =>
            withResource(fetched)(toSpillableResult(footer, _, numRows, bytesRead))
        }
      }
      result.setExecutionTime(filterTime, System.nanoTime() - bufferStartTime)
      result
    }

    /**
     * Prune the split's row groups on statistics with the pushed filter and fetch the surviving
     * column chunks. Returns them with the surviving row count, taken while the reader is open,
     * or None when nothing survived. The pushed filter and the dictionary compare may still drop
     * rows at decode, so the count is an upper bound.
     */
    private def planAndFetchForPool(
        footer: HostMemoryBuffer): Option[(FetchedColumnChunks, Long)] = {
      withResource(HybridScanBase.openReader(footer, parquetOptions)) { reader =>
        withResource(config.filters.ast.map(_.compile())) { ast =>
          planAndFetch(hybridFile(file), reader, ast, allowDictionaryPruning = false).map {
            fetched =>
              closeOnExcept(fetched)(f => (f, reader.totalRowsInRowGroups(f.rowGroups)))
          }
        }
      }
    }

    /**
     * Wrap the fetched chunks and the footer as spillables for the task thread. Each spillable
     * owns an incremented reference; the caller closes the originals.
     */
    private def toSpillableResult(
        footer: HostMemoryBuffer,
        fetched: FetchedColumnChunks,
        numRows: Long,
        bytesRead: Long): HybridFileResult = {
      footer.incRefCount()
      closeOnExcept(SpillableHostBuffer(footer, footer.getLength,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)) { spillFooter =>
        val spillChunks = fetched.hostChunks.safeMap { hmb =>
          hmb.incRefCount()
          SpillableHostBuffer(hmb, hmb.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }
        closeOnExcept(spillChunks) { _ =>
          val meta = SingleHMBAndMeta(spillChunks, spillChunks.map(_.length).sum, numRows,
            Seq.empty)
          HybridFileResult(file, spillFooter, fetched.rowGroups, Array(meta), bytesRead)
        }
      }
    }
  }

  /** Decode a pool thread's result on the task thread. The result stays the caller's to close. */
  private def decodeResult(result: HybridFileResult): Option[Table] = {
    val chunks = result.memBuffersAndSizes.head.hmbs
    withResource(result.footer.getDataHostBuffer()) { footer =>
      decodeFetched(hybridFile(result.partitionedFile), footer,
        new FetchedColumnChunks(result.rowGroups, chunks.safeMap(_.getDataHostBuffer())))
    }
  }

  /**
   * The task phase, the counterpart to a pool thread's [[planAndFetch]]: parse the footer into a
   * fresh reader, since the plan phase's reader is on another thread, prune the fetched chunks
   * with the dictionaries the pool could not compare, and decode what survives. Takes `fetched`;
   * the footer stays the caller's to close.
   */
  private def decodeFetched(
      file: HybridScanFile,
      footer: HostMemoryBuffer,
      fetched: FetchedColumnChunks): Option[Table] = {
    val ast = closeOnExcept(fetched)(_ => file.scanFilters.ast.map(_.compile()))
    withResource(ast) { _ =>
      val parsed = closeOnExcept(fetched)(_ => HybridScanBase.openReader(footer,
        file.parquetOptions))
      withResource(parsed) { reader =>
        pruneFetchedWithDictionaries(file, reader, ast, fetched)
          .flatMap(materializeFetched(file, reader, ast, _))
      }
    }
  }

  /**
   * Prune already-fetched chunks to the row groups the filter's dictionaries cannot rule out,
   * slicing the dictionary pages out of the chunks rather than reading them again. The pruned row
   * groups' IO is already spent, but their device copy and decode are saved. Takes `fetched` and
   * returns what to decode: `fetched` itself when nothing is pruned, the surviving chunks when
   * some are, or None when no row group survives.
   */
  private def pruneFetchedWithDictionaries(
      file: HybridScanFile,
      reader: HybridScanReader,
      ast: Option[CompiledExpression],
      fetched: FetchedColumnChunks): Option[FetchedColumnChunks] = {
    if (ast.isEmpty ||
        !file.scanFilters.pushed.exists(FilterToAstConverter.canPruneWithDictionary)) {
      Some(fetched)
    } else {
      val survivors = closeOnExcept(fetched) { f =>
        ast.foreach(reader.setFilter)
        NvtxRegistry.HYBRID_SCAN_FILTER_DICT {
          filterRowGroupsWithDictionaries(reader, f.rowGroups,
            sliceDictPagesFromChunks(reader, f.rowGroups, f.hostChunks, _))
        }
      }
      if (survivors.length == fetched.rowGroups.length) {
        Some(fetched)
      } else {
        withResource(fetched) { f =>
          if (survivors.isEmpty) None else Some(selectSurvivingChunks(f, survivors))
        }
      }
    }
  }

  /**
   * The dictionary pages for `dictRanges`, sliced from the fetched chunks. A dictionary page is
   * the first page of its column chunk, so a non-empty range starts where exactly one chunk does
   * and is that chunk's leading bytes. An empty range (a chunk the reader will not prune with)
   * keeps its slot as an empty buffer, since the reader matches buffers to ranges by position.
   * The slices are the caller's to close.
   */
  private def sliceDictPagesFromChunks(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      chunks: Array[HostMemoryBuffer],
      dictRanges: Array[ByteRange]): Array[HostMemoryBuffer] = {
    val chunkRanges = reader.allColumnChunksByteRanges(rowGroups)
    require(chunkRanges.length == chunks.length,
      "hybrid scan column chunk count does not match the fetched buffers")
    val chunkByOffset = chunkRanges.indices.map(i => chunkRanges(i).offset() -> i).toMap
    dictRanges.safeMap { r =>
      if (r.size() == 0) {
        chunks.head.slice(0, 0)
      } else {
        chunks(chunkByOffset(r.offset())).slice(0, r.size())
      }
    }
  }

  /**
   * The chunks of the surviving row groups, in row group order, as new slices that own their own
   * references, so `fetched` can be closed independently. Every selected column has one chunk in
   * every row group, so each row group's run of chunks is the same length.
   */
  private def selectSurvivingChunks(
      fetched: FetchedColumnChunks,
      survivors: Array[Int]): FetchedColumnChunks = {
    val numRowGroups = fetched.rowGroups.length
    val chunksPerRowGroup = fetched.hostChunks.length / numRowGroups
    require(chunksPerRowGroup * numRowGroups == fetched.hostChunks.length,
      "hybrid scan column chunks do not divide evenly into row groups")
    val survivorSet = survivors.toSet
    val kept = fetched.rowGroups.indices.filter(i => survivorSet.contains(fetched.rowGroups(i)))
    val keptChunks = kept.flatMap { i =>
      fetched.hostChunks.slice(i * chunksPerRowGroup, (i + 1) * chunksPerRowGroup)
    }.toArray
    new FetchedColumnChunks(kept.map(fetched.rowGroups(_)).toArray,
      keptChunks.safeMap(c => c.slice(0, c.getLength)))
  }
}
