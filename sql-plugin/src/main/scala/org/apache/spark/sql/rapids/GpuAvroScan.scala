/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.{FileNotFoundException, IOException, OutputStream}
import java.net.URI

import scala.annotation.tailrec
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.language.implicitConversions

import ai.rapids.cudf.{AvroOptions => CudfAvroOptions,HostMemoryBuffer, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.{BUFFER_TIME, FILTER_TIME, GPU_DECODE_TIME, NUM_OUTPUT_BATCHES, READ_FS_TIME, SCAN_TIME, WRITE_BUFFER_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.io.async.{AsyncRunner, UnboundedAsyncRunner}
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shims.ShimFilePartitionReaderFactory
import org.apache.avro.Schema
import org.apache.avro.file.DataFileConstants.SYNC_SIZE
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AvroOptions, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.AvroUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuAvroScan {

  def tagSupport(scanMeta: ScanMeta[AvroScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {

    if (!meta.conf.isAvroEnabled) {
      meta.willNotWorkOnGpu("Avro input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO} to true")
    }

    if (!meta.conf.isAvroReadEnabled) {
      meta.willNotWorkOnGpu("Avro input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO_READ} to true")
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new AvroOptions(options, hadoopConf)
    AvroUtils.tagSupport(parsedOptions, meta)

    FileFormatChecks.tag(meta, readSchema, AvroFormatType, ReadFileOp)
  }

}

case class GpuAvroScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    rapidsConf: RapidsConf,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    queryUsesInputFile: Boolean = false) extends FileScan with GpuScan {
  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val parsedOptions = new AvroOptions(caseSensitiveMap, hadoopConf)
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    if (rapidsConf.isAvroPerFileReadEnabled) {
      GpuAvroPartitionReaderFactory(sparkSession.sessionState.conf, rapidsConf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, parsedOptions, metrics,
        options.asScala.toMap)
    } else {
      val poolConfBuilder = ThreadPoolConfBuilder(rapidsConf)
      GpuAvroMultiFilePartitionReaderFactory(sparkSession.sessionState.conf,
        rapidsConf, broadcastedConf, dataSchema, readDataSchema, readPartitionSchema,
        parsedOptions, metrics, pushedFilters, poolConfBuilder, queryUsesInputFile)
    }
  }

  // overrides nothing in 330
  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def equals(obj: Any): Boolean = obj match {
    case a: GpuAvroScan =>
      super.equals(a) && dataSchema == a.dataSchema && options == a.options &&
          equivalentFilters(pushedFilters, a.pushedFilters) &&
          queryUsesInputFile == a.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def withInputFile(): GpuScan = copy(queryUsesInputFile = true)
}

/** Avro partition reader factory to build columnar reader */
case class GpuAvroPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    @transient rapidsConf: RapidsConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    avroOptions: AvroOptions,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) with Logging {

  private val debugDumpPrefix = rapidsConf.avroDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.avroDebugDumpAlways
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val startTime = System.nanoTime()
    val blockMeta = AvroFileFilterHandler(conf, avroOptions).filterBlocks(partFile)
    metrics.get(FILTER_TIME).foreach {
      _ += (System.nanoTime() - startTime)
    }
    val reader = new PartitionReaderWithBytesRead(new GpuAvroPartitionReader(conf, partFile,
      blockMeta, readDataSchema, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }
}

/**
 * The multi-file partition reader factory for cloud or coalescing reading of avro file format.
 */
case class GpuAvroMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    @transient rapidsConf: RapidsConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: AvroOptions,
    metrics: Map[String, GpuMetric],
    filters: Array[Filter],
    poolConfBuilder: ThreadPoolConfBuilder,
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val debugDumpPrefix = rapidsConf.avroDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.avroDebugDumpAlways
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  private val maxNumFileProcessed = rapidsConf.maxNumAvroFilesParallel

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isAvroCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

  override val canUseMultiThreadReader: Boolean = rapidsConf.isAvroMultiThreadReadEnabled

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   */
  override final def getFileFormatShortName: String = "AVRO"

  /**
   * Build the PartitionReader for cloud reading
   */
  @scala.annotation.nowarn(
    "msg=value ignoreExtension in class AvroOptions is deprecated*"
  )
  override def buildBaseColumnarReaderForCloud(
      partFiles: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val files = if (options.ignoreExtension) {
      partFiles
    } else {
      partFiles.filter(_.filePath.toString().endsWith(".avro"))
    }
    val poolConf = poolConfBuilder.build()
    val reader = new GpuMultiFileCloudAvroPartitionReader(
      conf, files, poolConf, maxNumFileProcessed,
      filters, metrics, ignoreCorruptFiles, ignoreMissingFiles, debugDumpPrefix, debugDumpAlways,
      readDataSchema, partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes)
    // NOTE: Initialize must happen after the initialization of the reader, to ensure everything
    // inside the reader being fully initialized.
    if (conf.getBoolean("rapids.sql.scan.prefetch", false)) {
      reader.eagerPrefetchInit()
    }
    reader
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return a PartitionReader of coalescing reading
   */
  override def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val clippedBlocks = ArrayBuffer[AvroSingleDataBlockInfo]()
    val mapPathHeader = LinkedHashMap[Path, Header]()
    val filterHandler = AvroFileFilterHandler(conf, options)

    metrics.getOrElse(FILTER_TIME, NoopMetric).ns {
      metrics.getOrElse(SCAN_TIME, NoopMetric).ns {
        files.foreach { file =>
          val singleFileInfo = try {
            filterHandler.filterBlocks(file)
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: ${file.filePath}", e)
              AvroBlockMeta(null, 0L, Seq.empty)
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
              logWarning(
                s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
              AvroBlockMeta(null, 0L, Seq.empty)
          }
          val fPath = new Path(new URI(file.filePath.toString()))
          clippedBlocks ++= singleFileInfo.blocks.map(block =>
            AvroSingleDataBlockInfo(
              fPath,
              AvroDataBlock(block),
              file.partitionValues,
              AvroSchemaWrapper(SchemaConverters.toAvroType(readDataSchema)),
              readDataSchema,
              AvroExtraInfo()))
          if (singleFileInfo.blocks.nonEmpty) {
            // No need to check the header since it can not be null when blocks is not empty here.
            mapPathHeader.put(fPath, singleFileInfo.header)
          }
        }
      }
    }
    val poolConf = poolConfBuilder.build()
    new GpuMultiFileAvroPartitionReader(conf, files, clippedBlocks.toSeq, readDataSchema,
      partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
      poolConf, debugDumpPrefix, debugDumpAlways, metrics, mapPathHeader.toMap)
  }

}

/** A trait collecting common methods across the 3 kinds of avro readers */
trait GpuAvroReaderBase extends Logging { self: FilePartitionReaderBase =>
  def debugDumpPrefix: Option[String]

  def debugDumpAlways: Boolean

  def readDataSchema: StructType

  def conf: Configuration

  // Same default buffer size with Parquet readers.
  val cacheBufferSize = conf.getInt("avro.read.allocation.size", 8 * 1024 * 1024)

  /**
   * Read the host data to GPU for decoding, and return it as a cuDF Table.
   * The input host buffer should contain valid data, otherwise the behavior is
   * undefined.
   * 'splits' is used only for debugging.
   */
  protected final def sendToGpuUnchecked(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      splits: Array[PartitionedFile]): Table = {
    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, hostBuf, 0, bufSize, prefix, ".avro")
        logWarning(s"Wrote data for ${splits.mkString("; ")} to $p")
      }
    }
    val readOpts = CudfAvroOptions.builder()
      .includeColumn(readDataSchema.fieldNames.toSeq: _*)
      .build()
    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val table = try {
      RmmRapidsRetryIterator.withRetryNoSplit[Table] {
        NvtxIdWithMetrics(NvtxRegistry.AVRO_DECODE, metrics(GPU_DECODE_TIME)) {
          Table.readAvro(readOpts, hostBuf, 0, bufSize)
        }
      }
    } catch {
      case e: Exception =>
        val dumpMsg = debugDumpPrefix.map { prefix =>
          if (!debugDumpAlways) {
            val p = DumpUtils.dumpBuffer(conf, hostBuf, 0, bufSize, prefix, ".avro")
            s", data dumped to $p"
          } else {
            ""
          }
        }.getOrElse("")
        throw new IOException(
          s"Error when processing file splits [${splits.mkString("; ")}]$dumpMsg", e)
    }
    table
  }

  /**
   * Send a host buffer to GPU for decoding, and return it as a ColumnarBatch.
   * The input hostBuf will be closed after returning, please do not use it anymore.
   * 'splits' is used only for debugging.
   */
  protected final def sendToGpu(
      hostBuf: SpillableHostBuffer,
      bufSize: Long,
      splits: Array[PartitionedFile]): Option[ColumnarBatch] = {
    if (bufSize == 0) {
      hostBuf.close()
      None
    } else {
      val dataBuf = withRetryNoSplit(hostBuf)(_.getDataHostBuffer())
      val t = withResource(dataBuf)(sendToGpuUnchecked(_, bufSize, splits))
      withResource(t) { _ =>
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(t)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        metrics(NUM_OUTPUT_BATCHES) += 1
        // convert to batch
        Some(GpuColumnVector.from(t, GpuColumnVector.extractTypes(readDataSchema)))
      }
    } // end of else
  }

  /**
   * Get the block chunk according to the max batch size and max rows.
   *
   * @param blockIter blocks to be evaluated
   * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader
   *                             reads per batch
   * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader
   *                              reads per batch
   * @return
   */
  protected final def populateCurrentBlockChunk(
      blockIter: BufferedIterator[BlockInfo],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long): Seq[BlockInfo] = {

    val currentChunk = new ArrayBuffer[BlockInfo]
    var numRows, numBytes, numAvroBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.count > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.count <= maxReadBatchSizeRows) {
          val estBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema, peekedRowGroup.count)
          if (numBytes == 0 || numBytes + estBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.count
            numAvroBytes += currentChunk.last.dataSize
            numBytes += estBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()
    logDebug(s"Loaded $numRows rows from Avro. bytes read: $numAvroBytes. " +
      s"Estimated GPU bytes: $numBytes")
    currentChunk.toSeq
  }

  /** Read a split into a host buffer, preparing for sending to GPU */
  protected final def readPartFile(
      partFilePath: Path,
      blocks: Seq[BlockInfo],
      headerSize: Long,
      conf: Configuration): (SpillableHostBuffer, Long) = {
    NvtxRegistry.AVRO_BUFFER_FILE_SPLIT {
      if (blocks.isEmpty) {
        // No need to check the header here since it can not be null when blocks is not empty.
        return (null, 0L)
      }
      val estOutSize = estimateOutputSize(blocks, headerSize)
      withResource(partFilePath.getFileSystem(conf).open(partFilePath)) { in =>
        closeOnExcept(HostMemoryBuffer.allocate(estOutSize)) { hmb =>
          withResource(new HostMemoryOutputStream(hmb)) { out =>
            val headerAndBlocks = BlockInfo(0, headerSize, 0, 0) +: blocks
            copyBlocksData(headerAndBlocks, in, out)
            // check we didn't go over memory
            if (out.getPos > estOutSize) {
              throw new QueryExecutionException(s"Calculated buffer size $estOutSize is" +
                s" too small, actual written: ${out.getPos}")
            }
            (SpillableHostBuffer(hmb, out.getPos, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
              out.getPos)
          }
        }
      }
    }
  }

  /** Estimate the total size from the given blocks and header */
  protected final def estimateOutputSize(blocks: Seq[BlockInfo], headerSize: Long): Long = {
    // Start from the Header
    var totalSize: Long = headerSize
    // Add all blocks
    totalSize += blocks.map(_.blockSize).sum
    totalSize
  }

  /** Copy the data specified by the blocks from `in` to `out` */
  protected final def copyBlocksData(
      blocks: Seq[BlockInfo],
      in: FSDataInputStream,
      out: OutputStream,
      sync: Option[Array[Byte]] = None): Seq[BlockInfo] = {
    val copyRanges = sync.map { s =>
      assert(s.size >= SYNC_SIZE)
      // Copy every block without the tailing sync marker if a sync is given. This
      // is for coalescing reader who requires to append this given sync marker
      // to each block. Then we can not merge sequential blocks.
      blocks.map(b => CopyRange(b.blockStart, b.blockSize - SYNC_SIZE))
    }.getOrElse(computeCopyRanges(blocks))

    val copySyncFunc: OutputStream => Unit = if (sync.isEmpty) {
      out => // do nothing
    } else {
      out => out.write(sync.get, 0, SYNC_SIZE)
    }
    // copy cache, default to 8MB
    val copyCache = new Array[Byte](cacheBufferSize)
    var readTime, writeTime = 0L

    copyRanges.foreach { range =>
      if (in.getPos != range.offset) {
        in.seek(range.offset)
      }
      var bytesLeft = range.length
      while (bytesLeft > 0) {
        // downcast is safe because copyBuffer.length is an int
        val readLength = Math.min(bytesLeft, copyCache.length).toInt
        val start = System.nanoTime()
        in.readFully(copyCache, 0, readLength)
        val mid = System.nanoTime()
        out.write(copyCache, 0, readLength)
        val end = System.nanoTime()
        readTime += (mid - start)
        writeTime += (end - mid)
        bytesLeft -= readLength
      }
      // append the sync marker to the block data for the coalescing reader.
      copySyncFunc(out)
    }
    metrics.get(READ_FS_TIME).foreach(_.add(readTime))
    metrics.get(WRITE_BUFFER_TIME).foreach(_.add(writeTime))
    blocks
  }

  /**
   * Calculate the copy ranges from blocks.
   * And it will try to combine the sequential blocks.
   */
  private def computeCopyRanges(blocks: Seq[BlockInfo]): Seq[CopyRange] = {
    var currentCopyStart, currentCopyEnd = 0L
    val copyRanges = new ArrayBuffer[CopyRange]()

    blocks.foreach { block =>
      if (currentCopyEnd != block.blockStart) {
        if (currentCopyEnd != 0) {
          copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
        }
        currentCopyStart = block.blockStart
        currentCopyEnd = currentCopyStart
      }
      currentCopyEnd += block.blockSize
    }

    if (currentCopyEnd != currentCopyStart) {
      copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
    }
    copyRanges.toSeq
  }

}

/** A PartitionReader that reads an AVRO file split on the GPU. */
class GpuAvroPartitionReader(
    override val conf: Configuration,
    partFile: PartitionedFile,
    blockMeta: AvroBlockMeta,
    override val readDataSchema: StructType,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric])
  extends FilePartitionReaderBase(conf, execMetrics) with GpuAvroReaderBase {

  private val partFilePath = new Path(new URI(partFile.filePath.toString()))
  private val blockIterator: BufferedIterator[BlockInfo] = blockMeta.blocks.iterator.buffered

  override def next(): Boolean = {
    if (batchIter.hasNext) {
      return true
    }
    batchIter = EmptyGpuColumnarBatchIterator
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
      } else {
        batchIter = readBatch() match {
          case Some(batch) => new SingleGpuColumnarBatchIterator(batch)
          case _ => EmptyGpuColumnarBatchIterator
        }
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  private def readBatch(): Option[ColumnarBatch] = {
    NvtxRegistry.AVRO_READ_BATCH {
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes)
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.count).sum.toInt
        if (numRows == 0) {
          None
        } else {
          Some(new ColumnarBatch(Array.empty, numRows.toInt))
        }
      } else {
        if (currentChunkedBlocks.isEmpty) {
          None
        } else {
          val (dataBuffer, dataSize) = metrics(BUFFER_TIME).ns {
            readPartFile(partFilePath, currentChunkedBlocks,
              blockMeta.headerSize, conf)
          }
          sendToGpu(dataBuffer, dataSize, Array(partFile))
        }
      }
    }
  }

}

/**
 * A PartitionReader that can read multiple AVRO files in parallel.
 * This is most efficient running in a cloud environment where the I/O of reading is slow.
 *
 * When reading a file, it
 *   - seeks to the start position of the first block located in this partition.
 *   - next, parses the meta and sync, rewrites the meta and sync, and copies the data to a
 *     batch buffer per block, until reaching the last one of the current partition.
 *   - sends batches to GPU at last.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param poolConf thread pool configurations
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param filters filters passed into the filterHandler
 * @param execMetrics the metrics
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated AVRO data or null
 * @param debugDumpAlways whether to debug dump always or only on errors
 * @param readDataSchema the Spark schema describing what will be read
 * @param partitionSchema Schema of partitions.
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows to be read per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes to be read per batch
 * @param maxGpuColumnSizeBytes maximum number of bytes for a GPU column
 */
class GpuMultiFileCloudAvroPartitionReader(
    override val conf: Configuration,
    files: Array[PartitionedFile],
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric],
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long)
  extends MultiFileCloudPartitionReaderBase(conf,
    files, poolConf, maxNumFileProcessed, filters,
    execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    ignoreCorruptFiles) with MultiFileReaderFunctions with GpuAvroReaderBase {

  override def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
    Iterator[ColumnarBatch] = fileBufsAndMeta match {
    case buffer: AvroHostBuffersWithMeta =>
      val bufsAndSizes = buffer.memBuffersAndSizes
      val bufAndSizeInfo = bufsAndSizes.head
      val partitionValues = buffer.partitionedFile.partitionValues
      val batchIter = if (bufAndSizeInfo.hmbs.isEmpty) {
        // Not reading any data, but add in partition data if needed
        // Someone is going to process this data, even if it is just a row count
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val emptyBatch = new ColumnarBatch(Array.empty, bufAndSizeInfo.numRows.toInt)
        BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(emptyBatch,
          partitionValues, partitionSchema, maxGpuColumnSizeBytes)
      } else {
        require(bufAndSizeInfo.hmbs.length == 1)
        val maybeBatch = sendToGpu(bufAndSizeInfo.hmbs.head, bufAndSizeInfo.bytes, files)
        // we have to add partition values here for this batch, we already verified that
        // it's not different for all the blocks in this batch
        maybeBatch match {
          case Some(batch) => BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
            partitionValues, partitionSchema, maxGpuColumnSizeBytes)
          case None => EmptyGpuColumnarBatchIterator
        }
      }
      // Update the current buffers
      closeOnExcept(batchIter) { _ =>
        if (bufsAndSizes.length > 1) {
          val updatedBuffers = bufsAndSizes.drop(1)
          currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
        } else {
          currentFileHostBuffers = None
        }
      }

      batchIter
    case t =>
      throw new RuntimeException(s"Unknown avro buffer type: ${t.getClass.getSimpleName}")
  }

  override final def getFileFormatShortName: String = "AVRO"

  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      config: Configuration,
      filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] =
    new ReadBatchRunner(tc, file, config, filters)

  /** Two utils classes */
  private case class AvroHostBuffersWithMeta(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[SingleHMBAndMeta],
    override val bytesRead: Long) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      taskContext: TaskContext,
      partFile: PartitionedFile,
      config: Configuration,
      filters: Array[Filter]) extends UnboundedAsyncRunner[BufferInfo] with Logging {

    override def callImpl(): BufferInfo = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${partFile.filePath}", e)
          AvroHostBuffersWithMeta(partFile, Array(SingleHMBAndMeta.empty()), 0)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${partFile.filePath}", e)
          AvroHostBuffersWithMeta(partFile, Array(SingleHMBAndMeta.empty()), 0)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def createBufferAndMeta(
        arrayBufSize: Array[SingleHMBAndMeta],
        startingBytesRead: Long): HostMemoryBuffersWithMetaDataBase = {
      val bytesRead = fileSystemBytesRead() - startingBytesRead
      AvroHostBuffersWithMeta(partFile, arrayBufSize, bytesRead)
    }

    private val stopPosition = partFile.start + partFile.length

    /**
     * Read the split to one or more batches.
     * Here is overview of the process:
     *     - some preparation
     *     - while (has next block in this split) {
     *     -   1) peek the current head block and estimate the batch buffer size
     *     -   2) read blocks as many as possible to fill the batch buffer
     *     -   3) One batch is done, append it to the list
     *     - }
     *     - post processing
     */
    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val bufferStartTime = System.nanoTime()
      val startingBytesRead = fileSystemBytesRead()
      val result =
        withResource(AvroFileReader.openDataReader(partFile.filePath.toString(), config)) {
          reader =>
          // Go to the start of the first block after the start position
          reader.sync(partFile.start)
          if (!reader.hasNextBlock || isDone) {
            // no data or got close before finishing, return null buffer and zero size
            createBufferAndMeta(
              Array(SingleHMBAndMeta.empty()),
              startingBytesRead)
          } else {
            val hostBuffers = new ArrayBuffer[SingleHMBAndMeta]
            try {
              val headerSize = reader.headerSize
              var isBlockSizeEstimated = false
              var estBlocksSize = 0L
              var totalRowsNum = 0
              var curBlock: MutableBlockInfo = null
              while (reader.hasNextBlock && !reader.pastSync(stopPosition)) {
                // Get the block metadata first
                curBlock = reader.peekBlock(curBlock)
                if (!isBlockSizeEstimated) {
                  // Initialize the estimated block total size.
                  // The AVRO file has no special section for block metadata, and collecting the
                  // block meta through the file is quite expensive for files in cloud. So we do
                  // not know the target buffer size ahead. Then we have to do an estimation.
                  //   "the estimated total block size = partFile.length + additional space"
                  // Letting "additional space = one block length * 1.2" is because we may
                  // move the start and stop positions when reading this split to keep the
                  // integrity of edge blocks.
                  // One worst case is the stop position is one byte after a block start,
                  // then we need to read the whole block into the current batch. And this block
                  // may be larger than the first block. So we preserve an additional space
                  // whose size is 'one block length * 1.2' to try to avoid reading it to a new
                  // batch.
                  estBlocksSize = partFile.length + (curBlock.blockSize * 1.2F).toLong
                  isBlockSizeEstimated = true
                }

                var estSizeToRead = if (estBlocksSize > maxReadBatchSizeBytes) {
                  maxReadBatchSizeBytes
                } else if (estBlocksSize < curBlock.blockSize) {
                  // This may happen only for the last block.
                  logInfo("Less buffer is estimated, read the last block into a new batch.")
                  curBlock.blockSize
                } else {
                  estBlocksSize
                }
                val optHmb = if (readDataSchema.nonEmpty) {
                  Some(HostMemoryBuffer.allocate(headerSize + estSizeToRead))
                } else None
                // Allocate the buffer for the header and blocks for a batch
                closeOnExcept(optHmb) { _ =>
                  val optOut = optHmb.map { hmb =>
                    val out = new HostMemoryOutputStream(hmb)
                    // Write the header to the output stream
                    AvroFileWriter(out).writeHeader(reader.header)
                    out
                  }
                  // Read the block data to the output stream
                  var batchRowsNum: Int = 0
                  var batchSize: Long = 0
                  var hasNextBlock = true
                  do {
                    if (optOut.nonEmpty) {
                      reader.readNextRawBlock(optOut.get)
                    } else {
                      // skip the current block
                      reader.skipCurrentBlock()
                    }
                    batchRowsNum += curBlock.count.toInt
                    estSizeToRead -= curBlock.blockSize
                    batchSize += curBlock.blockSize
                    // Continue reading the next block into the current batch when
                    //  - the next block exists, and
                    //  - the remaining buffer is enough to hold the next block, and
                    //  - the batch rows number does not go beyond the upper limit.
                    hasNextBlock = reader.hasNextBlock && !reader.pastSync(stopPosition)
                    if (hasNextBlock) {
                      curBlock = reader.peekBlock(curBlock)
                    }
                  } while (hasNextBlock && curBlock.blockSize <= estSizeToRead &&
                      batchRowsNum <= maxReadBatchSizeRows)

                  // One batch is done
                  optOut.foreach { out =>
                    val shb = SpillableHostBuffer(optHmb.get, optHmb.get.getLength,
                      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
                    hostBuffers +=
                      (SingleHMBAndMeta(Array(shb), out.getPos, batchRowsNum, Seq.empty))
                  }
                  totalRowsNum += batchRowsNum
                  estBlocksSize -= batchSize
                }
              } // end of while

              val bufAndSize: Array[SingleHMBAndMeta] = if (readDataSchema.isEmpty) {
                hostBuffers.safeClose()
                Array(SingleHMBAndMeta.empty(totalRowsNum))
              } else if (isDone) {
                // got close before finishing, return null buffer and zero size
                hostBuffers.safeClose()
                Array(SingleHMBAndMeta.empty())
              } else {
                hostBuffers.toArray
              }
              createBufferAndMeta(bufAndSize, startingBytesRead)
            } catch {
              case e: Throwable =>
                hostBuffers.safeClose(e)
                throw e
            }
          }
        } // end of withResource(reader)
      val bufferTime = System.nanoTime() - bufferStartTime
      // multi-file avro scanner does not filter and then buffer, it just buffers
      result.setExecutionTime(0, bufferTime)
      result
    } // end of doRead
  } // end of Class ReadBatchRunner

}

/**
 * A PartitionReader that can read multiple AVRO files up to the certain size. It will
 * coalesce small files together and copy the block data in a separate thread pool to speed
 * up processing the small files before sending down to the GPU.
 */
class GpuMultiFileAvroPartitionReader(
    override val conf: Configuration,
    splits: Array[PartitionedFile],
    clippedBlocks: Seq[AvroSingleDataBlockInfo],
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    poolConf: ThreadPoolConf,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    execMetrics: Map[String, GpuMetric],
    mapPathHeader: Map[Path, Header])
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedBlocks,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
    poolConf, execMetrics) with GpuAvroReaderBase {

  override def checkIfNeedToSplitDataBlock(
      currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    val nextHeader = mapPathHeader.get(nextBlockInfo.filePath).get
    val curHeader = mapPathHeader.get(currentBlockInfo.filePath).get
    // Split into another block when a key exists in both of the two headers' metadata,
    // and maps to different values.
    if (Header.hasConflictInMetadata(nextHeader, curHeader)) {
      logInfo(s"Avro metadata in the next file ${nextBlockInfo.filePath}" +
        s" conflicts with the current one in file ${currentBlockInfo.filePath}," +
        s" splitting it into a new batch!")
      return true
    }
    false
  }

  override protected def createBatchContext(
      chunkedBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      clippedSchema: SchemaBase): BatchContext = {
    // Get headers according to the input paths.
    val headers = chunkedBlocks.keys.map(mapPathHeader.get(_).get)
    // Merge the meta, it is safe because the compatibility has been verifed
    // in 'checkIfNeedToSplitDataBlock'
    val mergedHeader = Header.mergeMetadata(headers.toSeq)
    assert(mergedHeader.nonEmpty, "No header exists")
    AvroBatchContext(chunkedBlocks, clippedSchema, mergedHeader.get)
  }

  override def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long = {
    val allBlocks = batchContext.origChunkedBlocks.values.flatten.toSeq
    val headerSize = Header.headerSizeInBytes(batchContext.mergedHeader)
    estimateOutputSize(allBlocks, headerSize)
  }

  override def writeFileHeader(buffer: HostMemoryBuffer, bContext: BatchContext): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      AvroFileWriter(out).writeHeader(bContext.mergedHeader)
      out.getPos
    }
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: collection.Seq[DataBlockBase], batchContext: BatchContext): Long = {
    // In 'calculateEstimatedBlocksOutputSize', we have got the true size for
    // Header + All Blocks.
    footerOffset
  }

  override def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], bContext: BatchContext): (HostMemoryBuffer, Long) = {
    // AVRO files have no footer, do nothing
    (buffer, footerOffset)
  }

  override def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase,  readSchema: StructType,
      extraInfo: ExtraInfo): GpuDataProducer[Table] = {
    val tableReader = withResource(dataBuffer) { _ =>
      new SingleGpuDataProducer(sendToGpuUnchecked(dataBuffer, dataSize, splits))
    }
    GpuDataProducer.wrap(tableReader) { table =>
      if (readDataSchema.length < table.getNumberOfColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns}")
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      table
    }
  }

  override final def getFileFormatShortName: String = "AVRO"

  override def getBatchRunner(
      tc: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): AsyncRunner[(Seq[DataBlockBase], Long)] =
    new AvroCopyBlocksRunner(tc, file, outhmb, blocks, offset, batchContext)

  // The runner to copy blocks to offset of HostMemoryBuffer
  class AvroCopyBlocksRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext) extends UnboundedAsyncRunner[(Seq[DataBlockBase], Long)] {

    private val headerSync = Some(batchContext.mergedHeader.sync)

    override def callImpl(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        val startBytesRead = fileSystemBytesRead()
        val res = withResource(outhmb) { _ =>
          withResource(file.getFileSystem(conf).open(file)) { in =>
            withResource(new HostMemoryOutputStream(outhmb)) { out =>
              copyBlocksData(blocks.toSeq, in, out, headerSync)
            }
          }
        }
        val bytesRead = fileSystemBytesRead() - startBytesRead
        (res, bytesRead)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  // Some implicits for conversions between the base class to the sub-class
  implicit def toAvroSchema(schema: SchemaBase): Schema =
    schema.asInstanceOf[AvroSchemaWrapper].schema

  implicit def toBlockInfo(block: DataBlockBase): BlockInfo =
    block.asInstanceOf[AvroDataBlock].blockInfo

  implicit def toBlockInfos(blocks: Seq[DataBlockBase]): Seq[BlockInfo] =
    blocks.map(toBlockInfo(_))

  implicit def toBlockBases(blocks: Seq[BlockInfo]): Seq[DataBlockBase] =
    blocks.map(AvroDataBlock(_))

  implicit def toAvroExtraInfo(in: ExtraInfo): AvroExtraInfo =
    in.asInstanceOf[AvroExtraInfo]

  implicit def toAvroBatchContext(in: BatchContext): AvroBatchContext =
    in.asInstanceOf[AvroBatchContext]

}

/** A tool to filter Avro blocks */
case class AvroFileFilterHandler(
    hadoopConf: Configuration,
    @transient options: AvroOptions) extends Logging {

  @scala.annotation.nowarn(
    "msg=value ignoreExtension in class AvroOptions is deprecated*"
  )
  val ignoreExtension = options.ignoreExtension

  def filterBlocks(partFile: PartitionedFile): AvroBlockMeta = {
    if (ignoreExtension || partFile.filePath.toString().endsWith(".avro")) {
      withResource(AvroFileReader.openMetaReader(partFile.filePath.toString(), hadoopConf)) {
        reader =>
        // Get blocks only belong to this split
        reader.sync(partFile.start)
        val partBlocks = reader.getPartialBlocks(partFile.start + partFile.length)
        AvroBlockMeta(reader.header, reader.headerSize, partBlocks)
      }
    } else {
      AvroBlockMeta(null, 0L, Seq.empty)
    }
  }
}

/**
 * Avro block meta info
 *
 * @param header the header of avro file
 * @param blocks the total block info of avro file
 */
case class AvroBlockMeta(header: Header, headerSize: Long, blocks: Seq[BlockInfo])

/**
 * CopyRange to indicate from where to copy.
 *
 * @param offset from where to copy
 * @param length how many bytes to copy
 */
private case class CopyRange(offset: Long, length: Long)

/** Extra information */
case class AvroExtraInfo() extends ExtraInfo

/** avro schema wrapper */
case class AvroSchemaWrapper(schema: Schema) extends SchemaBase {
  override def isEmpty: Boolean = schema.getFields.isEmpty
}

/** avro BlockInfo wrapper */
case class AvroDataBlock(blockInfo: BlockInfo) extends DataBlockBase {
  override def getRowCount: Long = blockInfo.count
  override def getReadDataSize: Long = blockInfo.dataSize
  override def getBlockSize: Long = blockInfo.blockSize
}

case class AvroSingleDataBlockInfo(
  filePath: Path,
  dataBlock: AvroDataBlock,
  partitionValues: InternalRow,
  schema: AvroSchemaWrapper,
  readSchema: StructType,
  extraInfo: AvroExtraInfo) extends SingleDataBlockInfo

case class AvroBatchContext(
  override val origChunkedBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
  override val schema: SchemaBase,
  mergedHeader: Header) extends BatchContext(origChunkedBlocks, schema)
