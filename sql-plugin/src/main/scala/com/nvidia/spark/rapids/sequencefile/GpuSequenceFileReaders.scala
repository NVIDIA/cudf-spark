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

package com.nvidia.spark.rapids.sequencefile

import java.io.{DataOutputStream, EOFException, IOException, OutputStream}
import java.net.URI
import java.util

import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, HostColumnVectorCore,
  HostMemoryBuffer}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.io.async.{AsyncMetrics, AsyncResult, DecayReleaseResult,
  MemoryBoundedAsyncRunner}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, Path}
import org.apache.hadoop.io.{DataOutputBuffer, SequenceFile}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

private object SequenceFileReaderLimits {
  val MAX_BATCH_OUTPUT_BYTES: Long = 256L * 1024 * 1024
  val MAX_BATCH_ROWS: Int = 4 * 1024 * 1024
  val MAX_OFFSETS_FRACTION_DENOMINATOR: Int = 4
  val MIN_INITIAL_DATA_BYTES: Long = 64L * 1024
  val MIN_INITIAL_ROWS: Int = 1024
  val ESTIMATED_COMPRESSED_BYTES_PER_ROW: Long = 1024
}

private final class EofTrackingInputStream(input: FSDataInputStream) extends FSInputStream {
  private var eof = false

  def reachedEof: Boolean = eof

  override def read(): Int = {
    val result = input.read()
    if (result < 0) eof = true
    result
  }

  override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    val result = input.read(bytes, offset, length)
    if (result < 0) eof = true
    result
  }

  override def seek(position: Long): Unit = input.seek(position)

  override def getPos: Long = input.getPos

  override def seekToNewSource(position: Long): Boolean = input.seekToNewSource(position)

  override def close(): Unit = input.close()
}

private final class BinaryRecordReader(
    reader: SequenceFile.Reader,
    input: EofTrackingInputStream,
    file: PartitionedFile,
    splitEnd: Long) extends AutoCloseable {
  private val key = new DataOutputBuffer
  private val value = reader.createValueBytes()
  private var more = reader.getPosition < splitEnd

  def next(): Boolean = {
    if (!more) {
      return false
    }
    key.reset()
    val position = reader.getPosition
    val hasNext = try {
      reader.nextRaw(key, value) >= 0
    } catch {
      case e: EOFException if input.reachedEof =>
        val truncated = new EOFException(s"Truncated SequenceFile in ${file.filePath}")
        truncated.initCause(e)
        throw truncated
    }
    if (!hasNext && input.reachedEof) {
      throw new EOFException(s"Truncated SequenceFile in ${file.filePath}")
    }
    if (!hasNext || (position >= splitEnd && reader.syncSeen)) {
      more = false
    }
    more
  }

  def writeKey(out: DataOutputStream): Unit = out.write(key.getData, 0, key.getLength)

  def writeValue(out: DataOutputStream): Unit = value.writeUncompressedBytes(out)

  override def close(): Unit = reader.close()
}

private object BinaryRecordReader {
  def open(conf: Configuration, file: PartitionedFile): BinaryRecordReader = {
    val path = new Path(new URI(file.filePath.toString))
    val fileSystem = path.getFileSystem(conf)
    val rawInput = fileSystem.open(path)
    val input = new EofTrackingInputStream(rawInput)
    var reader: SequenceFile.Reader = null
    try {
      val fileSize = if (file.fileSize > 0) file.fileSize
      else fileSystem.getFileStatus(path).getLen
      reader = new SequenceFile.Reader(
        conf,
        SequenceFile.Reader.stream(new FSDataInputStream(input)),
        SequenceFile.Reader.length(fileSize))
      if (file.start > reader.getPosition) {
        reader.sync(file.start)
      }
      val splitEnd = math.min(fileSize, Math.addExact(file.start, file.length))
      new BinaryRecordReader(reader, input, file, splitEnd)
    } catch {
      case t: Throwable =>
        if (reader == null) {
          input.safeClose(t)
        } else {
          reader.safeClose(t)
        }
        throw t
    }
  }
}

private final class BatchCapacityExceeded extends IOException

private final class BinaryHostColumnBuilder(
    maxDataCapacity: Long,
    maxRowCapacity: Int,
    initialDataCapacity: Long,
    initialRowCapacity: Int) extends AutoCloseable {
  private var data: HostMemoryBuffer = null
  private var offsets: HostMemoryBuffer = null
  private var hostOutput: HostMemoryOutputStream = null
  private var dataOutput: DataOutputStream = null
  private var rows = 0

  private val growableOutput = new OutputStream {
    override def write(value: Int): Unit = {
      growData(Math.addExact(hostOutput.getPos, 1L))
      hostOutput.write(value)
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      growData(Math.addExact(hostOutput.getPos, length.toLong))
      hostOutput.write(bytes, offset, length)
    }
  }

  try {
    require(initialDataCapacity > 0 && initialDataCapacity <= maxDataCapacity)
    require(initialRowCapacity > 0 && initialRowCapacity <= maxRowCapacity)
    data = HostAlloc.alloc(initialDataCapacity, preferPinned = true)
    offsets = HostAlloc.alloc(
      (initialRowCapacity.toLong + 1L) * DType.INT32.getSizeInBytes, preferPinned = true)
    hostOutput = new HostMemoryOutputStream(data)
    dataOutput = new DataOutputStream(growableOutput)
    offsets.setInt(0, 0)
  } catch {
    case t: Throwable =>
      dataOutput.safeClose(t)
      hostOutput.safeClose(t)
      data.safeClose(t)
      offsets.safeClose(t)
      throw t
  }

  private def growData(requiredCapacity: Long): Unit = {
    if (requiredCapacity > maxDataCapacity) {
      throw new BatchCapacityExceeded
    }
    if (requiredCapacity > data.getLength) {
      val newCapacity = math.min(maxDataCapacity,
        math.max(requiredCapacity, Math.multiplyExact(data.getLength, 2L)))
      val oldPosition = hostOutput.getPos
      closeOnExcept(HostAlloc.alloc(newCapacity, preferPinned = true)) { newData =>
        newData.copyFromHostBuffer(0, data, 0, oldPosition)
        val newHostOutput = new HostMemoryOutputStream(newData)
        newHostOutput.seek(oldPosition)
        hostOutput.safeClose(null)
        data.safeClose(null)
        data = newData
        hostOutput = newHostOutput
      }
    }
  }

  private def growOffsets(requiredRows: Int): Unit = {
    val currentRowCapacity = offsets.getLength / DType.INT32.getSizeInBytes - 1L
    if (requiredRows > currentRowCapacity) {
      val newRowCapacity = math.min(maxRowCapacity.toLong,
        math.max(requiredRows.toLong, Math.multiplyExact(currentRowCapacity, 2L)))
      closeOnExcept(HostAlloc.alloc(
          (newRowCapacity + 1L) * DType.INT32.getSizeInBytes, preferPinned = true)) {
        newOffsets =>
          newOffsets.copyFromHostBuffer(0, offsets, 0,
            (rows.toLong + 1L) * DType.INT32.getSizeInBytes)
          offsets.safeClose(null)
          offsets = newOffsets
      }
    }
  }

  def append(write: DataOutputStream => Unit): Long = {
    require(rows < maxRowCapacity, "SequenceFile binary exceeds the row limit")
    growOffsets(rows + 1)
    val start = hostOutput.getPos
    write(dataOutput)
    val written = hostOutput.getPos - start
    rows += 1
    offsets.setInt(rows.toLong * DType.INT32.getSizeInBytes,
      Math.toIntExact(hostOutput.getPos))
    written
  }

  def finish(): Array[SpillableHostBuffer] = {
    var dataSpill: SpillableHostBuffer = null
    var offsetsSpill: SpillableHostBuffer = null
    try {
      val dataBuffer = data
      dataSpill = SpillableHostBuffer(
        dataBuffer, hostOutput.getPos, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      data = null
      val offsetsBuffer = offsets
      offsetsSpill = SpillableHostBuffer(
        offsetsBuffer,
        (rows.toLong + 1L) * DType.INT32.getSizeInBytes,
        SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      offsets = null
      Array(dataSpill, offsetsSpill)
    } catch {
      case t: Throwable =>
        dataSpill.safeClose(t)
        offsetsSpill.safeClose(t)
        throw t
    }
  }

  override def close(): Unit = {
    dataOutput.safeClose(null)
    hostOutput.safeClose(null)
    data.safeClose(null)
    offsets.safeClose(null)
    hostOutput = null
    data = null
    offsets = null
  }
}

private[sequencefile] class GpuSequenceFilePartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    keyFirst: Boolean,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    execMetrics: Map[String, GpuMetric],
    maxReadBatchSizeRows: Int,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    keepReadsInOrder: Boolean)
  extends MultiFileCloudPartitionReaderBase(
    conf,
    files,
    poolConf,
    maxNumFileProcessed,
    Array.empty,
    execMetrics,
    maxReadBatchSizeRows,
    maxReadBatchSizeBytes,
    keepReadsInOrder = keepReadsInOrder,
    combineConf = CombineConf(Seq(
      SequenceFileReaderLimits.MAX_BATCH_OUTPUT_BYTES,
      maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes).min, 0)) with MultiFileReaderFunctions {

  private val projectedColumns = 2
  private val batchCapacity = Seq(
    SequenceFileReaderLimits.MAX_BATCH_OUTPUT_BYTES,
    maxReadBatchSizeBytes,
    maxGpuColumnSizeBytes).min
  private val maxRowsForOffsets = batchCapacity /
    (projectedColumns.toLong * DType.INT32.getSizeInBytes *
      SequenceFileReaderLimits.MAX_OFFSETS_FRACTION_DENOMINATOR) - 1L
  private val rowCapacity = math.min(
    math.min(maxReadBatchSizeRows.toLong, SequenceFileReaderLimits.MAX_BATCH_ROWS.toLong),
    maxRowsForOffsets).toInt
  require(rowCapacity > 0, "SequenceFile binary batch limit is too small")
  private val offsetsCapacity = projectedColumns.toLong * (rowCapacity.toLong + 1L) *
    DType.INT32.getSizeInBytes
  private val payloadCapacity = batchCapacity - offsetsCapacity
  require(payloadCapacity > 0, "SequenceFile binary batch limit leaves no room for payloads")
  // This covers both columns plus one builder's grow-and-copy overlap.
  private val requiredHostMemory = Math.multiplyExact(3L, batchCapacity)
  private val keyIndex = if (keyFirst) 0 else 1
  private val valueIndex = 1 - keyIndex
  private val tracker = new ResultTracker
  private val outputBatchesMetric = execMetrics.getOrElse(NUM_OUTPUT_BATCHES, NoopMetric)
  private val copyMetric = execMetrics.getOrElse(COPY_BUFFER_TIME, NoopMetric)

  private sealed trait SequenceFileBuffers extends HostMemoryBuffersWithMetaDataBase {
    def sourceBuffers: Array[SequenceFileHostBuffers]
  }

  private final class SequenceFileHostBuffers(
      override val partitionedFile: PartitionedFile,
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long) extends SequenceFileBuffers {
    require(memBuffersAndSizes.length == 1)

    override lazy val sourceBuffers: Array[SequenceFileHostBuffers] = Array(this)

    override def close(): Unit = synchronized {
      tracker.remove(this)
      super.close()
    }
  }

  private final class CombinedSequenceFileHostBuffers(
      override val sourceBuffers: Array[SequenceFileHostBuffers]) extends SequenceFileBuffers {
    require(sourceBuffers.nonEmpty)

    override val partitionedFile: PartitionedFile = sourceBuffers.head.partitionedFile
    override val memBuffersAndSizes: Array[SingleHMBAndMeta] =
      sourceBuffers.flatMap(_.memBuffersAndSizes)
    override val bytesRead: Long = sourceBuffers.foldLeft(0L) { (total, buffers) =>
      Math.addExact(total, buffers.bytesRead)
    }

    setExecutionTime(
      sourceBuffers.foldLeft(0L)((total, buffers) =>
        Math.addExact(total, buffers.getFilterTime)),
      sourceBuffers.foldLeft(0L)((total, buffers) =>
        Math.addExact(total, buffers.getBufferTime)))
    setScheduleTime(sourceBuffers.foldLeft(0L)((total, buffers) =>
      Math.addExact(total, buffers.getScheduleTime)))

    override def close(): Unit = sourceBuffers.safeClose()
  }

  private final class ResultTracker extends AutoCloseable {
    private val published = mutable.HashSet.empty[SequenceFileHostBuffers]
    private var closed = false

    def publish(result: SequenceFileHostBuffers): SequenceFileHostBuffers = {
      try {
        val retain = synchronized {
          if (closed) false else {
            published += result
            true
          }
        }
        if (!retain) result.close()
        result
      } catch {
        case t: Throwable =>
          result.safeClose(t)
          throw t
      }
    }

    def remove(result: SequenceFileHostBuffers): Unit = synchronized {
      published -= result
    }

    def claim(results: Array[SequenceFileHostBuffers]): Boolean = synchronized {
      if (closed || !results.forall(published.contains)) {
        false
      } else {
        results.foreach(published.remove)
        true
      }
    }

    override def close(): Unit = {
      val toClose = synchronized {
        closed = true
        val copy = published.toArray
        published.clear()
        copy
      }
      toClose.safeClose()
    }
  }

  private class ReadBatchRunner(
      taskContext: TaskContext,
      file: PartitionedFile,
      config: Configuration) extends MemoryBoundedAsyncRunner[BufferInfo] {

    override def sparkTaskContext: Option[TaskContext] = Some(taskContext)

    override val requiredMemoryBytes: Long = requiredHostMemory

    override protected def buildResult(
        resultData: BufferInfo,
        metrics: AsyncMetrics): AsyncResult[BufferInfo] = {
      val releaseCallback = () => {
        if (isHoldingResource) {
          withStateLock { _ => releaseResourceCallback() }
        }
      }
      new DecayReleaseResult(resultData, metrics, releaseCallback)
    }

    override protected def callImpl(): BufferInfo = {
      TrampolineUtil.setTaskContext(taskContext)
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      val start = System.nanoTime()
      try {
        val result = readFile(file, config)
        result.setExecutionTime(0L, System.nanoTime() - start)
        tracker.publish(result)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  private def closeBuilders(
      builders: Array[BinaryHostColumnBuilder],
      error: Throwable): Unit = {
    builders.filter(_ != null).safeClose(error)
  }

  private def finishBuilders(
      builders: Array[BinaryHostColumnBuilder],
      rows: Int): SingleHMBAndMeta = {
    val buffers = new Array[SpillableHostBuffer](builders.length * 2)
    try {
      var index = 0
      var bytes = 0L
      builders.foreach { builder =>
        val finished = builder.finish()
        buffers(index) = finished(0)
        buffers(index + 1) = finished(1)
        bytes = Math.addExact(bytes, Math.addExact(finished(0).length, finished(1).length))
        index += 2
      }
      SingleHMBAndMeta(buffers, bytes, rows, Seq.empty)
    } catch {
      case t: Throwable =>
        buffers.safeClose(t)
        throw t
    }
  }

  private def newBuilders(file: PartitionedFile): Array[BinaryHostColumnBuilder] = {
    val builders = new Array[BinaryHostColumnBuilder](projectedColumns)
    try {
      val cappedFileLength = math.min(file.length, payloadCapacity)
      val initialPayloadCapacity = math.min(payloadCapacity,
        math.max(SequenceFileReaderLimits.MIN_INITIAL_DATA_BYTES,
          Math.multiplyExact(cappedFileLength, 2L)))
      val initialDataCapacity = math.max(1L, initialPayloadCapacity / projectedColumns)
      val initialRowCapacity = math.min(rowCapacity.toLong,
        math.max(SequenceFileReaderLimits.MIN_INITIAL_ROWS.toLong,
          file.length / SequenceFileReaderLimits.ESTIMATED_COMPRESSED_BYTES_PER_ROW)).toInt
      var index = 0
      while (index < builders.length) {
        builders(index) = new BinaryHostColumnBuilder(
          payloadCapacity, rowCapacity, initialDataCapacity, initialRowCapacity)
        index += 1
      }
      builders
    } catch {
      case t: Throwable =>
        closeBuilders(builders, t)
        throw t
    }
  }

  private def byteLimitExceeded(
      file: PartitionedFile,
      rowCount: Int): UnsupportedOperationException = {
    val scope = if (rowCount == 0) "record" else "split"
    new UnsupportedOperationException(
      s"SequenceFile binary $scope exceeds the $payloadCapacity byte decompressed " +
        s"single-batch limit: ${file.filePath}")
  }

  private def readFile(
      file: PartitionedFile,
      config: Configuration): SequenceFileHostBuffers = {
    val startingBytesRead = fileSystemBytesRead()
    var builders: Array[BinaryHostColumnBuilder] = null
    try {
      builders = newBuilders(file)
      val rows = withResource(BinaryRecordReader.open(new Configuration(config), file)) { reader =>
        var rowCount = 0
        var payloadBytes = 0L
        while (reader.next()) {
          if (rowCount == rowCapacity) {
            throw new UnsupportedOperationException(
              s"SequenceFile binary split exceeds the $rowCapacity row single-batch limit; " +
                s"reduce the source RDD's input split size: ${file.filePath}")
          }
          val recordBytes = try {
            val keyBytes = builders(keyIndex).append(reader.writeKey)
            Math.addExact(keyBytes, builders(valueIndex).append(reader.writeValue))
          } catch {
            case _: BatchCapacityExceeded => throw byteLimitExceeded(file, rowCount)
          }
          if (recordBytes > payloadCapacity - payloadBytes) {
            throw byteLimitExceeded(file, rowCount)
          }
          payloadBytes = Math.addExact(payloadBytes, recordBytes)
          rowCount += 1
        }
        rowCount
      }
      val measuredBytesRead = Math.max(0L, fileSystemBytesRead() - startingBytesRead)
      // A zero bytesRead makes the async executor release its budget before these buffers close.
      val bytesRead = if (measuredBytesRead > 0) measuredBytesRead else math.max(1L, file.length)
      val info = finishBuilders(builders, rows)
      closeOnExcept(info) { ownedInfo =>
        new SequenceFileHostBuffers(file, Array(ownedInfo), bytesRead)
      }
    } catch {
      case t: Throwable =>
        if (builders != null) closeBuilders(builders, t)
        throw t
    } finally {
      if (builders != null) closeBuilders(builders, null)
    }
  }

  private def copyToDevice(
      dataSpill: SpillableHostBuffer,
      offsetsSpill: SpillableHostBuffer,
      rows: Int): ColumnVector = {
    withResource(dataSpill.getDataHostBuffer()) { data =>
      withResource(offsetsSpill.getDataHostBuffer()) { offsets =>
        data.incRefCount()
        val child = try {
          new HostColumnVectorCore(
            DType.UINT8,
            dataSpill.length,
            util.Optional.of[java.lang.Long](0L),
            data,
            null,
            null,
            new util.ArrayList[HostColumnVectorCore]())
        } catch {
          case t: Throwable =>
            data.close()
            throw t
        }
        val hostColumn = closeOnExcept(child) { ownedChild =>
          val children = new util.ArrayList[HostColumnVectorCore](1)
          children.add(ownedChild)
          offsets.incRefCount()
          try {
            new HostColumnVector(
              DType.LIST,
              rows,
              util.Optional.of[java.lang.Long](0L),
              null,
              null,
              offsets,
              children)
          } catch {
            case t: Throwable =>
              offsets.close()
              throw t
          }
        }
        withResource(hostColumn)(_.copyToDevice())
      }
    }
  }

  private def copyAndConcatenate(
      infos: Array[SingleHMBAndMeta],
      columnIndex: Int): ColumnVector = {
    val deviceColumns = new Array[ColumnVector](infos.length)
    try {
      var index = 0
      while (index < infos.length) {
        val info = infos(index)
        deviceColumns(index) = copyToDevice(
          info.hmbs(columnIndex * 2), info.hmbs(columnIndex * 2 + 1), info.numRows.toInt)
        index += 1
      }
      if (deviceColumns.length == 1) {
        val result = deviceColumns.head
        deviceColumns(0) = null
        result
      } else {
        ColumnVector.concatenate(deviceColumns: _*)
      }
    } finally {
      deviceColumns.safeClose()
    }
  }

  override def readBatches(
      fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
    val buffers = fileBufsAndMeta.asInstanceOf[SequenceFileBuffers]
    if (!tracker.claim(buffers.sourceBuffers)) {
      buffers.close()
      throw new IllegalStateException("SequenceFile buffers were closed before GPU transfer")
    }
    val batch = withRetryNoSplit(buffers) { current =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val infos = current.memBuffersAndSizes
      val rows = Math.toIntExact(infos.foldLeft(0L)((total, info) =>
        Math.addExact(total, info.numRows)))
      val columns = new Array[SparkVector](projectedColumns)
      closeOnExcept(columns) { _ =>
        copyMetric.ns {
          var index = 0
          while (index < columns.length) {
            val device = copyAndConcatenate(infos, index)
            columns(index) = closeOnExcept(device) { ownedDevice =>
              GpuColumnVector.from(ownedDevice, BinaryType)
            }
            index += 1
          }
        }
        new ColumnarBatch(columns, rows)
      }
    }
    outputBatchesMetric += 1
    new SingleGpuColumnarBatchIterator(batch)
  }

  override def canUseCombine: Boolean = true

  override def combineHMBs(
      results: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    require(results.nonEmpty)
    val buffers = results.map(_.asInstanceOf[SequenceFileBuffers])
    var accepted = 0
    var bytes = 0L
    var rows = 0L
    var keepAdding = true
    while (accepted < buffers.length && keepAdding) {
      val next = buffers(accepted)
      val nextBytes = next.memBuffersAndSizes.foldLeft(0L)((total, info) =>
        Math.addExact(total, info.bytes))
      val nextRows = next.memBuffersAndSizes.foldLeft(0L)((total, info) =>
        Math.addExact(total, info.numRows))
      require(nextBytes <= batchCapacity && nextRows <= rowCapacity)
      if (accepted == 0 ||
          (nextBytes <= batchCapacity - bytes && nextRows <= rowCapacity - rows)) {
        bytes += nextBytes
        rows += nextRows
        accepted += 1
      } else {
        keepAdding = false
      }
    }
    if (accepted < results.length) {
      combineLeftOverFiles = Some(results.drop(accepted))
    }
    val sources = buffers.take(accepted).flatMap(_.sourceBuffers)
    if (sources.length == 1) sources.head else new CombinedSequenceFileHostBuffers(sources)
  }

  override protected def getNumFilesInHostBuffers(
      fileInfo: HostMemoryBuffersWithMetaDataBase): Int =
    fileInfo.asInstanceOf[SequenceFileBuffers].sourceBuffers.length

  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      config: Configuration,
      filters: Array[Filter]): MemoryBoundedAsyncRunner[BufferInfo] = {
    new ReadBatchRunner(tc, file, config)
  }

  override final def getFileFormatShortName: String = "SequenceFileBinary"

  override def close(): Unit = {
    tracker.close()
    super.close()
  }
}

private[rapids] case class GpuSequenceFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    keyFirst: Boolean,
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric])
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  override protected val canUseCoalesceFilesReader: Boolean = false

  override protected val canUseMultiThreadReader: Boolean = true

  private val maxNumFileProcessed = rapidsConf.multiThreadReadNumThreads
  private val keepReadsInOrder = rapidsConf.getMultithreadedReaderKeepOrder

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    new GpuSequenceFilePartitionReader(
      conf,
      files,
      keyFirst,
      poolConfBuilder.build(),
      maxNumFileProcessed,
      metrics,
      maxReadBatchSizeRows,
      maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes,
      keepReadsInOrder)
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    throw new IllegalStateException("SequenceFile binary does not support coalescing reads")
  }

  override protected def getFileFormatShortName: String = "SequenceFileBinary"
}

object GpuSequenceFileRDDReader {
  def createReaderFactory(
      sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      keyFirst: Boolean,
      rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric]): PartitionReaderFactory = {
    GpuSequenceFilePartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      keyFirst,
      rapidsConf,
      ThreadPoolConfBuilder(rapidsConf),
      metrics)
  }
}
