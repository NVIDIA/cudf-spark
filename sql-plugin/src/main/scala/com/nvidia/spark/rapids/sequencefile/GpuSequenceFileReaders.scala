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

import java.io.{DataOutputStream, EOFException}
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
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

private object SequenceFileReaderLimits {
  val MAX_FILE_OUTPUT_BYTES: Long = 256L * 1024 * 1024
  val MAX_FILE_ROWS: Int = 4 * 1024 * 1024
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
    file: PartitionedFile) extends AutoCloseable {
  private val key = new DataOutputBuffer
  private val value = reader.createValueBytes()

  def next(): Boolean = {
    key.reset()
    val hasNext = try {
      reader.nextRaw(key, value) >= 0
    } catch {
      case e: EOFException if input.reachedEof =>
        val truncated = new EOFException(s"Truncated SequenceFile block in ${file.filePath}")
        truncated.initCause(e)
        throw truncated
    }
    if (!hasNext && input.reachedEof) {
      throw new EOFException(s"Truncated SequenceFile block in ${file.filePath}")
    }
    hasNext
  }

  def keyLength: Int = key.getLength

  def valueLength: Int = value.getSize

  def writeKey(out: DataOutputStream): Unit = out.write(key.getData, 0, key.getLength)

  def writeValue(out: DataOutputStream): Unit = value.writeUncompressedBytes(out)

  override def close(): Unit = reader.close()
}

private object BinaryRecordReader {
  def open(conf: Configuration, file: PartitionedFile): BinaryRecordReader = {
    require(file.start == 0,
      s"SequenceFile binary only supports whole files: ${file.filePath}")
    val path = new Path(new URI(file.filePath.toString))
    val rawInput = path.getFileSystem(conf).open(path)
    val input = new EofTrackingInputStream(rawInput)
    var reader: SequenceFile.Reader = null
    try {
      reader = new SequenceFile.Reader(
        conf,
        SequenceFile.Reader.stream(new FSDataInputStream(input)),
        SequenceFile.Reader.length(file.length))
      if (!reader.isBlockCompressed) {
        throw new UnsupportedOperationException(
          s"SequenceFile binary requires BLOCK compression: ${file.filePath}")
      }
      new BinaryRecordReader(reader, input, file)
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

  try {
    require(initialDataCapacity > 0 && initialDataCapacity <= maxDataCapacity)
    require(initialRowCapacity > 0 && initialRowCapacity <= maxRowCapacity)
    data = HostAlloc.alloc(initialDataCapacity, preferPinned = true)
    offsets = HostAlloc.alloc(
      (initialRowCapacity.toLong + 1L) * DType.INT32.getSizeInBytes, preferPinned = true)
    hostOutput = new HostMemoryOutputStream(data)
    dataOutput = new DataOutputStream(hostOutput)
    offsets.setInt(0, 0)
  } catch {
    case t: Throwable =>
      dataOutput.safeClose(t)
      data.safeClose(t)
      offsets.safeClose(t)
      throw t
  }

  private def growData(requiredCapacity: Long): Unit = {
    if (requiredCapacity > data.getLength) {
      val newCapacity = math.min(maxDataCapacity,
        math.max(requiredCapacity, Math.multiplyExact(data.getLength, 2L)))
      val oldPosition = hostOutput.getPos
      closeOnExcept(HostAlloc.alloc(newCapacity, preferPinned = true)) { newData =>
        newData.copyFromHostBuffer(0, data, 0, oldPosition)
        val newHostOutput = new HostMemoryOutputStream(newData)
        newHostOutput.seek(oldPosition)
        val newDataOutput = new DataOutputStream(newHostOutput)
        dataOutput.safeClose(null)
        data.safeClose(null)
        data = newData
        hostOutput = newHostOutput
        dataOutput = newDataOutput
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

  def append(expectedBytes: Int)(write: DataOutputStream => Unit): Unit = {
    require(rows < maxRowCapacity, "SequenceFile binary exceeds the row limit")
    val requiredDataCapacity = Math.addExact(hostOutput.getPos, expectedBytes.toLong)
    require(expectedBytes >= 0 && requiredDataCapacity <= maxDataCapacity,
      "SequenceFile binary exceeds the decompressed output limit")
    growData(requiredDataCapacity)
    growOffsets(rows + 1)
    val start = hostOutput.getPos
    write(dataOutput)
    require(hostOutput.getPos - start == expectedBytes,
      s"SequenceFile field wrote ${hostOutput.getPos - start} bytes, expected $expectedBytes")
    rows += 1
    offsets.setInt(rows.toLong * DType.INT32.getSizeInBytes,
      Math.toIntExact(hostOutput.getPos))
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
    data.safeClose(null)
    offsets.safeClose(null)
    data = null
    offsets = null
  }
}

private[sequencefile] class GpuSequenceFilePartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    readDataSchema: StructType,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    execMetrics: Map[String, GpuMetric],
    maxReadBatchSizeRows: Int,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    queryUsesInputFile: Boolean)
  extends MultiFileCloudPartitionReaderBase(
    conf,
    files,
    poolConf,
    maxNumFileProcessed,
    Array.empty,
    execMetrics,
    maxReadBatchSizeRows,
    maxReadBatchSizeBytes,
    combineConf = CombineConf(Seq(
      SequenceFileReaderLimits.MAX_FILE_OUTPUT_BYTES,
      maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes).min, 0)) {

  private val projectedColumns = readDataSchema.length
  private val batchCapacity = Seq(
    SequenceFileReaderLimits.MAX_FILE_OUTPUT_BYTES,
    maxReadBatchSizeBytes,
    maxGpuColumnSizeBytes).min
  private val rowCapacity = if (projectedColumns == 0) {
    math.min(maxReadBatchSizeRows, SequenceFileReaderLimits.MAX_FILE_ROWS)
  } else {
    val maxRowsForOffsets = batchCapacity /
      (projectedColumns.toLong * DType.INT32.getSizeInBytes *
        SequenceFileReaderLimits.MAX_OFFSETS_FRACTION_DENOMINATOR) - 1L
    math.min(
      math.min(maxReadBatchSizeRows.toLong, SequenceFileReaderLimits.MAX_FILE_ROWS.toLong),
      maxRowsForOffsets).toInt
  }
  require(rowCapacity > 0, "SequenceFile binary batch limit is too small")
  private val offsetsCapacity = projectedColumns.toLong * (rowCapacity.toLong + 1L) *
    DType.INT32.getSizeInBytes
  private val payloadCapacity = if (projectedColumns == 0) 0L else {
    batchCapacity - offsetsCapacity
  }
  require(projectedColumns == 0 || payloadCapacity > 0,
    "SequenceFile binary batch limit leaves no room for payloads")
  private val requiredHostMemory = math.max(1L,
    Math.multiplyExact(3L, payloadCapacity + offsetsCapacity))
  private val keyIndex = readDataSchema.fieldNames.indexOf(
    SequenceFileBinaryFileFormat.KEY_FIELD)
  private val valueIndex = readDataSchema.fieldNames.indexOf(
    SequenceFileBinaryFileFormat.VALUE_FIELD)
  private val wantsKey = keyIndex >= 0
  private val wantsValue = valueIndex >= 0
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

    override lazy val sourceBuffers: Array[SequenceFileHostBuffers] = Array(this)

    override def close(): Unit = {
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

  private def readFile(
      file: PartitionedFile,
      config: Configuration): SequenceFileHostBuffers = {
    val builders = new Array[BinaryHostColumnBuilder](projectedColumns)
    try {
      val cappedFileLength = math.min(file.length, payloadCapacity)
      val initialDataCapacity = if (projectedColumns == 0) 0L else {
        val initialPayloadCapacity = math.min(payloadCapacity,
          math.max(SequenceFileReaderLimits.MIN_INITIAL_DATA_BYTES,
            Math.multiplyExact(cappedFileLength, 2L)))
        math.max(1L, initialPayloadCapacity / projectedColumns)
      }
      val initialRowCapacity = math.min(rowCapacity.toLong,
        math.max(SequenceFileReaderLimits.MIN_INITIAL_ROWS.toLong,
          file.length / SequenceFileReaderLimits.ESTIMATED_COMPRESSED_BYTES_PER_ROW)).toInt
      var index = 0
      while (index < builders.length) {
        builders(index) = new BinaryHostColumnBuilder(
          payloadCapacity, rowCapacity, initialDataCapacity, initialRowCapacity)
        index += 1
      }
      val rows = withResource(BinaryRecordReader.open(new Configuration(config), file)) { reader =>
        var rowCount = 0
        var payloadBytes = 0L
        while (reader.next()) {
          if (rowCount == rowCapacity) {
            throw new UnsupportedOperationException(
              s"SequenceFile binary exceeds the $rowCapacity row file limit: ${file.filePath}")
          }
          val keyLength = if (wantsKey) reader.keyLength else 0
          val valueLength = if (wantsValue) reader.valueLength else 0
          val recordBytes = Math.addExact(keyLength.toLong, valueLength.toLong)
          payloadBytes = Math.addExact(payloadBytes, recordBytes)
          if (payloadBytes > payloadCapacity && projectedColumns > 0) {
            throw new UnsupportedOperationException(
              s"SequenceFile binary exceeds the $payloadCapacity byte decompressed output " +
                "limit: " +
                file.filePath)
          }
          if (wantsKey) builders(keyIndex).append(keyLength)(reader.writeKey)
          if (wantsValue) builders(valueIndex).append(valueLength)(reader.writeValue)
          rowCount += 1
        }
        rowCount
      }
      val bufferInfo = if (projectedColumns == 0) {
        SingleHMBAndMeta.empty(rows)
      } else {
        finishBuilders(builders, rows)
      }
      closeOnExcept(bufferInfo) { ownedBufferInfo =>
        new SequenceFileHostBuffers(file, Array(ownedBufferInfo), file.length)
      }
    } catch {
      case t: Throwable =>
        closeBuilders(builders, t)
        throw t
    } finally {
      closeBuilders(builders, null)
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
    buffers.sourceBuffers.foreach(tracker.remove)
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

  override def canUseCombine: Boolean = !queryUsesInputFile

  override def combineHMBs(
      results: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    require(results.nonEmpty)
    val buffers = results.map(_.asInstanceOf[SequenceFileBuffers])
    var accepted = 0
    var bytes = 0L
    var rows = 0L
    var keepAdding = true
    while (accepted < buffers.length && keepAdding) {
      val nextBytes = buffers(accepted).memBuffersAndSizes.foldLeft(0L)((total, info) =>
        Math.addExact(total, info.bytes))
      val nextRows = buffers(accepted).memBuffersAndSizes.foldLeft(0L)((total, info) =>
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
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  override protected val canUseCoalesceFilesReader: Boolean = false

  override protected val canUseMultiThreadReader: Boolean = true

  private val maxNumFileProcessed = rapidsConf.multiThreadReadNumThreads

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    require(partitionSchema.isEmpty, "SequenceFile binary does not support partition columns")
    new GpuSequenceFilePartitionReader(
      conf,
      files,
      readDataSchema,
      poolConfBuilder.build(),
      maxNumFileProcessed,
      metrics,
      maxReadBatchSizeRows,
      maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes,
      queryUsesInputFile)
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    throw new IllegalStateException("SequenceFile binary does not support coalescing reads")
  }

  override protected def getFileFormatShortName: String = "SequenceFileBinary"
}
