/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.{BufferedOutputStream, DataOutputStream, OutputStream}

import scala.collection.mutable

import ai.rapids.cudf.{HostBufferConsumer, HostMemoryBuffer, JCudfSerialization, TableWriter}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRestoreOnRetry, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.io.async.{AsyncOutputStream, TrafficController}
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsOutputFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{ColumnarWriteTaskStatsTracker, GpuWriteTaskStatsTracker}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A factory that produces [[ColumnarOutputWriter]]s.  A new [[ColumnarOutputWriterFactory]] is
 * created on the driver side, and then gets serialized to executor side to create
 * [[ColumnarOutputWriter]]s. This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.OutputWriterFactory`.
 */
abstract class ColumnarOutputWriterFactory extends Serializable {
  /** Returns the default partition flush size in bytes, format specific */
  def partitionFlushSize(context: TaskAttemptContext): Long = 128L * 1024L * 1024L // 128M

  /** Returns the file extension to be used when writing files out. */
  def getFileExtension(context: TaskAttemptContext): String

  /**
   * When writing to a `org.apache.spark.sql.execution.datasources.HadoopFsRelation`, this method
   * gets called by each task on executor side to instantiate new [[ColumnarOutputWriter]]s.
   *
   * @param path Path to write the file.
   * @param dataSchema Schema of the columnar data to be written. Partition columns are not
   *                   included in the schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   */
  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
      debugOutputPath: Option[String],
      fileIO: RapidsFileIO): ColumnarOutputWriter
}

/**
 * This is used to write columnar data to a file system. Subclasses of [[ColumnarOutputWriter]]
 * must provide a zero-argument constructor. This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.OutputWriter`.
 */
abstract class ColumnarOutputWriter(context: TaskAttemptContext,
    dataSchema: StructType,
    nvtxId: NvtxId,
    includeRetry: Boolean,
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
    debugDumpPath: Option[String],
    holdGpuBetweenBatches: Boolean = false,
    useAsyncWrite: Boolean = false,
    rapidsFileIO: RapidsFileIO) extends HostBufferConsumer with Logging {

  // Length of the file written so far. This is used to track the size of the file
  private var fileLength: Long = 0L

  /** Returns length of the file written so far. */
  def getFileLength: Long = fileLength

  protected val tableWriter: TableWriter
  private lazy val debugDumpOutputStream: Option[OutputStream] = try {
    debugDumpPath.map { path =>
      val tc = TaskContext.get()
      logWarning(s"DEBUG FILE OUTPUT ${nvtxId.name} FOR " +
        s"STAGE ${tc.stageId()} TASK ${tc.taskAttemptId()} is $path")
      val hadoopPath = new Path(path)
      val fs = hadoopPath.getFileSystem(conf)
      new DataOutputStream(new BufferedOutputStream(fs.create(hadoopPath, false)))
    }
  } catch {
    case e: Exception =>
      logError(s"Could Not Write Debug Table $debugDumpPath", e)
      None
  }

  /**
   * Write out a debug batch to the debug output stream if it is configured.
   * If it is not configured, this is a noop. If an exception happens the exception
   * is ignored, but it is logged.
   */
  private def debugWriteBatch(batch: ColumnarBatch): Unit = {
    debugDumpOutputStream.foreach { output =>
      try {
        withResource(GpuColumnVector.from(batch)) { table =>
          JCudfSerialization.writeToStream(table, output, 0, table.getRowCount)
        }
        output.flush()
      } catch {
        case t: Throwable =>
          logError(s"Could Not Write Debug Table $debugDumpPath", t)
      }
    }
  }

  protected val conf: Configuration = context.getConfiguration

  private val trafficController: TrafficController = TrafficController.getWriteInstance

  private def openOutputFile(): RapidsOutputFile = {
    rapidsFileIO.newOutputFile(path())
  }

  // This is implemented as a method to make it easier to subclass
  // ColumnarOutputWriter in the tests, and override this behavior.
  protected def getOutputStream: OutputStream = {
    if (useAsyncWrite) {
      logWarning("Async output write enabled")
      AsyncOutputStream(() => openOutputFile().create(false), trafficController, statsTrackers)
    } else {
      openOutputFile().create(false)
    }
  }

  protected val outputStream: OutputStream = getOutputStream

  private[this] val tempBuffer = new Array[Byte](128 * 1024)
  private[this] var anythingWritten = false
  private[this] val buffers = mutable.Queue[(HostMemoryBuffer, Long)]()

  override
  def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
    buffers += Tuple2(buffer, len)
    fileLength += len
  }

  def writeBufferedData(): Long = {
    val start = System.nanoTime()
    ColumnarOutputWriter.writeBufferedData(buffers, tempBuffer, outputStream)
    System.nanoTime() - start
  }

  def dropBufferedData(): Unit = buffers.dequeueAll {
    case (buffer, _) =>
      buffer.close()
      true
  }

  private[this] def updateStatistics(
      writeStartTime: Long,
      gpuTime: Long,
      writeIOTime: Long): Unit = {
    // Update statistics
    val writeTime = System.nanoTime - writeStartTime - gpuTime
    statsTrackers.foreach {
      case gpuTracker: GpuWriteTaskStatsTracker =>
        gpuTracker.addWriteTime(writeTime)
        gpuTracker.addGpuTime(gpuTime)
        gpuTracker.addWriteIOTime(writeIOTime)
      case _ =>
    }
  }

  protected def throwIfRebaseNeededInExceptionMode(batch: ColumnarBatch): Unit = {
    // NOOP for now, but allows a child to override this
  }


  /**
   * Persists a columnar batch. Invoked on the executor side. When writing to dynamically
   * partitioned tables, dynamic partition columns are not included in columns to be written.
   *
   * NOTE: This method will close `spillableBatch`. We do this because we want
   * to free GPU memory after the GPU has finished encoding the data but before
   * it is written to the distributed filesystem. The GPU semaphore is released
   * during the distributed filesystem transfer to allow other tasks to start/continue
   * GPU processing.
   */
  def writeSpillableAndClose(spillableBatch: SpillableColumnarBatch): Long = {
    val writeStartTime = System.nanoTime
    closeOnExcept(spillableBatch) { _ =>
      val cb = withRetryNoSplit[ColumnarBatch] {
        spillableBatch.getColumnarBatch()
      }
      // run pre-flight checks and update stats
      withResource(cb) { _ =>
        throwIfRebaseNeededInExceptionMode(cb)
        // NOTE: it is imperative that `newBatch` is not in a retry block.
        // Otherwise it WILL corrupt writers that generate metadata in this method (like delta)
        statsTrackers.foreach(_.newBatch(path(), cb))
      }
    }
    val gpuTime = if (includeRetry) {
      //TODO: we should really apply the transformations to cast timestamps
      // to the expected types before spilling but we need a SpillableTable
      // rather than a SpillableColumnBatch to be able to do that
      // See https://github.com/NVIDIA/spark-rapids/issues/8262
      withRetry(spillableBatch, splitSpillableInHalfByRows) { attempt =>
        withRestoreOnRetry(checkpointRestore) {
          bufferBatchAndClose(attempt.getColumnarBatch())
        }
      }.sum
    } else {
      withResource(spillableBatch) { _ =>
        bufferBatchAndClose(spillableBatch.getColumnarBatch())
      }
    }
    // we successfully buffered to host memory, release the semaphore and write
    // the buffered data to the FS
    if (!holdGpuBetweenBatches) {
      logDebug("Releasing semaphore between batches")
      GpuSemaphore.releaseIfNecessary(TaskContext.get)
    }

    val ioTime = writeBufferedData()
    updateStatistics(writeStartTime, gpuTime, ioTime)
    spillableBatch.numRows()
  }

  // protected for testing
  protected[this] def bufferBatchAndClose(batch: ColumnarBatch): Long = {
    val startTimestamp = System.nanoTime
    nvtxId {
      withResource(transformAndClose(batch)) { maybeTransformed =>
        encodeAndBufferToHost(maybeTransformed)
      }
    }
    // time spent on GPU encoding to the host sink
    System.nanoTime - startTimestamp
  }

  /** Apply any necessary casts before writing batch out */
  def transformAndClose(cb: ColumnarBatch): ColumnarBatch = cb

  private val checkpointRestore = new Retryable {
    override def checkpoint(): Unit = ()
    override def restore(): Unit = dropBufferedData()
  }

  private def encodeAndBufferToHost(batch: ColumnarBatch): Unit = {
    withResource(GpuColumnVector.from(batch)) { table =>
      // `anythingWritten` is set here as an indication that there was data at all
      // to write, even if the `tableWriter.write` method fails. If we fail to write
      // and the task fails, any output is going to be discarded anyway, so no data
      // corruption to worry about. Otherwise, we should retry (OOM case).
      // If we have nothing to write, we won't flip this flag to true and we will
      // buffer an empty batch on close() to work around issues in cuDF
      // where corrupt files can be written if nothing is encoded via the writer.
      anythingWritten = true

      debugWriteBatch(batch)
      // tableWriter.write() serializes the table into the HostMemoryBuffer, and buffers it
      // by calling handleBuffer() on the ColumnarOutputWriter. It may not write to the
      // output stream just yet.
      tableWriter.write(table)
    }
  }

  /**
   * Closes the [[ColumnarOutputWriter]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  def close(): Unit = {
    if (!anythingWritten) {
      // This prevents writing out bad files
      bufferBatchAndClose(GpuColumnVector.emptyBatch(dataSchema))
    }
    tableWriter.close()
    GpuSemaphore.releaseIfNecessary(TaskContext.get())
    writeBufferedData()
    outputStream.close()
    debugDumpOutputStream.foreach { os =>
      os.close()
    }
  }

  /**
   * The file path to write. Invoked on the executor side.
   */
  def path(): String
}

object ColumnarOutputWriter {
  // write buffers to outputStream via tempBuffer and close buffers
  def writeBufferedData(buffers: mutable.Queue[(HostMemoryBuffer, Long)],
      tempBuffer: Array[Byte], outputStream: OutputStream): Unit = {
    val toProcess = buffers.dequeueAll(_ => true)
    try {
      toProcess.foreach { case (buffer, len) =>
        var offset: Long = 0
        var left = len
        while (left > 0) {
          val toCopy = math.min(tempBuffer.length, left).toInt
          buffer.getBytes(tempBuffer, 0, offset, toCopy)
          outputStream.write(tempBuffer, 0, toCopy)
          left = left - toCopy
          offset = offset + toCopy
        }
      }
    } finally {
      toProcess.map { case (buffer, _) => buffer }.safeClose()
    }
  }
}
