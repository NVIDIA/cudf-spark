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

package org.apache.spark.shuffle.sort.io

import java.io.{File, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.util.Optional

import com.nvidia.spark.rapids.{HostAlloc, RapidsConf}
import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter, WritableByteChannelWrapper}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.util.Utils

/**
 * RAPIDS-optimized ShuffleMapOutputWriter that writes to host memory first
 * (if sufficient memory is available), then spills to disk if needed.
 * Particularly useful for partial sorted files in multi-batch scenarios.
 */
class RapidsLocalDiskShuffleMapOutputWriter(
    shuffleId: Int,
    mapId: Long,
    numPartitions: Int,
    blockResolver: IndexShuffleBlockResolver,
    sparkConf: SparkConf)
  extends ShuffleMapOutputWriter with Logging {

  private val partitionLengths = new Array[Long](numPartitions)
  private var lastPartitionId = -1
  private var currChannelPosition = 0L
  private var bytesWrittenToMergedFile = 0L

  private val outputFile = blockResolver.getDataFile(shuffleId, mapId)
  private var outputTempFile: File = null

  // RAPIDS configuration
  private val rapidsConf = new RapidsConf(sparkConf)
  private val initialBufferSize = rapidsConf.partialFileBufferInitialSize
  private val maxBufferSize = rapidsConf.partialFileBufferMaxSize
  private val memoryThreshold = rapidsConf.partialFileBufferMemoryThreshold

  // Read Spark's shuffle sync configuration to maintain compatibility
  private val syncWrites = sparkConf.get("spark.shuffle.sync", "false").toBoolean

  // RAPIDS optimization: use SpillablePartialFileHandle for unified storage
  private var partialFileHandle: Option[SpillablePartialFileHandle] = None
  private var storageInitAttempted: Boolean = false
  private var forceFileOnly: Boolean = false

  // Track completed partition count for predictive buffer sizing
  private var completedPartitionCount: Int = 0
  private var completedPartitionBytes: Long = 0L

  /**
   * Provides capacity hints for buffer expansion based on partition write statistics.
   *
   * The prediction logic:
   * - If we have completed at least one partition, calculate average bytes per partition
   * - Estimate total bytes needed as: avgBytesPerPartition * numPartitions
   * - Add a safety margin (1.2x) to account for partition size variance
   * - If no partitions completed yet, fall back to doubling the required capacity
   *
   * @param currentBytesWritten Total bytes written so far (we use completed partition
   *                            statistics instead for more accurate prediction)
   * @param requiredCapacity The minimum capacity needed to continue writing
   * @return Suggested new capacity based on prediction
   */
  private def capacityHintProvider(
      currentBytesWritten: Long,
      requiredCapacity: Long): Long = {
    // Note: currentBytesWritten is unused - we use completedPartitionBytes instead
    // for more accurate prediction based on completed partitions only
    val _ = currentBytesWritten
    if (completedPartitionCount > 0) {
      // Calculate average bytes per completed partition
      val avgBytesPerPartition = completedPartitionBytes.toDouble / completedPartitionCount
      // Estimate total needed with 1.2x safety margin for partition size variance
      val estimatedTotal = (avgBytesPerPartition * numPartitions * 1.2).toLong
      // Return the larger of estimated total or required capacity
      val suggested = math.max(estimatedTotal, requiredCapacity)
      logDebug(s"Capacity hint: completedPartitions=$completedPartitionCount, " +
        s"completedBytes=$completedPartitionBytes, avgPerPartition=$avgBytesPerPartition, " +
        s"totalPartitions=$numPartitions, estimated=$estimatedTotal, suggested=$suggested")
      suggested
    } else {
      // No completed partitions yet, use simple doubling strategy
      // This happens during the first partition write
      val suggested = requiredCapacity * 2
      logDebug(s"Capacity hint: no completed partitions yet, " +
        s"suggesting $suggested (2x required=$requiredCapacity)")
      suggested
    }
  }

  /**
   * Force this writer to use file-only mode, bypassing memory-based buffering.
   * This is useful for scenarios where memory buffering is not beneficial,
   * such as final merge operations.
   *
   * This method must be called before any partition writer is requested.
   */
  def setForceFileOnlyMode(): Unit = {
    if (storageInitAttempted) {
      throw new IllegalStateException(
        "Cannot set force file-only mode after storage has been initialized. " +
          "Storage was initialized when getPartitionWriter() was called. " +
          "Call setForceFileOnlyMode() before requesting any partition writers.")
    }
    forceFileOnly = true
  }

  // Try to initialize storage on first partition write
  private def ensureStorageInitialized(): Unit = {
    if (!storageInitAttempted) {
      storageInitAttempted = true
      outputTempFile = Utils.tempFileWith(outputFile)

      // Check if file-only mode is forced
      if (forceFileOnly) {
        // Force file-only mode (e.g., for final merge operations)
        logDebug(s"Using forced file-only mode for shuffle $shuffleId map $mapId")
        val handle = SpillablePartialFileHandle.createFileOnly(
          outputTempFile, syncWrites)
        partialFileHandle = Some(handle)
      } else if (HostAlloc.isUsageBelowThreshold(memoryThreshold)) {
        // Memory sufficient: use MEMORY_WITH_SPILL mode with predictive sizing
        try {
          val handle = SpillablePartialFileHandle.createMemoryWithSpill(
            initialCapacity = initialBufferSize,
            maxBufferSize = maxBufferSize,
            memoryThreshold = memoryThreshold,
            spillFile = outputTempFile,
            priority = Long.MinValue,
            syncWrites = syncWrites,
            capacityHintProvider = Some(capacityHintProvider))
          partialFileHandle = Some(handle)
          logDebug(s"Using memory-with-spill mode for shuffle $shuffleId map $mapId " +
            s"with predictive sizing (initial=${initialBufferSize / 1024 / 1024}MB, " +
            s"max=${maxBufferSize / 1024 / 1024}MB, numPartitions=$numPartitions)")
        } catch {
          case e: Exception =>
            logWarning(s"Failed to create memory buffer, " +
              s"falling back to file-only", e)
            val handle = SpillablePartialFileHandle.createFileOnly(
              outputTempFile, syncWrites)
            partialFileHandle = Some(handle)
        }
      } else {
        // Memory scarce: use FILE_ONLY mode
        logDebug(s"Host memory usage high, using file-only mode for shuffle " +
          s"$shuffleId map $mapId")
        val handle = SpillablePartialFileHandle.createFileOnly(
          outputTempFile, syncWrites)
        partialFileHandle = Some(handle)
      }
    }
  }

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    if (reducePartitionId <= lastPartitionId) {
      throw new IllegalArgumentException(
        "Partitions should be requested in increasing order.")
    }
    lastPartitionId = reducePartitionId

    // Initialize storage on first partition
    ensureStorageInitialized()

    // Record current position for partition length calculation
    currChannelPosition = partialFileHandle.map(_.getTotalBytesWritten).getOrElse(0L)
    new RapidsPartitionWriter(reducePartitionId)
  }

  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = {
    // Finish write phase to enable spilling and finalize data
    partialFileHandle.foreach { handle =>
      handle.finishWrite()

      // If memory-based and not spilled yet, force spill to create file
      // writeMetadataFileAndCommit requires a valid file
      if (handle.isMemoryBased && !handle.isSpilled) {
        handle.spillForCommit()
      }
    }

    val resolvedTmp = if (outputTempFile != null && outputTempFile.isFile) {
      outputTempFile
    } else {
      null
    }

    logDebug(s"Writing shuffle index file for mapId $mapId with length " +
      s"${partitionLengths.length}")
    blockResolver.writeMetadataFileAndCommit(
      shuffleId, mapId, partitionLengths, checksums, resolvedTmp)

    // Close the partial file handle to release any remaining resources
    // (e.g., host buffer if spill() was not called due to empty partitions)
    partialFileHandle.foreach(_.close())
    partialFileHandle = None

    MapOutputCommitMessage.of(partitionLengths)
  }

  override def abort(error: Throwable): Unit = {
    partialFileHandle.foreach(_.close())
    partialFileHandle = None
    if (outputTempFile != null && outputTempFile.exists() && !outputTempFile.delete()) {
      logWarning(s"Failed to delete temporary shuffle file at " +
        s"${outputTempFile.getAbsolutePath}")
    }
  }

  /**
   * Get the partial file handle for accessing data.
   */
  def getPartialFileHandle(): Option[SpillablePartialFileHandle] = partialFileHandle

  /**
   * Get partition lengths array directly (for extracting without reflection).
   */
  def getPartitionLengths(): Array[Long] = partitionLengths

  /**
   * Finish write phase to finalize data (called before extraction).
   */
  def finishWritePhase(): Unit = {
    partialFileHandle.foreach(_.finishWrite())
  }

  private class RapidsPartitionWriter(partitionId: Int) extends ShufflePartitionWriter {
    private var partStream: OutputStream = null
    private var partChannel: WritableByteChannelWrapper = null

    override def openStream(): OutputStream = {
      if (partStream == null) {
        partStream = new PartitionWriterStream(partitionId)
      }
      partStream
    }

    override def openChannelWrapper(): Optional[WritableByteChannelWrapper] = {
      if (partChannel == null) {
        partChannel = new PartitionWriterChannel(partitionId)
      }
      Optional.of(partChannel)
    }

    override def getNumBytesWritten(): Long = {
      if (partChannel != null) {
        partChannel.asInstanceOf[PartitionWriterChannel].getCount
      } else if (partStream != null) {
        partStream.asInstanceOf[PartitionWriterStream].getCount()
      } else {
        0L
      }
    }
  }

  // Unified stream writer using SpillablePartialFileHandle
  private class PartitionWriterStream(partitionId: Int) extends OutputStream {
    private var count = 0L
    private var isClosed = false

    def getCount(): Long = count

    override def write(b: Int): Unit = {
      verifyNotClosed()
      partialFileHandle.foreach(_.write(b))
      count += 1
    }

    override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
      verifyNotClosed()
      partialFileHandle.foreach(_.write(buf, pos, length))
      count += length
    }

    override def close(): Unit = {
      isClosed = true
      partitionLengths(partitionId) = count
      bytesWrittenToMergedFile += count
      // Update statistics for predictive buffer sizing
      completedPartitionCount += 1
      completedPartitionBytes += count
    }

    private def verifyNotClosed(): Unit = {
      if (isClosed) {
        throw new IllegalStateException(
          "Attempting to write to a closed block output stream.")
      }
    }
  }

  // Unified channel writer using SpillablePartialFileHandle
  private class PartitionWriterChannel(partitionId: Int)
    extends WritableByteChannelWrapper {

    private val startPosition = currChannelPosition

    def getCount: Long = {
      partialFileHandle.map(_.getTotalBytesWritten).getOrElse(0L) - startPosition
    }

    override def channel(): WritableByteChannel = new WritableByteChannel {
      private var channelOpen = true

      override def write(src: ByteBuffer): Int = {
        if (!channelOpen) {
          throw new IOException("Channel is closed")
        }
        val remaining = src.remaining()
        val temp = new Array[Byte](remaining)
        src.get(temp)
        partialFileHandle.foreach(_.write(temp, 0, remaining))
        remaining
      }

      override def isOpen: Boolean = channelOpen

      override def close(): Unit = {
        channelOpen = false
      }
    }

    override def close(): Unit = {
      val bytesWritten = getCount
      partitionLengths(partitionId) = bytesWritten
      bytesWrittenToMergedFile += bytesWritten
      // Update statistics for predictive buffer sizing
      completedPartitionCount += 1
      completedPartitionBytes += bytesWritten
    }
  }
}

