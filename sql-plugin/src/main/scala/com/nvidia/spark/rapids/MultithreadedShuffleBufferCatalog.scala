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

package com.nvidia.spark.rapids

import java.io.{InputStream, IOException}
import java.lang.{Boolean => JBoolean}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.util.HashSet
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import _root_.io.netty.handler.stream.ChunkedStream
import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.AbstractFileRegion
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

/**
 * A segment of data within a SpillablePartialFileHandle.
 * 
 * @param handle the partial file handle containing the data
 * @param offset starting offset within the handle
 * @param length number of bytes in this segment
 */
case class PartitionSegment(
    handle: SpillablePartialFileHandle,
    offset: Long,
    length: Long)

/**
 * Catalog for managing shuffle data in MULTITHREADED mode without merging.
 * 
 * Instead of merging partial files into a single shuffle file, this catalog
 * stores references to segments within partial files. When a reducer requests
 * a shuffle block, the catalog dynamically assembles the data from all
 * relevant segments.
 * 
 * This approach avoids the I/O cost of merging. The data may be kept in memory
 * (MEMORY_WITH_SPILL mode) or stored directly on disk (ONLY_FILE mode) depending
 * on memory pressure - both modes work with this skip-merge design.
 */
class MultithreadedShuffleBufferCatalog extends Logging {

  /**
   * Map from ShuffleBlockId to list of segments.
   * A partition may have multiple segments if there are multiple batches.
   */
  private val partitionSegments = 
    new ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[PartitionSegment]]()

  /** Track active shuffles for cleanup */
  private val activeShuffles = new ConcurrentHashMap[Int, JBoolean]()

  /**
   * Register a shuffle as active.
   * Must be called before adding any partitions for this shuffle.
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffles.put(shuffleId, true)
  }

  /**
   * Add a partition segment to the catalog.
   * 
   * @param shuffleId shuffle identifier
   * @param mapId map task identifier
   * @param partitionId reduce partition identifier
   * @param handle the partial file handle containing the data
   * @param offset starting offset within the handle
   * @param length number of bytes for this partition
   */
  def addPartition(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      handle: SpillablePartialFileHandle,
      offset: Long,
      length: Long): Unit = {
    if (length <= 0) {
      return // Skip empty partitions
    }

    val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    val segment = PartitionSegment(handle, offset, length)

    partitionSegments.compute(blockId, (_, existing) => {
      val segments = if (existing == null) new ArrayBuffer[PartitionSegment]() else existing
      segments += segment
      segments
    })
  }

  /**
   * Check if the catalog has data for a given block.
   */
  def hasData(blockId: ShuffleBlockId): Boolean = {
    partitionSegments.containsKey(blockId)
  }

  /**
   * Check if a shuffle is being managed by this catalog.
   */
  def hasActiveShuffle(shuffleId: Int): Boolean = {
    activeShuffles.containsKey(shuffleId)
  }

  /**
   * Get all active shuffle IDs.
   * Used during executor shutdown to clean up all remaining shuffles.
   */
  def getActiveShuffleIds: Seq[Int] = {
    import scala.collection.JavaConverters._
    activeShuffles.keySet().asScala.map(_.intValue()).toSeq
  }

  /**
   * Get a ManagedBuffer that reads data from all segments for a block.
   * The buffer dynamically assembles data from multiple partial files if needed.
   */
  def getMergedBuffer(blockId: ShuffleBlockId): ManagedBuffer = {
    val segments = partitionSegments.get(blockId)
    if (segments == null || segments.isEmpty) {
      throw new IllegalArgumentException(s"No data found for block $blockId")
    }

    new MultiBatchManagedBuffer(segments.toSeq)
  }

  /**
   * Get a ManagedBuffer for a batch of shuffle blocks (used in batch fetch optimization).
   * This method handles ShuffleBlockBatchId which represents multiple reduce partitions.
   */
  def getMergedBatchBuffer(batchId: ShuffleBlockBatchId): ManagedBuffer = {
    val allSegments = new ArrayBuffer[PartitionSegment]()

    for (reduceId <- batchId.startReduceId until batchId.endReduceId) {
      val blockId = ShuffleBlockId(batchId.shuffleId, batchId.mapId, reduceId)
      val segments = partitionSegments.get(blockId)
      if (segments != null) {
        allSegments ++= segments
      }
    }

    if (allSegments.isEmpty) {
      throw new IllegalArgumentException(s"No data found for batch block $batchId")
    }

    new MultiBatchManagedBuffer(allSegments.toSeq)
  }

  /**
   * Unregister a shuffle and clean up all associated data.
   *
   * @param shuffleId the shuffle ID to unregister
   * @return optional cleanup statistics (None if this catalog has no data for the shuffle)
   */
  def unregisterShuffle(shuffleId: Int): Option[ShuffleCleanupStats] = {
    activeShuffles.remove(shuffleId)

    // Find and remove all blocks for this shuffle
    val iterator = partitionSegments.keySet().iterator()
    val toRemove = new ArrayBuffer[ShuffleBlockId]()
    while (iterator.hasNext) {
      val blockId = iterator.next()
      if (blockId.shuffleId == shuffleId) {
        toRemove += blockId
      }
    }

    // Collect unique handles and gather statistics before closing
    val closedHandles = new HashSet[SpillablePartialFileHandle]()
    var bytesFromMemory = 0L
    var bytesFromDisk = 0L
    var numExpansions = 0
    var numSpills = 0
    var numForcedFileOnly = 0

    toRemove.foreach { blockId =>
      val segments = partitionSegments.remove(blockId)
      if (segments != null) {
        segments.foreach { segment =>
          // Only process each handle once (multiple partitions may share a handle)
          if (!closedHandles.contains(segment.handle)) {
            closedHandles.add(segment.handle)

            // Collect statistics before closing
            val handle = segment.handle
            val totalBytes = handle.getTotalBytesWritten
            if (handle.isMemoryBased && !handle.isSpilled) {
              bytesFromMemory += totalBytes
            } else {
              bytesFromDisk += totalBytes
            }

            // Collect behavior counters
            numExpansions += handle.getExpansionCount
            numSpills += handle.getSpillCount
            if (handle.isFileOnly) {
              numForcedFileOnly += 1
            }

            try {
              handle.close()
            } catch {
              case e: Exception =>
                logError(s"Failed to close handle for shuffle $shuffleId", e)
            }
          }
        }
      }
    }

    logDebug(s"Unregistered shuffle $shuffleId: closed ${closedHandles.size()} handles, " +
      s"bytesFromMemory=$bytesFromMemory, bytesFromDisk=$bytesFromDisk, " +
      s"numExpansions=$numExpansions, numSpills=$numSpills, numForcedFileOnly=$numForcedFileOnly")

    // Return statistics if we had any data
    if (bytesFromMemory > 0 || bytesFromDisk > 0 ||
        numExpansions > 0 || numSpills > 0 || numForcedFileOnly > 0) {
      Some(ShuffleCleanupStats(shuffleId, bytesFromMemory, bytesFromDisk,
        numExpansions, numSpills, numForcedFileOnly))
    } else {
      None
    }
  }
}

/**
 * A ManagedBuffer that reads data from multiple partition segments.
 * 
 * This buffer dynamically assembles data from multiple SpillablePartialFileHandle
 * segments when createInputStream() is called. Each segment may be in memory or
 * on disk, and the buffer handles both cases transparently.
 */
class MultiBatchManagedBuffer(segments: Seq[PartitionSegment]) extends ManagedBuffer {

  override def size(): Long = segments.map(_.length).sum

  override def nioByteBuffer(): ByteBuffer = {
    // This method loads all data into memory. It's required by the ManagedBuffer interface
    // but is NOT used in the network transfer path - Spark's network layer uses
    // convertToNetty() which returns our streaming MultiSegmentFileRegion.
    // This method may be called by other code paths (e.g., local block reading).
    val totalSize = size().toInt
    val buffer = ByteBuffer.allocate(totalSize)
    val bytes = new Array[Byte](8192) // Read buffer

    segments.foreach { segment =>
      var remaining = segment.length
      var position = segment.offset
      while (remaining > 0) {
        val toRead = math.min(remaining, bytes.length).toInt
        val bytesRead = segment.handle.readAt(position, bytes, 0, toRead)
        if (bytesRead <= 0) {
          throw new IOException(
            s"Unexpected EOF reading segment at position $position, " +
            s"expected ${segment.length} bytes")
        }
        buffer.put(bytes, 0, bytesRead)
        position += bytesRead
        remaining -= bytesRead
      }
    }

    buffer.flip()
    buffer
  }

  override def createInputStream(): InputStream = {
    new MultiSegmentInputStream(segments)
  }

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this

  override def convertToNetty(): AnyRef = {
    // Return a custom FileRegion that streams data in chunks, avoiding loading all
    // data into memory at once. This addresses concerns about large shuffle blocks.
    new MultiSegmentFileRegion(segments)
  }

  // Spark 4.0+ adds convertToNettyForSsl() abstract method.
  // We provide this method for Spark 4.0+ compatibility. In Spark 3.x, this is just
  // a regular method (parent class doesn't have it). In Spark 4.0+, this overrides
  // the abstract method.
  //
  // SSL mode cannot use FileRegion (zero-copy) because data must be encrypted.
  // Return ChunkedStream for streaming encryption, consistent with Spark's
  // FileSegmentManagedBuffer.convertToNettyForSsl() implementation.
  // Chunk size 64KB matches Spark's default (spark.network.ssl.maxEncryptedBlockSize).
  def convertToNettyForSsl(): AnyRef = {
    new ChunkedStream(createInputStream(), 64 * 1024)
  }
}

/**
 * An InputStream that reads from multiple partition segments sequentially.
 */
class MultiSegmentInputStream(segments: Seq[PartitionSegment]) extends InputStream {

  private var currentSegmentIndex: Int = 0
  private var currentPosition: Long = if (segments.nonEmpty) segments.head.offset else 0
  private var bytesReadInCurrentSegment: Long = 0

  override def read(): Int = {
    val buf = new Array[Byte](1)
    val n = read(buf, 0, 1)
    if (n == -1) -1 else buf(0) & 0xFF
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    // Use loop instead of recursion to avoid StackOverflowError with many segments
    while (currentSegmentIndex < segments.size) {
      val segment = segments(currentSegmentIndex)
      val remainingInSegment = segment.length - bytesReadInCurrentSegment

      if (remainingInSegment <= 0) {
        // Move to next segment
        currentSegmentIndex += 1
        if (currentSegmentIndex < segments.size) {
          currentPosition = segments(currentSegmentIndex).offset
          bytesReadInCurrentSegment = 0
        }
        // Continue loop to try next segment
      } else {
        val toRead = math.min(len, remainingInSegment).toInt
        val bytesRead = segment.handle.readAt(currentPosition, b, off, toRead)

        if (bytesRead > 0) {
          currentPosition += bytesRead
          bytesReadInCurrentSegment += bytesRead
        }

        return bytesRead
      }
    }

    -1 // EOF - all segments exhausted
  }

  override def available(): Int = {
    if (currentSegmentIndex >= segments.size) {
      0
    } else {
      val remaining = segments.drop(currentSegmentIndex).map { seg =>
        if (seg == segments(currentSegmentIndex)) {
          seg.length - bytesReadInCurrentSegment
        } else {
          seg.length
        }
      }.sum
      math.min(remaining, Int.MaxValue).toInt
    }
  }

  /**
   * Close is a no-op because the underlying SpillablePartialFileHandle resources
   * are managed by MultithreadedShuffleBufferCatalog and will be closed when
   * the shuffle is unregistered.
   */
  override def close(): Unit = {
    // No-op: handles are managed by MultithreadedShuffleBufferCatalog
  }
}

/**
 * A FileRegion implementation that streams data from multiple partition segments.
 *
 * This class enables network transfer of shuffle data by reading from segments
 * via readAt() and writing to the target channel. Data is read in chunks (64KB)
 * to limit memory usage during transfer.
 *
 * Spark's MessageWithHeader only accepts ByteBuf or FileRegion. By implementing
 * FileRegion, we can provide streaming transfer while remaining compatible with
 * Spark's network layer.
 */
class MultiSegmentFileRegion(segments: Seq[PartitionSegment]) extends AbstractFileRegion {

  private val totalSize: Long = segments.map(_.length).sum
  private var totalTransferred: Long = 0

  // Buffer size for each transferTo call (64KB chunks)
  private val CHUNK_SIZE = 64 * 1024

  // Reusable buffer for reading data (avoids allocation per transferTo call)
  private val readBuffer = new Array[Byte](CHUNK_SIZE)

  // Track current position within the logical data stream
  private var currentSegmentIndex: Int = 0
  private var bytesTransferredInCurrentSegment: Long = 0

  override def count(): Long = totalSize

  override def position(): Long = 0

  override def transferred(): Long = totalTransferred

  /**
   * Transfer data to the target channel in chunks.
   *
   * This method reads data from segments using readAt() and writes to the channel.
   * Each call transfers up to CHUNK_SIZE bytes.
   *
   * @param target the channel to write data to
   * @param position the current transfer position (should equal totalTransferred)
   * @return the number of bytes transferred in this call
   */
  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    if (position != totalTransferred) {
      throw new IllegalArgumentException(
        s"Invalid position: expected $totalTransferred but got $position")
    }

    if (totalTransferred >= totalSize) {
      return 0 // All data transferred
    }

    // Find the current segment and read data
    while (currentSegmentIndex < segments.size) {
      val segment = segments(currentSegmentIndex)
      val remainingInSegment = segment.length - bytesTransferredInCurrentSegment

      if (remainingInSegment <= 0) {
        // Move to next segment
        currentSegmentIndex += 1
        bytesTransferredInCurrentSegment = 0
      } else {
        // Read from current segment using readAt
        val toRead = math.min(remainingInSegment, CHUNK_SIZE).toInt
        val handlePosition = segment.offset + bytesTransferredInCurrentSegment
        val bytesRead = segment.handle.readAt(handlePosition, readBuffer, 0, toRead)

        if (bytesRead > 0) {
          // Write to target channel
          val writeBuffer = ByteBuffer.wrap(readBuffer, 0, bytesRead)
          var written = 0
          while (writeBuffer.hasRemaining) {
            val w = target.write(writeBuffer)
            if (w < 0) {
              throw new IOException("Failed to write to target channel")
            }
            written += w
          }

          bytesTransferredInCurrentSegment += written
          totalTransferred += written
          return written
        } else if (bytesRead < 0) {
          throw new IOException(
            s"Unexpected EOF reading segment at position $handlePosition")
        }
      }
    }

    0 // All segments exhausted
  }

  override protected def deallocate(): Unit = {
    // No resources to release - handles are managed by MultithreadedShuffleBufferCatalog
  }
}
