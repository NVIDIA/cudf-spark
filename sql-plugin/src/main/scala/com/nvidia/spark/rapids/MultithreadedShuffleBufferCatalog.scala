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
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.util.HashSet
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import _root_.io.netty.handler.stream.ChunkedStream
import com.nvidia.spark.rapids.spill.{ClosedPartialFileHandleException, SpillablePartialFileHandle}

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.AbstractFileRegion
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}

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
 * Owns a temporary read lease on one or more partial shuffle file handles.
 *
 * A lease is held while a buffer, stream, or file region may still read the handles. Closing the
 * lease releases every handle exactly once; each handle defers its physical close until its last
 * lease is released (see `SpillablePartialFileHandle.acquireRead`/`releaseRead`).
 */
private[rapids] final class ShuffleHandleLease(handles: Seq[SpillablePartialFileHandle])
    extends AutoCloseable {
  private var released: Boolean = false

  override def close(): Unit = {
    val handlesToRelease = synchronized {
      if (released) {
        Seq.empty
      } else {
        released = true
        handles
      }
    }
    ShuffleHandleLease.releaseAll(handlesToRelease.reverseIterator)
  }
}

private[rapids] object ShuffleHandleLease {
  /** Acquire a read lease on every handle, rolling back the partial set if any acquire fails. */
  def acquire(handles: Seq[SpillablePartialFileHandle]): ShuffleHandleLease = {
    val retained = new ArrayBuffer[SpillablePartialFileHandle](handles.size)
    try {
      handles.foreach { handle =>
        handle.acquireRead()
        retained += handle
      }
      new ShuffleHandleLease(retained.toSeq)
    } catch {
      case t: Throwable =>
        try {
          releaseAll(retained.reverseIterator)
        } catch {
          case releaseFailure: Throwable =>
            t.addSuppressed(releaseFailure)
        }
        throw t
    }
  }

  private def releaseAll(handles: Iterator[SpillablePartialFileHandle]): Unit = {
    var firstFailure: Throwable = null
    handles.foreach { handle =>
      try {
        handle.releaseRead()
      } catch {
        case t: Throwable =>
          if (firstFailure == null) {
            firstFailure = t
          } else {
            firstFailure.addSuppressed(t)
          }
      }
    }
    if (firstFailure != null) {
      throw firstFailure
    }
  }
}

private[rapids] object MultithreadedShuffleBufferCatalog {
  def missingDataMessage(blockId: BlockId): String = blockId match {
    case _: ShuffleBlockBatchId => s"No data found for batch block $blockId"
    case _ => s"No data found for block $blockId"
  }
}

/**
 * All segments of one map task's output, grouped by reduce partition. It is built once and never
 * changed, so a reader that finds it sees the whole map output. Empty partitions are not stored.
 *
 * @param handles every partial file handle the output owns, including ones that hold no data
 */
final class MapOutputSegments private (
    reduceIds: Array[Int],
    segmentsByReduceId: Array[Array[PartitionSegment]],
    val handles: Seq[SpillablePartialFileHandle]) {

  def contains(reduceId: Int): Boolean = {
    java.util.Arrays.binarySearch(reduceIds, reduceId) >= 0
  }

  /** Total bytes per reduce id, which is what the map task reports in its MapStatus. */
  def partitionLengths(numPartitions: Int): Array[Long] = {
    val lengths = new Array[Long](numPartitions)
    reduceIds.indices.foreach { i =>
      lengths(reduceIds(i)) = segmentsByReduceId(i).map(_.length).sum
    }
    lengths
  }

  /** Segments of the reduce ids in [startReduceId, endReduceId), in reduce id then write order. */
  def segments(startReduceId: Int, endReduceId: Int): Seq[PartitionSegment] = {
    val found = java.util.Arrays.binarySearch(reduceIds, startReduceId)
    var i = if (found >= 0) found else -(found + 1)
    val result = new ArrayBuffer[PartitionSegment]()
    while (i < reduceIds.length && reduceIds(i) < endReduceId) {
      result ++= segmentsByReduceId(i)
      i += 1
    }
    result.toSeq
  }
}

object MapOutputSegments {
  /** Collects the segments of one map task's output. Not thread-safe. */
  final class Builder {
    // Indexed by reduce id, null where a reduce id has no segment, so build() needs no sort.
    private val segmentsByReduceId = new ArrayBuffer[ArrayBuffer[PartitionSegment]]()
    private val handles = new ArrayBuffer[SpillablePartialFileHandle]()
    private val handleSet = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[SpillablePartialFileHandle, java.lang.Boolean]())

    /** Adds a partial file whose partitions are stored back to back in reduce id order. */
    def addPartialFile(
        handle: SpillablePartialFileHandle,
        partitionLengths: Array[Long]): Builder = {
      addHandle(handle)
      var offset = 0L
      partitionLengths.indices.foreach { reduceId =>
        addSegment(reduceId, handle, offset, partitionLengths(reduceId))
        offset += partitionLengths(reduceId)
      }
      this
    }

    /** Adds one segment. An empty segment is dropped, and so is its handle. */
    private[rapids] def add(
        reduceId: Int,
        handle: SpillablePartialFileHandle,
        offset: Long,
        length: Long): Builder = {
      if (length > 0) {
        addHandle(handle)
        addSegment(reduceId, handle, offset, length)
      }
      this
    }

    def build(): MapOutputSegments = {
      val reduceIds = segmentsByReduceId.indices.filter(segmentsByReduceId(_) != null).toArray
      new MapOutputSegments(reduceIds, reduceIds.map(id => segmentsByReduceId(id).toArray),
        handles.toList)
    }

    private def addSegment(
        reduceId: Int,
        handle: SpillablePartialFileHandle,
        offset: Long,
        length: Long): Unit = {
      if (length > 0) {
        while (segmentsByReduceId.length <= reduceId) {
          segmentsByReduceId += null
        }
        if (segmentsByReduceId(reduceId) == null) {
          segmentsByReduceId(reduceId) = new ArrayBuffer[PartitionSegment]()
        }
        segmentsByReduceId(reduceId) += PartitionSegment(handle, offset, length)
      }
    }

    private def addHandle(handle: SpillablePartialFileHandle): Unit = {
      if (handleSet.add(handle)) {
        handles += handle
      }
    }
  }
}

/**
 * One registration of a shuffle. Cleanup detaches it whole and never removes an output from it,
 * so a reader that resolved it before cleanup still sees complete map outputs.
 */
private[rapids] final class ShuffleState {
  private val outputs = new ConcurrentHashMap[Long, MapOutputSegments]()
  private var closed = false

  def output(mapId: Long): MapOutputSegments = outputs.get(mapId)

  /**
   * Stores the output unless cleanup has closed this registration. If the map id already has an
   * output, the first one stays, as Spark keeps the first committed attempt of a map. Returns the
   * output kept for the map id, or None if closed. The caller closes an output that is not kept.
   */
  def tryPublish(mapId: Long, output: MapOutputSegments): Option[MapOutputSegments] =
    synchronized {
      if (closed) None else Some(Option(outputs.putIfAbsent(mapId, output)).getOrElse(output))
    }

  /** Refuses any later publish and returns the outputs this registration holds. */
  def close(): Seq[MapOutputSegments] = synchronized {
    import scala.collection.JavaConverters._
    closed = true
    outputs.values().asScala.toList
  }
}

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

  /** Registered shuffles. unregisterShuffle detaches a registration whole. */
  private val shuffles = new ConcurrentHashMap[Int, ShuffleState]()

  /**
   * Register a shuffle as active. Must be called before publishing any map output for it.
   * Registering again keeps the current registration; after cleanup it starts a new, empty one.
   */
  def registerShuffle(shuffleId: Int): Unit = {
    shuffles.computeIfAbsent(shuffleId, _ => new ShuffleState)
  }

  /**
   * Publish one map task's output. Returns the output the catalog keeps for the map id, which is
   * an earlier attempt's if one was already published, or None if the shuffle is no longer
   * registered. The catalog owns the output's handles either way and closes them if not kept.
   */
  def publishMapOutput(
      shuffleId: Int,
      mapId: Long,
      output: MapOutputSegments): Option[MapOutputSegments] = {
    // If cleanup closed the registration found first, a map task may have registered it again.
    val kept = tryPublish(shuffleId, mapId, output).orElse(tryPublish(shuffleId, mapId, output))
    if (!kept.exists(_ eq output)) {
      if (kept.isEmpty) {
        logInfo(s"Discarding output of map $mapId: shuffle $shuffleId is no longer registered")
      }
      closeHandles(shuffleId, output.handles)
    }
    kept
  }

  /**
   * Publish a map task's output for the writer, which reports the returned lengths in its
   * MapStatus: the lengths of the output the catalog keeps, an earlier attempt's if one exists.
   *
   * @throws IllegalStateException if the shuffle was cleaned up before the publish, so the task
   *                               is retried instead of advertising data the catalog discarded
   */
  def publishMapOutputOrFail(
      shuffleId: Int,
      mapId: Long,
      output: MapOutputSegments,
      numPartitions: Int): Array[Long] = {
    val kept = publishMapOutput(shuffleId, mapId, output).getOrElse {
      throw new IllegalStateException(s"Shuffle $shuffleId was cleaned up on this executor " +
        s"before map $mapId published its output")
    }
    kept.partitionLengths(numPartitions)
  }

  /** The current registration of a shuffle, or null. */
  private[rapids] def registration(shuffleId: Int): ShuffleState = shuffles.get(shuffleId)

  private def tryPublish(
      shuffleId: Int,
      mapId: Long,
      output: MapOutputSegments): Option[MapOutputSegments] = {
    val state = registration(shuffleId)
    if (state == null) None else state.tryPublish(mapId, output)
  }

  /**
   * Check if the catalog has data for a given block.
   */
  def hasData(blockId: ShuffleBlockId): Boolean = {
    val output = mapOutput(blockId.shuffleId, blockId.mapId)
    output != null && output.contains(blockId.reduceId)
  }

  /**
   * Check if a shuffle is being managed by this catalog.
   */
  def hasActiveShuffle(shuffleId: Int): Boolean = {
    shuffles.containsKey(shuffleId)
  }

  /**
   * Get all active shuffle IDs.
   * Used during executor shutdown to clean up all remaining shuffles.
   */
  def getActiveShuffleIds: Seq[Int] = {
    import scala.collection.JavaConverters._
    shuffles.keySet().asScala.map(_.intValue()).toSeq
  }

  /**
   * Get a ManagedBuffer that reads data from all segments for a block.
   * The buffer dynamically assembles data from multiple partial files if needed.
   */
  def getMergedBuffer(blockId: ShuffleBlockId): ManagedBuffer = {
    mergedBuffer(blockId, blockId.shuffleId, blockId.mapId, blockId.reduceId,
      blockId.reduceId + 1)
  }

  /**
   * Get a ManagedBuffer for a batch of shuffle blocks (used in batch fetch optimization).
   * This method handles ShuffleBlockBatchId which represents multiple reduce partitions.
   */
  def getMergedBatchBuffer(batchId: ShuffleBlockBatchId): ManagedBuffer = {
    mergedBuffer(batchId, batchId.shuffleId, batchId.mapId, batchId.startReduceId,
      batchId.endReduceId)
  }

  // A map output is found whole or not at all, so a buffer never holds part of what was asked.
  private def mergedBuffer(
      blockId: BlockId,
      shuffleId: Int,
      mapId: Long,
      startReduceId: Int,
      endReduceId: Int): ManagedBuffer = {
    val output = mapOutput(shuffleId, mapId)
    val segments =
      if (output == null) Seq.empty else output.segments(startReduceId, endReduceId)
    if (segments.isEmpty) {
      throw new IllegalArgumentException(
        MultithreadedShuffleBufferCatalog.missingDataMessage(blockId))
    }

    new MultiBatchManagedBuffer(segments, blockId)
  }

  private def mapOutput(shuffleId: Int, mapId: Long): MapOutputSegments = {
    val state = registration(shuffleId)
    if (state == null) null else state.output(mapId)
  }

  /**
   * Unregister a shuffle and clean up all associated data.
   *
   * @param shuffleId the shuffle ID to unregister
   * @return optional cleanup statistics (None if this catalog has no data for the shuffle)
   */
  def unregisterShuffle(shuffleId: Int): Option[ShuffleCleanupStats] = {
    // Close the registration in the same step that detaches it, so a writer that looked it up
    // earlier cannot publish into a registration that is no longer reachable.
    var outputs: Seq[MapOutputSegments] = Seq.empty
    shuffles.computeIfPresent(shuffleId, (_, state) => {
      outputs = state.close()
      null
    })

    // Collect unique handles and gather statistics before closing
    val closedHandles = new HashSet[SpillablePartialFileHandle]()
    var bytesFromMemory = 0L
    var bytesFromDisk = 0L
    var numExpansions = 0
    var numSpills = 0
    var numForcedFileOnly = 0

    outputs.foreach { output =>
      output.handles.foreach { handle =>
        // Only process each handle once
        if (closedHandles.add(handle)) {
          // Collect statistics before closing
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

          closeHandle(shuffleId, handle)
        }
      }
    }

    logDebug(s"Unregistered shuffle $shuffleId: cleanup requested for ${closedHandles.size()} " +
      s"handles, bytesFromMemory=$bytesFromMemory, bytesFromDisk=$bytesFromDisk, " +
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

  private def closeHandles(shuffleId: Int, handles: Seq[SpillablePartialFileHandle]): Unit = {
    handles.foreach(closeHandle(shuffleId, _))
  }

  // Drop catalog ownership; retained buffers, streams, and file regions keep the handle alive
  // through their read leases, so the physical close is deferred until the last lease is
  // released. close() propagates failures, so catch here so one bad handle does not abort
  // cleanup of the rest.
  private def closeHandle(shuffleId: Int, handle: SpillablePartialFileHandle): Unit = {
    try {
      handle.close()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to request close of handle for shuffle $shuffleId", e)
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
class MultiBatchManagedBuffer(
    segments: Seq[PartitionSegment],
    blockId: BlockId) extends ManagedBuffer {

  private val handles: Seq[SpillablePartialFileHandle] = segments.map(_.handle).distinct

  private def translateClosedHandleToMissingData[T](body: => T): T = {
    try {
      body
    } catch {
      case e: ClosedPartialFileHandleException =>
        throw new IllegalArgumentException(
          MultithreadedShuffleBufferCatalog.missingDataMessage(blockId), e)
    }
  }

  /** Guards bufferLeases while retain()/release() can be called from different threads. */
  private val retainLock = new Object

  /** Leases that keep this buffer's partial shuffle file handles open after retain(). */
  private val bufferLeases = new ArrayBuffer[ShuffleHandleLease]()

  override def size(): Long = segments.map(_.length).sum

  override def nioByteBuffer(): ByteBuffer = {
    val lease = translateClosedHandleToMissingData(ShuffleHandleLease.acquire(handles))
    try {
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
    } finally {
      lease.close()
    }
  }

  override def createInputStream(): InputStream = {
    translateClosedHandleToMissingData(new MultiSegmentInputStream(segments, handles))
  }

  override def retain(): ManagedBuffer = {
    val lease = translateClosedHandleToMissingData(ShuffleHandleLease.acquire(handles))
    retainLock.synchronized {
      bufferLeases += lease
    }
    this
  }

  override def release(): ManagedBuffer = {
    val lease = retainLock.synchronized {
      if (bufferLeases.nonEmpty) {
        Some(bufferLeases.remove(bufferLeases.size - 1))
      } else {
        None
      }
    }
    lease.foreach(_.close())
    this
  }

  override def convertToNetty(): AnyRef = {
    // Return a custom FileRegion that streams data in chunks, avoiding loading all
    // data into memory at once. This addresses concerns about large shuffle blocks.
    translateClosedHandleToMissingData(new MultiSegmentFileRegion(segments, handles))
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
 *
 * This stream is not thread-safe; callers should create one stream per reading thread and close it
 * from that owner thread.
 */
class MultiSegmentInputStream(
    segments: Seq[PartitionSegment],
    handles: Seq[SpillablePartialFileHandle]) extends InputStream {

  private var currentSegmentIndex: Int = 0
  private var currentPosition: Long = if (segments.nonEmpty) segments.head.offset else 0
  private var bytesReadInCurrentSegment: Long = 0
  // Keeps the partial shuffle file handles open until this stream is closed.
  private val lease = ShuffleHandleLease.acquire(handles)
  private var closed: Boolean = false

  override def read(): Int = {
    val buf = new Array[Byte](1)
    val n = read(buf, 0, 1)
    if (n == -1) -1 else buf(0) & 0xFF
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (closed) {
      throw new IOException("Stream is closed")
    }

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
    if (closed || currentSegmentIndex >= segments.size) {
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

  override def close(): Unit = {
    if (!closed) {
      closed = true
      lease.close()
    }
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
 *
 * Instances are not thread-safe; each transfer should use one FileRegion owned by one Netty write.
 */
class MultiSegmentFileRegion(
    segments: Seq[PartitionSegment],
    handles: Seq[SpillablePartialFileHandle]) extends AbstractFileRegion {

  private val totalSize: Long = segments.map(_.length).sum
  private var totalTransferred: Long = 0

  // Buffer size for each transferTo call (64KB chunks)
  private val CHUNK_SIZE = 64 * 1024

  // Reusable buffer for reading data (avoids allocation per transferTo call)
  private val readBuffer = new Array[Byte](CHUNK_SIZE)

  // Track current position within the logical data stream
  private var currentSegmentIndex: Int = 0
  private var bytesTransferredInCurrentSegment: Long = 0
  // Keeps the partial shuffle file handles open until this file region is deallocated.
  private val lease = ShuffleHandleLease.acquire(handles)

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
          // Write once and let Netty retry this FileRegion when the channel is not writable.
          val writeBuffer = ByteBuffer.wrap(readBuffer, 0, bytesRead)
          val written = target.write(writeBuffer)
          if (written < 0) {
            throw new IOException("Failed to write to target channel")
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
    lease.close()
  }
}
