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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ByteRange, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsInputFile}
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import com.nvidia.spark.rapids.parquet.HybridScanPipeline.HostBufferLayout
import com.nvidia.spark.rapids.parquet.ParquetPartitionReader.LocalCopy
import org.apache.hadoop.fs.Path

/**
 * Where a hybrid scan read's bytes come from.
 *
 * The reader plans a read as byte ranges of a Parquet file and asks for their contents.
 */
private[parquet] trait HybridScanByteSource extends AutoCloseable {
  /**
   * Fetch `ranges` into one host buffer laid out by [[HybridScanPipeline.hostBufferLayout]]. The
   * caller owns the buffer.
   */
  def fetchToHostBuffer(ranges: Array[ByteRange]): HostMemoryBuffer

  /**
   * Fetch `ranges` into host memory, returning one buffer per range in the order requested. An
   * empty range yields an empty buffer rather than being dropped, so the result stays aligned
   * with the request. The caller owns the buffers that come back.
   */
  final def fetch(ranges: Array[ByteRange]): Array[HostMemoryBuffer] = {
    if (ranges.isEmpty) {
      Array.empty
    } else {
      withResource(fetchToHostBuffer(ranges))(HybridScanPipeline.sliceRanges(_, ranges))
    }
  }

  /**
   * Whether a fetch of `ranges` can go straight to device memory with [[fetchToDevice]],
   * without this source staging it in host memory. cuFile may still stage it internally if it falls
   * back to its own compatibility mode. Such a fetch writes to the device, so it needs the GPU
   * semaphore.
   */
  def isDeviceReadDirect(ranges: Array[ByteRange]): Boolean = false

  /**
   * Fetch `ranges` into device memory, one buffer per range as [[fetch]] returns them. Only for
   * a fetch [[isDeviceReadDirect]] accepts. The caller owns the buffers that come back.
   *
   * The device allocation is not retried here: call this inside a retry block, which must fetch
   * again, since the buffers do not outlive an attempt.
   */
  def fetchToDevice(ranges: Array[ByteRange]): Array[DeviceMemoryBuffer] =
    throw new UnsupportedOperationException("this source cannot read to the device")

  override def close(): Unit = {}
}

/**
 * Ranges of a Parquet file, read from that file. Ranges already in the local file cache are
 * copied from there and the rest are read from the file by `rangeReader`, coalesced into as few
 * requests as the layout allows. Closing the source closes `rangeReader`.
 */
private[parquet] class HybridScanFileByteSource(
    fileIO: RapidsFileIO,
    filePath: Path,
    execMetrics: Map[String, GpuMetric],
    rangeReader: HybridScanRangeReader) extends HybridScanByteSource {

  def this(fileIO: RapidsFileIO, filePath: Path, execMetrics: Map[String, GpuMetric]) =
    this(fileIO, filePath, execMetrics, new RapidsFileIORangeReader(execMetrics))

  override def fetchToHostBuffer(ranges: Array[ByteRange]): HostMemoryBuffer = {
    val layout = HybridScanPipeline.hostBufferLayout(ranges)
    val buffer = withRetryNoSplit[HostMemoryBuffer] {
      HostMemoryBuffer.allocate(layout.totalSize)
    }
    closeOnExcept(buffer) { _ =>
      fillRanges(fileIO.newInputFile(filePath), buffer, ranges, layout)
    }
    buffer
  }

  private def fillRanges(
      inputFile: RapidsInputFile,
      buffer: HostMemoryBuffer,
      ranges: Array[ByteRange],
      layout: HostBufferLayout): Unit = {
    val remoteRanges = new ArrayBuffer[CopyRange](ranges.length)
    withResource(new ArrayBuffer[LocalCopy](ranges.length)) { cachedRanges =>
      layout.fileOrder.foreach { i =>
        val offset = ranges(i).offset()
        val size = ranges(i).size()
        // An empty range has nothing to read but still owns a slot in the result, so that the
        // caller can keep passing the buffers to the reader in the order it asked for them.
        if (size > 0) {
          FileCache.get.getDataRangeChannel(inputFile, offset, size) match {
            case Some(channel) => cachedRanges += LocalCopy(channel, size, layout.offsets(i))
            case None => remoteRanges += new CopyRange(offset, size, layout.offsets(i))
          }
        }
      }
      val out = new HostMemoryOutputStream(buffer)
      cachedRanges.foreach { cached =>
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_HITS, NoopMetric) += 1
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_HITS_SIZE, NoopMetric) += cached.length
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_READ_TIME, NoopMetric).ns {
          out.seek(cached.outputOffset)
          out.copyFromChannel(cached.channel, cached.length)
        }
      }
    }
    if (remoteRanges.nonEmpty) {
      rangeReader.readToHost(inputFile, buffer, GpuParquetScan.coalesceReads(remoteRanges.toSeq))
      remoteRanges.foreach { range =>
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_MISSES, NoopMetric) += 1
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_MISSES_SIZE, NoopMetric) += range.getLength
        FileCache.get.startDataRangeCache(inputFile, range.getInputOffset, range.getLength)
            .foreach(_.complete(buffer.slice(range.getOutputOffset, range.getLength)))
      }
    }
  }

  /**
   * A device fetch bypasses the file cache, so a fetch with any range in the cache is left to
   * the host path, which reads the cached ranges from there and the rest from the file.
   */
  override def isDeviceReadDirect(ranges: Array[ByteRange]): Boolean =
    rangeReader.isDeviceReadDirect(ranges) && !anyRangeCached(ranges)

  private def anyRangeCached(ranges: Array[ByteRange]): Boolean = {
    lazy val inputFile = fileIO.newInputFile(filePath)
    ranges.exists { range =>
      range.size() > 0 &&
        FileCache.get.getDataRangeChannel(inputFile, range.offset(), range.size())
          .exists(withResource(_)(_ => true))
    }
  }

  /** Bypasses the file cache. */
  override def fetchToDevice(ranges: Array[ByteRange]): Array[DeviceMemoryBuffer] = {
    if (ranges.isEmpty) {
      Array.empty
    } else {
      import HybridScanFileByteSource._
      val layout = deviceBufferLayout(ranges, rangeReader.fileSize)
      // RMM aligns an allocation to only 256 bytes, so the slack lets the layout start on an
      // aligned address. Each slice holds a reference to the allocation, so closing it here
      // leaves the slices valid and releases the memory, slack included, once the last one is
      // closed.
      withResource(DeviceMemoryBuffer.allocate(layout.totalSize + GDS_ALIGNMENT - 1)) { alloc =>
        val start = alignUp(alloc.getAddress) - alloc.getAddress
        withResource(alloc.slice(start, layout.totalSize)) { buffer =>
          rangeReader.readToDevice(buffer, layout.reads)
          execMetrics.getOrElse(HYBRID_SCAN_DEVICE_BYTES_READ, NoopMetric) +=
            layout.reads.map(_.getLength).sum
          ranges.indices.toArray.safeMap { i =>
            buffer.slice(layout.offsets(i), ranges(i).size())
          }
        }
      }
    }
  }

  override def close(): Unit = rangeReader.close()
}

private[parquet] object HybridScanFileByteSource {
  /**
   * cuFile reads straight to the device only when the file offset, the size and the device
   * address of a read are all multiples of this. Otherwise it stages the whole read through its
   * own GPU bounce buffers.
   */
  val GDS_ALIGNMENT = 4096L

  def alignDown(value: Long): Long = value - value % GDS_ALIGNMENT

  def alignUp(value: Long): Long = alignDown(value + GDS_ALIGNMENT - 1)

  /**
   * Where a set of byte ranges lands in a device buffer that starts on a [[GDS_ALIGNMENT]]
   * boundary.
   *
   * @param reads     the reads to issue, at their offsets in the buffer
   * @param offsets   where each range starts in the buffer, indexed as the ranges are
   * @param totalSize how large the buffer has to be
   */
  case class DeviceBufferLayout(reads: Seq[CopyRange], offsets: Array[Long], totalSize: Long)

  /**
   * Lay the ranges out in file order, like [[HybridScanPipeline.hostBufferLayout]], but read
   * whole aligned blocks of the file into spans of the buffer that start on a [[GDS_ALIGNMENT]]
   * boundary. Ranges whose blocks overlap or touch share a span, so bytes that two ranges share,
   * or a block they both touch, are read once. Each range is sliced from its span at its file
   * offset's position within the span's blocks.
   *
   * Every read is aligned except where a span runs into the end of a file whose size is not a
   * whole number of blocks: that last partial block is a read of its own, so that only it is
   * unaligned.
   */
  def deviceBufferLayout(ranges: Array[ByteRange], fileSize: Long): DeviceBufferLayout = {
    // The reads stop at the end of the file, so a range past it would slice unread memory. An
    // empty range is never read, so it may be anywhere.
    ranges.foreach { r =>
      require(r.size() == 0 ||
          (r.offset() >= 0 && r.size() > 0 && r.size() <= fileSize - r.offset()),
        s"range at ${r.offset()} of ${r.size()} bytes is not within the file of $fileSize bytes")
    }
    val offsets = new Array[Long](ranges.length)
    val reads = new ArrayBuffer[CopyRange]()
    var totalSize = 0L
    var spanStart = 0L
    var blocksStart = -1L
    var blocksEnd = -1L

    def endSpan(): Unit = {
      if (blocksStart >= 0) {
        val readEnd = math.min(blocksEnd, fileSize)
        val alignedEnd = alignDown(readEnd)
        if (alignedEnd > blocksStart) {
          reads += new CopyRange(blocksStart, alignedEnd - blocksStart, spanStart)
        }
        if (readEnd > alignedEnd) {
          reads += new CopyRange(alignedEnd, readEnd - alignedEnd,
            spanStart + alignedEnd - blocksStart)
        }
        totalSize = spanStart + blocksEnd - blocksStart
      }
    }

    // An empty range has nothing to read and keeps offset 0, which any buffer can slice.
    ranges.indices.filter(i => ranges(i).size() > 0).sortBy(i => ranges(i).offset()).foreach { i =>
      val offset = ranges(i).offset()
      if (alignDown(offset) > blocksEnd) {
        endSpan()
        spanStart = totalSize
        blocksStart = alignDown(offset)
      }
      // A range nested in an earlier one ends before it, so the span keeps the furthest end.
      blocksEnd = math.max(blocksEnd, alignUp(offset + ranges(i).size()))
      offsets(i) = spanStart + offset - blocksStart
    }
    endSpan()
    DeviceBufferLayout(reads.toSeq, offsets, totalSize)
  }
}

/** How a [[HybridScanFileByteSource]] reads the ranges of its file that it does not have. */
private[parquet] trait HybridScanRangeReader extends AutoCloseable {
  /** Read `ranges` of `inputFile` into `buffer` at their output offsets. */
  def readToHost(
      inputFile: RapidsInputFile,
      buffer: HostMemoryBuffer,
      ranges: Seq[CopyRange]): Unit

  /** See [[HybridScanByteSource.isDeviceReadDirect]]. */
  def isDeviceReadDirect(ranges: Array[ByteRange]): Boolean = false

  /** Read `ranges` of the file into `buffer` at their output offsets. */
  def readToDevice(buffer: DeviceMemoryBuffer, ranges: Seq[CopyRange]): Unit =
    throw new UnsupportedOperationException("this reader cannot read to the device")

  /** The size of the file, which a device read's aligned blocks must not run past. */
  def fileSize: Long =
    throw new UnsupportedOperationException("this reader cannot read to the device")

  override def close(): Unit = {}
}

/** Reads through [[RapidsFileIO]], so from any filesystem or object store it supports. */
private[parquet] class RapidsFileIORangeReader(execMetrics: Map[String, GpuMetric])
  extends HybridScanRangeReader {

  override def readToHost(
      inputFile: RapidsInputFile,
      buffer: HostMemoryBuffer,
      ranges: Seq[CopyRange]): Unit = {
    GpuParquetScan.readRangesToHostMemory(inputFile, buffer, ranges, execMetrics)
  }
}
