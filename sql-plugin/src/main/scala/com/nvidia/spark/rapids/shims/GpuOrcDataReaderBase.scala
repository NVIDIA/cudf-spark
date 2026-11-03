/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.shims

import java.io.{EOFException, IOException}
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, HostMemoryOutputStream, NoopMetric}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{DataReader, OrcProto, StripeInformation}
import org.apache.orc.impl.DataReaderProperties

import org.apache.spark.sql.rapids.GpuTaskMetrics

abstract class GpuOrcDataReaderBase(
    props: DataReaderProperties,
    conf: Configuration,
    metrics: Map[String, GpuMetric]) extends DataReader {
  protected val filePathString = props.getPath.toString
  protected lazy val fileIO = new HadoopFileIO(conf)
  protected lazy val inputFile: RapidsInputFile = fileIO.newInputFile(filePathString)
  protected def fileCache: FileCache = FileCache.get
  protected val compression = props.getCompression
  private val hitMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_HITS)
  private val hitSizeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_HITS_SIZE)
  private val readTimeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_READ_TIME)
  private val missMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_MISSES)
  private val missSizeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_MISSES_SIZE)

  // cache of the last stripe footer that was read and the corresponding stripe info for it
  private var lastStripeFooter: OrcProto.StripeFooter = null
  private var lastStripeFooterInfo: StripeInformation = null

  /**
   * A requested ORC disk range after consulting the file cache.
   *
   * @param block the ORC range node represented by this read
   * @param inputOffset the absolute offset in the input file
   * @param length the number of bytes to read
   * @param outputOffset the destination offset in the output buffer
   * @param sequenceIndex the position of this range in the original request
   */
  private case class PlannedRead(
      block: DiskRangeList,
      inputOffset: Long,
      length: Int,
      outputOffset: Long,
      sequenceIndex: Int)

  /**
   * Adjacent remote ranges combined into one vectored read.
   *
   * first and last identify the ORC range nodes covered by the read, while range describes
   * where the combined bytes are read from and written to.
   */
  private case class CoalescedRemoteRead(
      first: DiskRangeList,
      last: DiskRangeList,
      range: CopyRange)

  private def planReads(
      rangeGroups: Seq[(Long, DiskRangeList)])(
      readCached: (DiskRangeList, Int, Long, SeekableByteChannel) => Unit):
      ArrayBuffer[PlannedRead] = {
    val plannedReads = new ArrayBuffer[PlannedRead]
    var sequenceIndex = 0
    rangeGroups.foreach { case (startPos, ranges) =>
      var outputOffset = startPos
      var current = ranges
      while (current != null) {
        val length = current.getLength
        if (length > 0) {
          val block = current
          val inputOffset = block.getOffset
          fileCache.getDataRangeChannel(inputFile, inputOffset, length) match {
            case Some(channel) =>
              hitMetric += 1
              hitSizeMetric += length
              withResource(channel) { cachedChannel =>
                readCached(block, length, outputOffset, cachedChannel)
              }
            case None =>
              missMetric += 1
              missSizeMetric += length
              plannedReads += PlannedRead(
                block, inputOffset, length, outputOffset, sequenceIndex)
          }
        }
        outputOffset += length
        sequenceIndex += 1
        current = current.next
      }
    }
    plannedReads
  }
  protected trait BlockLoader {
    /** Load data and potentially populate the filecache, returning the next range after last */
    def loadRemoteBlocks(
        first: DiskRangeList,
        last: DiskRangeList,
        data: ByteBuffer): DiskRangeList

    /** Load a single cached block, returning the possibly new disk range node */
    def loadCachedBlock(block: DiskRangeList, channel: SeekableByteChannel): DiskRangeList
  }


  protected def parseStripeFooter(buf: ByteBuffer, size: Int): OrcProto.StripeFooter

  override def open(): Unit = {
    // File cache may preclude need to open remote file, so open remote file lazily.
  }

  override def readStripeFooter(stripe: StripeInformation): OrcProto.StripeFooter = {
    if (stripe == lastStripeFooterInfo) {
      return lastStripeFooter
    }
    val offset = stripe.getOffset + stripe.getIndexLength + stripe.getDataLength
    val tailLength = stripe.getFooterLength.toInt
    val cacheChannel = fileCache.getDataRangeChannel(inputFile, offset, tailLength)
    lastStripeFooter = cacheChannel match {
      case Some(channel) =>
        withResource(channel) { cachedChannel =>
          hitMetric += 1
          hitSizeMetric += tailLength
          val tailBuf = ByteBuffer.allocate(tailLength)
          readTimeMetric.ns {
            while (tailBuf.hasRemaining) {
              if (cachedChannel.read(tailBuf) < 0) {
                throw new EOFException("Unexpected EOF while reading stripe footer")
              }
            }
            tailBuf.flip()
          }
          parseStripeFooter(tailBuf, tailLength)
        }
      case None =>
        missMetric += 1
        missSizeMetric += tailLength
        try {
          withResource(HostMemoryBuffer.allocate(tailLength, false)) { hmb =>
            readRangesToHostMemory(hmb, Seq(new CopyRange(offset, tailLength, 0)))
            // A direct ByteBuffer makes ORC use Hadoop's native direct decompressor, whose JNI
            // implementation may be unavailable. Copy to heap memory to use ORC's portable path.
            val tailBuf = ByteBuffer.allocate(tailLength)
            hmb.getBytes(tailBuf.array(), 0, 0, tailLength)
            val footer = parseStripeFooter(tailBuf, tailLength)
            fileCache.startDataRangeCache(inputFile, offset, tailLength).foreach { token =>
              token.complete(hmb.slice(0, tailLength))
            }
            footer
          }
        } catch {
          case e: IOException =>
            throw new IOException(
              s"Failed to read stripe footer $filePathString $offset:$tailLength", e)
        }
    }
    lastStripeFooterInfo = stripe
    lastStripeFooter
  }

  override def isTrackingDiskRanges: Boolean = false

  override def releaseBuffer(buffer: ByteBuffer): Unit = {
    throw new IllegalStateException("should not be trying to release buffer")
  }

  def copyFileDataToHostStream(out: HostMemoryOutputStream, ranges: DiskRangeList): Unit = {
    val startPos = out.getPos
    // Cache and remote reads write directly to the backing buffer without advancing the stream.
    copyFileDataToHostStream(out, Seq((startPos, ranges)))
    // Advance the stream after the data is written so it points to the next write position.
    out.seek(startPos + getTotalLength(ranges))
  }

  def copyFileDataToHostStream(
      out: HostMemoryOutputStream,
      rangeGroups: Seq[(Long, DiskRangeList)]): Unit = {
    val remoteReads = planReads(rangeGroups) { (_, length, outputOffset, channel) =>
      copyCachedRange(channel, length, outputOffset, out.buffer)
    }
    copyRemoteBlocksData(remoteReads.toSeq, out.buffer)
  }

  private def getTotalLength(ranges: DiskRangeList): Long = {
    var totalLength = 0L
    var current = ranges
    while (current != null) {
      totalLength += current.getLength
      current = current.next
    }
    totalLength
  }

  private def copyRemoteBlocksData(
      remoteReads: Seq[PlannedRead],
      output: HostMemoryBuffer): Unit = {
    if (remoteReads.nonEmpty) {
      val coalescedRanges = coalesceRemoteReads(remoteReads).map(_.range)
      try {
        readRangesToHostMemory(output, coalescedRanges)
      } catch {
        case e: IOException =>
          val rangeSummary = coalescedRanges.map(r =>
            s"${r.getInputOffset}:${r.getLength}").mkString(",")
          throw new IOException(s"Failed to read $filePathString ranges $rangeSummary", e)
      }
      remoteReads.foreach { read =>
        val cacheToken = fileCache.startDataRangeCache(inputFile, read.inputOffset, read.length)
        cacheToken.foreach { token =>
          token.complete(output.slice(read.outputOffset, read.length))
        }
      }
    }
  }

  private def coalesceRemoteReads(
      remoteReads: Seq[PlannedRead],
      maxLength: Long = Long.MaxValue): Seq[CoalescedRemoteRead] = {
    val coalesced = new ArrayBuffer[CoalescedRemoteRead](remoteReads.length)
    var current: CoalescedRemoteRead = null
    var lastSequenceIndex = -1

    remoteReads.foreach { read =>
      if (current == null) {
        current = CoalescedRemoteRead(read.block, read.block,
          new CopyRange(read.inputOffset, read.length, read.outputOffset))
      } else {
        val currentRange = current.range
        val inputIsContiguous =
          currentRange.getInputOffset + currentRange.getLength == read.inputOffset
        val outputIsContiguous =
          currentRange.getOutputOffset + currentRange.getLength == read.outputOffset
        val combinedLength = currentRange.getLength + read.length
        if (read.sequenceIndex == lastSequenceIndex + 1 && inputIsContiguous &&
            outputIsContiguous && combinedLength <= maxLength) {
          current = CoalescedRemoteRead(current.first, read.block,
            new CopyRange(currentRange.getInputOffset, combinedLength,
              currentRange.getOutputOffset))
        } else {
          coalesced += current
          current = CoalescedRemoteRead(read.block, read.block,
            new CopyRange(read.inputOffset, read.length, read.outputOffset))
        }
      }
      lastSequenceIndex = read.sequenceIndex
    }
    if (current != null) {
      coalesced += current
    }
    coalesced.toSeq
  }

  private def copyCachedRange(
      channel: SeekableByteChannel,
      length: Int,
      outputOffset: Long,
      output: HostMemoryBuffer): Unit = {
    readTimeMetric.ns {
      val outputBuffer = output.asByteBuffer(outputOffset, length)
      while (outputBuffer.hasRemaining) {
        if (channel.read(outputBuffer) < 0) {
          throw new EOFException("Unexpected EOF while reading cached ORC data")
        }
      }
    }
  }

  private def readRangesToHostMemory(
      output: HostMemoryBuffer,
      ranges: Seq[CopyRange]): Unit = {
    if (ranges.nonEmpty) {
      recordPerfIOBackend()
      inputFile.readVectored(output, ranges.asJava)
    }
  }

  private def recordPerfIOBackend(): Unit = {
    val scheme = props.getPath.toUri.getScheme
    if (scheme != null && scheme.startsWith("s3")) {
      GpuTaskMetrics.get.recordPerfioS3BackendOnce()
    }
  }

  override def close(): Unit = {}

  private def getMetric(metricName: String): GpuMetric = metrics.getOrElse(metricName, NoopMetric)

  protected def readDiskRanges(
      ranges: DiskRangeList,
      loader: BlockLoader): Unit = {
    val plannedReads = planReads(Seq((0L, ranges))) { (block, _, _, channel) =>
      readTimeMetric.ns {
        loader.loadCachedBlock(block, channel)
      }
    }
    var totalRemoteOutputSize = 0L
    val packedRemoteReads = plannedReads.map { read =>
      val packedRead = read.copy(outputOffset = totalRemoteOutputSize)
      totalRemoteOutputSize += read.length
      packedRead
    }
    // Each coalesced range is unpacked through a ByteBuffer, whose length is limited to
    // Int.MaxValue.
    val remoteReads = coalesceRemoteReads(packedRemoteReads.toSeq, Int.MaxValue)
    if (remoteReads.nonEmpty) {
      require(totalRemoteOutputSize > 0, "Remote ORC read data must not be empty")
      withResource(HostMemoryBuffer.allocate(totalRemoteOutputSize, false)) { remoteData =>
        val copyRanges = remoteReads.map(_.range)
        try {
          readRangesToHostMemory(remoteData, copyRanges)
        } catch {
          case e: IOException =>
            val rangeSummary = copyRanges.map(r =>
              s"${r.getInputOffset}:${r.getLength}").mkString(",")
            throw new IOException(s"Failed to read $filePathString ranges $rangeSummary", e)
        }
        remoteReads.foreach { read =>
          val size = read.range.getLength.toInt
          val bytes = new Array[Byte](size)
          remoteData.getBytes(bytes, 0, read.range.getOutputOffset, size)
          loader.loadRemoteBlocks(read.first, read.last, ByteBuffer.wrap(bytes))
        }
      }
    }
  }

  // [Scala 2.13] This is needed because org.apache.orc.DataReader defines a public clone() method
  // which should be overidden here as a public member. The Scala 2.13 compiler enforces this now
  // which was a bug in the compiler previously.
  override def clone(): DataReader = {
    super.clone().asInstanceOf[DataReader]
  }
}
