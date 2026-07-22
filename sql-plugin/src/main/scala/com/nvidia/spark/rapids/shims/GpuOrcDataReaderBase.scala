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

import java.io.{Closeable, EOFException, IOException}
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, HostMemoryOutputStream, NoopMetric}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
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

  private case class LocalCopy(
      channel: SeekableByteChannel,
      length: Long,
      outputOffset: Long) extends Closeable {
    override def close(): Unit = channel.close()
  }
  protected trait BlockLoader {
    /** Load data and potentially populate the filecache, returning the next range after last */
    def loadRemoteBlocks(
        baseOffset: Long,
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
    val tailBuf = ByteBuffer.allocate(tailLength)
    val cacheChannel = fileCache.getDataRangeChannel(inputFile, offset, tailLength)
    if (cacheChannel.isDefined) {
      withResource(cacheChannel.get) { channel =>
        hitMetric += 1
        hitSizeMetric += tailLength
        readTimeMetric.ns {
          while (tailBuf.hasRemaining) {
            if (channel.read(tailBuf) < 0) {
              throw new EOFException("Unexpected EOF while reading stripe footer")
            }
          }
          tailBuf.flip()
        }
      }
    } else {
      missMetric += 1
      missSizeMetric += tailLength
      try {
        withResource(HostMemoryBuffer.allocate(tailLength, false)) { hmb =>
          readRangesToHostMemory(hmb, Seq(new CopyRange(offset, tailLength, 0)))
          hmb.getBytes(tailBuf.array(), tailBuf.arrayOffset(), 0, tailLength)
        }
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Failed to read stripe footer $filePathString $offset:$tailLength", e)
      }
      val cacheToken = fileCache.startDataRangeCache(inputFile, offset, tailLength)
      cacheToken.foreach { token =>
        closeOnExcept(HostMemoryBuffer.allocate(tailLength, false)) { hmb =>
          hmb.setBytes(0, tailBuf.array(), tailBuf.arrayOffset(), tailLength)
          token.complete(hmb)
        }
      }
    }
    lastStripeFooter = parseStripeFooter(tailBuf, tailLength)
    lastStripeFooterInfo = stripe
    lastStripeFooter
  }

  override def isTrackingDiskRanges: Boolean = false

  override def releaseBuffer(buffer: ByteBuffer): Unit = {
    throw new IllegalStateException("should not be trying to release buffer")
  }

  def copyFileDataToHostStream(out: HostMemoryOutputStream, ranges: DiskRangeList): Unit = {
    val startPos = out.getPos
    copyFileDataToHostStream(out, Seq((startPos, ranges)))
    out.seek(startPos + getTotalLength(ranges))
  }

  def copyFileDataToHostStream(
      out: HostMemoryOutputStream,
      rangeGroups: Seq[(Long, DiskRangeList)]): Unit = {
    val remoteCopies = new ArrayBuffer[CopyRange]
    val originalPos = out.getPos
    withResource(new ArrayBuffer[LocalCopy]) { localCopies =>
      rangeGroups.foreach { case (startPos, ranges) =>
        var outputOffset = startPos
        var current = ranges
        while (current != null) {
          val length = current.getLength
          if (length > 0) {
            val channel = fileCache.getDataRangeChannel(inputFile, current.getOffset, length)
            if (channel.isDefined) {
              localCopies += LocalCopy(channel.get, length, outputOffset)
            } else {
              remoteCopies += new CopyRange(current.getOffset, length, outputOffset)
            }
          }
          outputOffset += length
          current = current.next
        }
      }
      localCopies.foreach { localCopy =>
        copyLocal(localCopy, out)
      }
    }
    copyRemoteBlocksData(remoteCopies.toSeq, out)
    // restore output position after ranges were copied out of order
    out.seek(originalPos)
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
      remoteCopies: Seq[CopyRange],
      out: HostMemoryOutputStream): Unit = {
    if (remoteCopies.nonEmpty) {
      val coalescedRanges = coalesceReads(remoteCopies)
      try {
        readRangesToHostMemory(out.buffer, coalescedRanges)
      } catch {
        case e: IOException =>
          val rangeSummary = coalescedRanges.map(r =>
            s"${r.getInputOffset}:${r.getLength}").mkString(",")
          throw new IOException(s"Failed to read $filePathString ranges $rangeSummary", e)
      }
      remoteCopies.foreach { range =>
        missMetric += 1
        missSizeMetric += range.getLength
        val cacheToken = fileCache.startDataRangeCache(
          inputFile, range.getInputOffset, range.getLength)
        cacheToken.foreach { token =>
          token.complete(out.buffer.slice(range.getOutputOffset, range.getLength))
        }
      }
    }
  }

  private def coalesceReads(ranges: Seq[CopyRange]): Seq[CopyRange] = {
    val coalesced = new ArrayBuffer[CopyRange](ranges.length)
    var currentRange: CopyRange = null
    var currentRangeEnd = 0L

    def addCurrentRange(): Unit = {
      if (currentRange != null) {
        val rangeLength = currentRangeEnd - currentRange.getInputOffset
        if (rangeLength == currentRange.getLength) {
          coalesced += currentRange
        } else {
          coalesced += new CopyRange(
            currentRange.getInputOffset, rangeLength, currentRange.getOutputOffset)
        }
        currentRange = null
        currentRangeEnd = 0L
      }
    }

    ranges.foreach { range =>
      val outputIsContiguous = currentRange != null &&
        currentRange.getOutputOffset + currentRangeEnd - currentRange.getInputOffset ==
          range.getOutputOffset
      if (range.getInputOffset == currentRangeEnd && outputIsContiguous) {
        currentRangeEnd += range.getLength
      } else {
        addCurrentRange()
        currentRange = range
        currentRangeEnd = range.getInputOffset + range.getLength
      }
    }
    addCurrentRange()
    coalesced.toSeq
  }

  private def copyLocal(item: LocalCopy, out: HostMemoryOutputStream): Unit = {
    hitMetric += 1
    hitSizeMetric += item.length
    readTimeMetric.ns {
      out.seek(item.outputOffset)
      out.copyFromChannel(item.channel, item.length)
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
      baseOffset: Long,
      loader: BlockLoader): Unit = {
    case class RangeState(
        block: DiskRangeList,
        cachedChannel: Option[SeekableByteChannel])
    sealed trait ReadOp
    case class CachedRead(
        block: DiskRangeList,
        channel: SeekableByteChannel) extends ReadOp
    case class RemoteRead(
        first: DiskRangeList,
        last: DiskRangeList,
        size: Int,
        outputOffset: Long) extends ReadOp

    val rangeStates = new ArrayBuffer[RangeState]
    val openedChannels = new ArrayBuffer[SeekableByteChannel]
    try {
      var current = ranges
      while (current != null) {
        val block = current
        val offset = block.getOffset + baseOffset
        val size = block.getLength
        val cachedChannel = fileCache.getDataRangeChannel(inputFile, offset, size)
        cachedChannel match {
          case Some(channel) =>
            openedChannels += channel
            hitMetric += 1
            hitSizeMetric += size
          case None =>
            missMetric += 1
            missSizeMetric += size
        }
        rangeStates += RangeState(block, cachedChannel)
        current = current.next
      }

      val ops = new ArrayBuffer[ReadOp]
      var outputOffset = 0L
      var i = 0
      while (i < rangeStates.length) {
        rangeStates(i) match {
          case RangeState(block, Some(channel)) =>
            ops += CachedRead(block, channel)
            i += 1
          case RangeState(first, None) =>
            var last = first
            var currentEnd = first.getEnd
            var j = i + 1
            while (j < rangeStates.length &&
                rangeStates(j).cachedChannel.isEmpty &&
                rangeStates(j).block.getOffset == currentEnd &&
                rangeStates(j).block.getEnd - first.getOffset <= Int.MaxValue) {
              last = rangeStates(j).block
              currentEnd = currentEnd.max(last.getEnd)
              j += 1
            }
            val size = (currentEnd - first.getOffset).toInt
            ops += RemoteRead(first, last, size, outputOffset)
            outputOffset += size
            i = j
        }
      }

      def loadReads(remoteData: Option[HostMemoryBuffer]): Unit = {
        ops.foreach {
          case CachedRead(block, channel) =>
            readTimeMetric.ns {
              loader.loadCachedBlock(block, channel)
            }
          case RemoteRead(first, last, size, bufferOffset) =>
            val bytes = new Array[Byte](size)
            remoteData.get.getBytes(bytes, 0, bufferOffset, size)
            loader.loadRemoteBlocks(baseOffset, first, last, ByteBuffer.wrap(bytes))
        }
      }

      if (outputOffset == 0) {
        loadReads(None)
      } else {
        withResource(HostMemoryBuffer.allocate(outputOffset, false)) { hmb =>
          val copyRanges = ops.collect {
            case RemoteRead(first, _, size, bufferOffset) =>
              new CopyRange(baseOffset + first.getOffset, size, bufferOffset)
          }
          try {
            readRangesToHostMemory(hmb, copyRanges.toSeq)
          } catch {
            case e: IOException =>
              val rangeSummary = copyRanges.map(r =>
                s"${r.getInputOffset}:${r.getLength}").mkString(",")
              throw new IOException(s"Failed to read $filePathString ranges $rangeSummary", e)
          }
          loadReads(Some(hmb))
        }
      }
    } finally {
      openedChannels.safeClose()
    }
  }

  // [Scala 2.13] This is needed because org.apache.orc.DataReader defines a public clone() method
  // which should be overidden here as a public member. The Scala 2.13 compiler enforces this now
  // which was a bug in the compiler previously.
  override def clone(): DataReader = {
    super.clone().asInstanceOf[DataReader]
  }
}
