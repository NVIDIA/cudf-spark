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

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuMetric
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.OrcProto
import org.apache.orc.impl.{BufferChunk, BufferChunkList, DataReaderProperties, InStream, OrcCodecPool}

abstract class GpuOrcDataReader320Plus(
    props: DataReaderProperties,
    conf: Configuration,
    metrics: Map[String, GpuMetric]) extends GpuOrcDataReaderBase(props, conf, metrics) {

  private class BufferChunkLoader(useDirect: Boolean) extends BlockLoader {
    override def loadRemoteBlocks(
        baseOffset: Long,
        first: DiskRangeList,
        last: DiskRangeList,
        data: ByteBuffer): DiskRangeList = {
      var current = first
      val offset = current.getOffset
      while (current ne last.next) {
        val buffer = if (current eq last) data else data.duplicate()
        buffer.position((current.getOffset - offset).toInt)
        buffer.limit((current.getEnd - offset).toInt)
        current.asInstanceOf[BufferChunk].setChunk(buffer)
        // see if the filecache wants any of this data
        val cacheToken = fileCache.startDataRangeCache(inputFile,
          baseOffset + current.getOffset, current.getLength)
        cacheToken.foreach { token =>
          val hmb = closeOnExcept(HostMemoryBuffer.allocate(current.getLength, false)) { hmb =>
            hmb.setBytes(0, buffer.array(),
              buffer.arrayOffset() + buffer.position(), current.getLength)
            hmb
          }
          token.complete(hmb)
        }
        current = current.next
      }
      current
    }

    override def loadCachedBlock(
        chunk: DiskRangeList,
        channel: SeekableByteChannel): DiskRangeList = {
      val buffer = if (useDirect) {
        ByteBuffer.allocateDirect(chunk.getLength)
      } else {
        ByteBuffer.allocate(chunk.getLength)
      }
      while (buffer.remaining() > 0) {
        if (channel.read(buffer) < 0) {
          throw new EOFException(s"Unexpected EOF while reading cached block for $filePathString")
        }
      }
      buffer.flip()
      chunk.asInstanceOf[BufferChunk].setChunk(buffer)
      chunk
    }
  }

  override protected def parseStripeFooter(buf: ByteBuffer, size: Int): OrcProto.StripeFooter = {
    OrcProto.StripeFooter.parseFrom(
      InStream.createCodedInputStream(InStream.create("footer",
        new BufferChunk(buf, 0), 0, size, compression)))
  }

  override def getCompressionOptions: InStream.StreamOptions = compression

  override def readFileData(chunks: BufferChunkList, forceDirect: Boolean): BufferChunkList = {
    if (chunks != null) {
      readDiskRanges(chunks.get, 0, new BufferChunkLoader(forceDirect))
    }
    chunks
  }

  override def close(): Unit = {
    if (compression.getCodec != null) {
      if (compression.getCodec != null) {
        OrcCodecPool.returnCodec(compression.getCodec.getKind, compression.getCodec)
        compression.withCodec(null)
      }
    }
    super.close()
  }
}