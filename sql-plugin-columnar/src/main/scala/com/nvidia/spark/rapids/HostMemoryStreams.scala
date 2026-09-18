/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import java.io.{EOFException, InputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

import ai.rapids.cudf.HostMemoryBuffer

/**
 * An implementation of OutputStream that writes to a HostMemoryBuffer.
 *
 * NOTE: Closing this output stream does NOT close the buffer!
 *
 * @param buffer the buffer to receive written data
 */
class HostMemoryOutputStream(val buffer: HostMemoryBuffer) extends OutputStream {
  protected var pos: Long = 0

  override def write(i: Int): Unit = {
    buffer.setByte(pos, i.toByte)
    pos += 1
  }

  override def write(bytes: Array[Byte]): Unit = {
    buffer.setBytes(pos, bytes, 0, bytes.length)
    pos += bytes.length
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    buffer.setBytes(pos, bytes, offset, len)
    pos += len
  }

  def write(data: ByteBuffer): Unit = {
    val numBytes = data.remaining()
    val outBuffer = buffer.asByteBuffer(pos, numBytes)
    outBuffer.put(data)
    pos += numBytes
  }

  def writeAsByteBuffer(length: Int): ByteBuffer = {
    val bb = buffer.asByteBuffer(pos, length)
    pos += length
    bb
  }

  def getPos: Long = pos

  def seek(newPos: Long): Unit = {
    pos = newPos
  }

  def copyFromChannel(channel: ReadableByteChannel, length: Long): Unit = {
    val endPos = pos + length
    assert(endPos <= buffer.getLength)
    while (pos != endPos) {
      val bytesToCopy = (endPos - pos).min(Integer.MAX_VALUE).toInt
      val bytebuf = buffer.asByteBuffer(pos, bytesToCopy)
      while (bytebuf.hasRemaining) {
        val channelReadBytes = channel.read(bytebuf)
        if (channelReadBytes < 0) {
          throw new EOFException("Unexpected EOF while reading from byte channel")
        }
      }
      pos += bytesToCopy
    }
  }
}

/** A HostMemoryOutputStream only counts the written bytes, nothing is actually written. */
final class NullHostMemoryOutputStream extends HostMemoryOutputStream(null) {
  override def write(i: Int): Unit = {
    pos += 1
  }

  override def write(bytes: Array[Byte]): Unit = {
    pos += bytes.length
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    pos += len
  }

  override def copyFromChannel(channel: ReadableByteChannel, length: Long): Unit = {
    val endPos = pos + length
    while (pos != endPos) {
      val bytesToCopy = (endPos - pos).min(Integer.MAX_VALUE)
      pos += bytesToCopy
    }
  }

}

trait HostMemoryInputStreamMixIn extends InputStream {
  protected val hmb: HostMemoryBuffer
  protected val hmbLength: Long

  protected var pos = 0L
  protected var mark = -1L

  override def read(): Int = {
    if (pos >= hmbLength) {
      -1
    } else {
      val result = hmb.getByte(pos)
      pos += 1
      // because byte in java is signed, result will be sign extended when it is auto cast to an
      // int for the return value. We need to mask off the upper bits to avoid returning a
      // negative value which indicated EOF.
      result & 0xFF
    }
  }

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    if (pos >= hmbLength) {
      -1
    } else {
      val numBytes = Math.min(available(), length)
      hmb.getBytes(buffer, offset, pos, numBytes)
      pos += numBytes
      numBytes
    }
  }

  def readByteBuffer(length: Int): ByteBuffer = {
    val bb = hmb.asByteBuffer(pos, length)
    pos += length
    bb
  }

  override def skip(count: Long): Long = {
    val oldPos = pos
    pos = Math.min(pos + count, hmbLength)
    pos - oldPos
  }

  override def available(): Int = Math.min(hmbLength - pos, Integer.MAX_VALUE).toInt

  override def mark(ignored: Int): Unit = {
    mark = pos
  }

  override def reset(): Unit = {
    if (mark <= 0) {
      throw new IOException("reset called before mark")
    }
    pos = mark
  }

  override def markSupported(): Boolean = true

  def getPos: Long = pos
}

/**
 * An implementation of InputStream that reads from a HostMemoryBuffer.
 *
 * NOTE: Closing this input stream does NOT close the buffer!
 *
 * @param hmb the buffer from which to read data
 * @param hmbLength the amount of data available in the buffer
 */
class HostMemoryInputStream(
    val hmb: HostMemoryBuffer,
    val hmbLength: Long) extends HostMemoryInputStreamMixIn {
}