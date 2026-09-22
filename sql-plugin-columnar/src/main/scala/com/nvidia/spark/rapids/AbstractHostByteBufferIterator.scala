/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.nio.ByteBuffer

import ai.rapids.cudf.{Cuda, HostMemoryBuffer, MemoryBuffer}

abstract class AbstractHostByteBufferIterator
    extends Iterator[ByteBuffer] {
  private[this] var nextBufferStart: Long = 0L

  val totalLength: Long

  protected val limit: Long = Integer.MAX_VALUE

  def getByteBuffer(offset: Long, length: Long): ByteBuffer

  override def hasNext: Boolean = nextBufferStart < totalLength

  override def next(): ByteBuffer = {
    val offset = nextBufferStart
    val length = Math.min(totalLength - nextBufferStart, limit)
    nextBufferStart += length
    getByteBuffer(offset, length)
  }
}

/**
 * Create an iterator that will emit ByteBuffer instances sequentially
 * to work around the 2GB ByteBuffer size limitation. This allows
 * the entire address range of a >2GB host buffer to be covered
 * by a sequence of ByteBuffer instances.
 * <p>NOTE: It is the caller's responsibility to ensure this iterator
 * does not outlive the host buffer. The iterator DOES NOT increment
 * the reference count of the host buffer to ensure it remains valid.
 *
 * @param hostBuffer host buffer to iterate
 * @return ByteBuffer iterator
 */
class HostByteBufferIterator(hostBuffer: HostMemoryBuffer)
    extends AbstractHostByteBufferIterator {
  override protected val limit: Long = Integer.MAX_VALUE

  override val totalLength: Long = if (hostBuffer == null) {
    0
  } else {
    hostBuffer.getLength
  }

  override def getByteBuffer(offset: Long, length: Long): ByteBuffer = {
    hostBuffer.asByteBuffer(offset, length.toInt)
  }
}

/**
 * Create an iterator that will emit ByteBuffer instances sequentially
 * to work around the 2GB ByteBuffer size limitation after copying a `MemoryBuffer`
 * (which is likely a `DeviceMemoryBuffer`) to a host-backed bounce buffer
 * that is likely smaller than 2GB.
 * @note It is the caller's responsibility to ensure this iterator
 *   does not outlive `memoryBuffer`. The iterator DOES NOT increment
 *   the reference count of `memoryBuffer` to ensure it remains valid.
 * @param memoryBuffer memory buffer to copy. This is likely a DeviceMemoryBuffer
 * @param bounceBuffer a host bounce buffer that will be used to stage copies onto the host
 * @param stream stream to synchronize on after staging to bounceBuffer
 * @return ByteBuffer iterator
 */
class MemoryBufferToHostByteBufferIterator(
    memoryBuffer: MemoryBuffer,
    bounceBuffer: HostMemoryBuffer,
    stream: Cuda.Stream)
    extends AbstractHostByteBufferIterator {
  override val totalLength: Long = if (memoryBuffer == null) {
    0
  } else {
    memoryBuffer.getLength
  }

  override protected val limit: Long =
    Math.min(bounceBuffer.getLength, Integer.MAX_VALUE)

  override def getByteBuffer(offset: Long, length: Long): ByteBuffer = {
    bounceBuffer
      .copyFromMemoryBufferAsync(0, memoryBuffer, offset, length, stream)
    stream.sync()
    bounceBuffer.asByteBuffer(0, length.toInt)
  }
}