/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import java.util

import ai.rapids.cudf.MemoryBuffer

import org.apache.spark.internal.Logging

/**
 * Class to hold a bounce buffer reference in `buffer`.
 *
 * It is `AutoCloseable`, where a call to `close` puts the bounce buffer
 * back into the corresponding source `BounceBufferManager`
 *
 * @param buffer - cudf MemoryBuffer to be used as a bounce buffer
 */
abstract class BounceBuffer(val buffer: MemoryBuffer) extends AutoCloseable {
  var isClosed = false

  def free(bb: BounceBuffer): Unit

  override def close(): Unit = {
    if (isClosed) {
      throw new IllegalStateException("Bounce buffer closed too many times")
    }
    free(this)
    isClosed = true
  }
}

/**
 * This class can hold 1 or 2 `BounceBuffer`s and is only used in the send case.
 *
 * Ideally, the device buffer is used if most of the data to be sent is on
 * the device. The host buffer is used in the opposite case.
 *
 * @param deviceBounceBuffer - device buffer to use for sends
 * @param hostBounceBuffer - optional host buffer to use for sends
 */
case class SendBounceBuffers(
    deviceBounceBuffer: BounceBuffer,
    hostBounceBuffer: Option[BounceBuffer]) extends AutoCloseable {

  def bounceBufferSize: Long = {
    deviceBounceBuffer.buffer.getLength
  }

  override def close(): Unit = {
    deviceBounceBuffer.close()
    hostBounceBuffer.foreach(_.close())
  }
}

/**
 * This classes manages a set of bounce buffers, that are instances of `MemoryBuffer`.
 * The size/quantity of buffers is configurable, and so is the allocator.
 * @param poolName a human-friendly name to use for debug logs
 * @param bufferSize the size of buffer to use
 * @param numBuffers the number of buffers to allocate on instantiation
 * @param allocator function that takes a size, and returns a `MemoryBuffer` instance.
 * @tparam T the specific type of MemoryBuffer i.e. `DeviceMemoryBuffer`,
 *           `HostMemoryBuffer`, etc.
 */
class BounceBufferManager[T <: MemoryBuffer](
    poolName: String,
    val bufferSize: Long,
    val numBuffers: Int,
    allocator: Long => T)
  extends AutoCloseable
  with Logging {

  class BounceBufferImpl(buff: MemoryBuffer) extends BounceBuffer(buff) {
    override def free(bb: BounceBuffer): Unit = {
      freeBuffer(bb)
    }
  }

  private[this] val freeBufferMap = new util.BitSet(numBuffers)

  private[this] val rootBuffer = allocator(bufferSize * numBuffers)

  freeBufferMap.set(0, numBuffers)

  /**
   * Acquires a [[BounceBuffer]] from the pool. Blocks if the pool is empty.
   *
   * @note calls to this function should have a lock on this [[BounceBufferManager]]
   * @return the acquired `BounceBuffer`
   */
  private def acquireBuffer(): BounceBuffer = {
    val bufferIndex = freeBufferMap.nextSetBit(0)
    while (bufferIndex < 0) {
      throw new IllegalStateException(s"Buffer pool $poolName has exhausted!")
    }

    logDebug(s"$poolName: Buffer index: ${bufferIndex}")
    freeBufferMap.clear(bufferIndex)
    val res = rootBuffer.slice(bufferIndex * bufferSize, bufferSize)
    new BounceBufferImpl(res)
  }

  def numFree(): Int = synchronized {
    freeBufferMap.cardinality()
  }

  /**
   * Acquire `possibleNumBuffers` buffers from the pool. This method will not block.
   * @param possibleNumBuffers number of buffers to acquire
   * @return a sequence of `BounceBuffer`s, or empty if the request can't be satisfied
   */
  def acquireBuffersNonBlocking(possibleNumBuffers: Int): Seq[BounceBuffer] = synchronized {
    if (numFree < possibleNumBuffers) {
      // would block
      logTrace(s"$poolName at capacity. numFree: ${numFree}, " +
        s"buffers required ${possibleNumBuffers}")
      return Seq.empty
    }
    val res = (0 until possibleNumBuffers).map(_ => acquireBuffer())
    logDebug(s"$poolName at acquire. Has numFree ${numFree}")
    res
  }

  /**
   * Free a `BounceBuffer`, putting it back into the pool.
   * @param bounceBuffer the memory buffer to free
   */
  def freeBuffer(bounceBuffer: BounceBuffer): Unit = synchronized {
    val buffer = bounceBuffer.buffer
    require(buffer.getAddress >= rootBuffer.getAddress
        && (buffer.getAddress - rootBuffer.getAddress) % bufferSize == 0,
      s"$poolName: foreign buffer being freed")
    val bufferIndex = (buffer.getAddress - rootBuffer.getAddress) / bufferSize
    require(bufferIndex < numBuffers,
      s"$poolName: buffer index invalid $bufferIndex should be less than $numBuffers")

    logDebug(s"$poolName: Free buffer index ${bufferIndex}")
    buffer.close()
    freeBufferMap.set(bufferIndex.toInt)
    notifyAll() // notify any waiters that are checking the state of this manager
  }

  /**
   * Returns the root (backing) `MemoryBuffer`. This is used for a transport
   * that wants to register the bounce buffers against hardware, for pinning purposes.
   * @return the root (backing) memory buffer
   */
  def getRootBuffer(): MemoryBuffer = rootBuffer

  override def close(): Unit = rootBuffer.close()
}
