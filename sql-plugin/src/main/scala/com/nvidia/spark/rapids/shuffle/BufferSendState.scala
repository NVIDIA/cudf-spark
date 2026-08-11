/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{RapidsShuffleHandle, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, BufferTransferRequest}

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rapids.RapidsShuffleSendPrepareException

/**
 * A helper case class to maintain the server side state in response to a transfer
 * request initiated by a peer.
 *
 * The class implements the Iterator interface.
 *
 * On next(), a set of RapidsBuffer are copied onto a bounce buffer, and a
 * `MemoryBuffer` slice of the bounce buffer is returned. Buffers are copied to the bounce
 * buffer in `TransferRequest` order. The receiver has the same conventions.
 *
 * Synchronization with `serverStream` is outside of this class. It is assumed that the caller
 * has several `BufferSendState` iterators and on calling next on this collection, it will
 * perform a sync on `serverStream` before handing it off to the transport.
 *
 * It also is AutoCloseable. close() should be called to free bounce buffers.
 *
 * In terms of the lifecycle of this object, it begins with the client asking for transfers to
 * start, it lasts through all buffers being transmitted, and ultimately finishes when a
 * TransferResponse is sent back to the client.
 *
 * @param transaction a request transaction
 * @param sendBounceBuffers - an object that contains a device and potentially a host
 *                          buffer also
 * @param requestHandler - impl of trait that interfaces to the catalog
 * @param serverStream - CUDA stream to use for copies.
 */
class BufferSendState(
    transaction: Transaction,
    sendBounceBuffers: SendBounceBuffers,
    requestHandler: RapidsShuffleRequestHandler,
    serverStream: Cuda.Stream = Cuda.DEFAULT_STREAM)
    extends AutoCloseable with Logging {

  class SendBlock(val bufferHandle: RapidsShuffleHandle) extends BlockWithSize {
    // we assume that the size of the buffer won't change as it goes to host/disk
    // we also are likely to assume this is just a device buffer, and so we should
    // copy to device and then send.
    override def size: Long = {
      if (bufferHandle.spillable != null) {
        bufferHandle.spillable.sizeInBytes
      } else {
        0L // degenerate
      }
    }
  }

  val peerExecutorId: Long = transaction.peerExecutorId()

  private[this] var isClosed = false

  private[this] val (peerBufferReceiveHeader: Long,
      bufferMetas: Array[BufferMeta],
      blocksToSend: Seq[SendBlock]) = {
    withResource(transaction.releaseMessage()) { mtb =>
      val transferRequest = ShuffleMetadata.getTransferRequest(mtb.getBuffer())
      val peerBufferReceiveHeader = transferRequest.id()

      val bufferMetas = new Array[BufferMeta](transferRequest.requestsLength())

      val btr = new BufferTransferRequest() // for reuse
      val blocksToSend = (0 until transferRequest.requestsLength()).map { ix =>
        val bufferTransferRequest = transferRequest.requests(btr, ix)
        val handle = requestHandler.getShuffleHandle(bufferTransferRequest.bufferId())
        bufferMetas(ix) = handle.tableMeta.bufferMeta()
        new SendBlock(handle)
      }
      (peerBufferReceiveHeader, bufferMetas, blocksToSend)
    }
  }

  // the header to use for all sends in this `BufferSendState` (sent to us
  // by the peer)
  def getPeerBufferReceiveHeader: Long = {
    peerBufferReceiveHeader
  }

  private[this] val windowedBlockIterator =
    new WindowedBlockIterator[SendBlock](blocksToSend, sendBounceBuffers.bounceBufferSize)

  // when the window has been exhausted
  private[this] var hasMoreBlocks = windowedBlockIterator.hasNext

  // take out the device and host bounce buffers from the `SendBounceBuffers` case class
  private[this] val deviceBounceBuffer: BounceBuffer =
    sendBounceBuffers.deviceBounceBuffer
  private[this] val hostBounceBuffer: BounceBuffer =
    sendBounceBuffers.hostBounceBuffer.orNull

  // ranges that we currently copying from (initialize with the first range)
  private[this] var blockRanges: Seq[BlockRange[SendBlock]] = windowedBlockIterator.next()

  private[this] var acquiredBuffs: Seq[RangeBuffer] = Seq.empty

  def getRequestTransaction: Transaction = synchronized {
    transaction
  }

  def hasMoreSends: Boolean = synchronized { hasMoreBlocks }

  private[this] def freeBounceBuffers(): Unit = {
    sendBounceBuffers.close()
  }

  def getTransferResponse(): RefCountedDirectByteBuffer = synchronized {
    new RefCountedDirectByteBuffer(
      ShuffleMetadata.buildBufferTransferResponse(bufferMetas))
  }

  override def close(): Unit = synchronized {
    if (hasMoreBlocks) {
      logWarning("Closing BufferSendState but we still have more blocks!")
    }
    if (isClosed){
      throw new IllegalStateException("ALREADY CLOSED!")
    }
    // close transaction
    withResource(transaction) { _ =>
      isClosed = true
      freeBounceBuffers()
      releaseAcquiredToCatalog()
    }
  }

  case class RangeBuffer(
      range: BlockRange[SendBlock], rapidsBuffer: MemoryBuffer)
      extends AutoCloseable {
    override def close(): Unit = {
      rapidsBuffer.close()
    }
  }

  /**
   * Prepares and returns a `MemoryBuffer` that can be used in a send.
   * @return - a memory buffer slice backed by either a host or device bounce buffer, depending on
   *         the tier location for buffers we are sending.
   * @throws org.apache.spark.shuffle.rapids.RapidsShuffleSendPrepareException when copies to the
   *                                                                    bounce buffer fail.
   */
  def getBufferToSend(): MemoryBuffer = synchronized {
    require(acquiredBuffs.isEmpty,
      "Called next without calling `releaseAcquiredToCatalog` first")

    var bounceBuffToUse: MemoryBuffer = null
    var buffOffset = 0L

    val buffsToSend = {
      if (hasMoreBlocks) {
        var deviceBuffs = 0L
        var hostBuffs = 0L

        var needsCleanup = false
        try {
          acquiredBuffs = blockRanges.safeMap { blockRange =>
            // we acquire these buffers now, and keep them until the caller releases them
            // using `releaseAcquiredToCatalog`
            //these are closed later, after we synchronize streams
            val spillable = blockRange.block.bufferHandle.spillable
            val buff = spillable.materialize()
            buff match {
              case _: DeviceMemoryBuffer =>
                deviceBuffs += blockRange.rangeSize()
              case _ =>
                hostBuffs += blockRange.rangeSize()
            }
            RangeBuffer(blockRange, buff)
          }

          logDebug(s"Occupancy for bounce buffer is " +
            s"[device=${deviceBuffs}, host=${hostBuffs}] Bytes")

          bounceBuffToUse = if (deviceBuffs >= hostBuffs || hostBounceBuffer == null) {
            deviceBounceBuffer.buffer
          } else {
            hostBounceBuffer.buffer
          }

          acquiredBuffs.foreach { case RangeBuffer(blockRange, memoryBuffer) =>
            needsCleanup = true
            require(blockRange.rangeSize() <= bounceBuffToUse.getLength - buffOffset)
            bounceBuffToUse.copyFromMemoryBufferAsync(
              buffOffset,
              memoryBuffer,
              blockRange.rangeStart,
              blockRange.rangeSize(),
              serverStream)
            buffOffset += blockRange.rangeSize()
          }
          needsCleanup = false
        } catch {
          case ex: Exception =>
            throw new RapidsShuffleSendPrepareException(
              s"Error while copying to bounce buffer for executor ${peerExecutorId} and " +
                  s"header ${TransportUtils.toHex(peerBufferReceiveHeader)}", ex)
        } finally {
          if (needsCleanup) {
            // we likely failed in `copyToMemoryBuffer`
            // unacquire buffs (removing ref count)
            releaseAcquiredToCatalog()
          }
        }

        val buffSlice = bounceBuffToUse.slice(0, buffOffset)

        if (windowedBlockIterator.hasNext) {
          blockRanges = windowedBlockIterator.next()
        } else {
          blockRanges = Seq.empty
          hasMoreBlocks = false
        }

        buffSlice
      } else {
        throw new NoSuchElementException("BufferSendState is already done, yet next was called")
      }
    }

    logDebug(s"Sending ${buffsToSend} for transfer request, " +
        s" [peer_executor_id=${transaction.peerExecutorId()}]")

    buffsToSend
  }

  /**
   * Called by the RapidsShuffleServer when it has synchronized with its stream,
   * allowing us to safely return buffers to the catalog to be potentially freed if spilling.
   */
  def releaseAcquiredToCatalog(): Unit = synchronized {
    acquiredBuffs.foreach(_.close())
    acquiredBuffs = Seq.empty
  }
}
