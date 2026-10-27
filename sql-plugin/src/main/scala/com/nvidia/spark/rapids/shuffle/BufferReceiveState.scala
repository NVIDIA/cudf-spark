/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, Cuda, DeviceMemoryBuffer, Rmm}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.NvtxRegistry
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.internal.Logging

case class ConsumedBatchFromBounceBuffer(
    contigBuffer: DeviceMemoryBuffer,
    meta: TableMeta,
    handler: RapidsShuffleFetchHandler)

/** A transfer request that the server failed: why, and the requests that will not arrive. */
case class FailedTransfer(reason: String, notReceived: Seq[PendingTransferRequest])

/**
 * A helper case class to maintain the state associated with a transfer request to a peer.
 *
 * On getBufferWhenReady(finalizeCb, size), a bounce buffer is either made
 * immediately available to `finalizeCb` or it will be made available later, via `toFinalize`.
 *
 * By convention, the `id` is used as the header for receives for this `BufferReceiveState`
 *
 * `consumeWindow` is called when data has arrived at the bounce buffer, copying
 * onto the ranges the bytes received.
 *
 * An error reply waits for outstanding windows before the client ends incomplete requests.
 * {{{
 * request issued --> [receiving] -- all tables consumed --> [done] -- close --> [closed]
 *                         |                                  ^                   ^
 *                    error reply                             |                   |
 *                         v                                  |                   |
 *                  [serverFailure] -- all tables consumed ----+                   |
 *                         |                                                      |
 *                  no outstanding windows                                        |
 *                         v                                                      |
 *                  [FailedTransfer] -- fail waiting readers ----------------------+
 * handler failure ---------------------------------------------------------------+
 * }}}
 *
 * Recording or taking the failure releases nothing. The copy thread returns unused bytes and
 * fails each distinct waiting reader before closing. The first close attempts to release the
 * bounce buffer, partial-table buffer and transport entry, even if one release fails.
 * Repeated close and late error replies release nothing.
 *
 * @param id - a numeric id that is used in all headers for this `BufferReceiveState`
 * @param bounceBuffer - bounce buffer to use (device memory strictly)
 * @param requests - collection of `PendingTransferRequest` as issued by iterators
 *                 currently requesting
 * @param transportOnClose - a callback invoked when the `BufferReceiveState` closes
 * @param stream - CUDA stream to use for allocations and copies
 */
class BufferReceiveState(
    val id: Long,
    bounceBuffer: BounceBuffer,
    requests: Seq[PendingTransferRequest],
    transportOnClose: () => Unit,
    stream: Cuda.Stream = Cuda.DEFAULT_STREAM)
    extends AutoCloseable with Logging {

  val transportBuffer = new CudfTransportBuffer(bounceBuffer.buffer)
  // we use this to keep a list (should be depth 1) of "requests for receives"
  //  => the transport is ready to receive again, but we are not done consuming the
  //     buffers from the previous receive, so we must delay the transport.
  var toFinalize = new util.ArrayDeque[TransportBuffer => Unit]()

  // if this is > 0, we are waiting to consume, so we need to queue up in `toFinalize`
  // any callbacks
  var toConsume = 0

  class ReceiveBlock(val request: PendingTransferRequest) extends BlockWithSize {
    override def size: Long = request.getLength
  }

  // flags whether we need to populate a window, and if the client should send a
  // transfer request
  private[this] var iterated = false

  // block ranges we'll work on next
  private[this] var nextBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // ranges we are working on now
  private[this] var currentBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // if a block overshoots a window, it will be in `workingOn`. This happens if the
  // block is larger than window also.
  private[this] var workingOn: DeviceMemoryBuffer = null

  // offset tracking
  private[this] var workingOnOffset: Long = 0L
  private[this] var bounceBufferByteOffset = 0L

  // get block ranges for us to work with
  private[this] val windowedBlockIterator = new WindowedBlockIterator[ReceiveBlock](
    requests.map(r => new ReceiveBlock(r)), bounceBuffer.buffer.getLength)

  private[this] var hasMoreBuffers_ = windowedBlockIterator.hasNext

  // how many requests, in order, were consumed into batches
  private[this] var received = 0

  // why the server failed this request, until `maybeTakeFailure` hands it over
  private[this] var serverFailure: String = null

  // set by close, after which neither a second close nor a failure the server reports later
  // releases anything
  private[this] var closed = false

  def getBufferWhenReady(finalizeCb: TransportBuffer => Unit, size: Long): Unit =
    synchronized {
      require(transportBuffer.getLength() >= size,
        "Asked to receive a buffer greater than the available bounce buffer.")

      if (toConsume == 0) {
        logDebug(s"Calling callback immediately for ${TransportUtils.toHex(id)}")
        finalizeCb(transportBuffer)
      } else {
        // have pending and haven't consumed it yet, consume will call the callback
        logDebug(s"Deferring callback for ${TransportUtils.toHex(id)}, " +
          s"have ${toFinalize.size} pending")
        toFinalize.add(finalizeCb)
      }
      toConsume += 1
    }

  def getRequests: Seq[PendingTransferRequest] = requests

  override def close(): Unit = synchronized {
    // RapidsShuffleClient.doHandleBounceBufferReceive closes the state after the last window.
    // Its catch closes the state again if that close or the window's transaction then fails.
    if (!closed) {
      closed = true
      if (workingOn != null) {
        logWarning(s"BufferReceiveState closing, but there are unfinished batches")
      }
      // each is released even when one before it fails, and the first failure is thrown
      Seq[AutoCloseable](bounceBuffer, workingOn, () => transportOnClose()).safeClose()
    }
  }

  /**
   * Calls `transferError` on each `RapidsShuffleFetchHandler`
   * @param errMsg - the message to pass onto the handlers
   */
  def errorOccurred(errMsg: String, throwable: Throwable = null): Unit = synchronized {
    // for current and future blocks, tell handlers of error
    (currentBlocks ++ windowedBlockIterator.toSeq.flatten)
      .foreach(_.block.request.handler.transferError(errMsg, throwable))
  }

  def hasMoreBlocks: Boolean = synchronized { hasMoreBuffers_ }

  /**
   * Records the server's failure, and returns it if `maybeTakeFailure` can hand it over now.
   * Otherwise `RapidsShuffleClient.doHandleBounceBufferReceive` tries again after each window
   * that leaves more to receive.
   * @param reason the error message for each reader whose request will not arrive
   * @return what `maybeTakeFailure` returns
   */
  def serverFailed(reason: String): Option[FailedTransfer] = synchronized {
    // A transfer request gets one terminal response, so this runs at most once per state. A
    // second failure while the first waits for a window keeps the first and is only logged.
    if (serverFailure == null) {
      serverFailure = reason
    } else {
      logWarning(s"Ignoring a second server failure for ${TransportUtils.toHex(id)}: $reason")
    }
    maybeTakeFailure()
  }

  /**
   * Hands over and clears the server's failure only when no window is left to consume and
   * nothing closed this state. While a window is left, UCX can still write into the bounce
   * buffer, and the copy thread has yet to copy out of it. The one caller that gets the failure
   * must end this state: return the bytes in flight for `notReceived` to the transport, fail
   * their readers and close this state.
   */
  def maybeTakeFailure(): Option[FailedTransfer] = synchronized {
    if (serverFailure != null && toConsume == 0 && !closed) {
      val failure = FailedTransfer(serverFailure, requests.drop(received))
      serverFailure = null
      Some(failure)
    } else {
      None
    }
  }

  private def advance(): Unit = {
    if (!hasMoreBuffers_) {
      throw new NoSuchElementException(
        s"BufferReceiveState is done for ${TransportUtils.toHex(id)}, yet " +
          s"received a call to next")
    }

    if (!iterated) {
      nextBlocks = windowedBlockIterator.next()
      iterated = true
    }
    currentBlocks = nextBlocks

    // The window handler's catch can close this state while this window is on its way. The
    // catch then tells this window's readers, which no earlier error reached.
    if (closed) {
      throw new IllegalStateException(
        s"BufferReceiveState ${TransportUtils.toHex(id)} is closed, yet received a window")
    }

    if (windowedBlockIterator.hasNext) {
      nextBlocks = windowedBlockIterator.next()
    } else {
      nextBlocks = Seq.empty
      hasMoreBuffers_ = false
    }
  }

  /**
   * When a receive is complete, the client calls `consumeWindow` to copy out
   * of the bounce buffer in this `BufferReceiveState` any complete batches, or to
   * buffer up a remaining batch in `workingOn`
   *
   * @return - a sequence of batches that were successfully consumed, or an empty
   *         sequence if still working on a batch.
   */
  def consumeWindow(): Seq[ConsumedBatchFromBounceBuffer] = synchronized {
    // once we reach 0 here the transport will be allowed to reuse the bounce buffer
    // e.g. after the synchronized block, or after we sync with GPU in this function.
    toConsume -= 1
    NvtxRegistry.CONSUME_WINDOW {
      advance()
      closeOnExcept(new ArrayBuffer[DeviceMemoryBuffer]()) { toClose =>
        val results = currentBlocks.flatMap { b =>
          val pendingTransferRequest = b.block.request
          RmmSpark.shuffleThreadWorkingOnTasks(pendingTransferRequest.handler.getTaskIds)
          val fullSize = pendingTransferRequest.tableMeta.bufferMeta().size()

          var contigBuffer: DeviceMemoryBuffer = null

          // Receive buffers are always in the device, and so it is safe to assume
          // that they are `BaseDeviceMemoryBuffer`s here.
          val deviceBounceBuffer = bounceBuffer.buffer.asInstanceOf[BaseDeviceMemoryBuffer]

          if (fullSize == b.rangeSize()) {
            // we have the full buffer!
            contigBuffer = Rmm.alloc(b.rangeSize(), stream)
            toClose.append(contigBuffer)

            contigBuffer.copyFromDeviceBufferAsync(0, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)
          } else {
            if (workingOn != null) {
              workingOn.copyFromDeviceBufferAsync(workingOnOffset, deviceBounceBuffer,
                bounceBufferByteOffset, b.rangeSize(), stream)

              workingOnOffset += b.rangeSize()
              if (workingOnOffset == fullSize) {
                toClose.append(workingOn)
                contigBuffer = workingOn
                workingOn = null
                workingOnOffset = 0
              }
            } else {
              // need to keep it around. Only the field owns it, so that if this window fails, the
              // catch in RapidsShuffleClient.doHandleBounceBufferReceive closes the state, which
              // frees it once.
              workingOn = Rmm.alloc(fullSize, stream)

              workingOn.copyFromDeviceBufferAsync(0, deviceBounceBuffer,
                bounceBufferByteOffset, b.rangeSize(), stream)

              workingOnOffset += b.rangeSize()
            }
          }
          bounceBufferByteOffset += b.rangeSize()
          if (bounceBufferByteOffset >= deviceBounceBuffer.getLength) {
            bounceBufferByteOffset = 0
          }

          if (contigBuffer != null) {
            Some(ConsumedBatchFromBounceBuffer(
              contigBuffer, pendingTransferRequest.tableMeta, pendingTransferRequest.handler))
          } else {
            None
          }
        }

        // Sync once, instead of for each copy.
        // We need to synchronize, because we can't ask ucx to overwrite our bounce buffer
        // unless all that data has truly moved to our final buffer in our stream
        stream.sync()

        results.foreach { result =>
          RmmSpark.poolThreadFinishedForTasks(result.handler.getTaskIds)
        }
        received += results.size

        // cpu is in sync, we can recycle the bounce buffer
        if (!toFinalize.isEmpty) {
          val firstCb = toFinalize.pop()
          firstCb(transportBuffer)
        }

        results
      }
    }
  }
}
