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

import java.io.IOException

import scala.util.control.NonFatal

import ai.rapids.cudf.{Cuda, CudaFatalException, DeviceMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{RapidsShuffleHandle, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, BufferTransferRequest}

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rapids.{RapidsShuffleSendPrepareException,
  ShuffleOutputGoneException}

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
 * A live request prepares and sends windows until one Outcome ends it. A lookup failure can
 * get an error reply before a state exists.
 * {{{
 * request arrival -- lookup succeeds --> [live] -- success reply queues --> [Completed]
 *                                         | ^
 *                                         +-+ OOM before deadline
 *                                         |
 *     +-----------------------------------+
 *     +-- output gone ------> [MissingOutput] -+
 *     +-- OOM at deadline --> [OomExpired] ----+--> error reply eligible
 *     +-- other failure ----> [Failed] --------+
 *     |
 *     +-- data send fails --> [SendFailed] ----+
 *     +-- server stops -----> [ServerClosed] --+--> no error reply
 *     +-- close ------------> [Closed] --------+
 * }}}
 *
 * The first finish wins. An in-flight data send leaves completion to its callback until the
 * transport stops. Failed in the final data callback gets no error reply.
 * Error replies require an open server and no loop-ending failure. The materialization OOM
 * deadline starts at request arrival and never resets when a window succeeds.
 *
 * @param transaction a request transaction
 * @param sendBounceBuffers - an object that contains a device and potentially a host
 *                          buffer also
 * @param requestHandler - impl of trait that interfaces to the catalog
 * @param serverStream - CUDA stream to use for copies.
 * @param requestArrivalNanos when the request arrived, on the server's clock
 */
class BufferSendState(
    transaction: Transaction,
    sendBounceBuffers: SendBounceBuffers,
    requestHandler: RapidsShuffleRequestHandler,
    serverStream: Cuda.Stream = Cuda.DEFAULT_STREAM,
    requestArrivalNanos: Long = System.nanoTime())
    extends AutoCloseable with Logging {

  class SendBlock(val tableId: Int, val bufferHandle: RapidsShuffleHandle) extends BlockWithSize {
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

  // how this state ended, or null while it is live; set once, by `finish`
  private[this] var outcome: BufferSendState.Outcome = null

  private[this] val (peerBufferReceiveHeader: Long,
      bufferMetas: Array[BufferMeta],
      blocksToSend: Seq[SendBlock]) = {
    withResource(transaction.releaseMessage()) { mtb =>
      val transferRequest = ShuffleMetadata.getTransferRequest(mtb.getBuffer())
      val peerBufferReceiveHeader = transferRequest.id()

      val bufferMetas = new Array[BufferMeta](transferRequest.requestsLength())

      val btr = new BufferTransferRequest() // for reuse
      val blocksToSend = try {
        (0 until transferRequest.requestsLength()).map { ix =>
          val bufferTransferRequest = transferRequest.requests(btr, ix)
          val tableId = bufferTransferRequest.bufferId()
          val handle = requestHandler.getShuffleHandle(tableId)
          bufferMetas(ix) = handle.tableMeta.bufferMeta()
          new SendBlock(tableId, handle)
        }
      } catch {
        case NonFatal(t) =>
          // the request is released once this block ends, so its ids are read here
          throw new BufferLookupException((0 until transferRequest.requestsLength()).map {
            ix => transferRequest.requests(btr, ix).bufferId()
          }, t)
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

  // How many requested blocks, in order, the prepared windows cover completely, and how many the
  // issued windows do. An error reply leaves out the blocks the issued windows cover completely.
  // The requester gets each issued window, or that send fails and no error reply follows.
  private[this] var blocksPrepared = 0
  private[this] var blocksIssued = 0

  // Retry attempts for this request's materialization OOMs since this state last prepared a
  // window. The backoff before a retry depends on this count. The retries end `retryNanos` after
  // the request arrived, however many of its windows were sent. That deadline lets the error
  // reply reach a reader that started waiting shortly before the request was sent. Other states
  // in the same server batch change neither the count nor the deadline.
  private[this] var oomRetryAttempts: Int = 0

  /**
   * Records a materialization OOM of this state's current window.
   * @param retryNanos how long after the request arrived its OOMs are retried
   * @return the attempt that retries it, counted from the last successful preparation, or
   *         None if the request is past its retries
   */
  private[shuffle] def recordOomAndGetRetryAttempt(
      nowNanos: Long,
      retryNanos: Long): Option[Int] =
    synchronized {
      if (nowNanos - requestArrivalNanos < retryNanos) {
        oomRetryAttempts += 1
        Some(oomRetryAttempts)
      } else {
        None
      }
    }

  private[shuffle] def resetOomRetryAttempts(): Unit = synchronized {
    oomRetryAttempts = 0
  }

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

  /** Records that the transport took the window that `getBufferToSend` prepared last. */
  def windowIssued(): Unit = synchronized {
    blocksIssued = blocksPrepared
  }

  /**
   * The error reply to this state's requester: the requested buffers that it will not get, and
   * why.
   * @param reason why the request failed, for the requester's error message
   */
  def getErrorResponse(reason: String): RefCountedDirectByteBuffer = synchronized {
    new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferErrorResponse(
      blocksToSend.drop(blocksIssued).map(_.tableId), reason))
  }

  /** Whether this state has ended. An ended state never becomes live again. */
  def isTerminal: Boolean = synchronized { outcome != null }

  // exposed for testing
  private[shuffle] def endedWith: Option[BufferSendState.Outcome] = synchronized {
    Option(outcome)
  }

  /**
   * Ends this state once: the first outcome is final, and later calls return false and do
   * nothing. It releases the acquired buffers, the bounce buffers and the request transaction,
   * attempting each even if releasing another failed. A failure to release that must end the
   * send loop (see `BufferSendState.endsSendLoop`) is rethrown for its caller to log: the send
   * loop ends with it, while the transport's callback wrapper, on a send's callback, and the
   * sweep at transport close log and contain it. Any other failure to release is logged here
   * and contained.
   *
   * While a send of this state is in flight only that send's callback may call this, or the
   * server's sweep once the transport has stopped, because the transport can still read the
   * bounce buffer until then.
   * @param errorReply the error reply to answer the requester with before the request
   *                   transaction closes, or None to send no reply
   * @return true if this call ended the state
   */
  def finish(
      result: BufferSendState.Outcome,
      errorReply: Option[BufferSendState.ErrorReply] = None): Boolean = {
    val first = synchronized {
      val isFirst = outcome == null
      if (isFirst) {
        outcome = result
      }
      isFirst
    }
    if (first) {
      if (result == BufferSendState.Completed && hasMoreSends) {
        logWarning("Completing BufferSendState but we still have more blocks!")
      }
      var error: Throwable = null
      def release(step: => Unit): Unit = {
        try {
          step
        } catch {
          case NonFatal(t) => if (error == null) error = t else error.addSuppressed(t)
        }
      }
      release(releaseAcquiredToCatalog())
      release(freeBounceBuffers())
      // No reply follows a release failure that ends the send loop, since the requester hears of
      // the failure when its connection closes. Replying and closing the transaction are calls
      // into the transport, so they run outside this state's monitor.
      if (error == null || !BufferSendState.endsSendLoop(error)) {
        errorReply.foreach(reply => BufferSendState.sendErrorReply(transaction, reply.gate,
          getErrorResponse(reply.reason)))
      }
      release(transaction.close())
      if (error != null) {
        if (BufferSendState.endsSendLoop(error)) {
          // not logged here: whoever catches it logs it
          throw error
        }
        logError(s"Error while releasing a BufferSendState that ended with $result", error)
      }
    }
    first
  }

  override def close(): Unit = finish(BufferSendState.Closed)

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
   * @note throws `RapidsShuffleSendPrepareException` when copies to the bounce buffer fail.
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
            val block = blockRange.block
            val spillable = block.bufferHandle.spillable
            val buff = try {
              spillable.materialize()
            } catch {
              case oom: OutOfMemoryError =>
                throw new RapidsShuffleSendPrepareException(
                  s"Memory exhausted while materializing a shuffle buffer for executor " +
                      s"${peerExecutorId} and header " +
                      s"${TransportUtils.toHex(peerBufferReceiveHeader)}: ${oom.toString}", oom)
              // A closed handle throws IllegalStateException, and a spill file deleted before
              // it opened IOException. Either means the output is gone only if the catalog let
              // go of the buffer; a failure on a buffer it still holds stays unclassified.
              case e @ (_: IllegalStateException | _: IOException)
                  if requestHandler.isShuffleBufferRemoved(block.tableId, block.bufferHandle) =>
                throw new RapidsShuffleSendPrepareException(
                  s"Shuffle buffer for table ${block.tableId} was removed before it could be " +
                      s"sent to executor ${peerExecutorId}",
                  new ShuffleOutputGoneException(
                    s"shuffle output for table ${block.tableId} is gone", e))
            }
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
          case ex: RapidsShuffleSendPrepareException =>
            throw ex
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
        blocksPrepared += blockRanges.count(_.isComplete())

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

    // Preparing this state's next send ends its continuous materialization-OOM episode.
    resetOomRetryAttempts()
    buffsToSend
  }

  /**
   * Called by the RapidsShuffleServer when it has synchronized with its stream,
   * allowing us to safely return buffers to the catalog to be potentially freed if spilling.
   */
  def releaseAcquiredToCatalog(): Unit = synchronized {
    // detached first, so that a close that throws leaves nothing to close a second time
    val toRelease = acquiredBuffs
    acquiredBuffs = Seq.empty
    toRelease.safeClose()
  }
}

/**
 * A transfer request whose buffers could not all be looked up, with the id of every buffer it
 * asked for, which cannot be read from the request once it is released.
 */
private[shuffle] class BufferLookupException(val tableIds: Seq[Int], cause: Throwable)
    extends RuntimeException(cause.getMessage, cause)

object BufferSendState extends Logging {
  /**
   * Lets a server queue its transfer responses, error replies included, only while it is open.
   * The server shuts it before the transport closes, then waits until every reply it let through
   * is queued. UCX's progress thread runs no task queued after the transport's close, so a reply
   * queued later would never be sent or released.
   */
  private[shuffle] trait ReplyGate {
    /** Runs `queue`, which queues a reply, if the server is open, and returns whether it ran. */
    def queueIfOpen(queue: => Unit): Boolean
  }

  /** An error reply: why the request failed, and the gate of the server that answers it. */
  private[shuffle] case class ErrorReply(reason: String, gate: ReplyGate)

  /**
   * Answers a failed transfer request with an error reply, through `gate`. The send's callback
   * releases the reply, whatever the send's status. This method releases it when the send cannot
   * start or the gate is shut. A reply that cannot be built or sent leaves its requester waiting
   * until it times out, as with no reply. The connection logs a failed send, and this method logs
   * any other failure.
   */
  private[shuffle] def sendErrorReply(
      request: Transaction,
      gate: ReplyGate,
      reply: => RefCountedDirectByteBuffer): Unit = {
    try {
      val response = try {
        reply
      } catch {
        // Building the reply allocates on the send loop, a direct buffer included. Running out
        // of memory there, heap or direct, leaves the request unanswered and the loop serving,
        // since nothing has been sent yet.
        case oom: OutOfMemoryError =>
          throw new IllegalStateException("No memory to build the error reply", oom)
      }
      val queued = try {
        gate.queueIfOpen {
          request.respond(response.acquire(), withResource(_) { _ =>
            response.close()
          })
        }
      } catch {
        // A respond that throws queued nothing, so its callback will not release the reply. A
        // fatal error can come after the send was queued, and then the reply is left alone.
        case NonFatal(t) =>
          response.safeClose(t)
          throw t
      }
      if (!queued) {
        logDebug(s"Not sending an error reply to $request: the server closed")
        response.close()
      }
    } catch {
      case NonFatal(t) => logError("Unable to send an error reply, so the requester will " +
          "wait until it times out", t)
    }
  }

  /**
   * Whether `error`, one of its causes or one of their suppressed errors must end the send loop
   * rather than be contained: a fatal CUDA error, after which the GPU is unusable, or an
   * interruption of the thread.
   */
  private[shuffle] def endsSendLoop(error: Throwable): Boolean = exists(error) {
    case _: CudaFatalException | _: InterruptedException => true
    case _ => false
  }

  /** Whether `error`, one of its causes or one of their suppressed errors is an interruption. */
  private[shuffle] def wasInterrupted(error: Throwable): Boolean =
    exists(error)(_.isInstanceOf[InterruptedException])

  private def exists(error: Throwable)(p: Throwable => Boolean): Boolean = {
    val seen = new java.util.IdentityHashMap[Throwable, Throwable]()
    def visit(e: Throwable): Boolean = e != null && seen.put(e, e) == null &&
        (p(e) || visit(e.getCause) || e.getSuppressed.exists(visit))
    visit(error)
  }

  /** How a [[BufferSendState]] ended. Only the first outcome recorded for a state counts. */
  sealed trait Outcome

  /** Every window was sent, and the transfer response was handed to the transport. */
  case object Completed extends Outcome

  /** The server failed to prepare or issue a send, for a reason other than the ones below. */
  case object Failed extends Outcome

  /** A requested buffer is gone: the catalog no longer holds it, as after an unregister. */
  case object MissingOutput extends Outcome

  /** Materialization ran out of memory once the request's OOM retry deadline had passed. */
  case object OomExpired extends Outcome

  /** The transport completed a data send with a status other than success. */
  case object SendFailed extends Outcome

  /** The server closed before the state could finish. */
  case object ServerClosed extends Outcome

  /** Closed directly, through `close()`. */
  case object Closed extends Outcome
}
