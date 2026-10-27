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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executor, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf.{Cuda, MemoryBuffer}
import com.nvidia.spark.rapids.{NvtxRegistry, RapidsConf, RapidsShuffleHandle, ShuffleBufferCatalog,
  ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rapids.{RapidsShuffleSendPrepareException,
  ShuffleOutputGoneException}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}

/**
 * Trait used for the server to get buffer metadata (for metadata requests), and
 * also to acquire a buffer (for transfer requests)
 */
trait RapidsShuffleRequestHandler {
  /**
   * This is a query into the manager to get the `TableMeta` corresponding to a
   * shuffle block.
   * @param shuffleBlockBatchId `ShuffleBlockBatchId` with (shuffleId, mapId,
   *                            startReduceId, endReduceId)
   * @return a sequence of `TableMeta` describing batches corresponding to a block.
   */
  def getShuffleBufferMetas(shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta]

  /**
   * Acquires (locks w.r.t. the memory tier) a `RapidsBuffer` corresponding to a table id.
   * @param tableId the unique id for a table in the catalog
   * @return a `RapidsBuffer` which is reference counted, and should be closed by the acquirer
   */
  def getShuffleHandle(tableId: Int): RapidsShuffleHandle

  /**
   * Whether the buffer behind a handle that `getShuffleHandle(tableId)` returned has been
   * removed since, which tells a failure to materialize a buffer that is gone from other
   * failures. Asked only after a materialization failed.
   * @param tableId the table id the handle was looked up with
   * @param handle the handle `getShuffleHandle` returned
   * @return true if the buffer is gone, false if it is still held or this cannot tell
   */
  def isShuffleBufferRemoved(tableId: Int, handle: RapidsShuffleHandle): Boolean = false
}

/** The request handler a shuffle manager gives its server: lookups in its executor's catalog. */
class CatalogShuffleRequestHandler(catalog: ShuffleBufferCatalog)
    extends RapidsShuffleRequestHandler {
  override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
    catalog.getShuffleBufferHandle(tableId)
  }

  override def isShuffleBufferRemoved(tableId: Int, handle: RapidsShuffleHandle): Boolean =
    catalog.isShuffleBufferRemoved(tableId, handle.spillable)

  override def getShuffleBufferMetas(sbbId: ShuffleBlockBatchId): Seq[TableMeta] = {
    (sbbId.startReduceId to sbbId.endReduceId).flatMap(rid => {
      catalog.blockIdToMetas(ShuffleBlockId(sbbId.shuffleId, sbbId.mapId, rid))
    })
  }
}

/**
 * A server that replies to shuffle metadata messages, and issues device/host memory sends.
 *
 * A single command thread is used to orchestrate sends/receives and to remove
 * from transport's progress thread.
 *
 * Close shuts the transfer reply gate before waiting for admitted queue attempts. A reply
 * decided while open still needs admission immediately before queueing.
 * {{{
 * [open] -- close --> [closing] -- queue attempts end --> [closed]
 *
 * reply decided while open --> [decided] -- gate open --> [queueing]
 *                                  |                         |
 *                             gate closed             returns or throws
 *                                  v                         v
 *                              [released]               [attempt ended]
 * reply decided while closing or closed --> no reply
 * }}}
 *
 * No reply is admitted once closing starts. Close waits for admitted queue attempts, including
 * throws, then waits a bounded time for the send loop. Closed means close returned.
 * Close does not wait for delivery or callbacks. Metadata replies do not use this gate.
 *
 * @param transport the transport we were configured with
 * @param serverConnection a connection object, which contains functions to send/receive
 * @param originalShuffleServerId spark's `BlockManagerId` for this executor
 * @param requestHandler instance of [[RapidsShuffleRequestHandler]]
 * @param exec Executor used to handle tasks that take time, and should not be in the
 *             transport's thread
 * @param bssExec Executor used to handle [[BufferSendState]]s that are waiting
 *                for bounce buffers to become available
 * @param rapidsConf plugin configuration instance
 */
class RapidsShuffleServer(transport: RapidsShuffleTransport,
                          serverConnection: ServerConnection,
                          val originalShuffleServerId: BlockManagerId,
                          requestHandler: RapidsShuffleRequestHandler,
                          exec: Executor,
                          bssExec: Executor,
                          rapidsConf: RapidsConf) extends AutoCloseable with Logging {

  def getId: BlockManagerId = {
    // upon seeing this port, the other side will try to connect to the port
    // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
    TrampolineUtil.newBlockManagerId(
      originalShuffleServerId.executorId,
      originalShuffleServerId.host,
      originalShuffleServerId.port,
      Some(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}=${getPort}"))
  }

  /**
   * On close, this is set to false to indicate that the server is shutting down. It is written
   * only under the `bssExec` monitor, where states are queued, so that nothing is queued after
   * the send loop has drained the queues.
   */
  @volatile private[this] var started = true

  // The transfer responses, error replies included, that `replyGate` let through and that are
  // still being queued. Guarded by the `bssExec` monitor, with `started`, so that close can wait
  // for them.
  private[this] var repliesQueueing = 0

  /**
   * This server's reply gate, open while `started`. It counts the replies it lets through in
   * `repliesQueueing`, and close waits for that count without a bound, since queueing a reply
   * takes no blocking call.
   */
  private[this] object replyGate extends BufferSendState.ReplyGate {
    override def queueIfOpen(queue: => Unit): Boolean = {
      val open = bssExec.synchronized {
        if (started) {
          repliesQueueing += 1
        }
        started
      }
      if (open) {
        try {
          queue
        } finally {
          bssExec.synchronized {
            repliesQueueing -= 1
            // only a closing server waits for the count, so the send loop is not woken here
            if (!started && repliesQueueing == 0) {
              bssExec.notifyAll()
            }
          }
        }
      }
      open
    }
  }

  private[shuffle] def currentTimeNanos(): Long = System.nanoTime()

  /**
   * How long a reader waits for its next batch, `spark.network.timeout`. A request's sends retry
   * a materialization OOM until this wait less a margin has passed since the request arrived, so
   * that the error reply reaches a reader still waiting. `RapidsShuffleServer.oomRetryNanos`
   * subtracts the margin.
   */
  private[shuffle] def oomRetryTimeoutNanos: Long =
    TimeUnit.SECONDS.toNanos(GpuShuffleEnv.shuffleFetchTimeoutSeconds)

  private[shuffle] def oomRetryBackoffMillis(retryAttempt: Int): Long = {
    val shift = math.min(retryAttempt - 1, 4)
    math.min(10L << shift, 100L)
  }

  private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit =
    Thread.sleep(backoffMillis)

  private def stopOomRetries(
      states: Seq[BufferSendState],
      errors: Seq[Throwable],
      reason: String): Unit = {
    val failure = new IllegalStateException(
      s"Unable to prepare shuffle sends. $reason These sends will not be retried.")
    errors.foreach(failure.addSuppressed)
    states.foreach(_.finish(BufferSendState.OomExpired,
      errorReply(BufferSendState.OomExpired, failure)))
    logError(failure.getMessage, failure)
    bssExec.synchronized {
      bssExec.notifyAll()
    }
  }

  private object ShuffleServerOps {
    /**
     * When a transfer request is received during a callback, the handle code is offloaded via this
     * event to the server thread.
     * @param tx the live transaction that should be closed by the handler
     */
    case class HandleMeta(tx: Transaction)

    /**
     * When transfer request is received (to begin sending buffers), the handling is offloaded via
     * this event on the server thread. Note that, [[BufferSendState]] encapsulates one more more
     * requests to send buffers, and [[HandleTransferRequest]] may be posted multiple times
     * in order to handle the request fully.
     * @param sendState instance of [[BufferSendState]] used to complete a transfer request.
     */
    case class HandleTransferRequest(sendState: Seq[BufferSendState])
  }

  import ShuffleServerOps._

  private var port: Int = -1

  /**
   * Returns a TCP port that is expected to respond to rapids shuffle protocol.
   * Throws if this server is not started yet, which is an illegal state.
   * @return the port
   */
  def getPort: Int = {
    if (port == -1) {
      throw new IllegalStateException("RapidsShuffleServer port is not initialized")
    }
    port
  }

  /**
   * Kick off the underlying connection, and listen for initial requests.
   */
  def start(): Unit = {
    port = serverConnection.startManagementPort(originalShuffleServerId.host)

    // register request type interest against the transport
    registerRequestHandler(MessageType.MetadataRequest)
    registerRequestHandler(MessageType.TransferRequest)
  }

  def handleOp(serverTask: Any): Unit = {
    try {
      serverTask match {
        case HandleMeta(tx) =>
          doHandleMetadataRequest(tx)
        case HandleTransferRequest(wt: Seq[BufferSendState]) =>
          doHandleTransferRequest(wt)
      }
    } catch {
      case t: Throwable => {
        logError("Exception occurred while handling shuffle server task.", t)
      }
    }
  }

  /**
   * Pushes a task onto the queue to be handled by the server executor.
   *
   * All callbacks handled in the server (from the transport) need to be offloaded into
   * this pool. Note, if this thread blocks we are blocking the progress thread of the transport.
   *
   * @param op One of the case classes in `ShuffleServerOps`
   */
  def asyncOrBlock(op: Any): Unit = {
    exec.execute(() => handleOp(op))
  }

  /**
   * Keep a list of BufferSendState that are waiting for bounce buffers.
   */
  private[this] val pendingTransfersQueue = new ConcurrentLinkedQueue[PendingTransferResponse]()
  private[this] val bssContinueQueue = new ConcurrentLinkedQueue[BufferSendState]()

  /**
   * Send states from just before their send until its callback runs, or until the catch around
   * the send if it throws. The transport may still read their bounce buffers, so only that
   * callback ends them, or `releaseAbandoned` once the transport has stopped. Guarded by its own
   * monitor, which also guards `inFlightSealed`.
   */
  private[this] val inFlight = new mutable.HashSet[BufferSendState]()

  /** Set by `releaseAbandoned`: no send may be issued after it. */
  private[this] var inFlightSealed = false

  // the send loop has started, and has exited after ending what was still queued
  @volatile private[this] var loopEntered = false
  private[this] val loopExited = new CountDownLatch(1)

  /**
   * Executor that loops until it finds bounce buffers for [[BufferSendState]],
   * and when it does it hands them off to a thread pool for handling.
   */
  bssExec.execute(() => {
    loopEntered = true
    var exitFailure: Throwable = null
    try {
      while (started) {
        val bssToIssue = new ArrayBuffer[BufferSendState]()
        var backOff = false
        try {
          var bssContinue = bssContinueQueue.poll()
          while (bssContinue != null) {
            // Only this loop ends a queued state, so a state that already ended was queued in
            // error, and driving it again would copy into a bounce buffer it no longer owns.
            if (bssContinue.isTerminal) {
              logWarning(s"Skipping $bssContinue, which was queued after it ended")
            } else {
              bssToIssue.append(bssContinue)
            }
            bssContinue = bssContinueQueue.poll()
          }

          var continue = true
          // checking `started` keeps the loop from taking bounce buffers that teardown frees
          while (started && !pendingTransfersQueue.isEmpty && continue) {
            // TODO: throttle on too big a send total so we don't acquire the world (in flight
            //  limit)
            val sendBounceBuffers =
              transport.tryGetSendBounceBuffers(1, 1)
            if (sendBounceBuffers.nonEmpty) {
              val pendingTransfer = pendingTransfersQueue.poll()
              // there's only one bounce buffer here for now
              newSendState(pendingTransfer, sendBounceBuffers.head).foreach(bssToIssue.append(_))
            } else {
              // TODO: make this a metric => "blocked while waiting on bounce buffers"
              logTrace(s"Can't acquire send bounce buffers")
              continue = false
            }
          }
        } catch {
          case NonFatal(t) =>
            // no send has been issued in this pass yet, so these states can end here
            bssToIssue.foreach(_.finish(BufferSendState.Failed,
              errorReply(BufferSendState.Failed, t)))
            bssToIssue.clear()
            if (BufferSendState.endsSendLoop(t)) {
              throw t
            }
            logError("Error while gathering shuffle sends. These sends will not be retried.", t)
            backOff = true
        }
        if (bssToIssue.nonEmpty) {
          doHandleTransferRequest(bssToIssue.toSeq)
        }

        bssExec.synchronized {
          if (started &&
              (backOff || (bssContinueQueue.isEmpty && pendingTransfersQueue.isEmpty))) {
            bssExec.wait(100)
          }
        }
      }
    } catch {
      case t: Throwable =>
        exitFailure = t
        throw t
    } finally {
      try {
        endQueuedOnExit()
      } catch {
        // keep the error the loop is exiting with, which a drain failure would replace
        case t: Throwable if exitFailure != null && (t ne exitFailure) =>
          exitFailure.addSuppressed(t)
      } finally {
        loopExited.countDown()
      }
    }
  })

  /**
   * Builds the send state of a transfer request. If its lookups fail, nothing was sent and the
   * request is not retried: this method releases the bounce buffers, sends the requester an
   * error reply if it can, and closes the request transaction.
   */
  private def newSendState(
      pendingTransfer: PendingTransferResponse,
      sendBounceBuffers: SendBounceBuffers): Option[BufferSendState] = {
    try {
      Some(new BufferSendState(
        pendingTransfer.tx,
        sendBounceBuffers,
        pendingTransfer.requestHandler,
        serverStream,
        pendingTransfer.arrivalNanos))
    } catch {
      case NonFatal(t) =>
        val gone = isOutputGone(t)
        if (gone) {
          // expected after an unregister, possibly once per request, so the trace is debug only
          logWarning(s"Unable to start transfer request ${pendingTransfer.tx}: the requested " +
              s"shuffle output is gone (${t.getMessage}). It will not be retried.")
          logDebug(s"Missing output for transfer request ${pendingTransfer.tx}", t)
        } else {
          logError(s"Unable to start transfer request ${pendingTransfer.tx}. " +
              "It will not be retried.", t)
        }
        closeQuietly("send bounce buffers", sendBounceBuffers)
        // a request whose own receive failed has no header to answer
        if (pendingTransfer.tx.getStatus == TransactionStatus.Success) {
          val requested = t match {
            case e: BufferLookupException => e.tableIds
            case _ => Seq.empty
          }
          errorReply(if (gone) BufferSendState.MissingOutput else BufferSendState.Failed, t)
            .foreach { reply =>
              BufferSendState.sendErrorReply(pendingTransfer.tx, reply.gate,
                new RefCountedDirectByteBuffer(
                  ShuffleMetadata.buildTransferErrorResponse(requested, reply.reason)))
            }
        }
        closeQuietly("a transfer request", pendingTransfer.tx)
        None
    }
  }

  /**
   * When the send loop exits, ends every state and closes every request still queued. They are
   * taken under the monitor with `started` false, so nothing can be queued after this.
   */
  private def endQueuedOnExit(): Unit = {
    val states = new ArrayBuffer[BufferSendState]()
    val requests = new ArrayBuffer[PendingTransferResponse]()
    bssExec.synchronized {
      started = false
      var state = bssContinueQueue.poll()
      while (state != null) {
        states.append(state)
        state = bssContinueQueue.poll()
      }
      var request = pendingTransfersQueue.poll()
      while (request != null) {
        requests.append(request)
        request = pendingTransfersQueue.poll()
      }
    }
    var failure: Throwable = null
    states.foreach { state =>
      try {
        state.finish(BufferSendState.ServerClosed)
      } catch {
        case NonFatal(t) => if (failure == null) failure = t else failure.addSuppressed(t)
      }
    }
    requests.foreach(r => closeQuietly("a transfer request", r.tx))
    if (failure != null) {
      throw failure
    }
  }

  private def isOutputGone(error: Throwable): Boolean = error match {
    case _: ShuffleOutputGoneException => true
    case e: BufferLookupException => isOutputGone(e.getCause)
    case ex: RapidsShuffleSendPrepareException =>
      ex.getCause.isInstanceOf[ShuffleOutputGoneException]
    case _ => false
  }

  /**
   * The error reply for a request that ended with `outcome` because of `cause`. There is none
   * when `cause` ends the send loop, since the executor then exits and the requester hears of it
   * when its connection closes. There is none once the server is closing either, which saves
   * building a reply that `replyGate` would not let through. The send loop can still be running
   * then, since the server's close and the transport's each wait for it a bounded time. A caller
   * that ends several states calls this once per state, since the server can close between two
   * of them.
   */
  private def errorReply(
      outcome: BufferSendState.Outcome,
      cause: Throwable): Option[BufferSendState.ErrorReply] = {
    if (BufferSendState.endsSendLoop(cause) || !started) {
      None
    } else {
      Some(BufferSendState.ErrorReply(RapidsShuffleServer.failureReason(outcome), replyGate))
    }
  }

  private def closeQuietly(what: String, resource: AutoCloseable): Unit = {
    try {
      resource.close()
    } catch {
      case NonFatal(t) => logError(s"Error while closing $what", t)
    }
  }

  // NOTE: this stream will likely move to its own non-blocking stream in the future
  val serverStream = Cuda.DEFAULT_STREAM

  /**
   * Handler for a metadata request. It queues request handlers for either
   * [[MessageType.MetadataRequest]] or [[MessageType.TransferRequest]], and re-issues
   * receives for either type of request.
   *
   * NOTE: This call must be non-blocking. It is called from the progress thread.
   *
   * @param messageType The message type received
   */
  private def registerRequestHandler(messageType: MessageType.Value): Unit = {
    logDebug(s"Registering ${messageType} request callback")
    serverConnection.registerRequestHandler(messageType, tx => {
      NvtxRegistry.HANDLE_META_REQUEST {
        messageType match {
          case MessageType.MetadataRequest =>
            asyncOrBlock(HandleMeta(tx))
          case MessageType.TransferRequest =>
            val pendingTransfer = PendingTransferResponse(tx, requestHandler, currentTimeNanos())
            val queued = bssExec.synchronized {
              if (started) {
                pendingTransfersQueue.add(pendingTransfer)
                bssExec.notifyAll()
              }
              started
            }
            if (queued) {
              logDebug(s"Got a transfer request ${pendingTransfer} from ${tx}. " +
                s"Pending requests [new=${pendingTransfersQueue.size}, " +
                s"continuing=${bssContinueQueue.size}]")
            } else {
              // the server is closing, so this request is neither served nor answered
              logDebug(s"Dropping transfer request ${pendingTransfer} from ${tx} after close")
              closeQuietly("a transfer request received after close", tx)
            }
        }
      }
    })
  }

  /** A transfer request waiting for send bounce buffers, and when it arrived. */
  case class PendingTransferResponse(
      tx: Transaction,
      requestHandler: RapidsShuffleRequestHandler,
      arrivalNanos: Long)

  /**
   * Handles the very first message that a client will send, in order to request Table/Buffer info.
   * @param tx: [[Transaction]] - a transaction object that carries status and payload
   */
  def doHandleMetadataRequest(tx: Transaction): Unit = {
    withResource(tx) { _ =>
      NvtxRegistry.DO_HANDLE_META {
        withResource(tx.releaseMessage()) { mtb =>
          if (tx.getStatus == TransactionStatus.Error) {
            logError("error getting metadata request: " + tx)
          } else {
            val req = ShuffleMetadata.getMetadataRequest(mtb.getBuffer())

            logDebug(s"Received request req:\n: ${ShuffleMetadata.printRequest(req)}")
            logDebug(s"HandleMetadataRequest for peerExecutorId ${tx.peerExecutorId()} and " +
              s"tx ${tx}")

            // NOTE: MetaUtils will have a simpler/better way of handling creating a response.
            // That said, at this time, I see some issues with that approach from the flatbuffer
            // library, so the code to create the metadata response will likely change.
            val responseTables = (0 until req.blockIdsLength()).flatMap { i =>
              val blockId = req.blockIds(i)
              // this is getting shuffle buffer ids
              requestHandler.getShuffleBufferMetas(
                ShuffleBlockBatchId(blockId.shuffleId(), blockId.mapId(),
                  blockId.startReduceId(), blockId.endReduceId()))
            }

            val metadataResponse =
              ShuffleMetadata.buildMetaResponse(responseTables)
            // Wrap the buffer so we keep a reference to it, and we destroy it later on .close
            val respBuffer = new RefCountedDirectByteBuffer(metadataResponse)
            val materializedResponse = ShuffleMetadata.getMetadataResponse(metadataResponse)

            logDebug(s"Response will be at header ${TransportUtils.toHex(tx.getHeader)}:\n" +
              s"${ShuffleMetadata.printResponse("responding", materializedResponse)}")

            val responseTx = tx.respond(respBuffer.getBuffer(),
              withResource(_) { responseTx =>
                responseTx.getStatus match {
                  case TransactionStatus.Success =>
                    withResource(respBuffer) { _ =>
                      val stats = responseTx.getStats
                      logDebug(s"Sent metadata ${stats.sendSize} in ${stats.txTimeMs} ms")
                    }
                  case TransactionStatus.Error =>
                    logError(s"Error sending metadata response in tx $tx")
                }
              })
            logDebug(s"Waiting for send metadata to complete: $responseTx")
          }
        }
      }
    }
  }

  /**
   * Queues send states for their next window.
   * @return the states it did not queue because the server is closing; the caller ends them,
   *         outside the monitor
   */
  // exposed for testing
  private [shuffle] def addToContinueQueue(
      bufferSendStates: Seq[BufferSendState]): Seq[BufferSendState] = {
    val queued = bssExec.synchronized {
      if (started) {
        bufferSendStates.foreach(bssContinueQueue.add)
        bssExec.notifyAll()
      }
      started
    }
    if (queued) Seq.empty else bufferSendStates
  }

  /**
   * This will kick off, or continue to work, a [[BufferSendState]] object
   * until all tables are fully transmitted.
   *
   * @param bufferSendStates state objects tracking sends needed to fulfill a TransferRequest
   */
  def doHandleTransferRequest(bufferSendStates: Seq[BufferSendState]): Unit = {
    val bssBuffers =
      new ArrayBuffer[(BufferSendState, MemoryBuffer)](bufferSendStates.size)
    // States whose send this pass reached. Their callback, or the catch around their send,
    // decides how they end, so the catch at the bottom leaves them alone.
    val sendsReached = new mutable.HashSet[BufferSendState]()
    try {
      var toTryAgain: ArrayBuffer[BufferSendState] = null
      var supressedErrors: ArrayBuffer[Throwable] = null
      bufferSendStates.foreach { bufferSendState =>
        NvtxRegistry.SHUFFLE_TRANSFER_REQUEST {
          require(bufferSendState.hasMoreSends, "Attempting to handle a complete transfer request.")

          // For each `BufferSendState` we ask for a bounce buffer fill up
          // so the server is servicing N (`bufferSendStates`) requests
          try {
            val buffersToSend = bufferSendState.getBufferToSend()
            bssBuffers.append((bufferSendState, buffersToSend))
          } catch {
            case ex: RapidsShuffleSendPrepareException if BufferSendState.endsSendLoop(ex) =>
              // the catch at the bottom ends this pass's states, then ends the loop
              throw ex
            case ex: RapidsShuffleSendPrepareException =>
              // We failed to prepare the send (copy to bounce buffer), and got an exception.
              // Put the `bufferSendState` back in the continue queue, so it can be retried.
              // If no `BufferSendState` could be handled, retry only transient OOM failures.
              if (toTryAgain == null) {
                toTryAgain = new ArrayBuffer[BufferSendState]()
                supressedErrors = new ArrayBuffer[Throwable]()
              }
              toTryAgain.append(bufferSendState)
              supressedErrors.append(ex)
          }
        }
      }

      var statesToRetry: Seq[BufferSendState] = Seq.empty
      if (toTryAgain != null) {
        // we failed at least 1 time to copy to the bounce buffer
        val allFailures = toTryAgain.toSeq.zip(supressedErrors.toSeq)
        def isMaterializationOom(error: Throwable): Boolean = error match {
          case ex: RapidsShuffleSendPrepareException =>
            ex.getCause.isInstanceOf[OutOfMemoryError]
          case _ => false
        }

        // A buffer that is gone never comes back, so its state ends now rather than retry. It
        // failed before copying anything, so no copy into its bounce buffer can be running.
        val (gone, failures) = allFailures.partition(f => isOutputGone(f._2))
        if (gone.nonEmpty) {
          // expected after an unregister, so the trace is debug only
          logWarning(s"Unable to send ${gone.size} transfer requests, because requested " +
              s"shuffle output is gone (${gone.head._2.getMessage}): " +
              RapidsShuffleServer.describeRequests(
                gone.map(f => (f._1.peerExecutorId, f._1.getPeerBufferReceiveHeader))) +
              ". These sends will not be retried.")
          logDebug("Missing output while preparing shuffle sends", gone.head._2)
          gone.foreach { case (state, error) =>
            state.finish(BufferSendState.MissingOutput,
              errorReply(BufferSendState.MissingOutput, error))
          }
        }

        // Preserve the existing fail-fast behavior when an entire batch fails and at least one
        // preparation failure is not a materialization OOM. The catch below ends these sends,
        // and the send loop goes on.
        if (bssBuffers.isEmpty && !failures.forall(f => isMaterializationOom(f._2))) {
          val ise = new IllegalStateException("Unable to prepare any sends. " +
              "This issue can occur when requesting too many shuffle blocks. " +
              "The sends will not be retried.")
          failures.foreach(f => ise.addSuppressed(f._2))
          throw ise
        }

        val oomFailures = failures.filter(f => isMaterializationOom(f._2))
        val oomDecisions = if (oomFailures.nonEmpty) {
          val nowNanos = currentTimeNanos()
          val retryNanos = RapidsShuffleServer.oomRetryNanos(oomRetryTimeoutNanos)
          oomFailures.map { case (state, error) =>
            (state, error, state.recordOomAndGetRetryAttempt(nowNanos, retryNanos))
          }
        } else {
          Seq.empty
        }
        val (retryableOom, expiredOom) = oomDecisions.partition(_._3.isDefined)

        if (expiredOom.nonEmpty) {
          stopOomRetries(
            expiredOom.map(_._1),
            expiredOom.map(_._2),
            "Their requests passed the OOM retry deadline, which comes before their readers' " +
                "spark.network.timeout runs out.")
        }

        val retryableOomStates = retryableOom.map(_._1).toSet
        statesToRetry = failures.collect {
          case (state, error)
              if !isMaterializationOom(error) || retryableOomStates.contains(state) =>
            state
        }

        if (bssBuffers.isEmpty && retryableOom.nonEmpty) {
          val retryAttempt = retryableOom.flatMap(_._3).max
          val backoffMillis = oomRetryBackoffMillis(retryAttempt)
          val message = s"Memory exhausted while preparing ${retryableOom.size} sends. " +
              s"Retry attempt $retryAttempt will start after $backoffMillis ms."
          if (retryAttempt == 1 || retryAttempt % 100 == 0) {
            logWarning(message)
          } else {
            logDebug(message)
          }
          // Avoid a hot loop because each failed materialization can repeat spill sweeps and
          // device synchronization before it reports the allocation failure.
          waitBeforeOomRetry(backoffMillis)
        } else if (statesToRetry.nonEmpty) {
          logWarning(s"Unable to prepare ${statesToRetry.size} sends. " +
              "This issue can occur when requesting many shuffle blocks. " +
              "The sends will be retried.")
        }
      }

      serverStream.sync()

      // need to release at this point, we do this after the sync so
      // we are sure we actually copied everything to the bounce buffer
      bufferSendStates.foreach(_.releaseAcquiredToCatalog())

      bssBuffers.foreach { case (bufferSendState, buffersToSend) =>
        sendsReached += bufferSendState
        // make sure we close the buffer slice
        withResource(buffersToSend) { _ =>
          issueSend(bufferSendState, buffersToSend)
        }
      }

      // Each BufferSendState owns its OOM retry window. Incidental co-batching cannot expire a
      // fresh state or reset a failing state when an unrelated state makes progress. Retries are
      // queued last, so a failure earlier in this pass never ends a state that is already queued.
      if (statesToRetry.nonEmpty) {
        addToContinueQueue(statesToRetry).foreach(_.finish(BufferSendState.ServerClosed))
      }
    } catch {
      // a raw interruption comes from the OOM backoff's sleep
      case t if NonFatal(t) || t.isInstanceOf[InterruptedException] =>
        // Failures while cleaning up are kept with the one that started it, so that a fatal
        // CUDA error or an interruption among them still ends the loop below.
        def keep(e: Throwable): Unit = if (e ne t) t.addSuppressed(e)
        try {
          serverStream.sync()
        } catch {
          case NonFatal(e) => keep(e)
        }
        bssBuffers.foreach { case (bufferSendState, buffersToSend) =>
          if (!sendsReached.contains(bufferSendState)) {
            buffersToSend.safeClose(t)
          }
        }
        bufferSendStates.foreach { bufferSendState =>
          if (!sendsReached.contains(bufferSendState)) {
            try {
              // The reply is decided per state, since ending one can add an error to `t` that
              // ends the loop. The states answered before that stay answered.
              bufferSendState.finish(BufferSendState.Failed, errorReply(BufferSendState.Failed, t))
            } catch {
              case NonFatal(e) => keep(e)
            }
          }
        }
        // The GPU is unusable after a fatal CUDA error, and an interruption asks the thread to
        // stop. Either ends the loop, as it always did, so that the executor exits and its
        // shuffle output is recomputed elsewhere.
        if (BufferSendState.endsSendLoop(t)) {
          if (BufferSendState.wasInterrupted(t)) {
            Thread.currentThread().interrupt()
          }
          throw t
        }
        logError("Error while handling shuffle sends. The sends not yet issued will not be " +
            "retried.", t)
    }
  }

  /**
   * Issues the send of one window. A send that throws queued nothing and never calls back, so
   * its state ends here; otherwise the state is in flight until its callback runs.
   */
  private def issueSend(bufferSendState: BufferSendState, buffersToSend: MemoryBuffer): Unit = {
    val registered = inFlight.synchronized {
      if (!inFlightSealed) {
        inFlight += bufferSendState
      }
      !inFlightSealed
    }
    if (!registered) {
      // the transport has stopped, so this send could never complete
      bufferSendState.finish(BufferSendState.ServerClosed)
    } else {
      val peerExecutorId = bufferSendState.peerExecutorId
      val sendHeader = bufferSendState.getPeerBufferReceiveHeader
      // [Scala 2.13] The compiler does not seem to be able to do the implicit SAM
      // conversion after expanding the call in the method call below. So we have to define the
      // callback here in a val and type it to TransactionCallback
      val txCallback: TransactionCallback =
        tx => onSendComplete(bufferSendState, buffersToSend, tx)
      try {
        serverConnection.send(peerExecutorId, MessageType.Buffer,
          // TODO: it may be nice to hide `sendHeader` in `Transaction`
          sendHeader, buffersToSend, txCallback)
        bufferSendState.windowIssued()
      } catch {
        case NonFatal(t) =>
          inFlight.synchronized {
            inFlight -= bufferSendState
          }
          logError(s"Error while issuing a send for $bufferSendState. " +
              "It will not be retried.", t)
          bufferSendState.finish(BufferSendState.Failed, errorReply(BufferSendState.Failed, t))
      }
    }
  }

  /**
   * The callback of one window's send, on the transport's progress thread, which must not
   * block. It takes no monitor while it calls into the transport.
   */
  private def onSendComplete(
      bufferSendState: BufferSendState,
      buffersToSend: MemoryBuffer,
      tx: Transaction): Unit = {
    withResource(tx) { bufferTx =>
      inFlight.synchronized {
        inFlight -= bufferSendState
      }
      if (bufferTx.getStatus != TransactionStatus.Success) {
        // A failed send can report InProgress here rather than Error, so this tests for
        // anything but success. No reply follows: a reply would take the same failing path.
        logError(s"Error while sending buffers $bufferTx.")
        bufferSendState.finish(BufferSendState.SendFailed)
        wakeLoop()
      } else if (bufferSendState.isTerminal) {
        logWarning(s"Send completed for $bufferSendState, which already ended")
      } else if (bufferSendState.hasMoreSends) {
        logDebug(s"Done with the send for $bufferSendState with $buffersToSend")
        // continue issuing sends.
        logDebug(s"Buffer send state $bufferSendState is NOT done. " +
          s"Still pending: ${pendingTransfersQueue.size}.")
        addToContinueQueue(Seq(bufferSendState)).foreach(_.finish(BufferSendState.ServerClosed))
      } else if (!started) {
        // A reply queued after the transport's close task would never run or release its
        // response, so a closing server ends the state without one.
        bufferSendState.finish(BufferSendState.ServerClosed)
        wakeLoop()
      } else {
        var outcome: BufferSendState.Outcome = BufferSendState.Failed
        try {
          val transferResponse = bufferSendState.getTransferResponse()

          val requestTx = bufferSendState.getRequestTransaction
          logDebug(s"Handling transfer request $requestTx for executor " +
            s"${bufferSendState.peerExecutorId} with $buffersToSend")

          // send the transfer response
          val queued = try {
            replyGate.queueIfOpen {
              requestTx.respond(transferResponse.acquire(), withResource(_) { responseTx =>
                withResource(transferResponse) { _ =>
                  responseTx.getStatus match {
                    case TransactionStatus.Cancelled | TransactionStatus.Error =>
                      logError(s"Error while handling TransferResponse: " +
                        s"${responseTx.getErrorMessage}")
                    case _ =>
                  }
                }
              })
            }
          } catch {
            case t: Throwable =>
              // a respond that throws queued nothing, so its callback will not release this
              closeQuietly("a transfer response", transferResponse)
              throw t
          }
          if (queued) {
            outcome = BufferSendState.Completed
          } else {
            // the server closed after the check above
            closeQuietly("a transfer response", transferResponse)
            outcome = BufferSendState.ServerClosed
          }
        } finally {
          logDebug(s"Buffer send state " +
            s"${TransportUtils.toHex(bufferSendState.getPeerBufferReceiveHeader)} " +
            s"is done, closing. Still pending: ${pendingTransfersQueue.size}.")
          bufferSendState.finish(outcome)
        }
        wakeLoop()
      }
    }
  }

  // wake up the bssExec, since a state that ended returned its bounce buffers
  private def wakeLoop(): Unit = bssExec.synchronized {
    bssExec.notifyAll()
  }

  override def close(): Unit = {
    bssExec.synchronized {
      started = false
      bssExec.notifyAll()
      // so that each reply let through is queued before the transport closes
      while (repliesQueueing > 0) {
        bssExec.wait()
      }
    }
    // The loop ends what is still queued as it exits. The wait is bounded, because a pass can
    // be stuck in a CUDA synchronization.
    if (loopEntered &&
        !loopExited.await(RapidsShuffleServer.CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
      logWarning("The shuffle server's send loop did not exit in time")
    }
  }

  // exposed for testing
  private[shuffle] def inFlightCount: Int = inFlight.synchronized(inFlight.size)

  /**
   * Called by the transport once it has closed: ends every send state whose callback never
   * came, and seals the in-flight set so that no send is issued after this.
   * @param transportStopped whether the transport's progress thread has terminated. If it has
   *                         not, the transport may still read those bounce buffers, so they are
   *                         leaked rather than freed.
   */
  def releaseAbandoned(transportStopped: Boolean): Unit = {
    val abandoned = inFlight.synchronized {
      inFlightSealed = true
      val states = inFlight.toList
      inFlight.clear()
      states
    }
    if (abandoned.nonEmpty) {
      if (transportStopped) {
        logWarning(s"Ending ${abandoned.size} shuffle sends whose completion never arrived")
        abandoned.foreach { state =>
          try {
            state.finish(BufferSendState.ServerClosed)
          } catch {
            case NonFatal(t) => logError(s"Error while ending abandoned send $state", t)
          }
        }
      } else {
        logWarning(s"Leaking ${abandoned.size} shuffle sends, because the transport did not " +
            "stop and may still read their bounce buffers")
      }
    }
  }
}

object RapidsShuffleServer {
  // how long close() waits for the send loop to exit
  private val CLOSE_WAIT_MILLIS = 500L

  // How much sooner than its reader's wait a request's OOM retries end, for the error reply to
  // reach the reader first. It covers the time from the reader starting to wait to the request
  // arriving, the retry pass that finds the end, and the reply's delivery. A request that waited
  // longer than that in its requester's queue, which the server cannot see, still arrives too
  // late, and its reader times out.
  private val OOM_REPLY_MARGIN_NANOS = TimeUnit.SECONDS.toNanos(10)

  /**
   * How long after its request arrived a send may retry a materialization OOM, given how long
   * the reader waits: that wait less a margin of 10 seconds, or of half the wait if it is
   * shorter than 20 seconds.
   */
  private[shuffle] def oomRetryNanos(readerWaitNanos: Long): Long =
    readerWaitNanos - math.min(OOM_REPLY_MARGIN_NANOS, readerWaitNanos / 2)

  /** Why a transfer request ended with `outcome`, in the error reply its requester gets. */
  private[shuffle] def failureReason(outcome: BufferSendState.Outcome): String = outcome match {
    case BufferSendState.MissingOutput => "the shuffle output it asked for is gone"
    case BufferSendState.OomExpired =>
      "memory ran out while preparing it, past its retry deadline"
    case _ => "the server failed to prepare or send it"
  }

  // how many requests a log line names before it only counts the rest
  private val MAX_REQUESTS_NAMED = 5

  /**
   * Names requests by the executor that sent each one and its receive header, the first few
   * of them, for a log line.
   * @param requests the executor id and receive header of each request
   */
  private[shuffle] def describeRequests(requests: Seq[(Long, Long)]): String = {
    val named = requests.take(MAX_REQUESTS_NAMED).map { case (executorId, header) =>
      s"executor $executorId header ${TransportUtils.toHex(header)}"
    }
    val more = requests.size - named.size
    named.mkString(", ") + (if (more > 0) s" and $more more" else "")
  }
}
