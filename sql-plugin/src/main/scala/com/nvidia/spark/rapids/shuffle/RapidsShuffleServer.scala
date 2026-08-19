/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{ConcurrentLinkedQueue, Executor}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, MemoryBuffer}
import com.nvidia.spark.rapids.{NvtxRegistry, RapidsConf, RapidsShuffleHandle, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rapids.RapidsShuffleSendPrepareException
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockBatchId}

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
   * Acquires (locks w.r.t. the memory tier) a [[RapidsBuffer]] corresponding to a table id.
   * @param tableId the unique id for a table in the catalog
   * @return a [[RapidsBuffer]] which is reference counted, and should be closed by the acquirer
   */
  def getShuffleHandle(tableId: Int): RapidsShuffleHandle
}

/**
 * A server that replies to shuffle metadata messages, and issues device/host memory sends.
 *
 * A single command thread is used to orchestrate sends/receives and to remove
 * from transport's progress thread.
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
   * On close, this is set to false to indicate that the server is shutting down.
   */
  private[this] var started = true

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
   * Executor that loops until it finds bounce buffers for [[BufferSendState]],
   * and when it does it hands them off to a thread pool for handling.
   */
  bssExec.execute(() => {
    while (started) {
      closeOnExcept(new ArrayBuffer[BufferSendState]()) { bssToIssue =>
        var bssContinue = bssContinueQueue.poll()
        while (bssContinue != null) {
          bssToIssue.append(bssContinue)
          bssContinue = bssContinueQueue.poll()
        }

        var continue = true
        while (!pendingTransfersQueue.isEmpty && continue) {
          // TODO: throttle on too big a send total so we don't acquire the world (in flight limit)
          val sendBounceBuffers =
            transport.tryGetSendBounceBuffers(1, 1)
          if (sendBounceBuffers.nonEmpty) {
            val pendingTransfer = pendingTransfersQueue.poll()
            bssToIssue.append(new BufferSendState(
              pendingTransfer.tx,
              sendBounceBuffers.head, // there's only one bounce buffer here for now
              pendingTransfer.requestHandler,
              serverStream))
          } else {
            // TODO: make this a metric => "blocked while waiting on bounce buffers"
            logTrace(s"Can't acquire send bounce buffers")
            continue = false
          }
        }
        if (bssToIssue.nonEmpty) {
          doHandleTransferRequest(bssToIssue.toSeq)
        }
      }

      bssExec.synchronized {
        if (bssContinueQueue.isEmpty && pendingTransfersQueue.isEmpty) {
          bssExec.wait(100)
        }
      }
    }
  })

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
            val pendingTransfer = PendingTransferResponse(tx, requestHandler)
            bssExec.synchronized {
              pendingTransfersQueue.add(pendingTransfer)
              bssExec.notifyAll()
            }
            logDebug(s"Got a transfer request ${pendingTransfer} from ${tx}. " +
              s"Pending requests [new=${pendingTransfersQueue.size}, " +
              s"continuing=${bssContinueQueue.size}]")
        }
      }
    })
  }

  case class PendingTransferResponse(tx: Transaction, requestHandler: RapidsShuffleRequestHandler)

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

  // exposed for testing
  private [shuffle] def addToContinueQueue(
      bufferSendStates: Seq[BufferSendState]): Unit = bssExec.synchronized {
    bufferSendStates.foreach(bssContinueQueue.add)
    bssExec.notifyAll()
  }

  /**
   * This will kick off, or continue to work, a [[BufferSendState]] object
   * until all tables are fully transmitted.
   *
   * @param bufferSendStates state objects tracking sends needed to fulfill a TransferRequest
   */
  def doHandleTransferRequest(bufferSendStates: Seq[BufferSendState]): Unit = {
    closeOnExcept(bufferSendStates) { _ =>
      val bssBuffers =
        new ArrayBuffer[(BufferSendState, MemoryBuffer)](bufferSendStates.size)

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
            case ex: RapidsShuffleSendPrepareException =>
              // We failed to prepare the send (copy to bounce buffer), and got an exception.
              // Put the `bufferSendState` back in the continue queue, so it can be retried.
              // If no `BufferSendState` could be handled without error, nothing is retried.
              // TODO: we should respond with a failure to the client.
              // Please see: https://github.com/NVIDIA/spark-rapids/issues/3040
              if (toTryAgain == null) {
                toTryAgain = new ArrayBuffer[BufferSendState]()
                supressedErrors = new ArrayBuffer[Throwable]()
              }
              toTryAgain.append(bufferSendState)
              supressedErrors.append(ex)
          }
        }
      }

      if (toTryAgain != null) {
        // we failed at least 1 time to copy to the bounce buffer
        if (bssBuffers.isEmpty) {
          // we were not able to handle anything, error out.
          val ise = new IllegalStateException("Unable to prepare any sends. " +
              "This issue can occur when requesting too many shuffle blocks. " +
              "The sends will not be retried.")
          supressedErrors.foreach(ise.addSuppressed)
          throw ise
        } else {
          // we at least handled 1 `BufferSendState`, lets continue to retry
          logWarning(s"Unable to prepare ${toTryAgain.size} sends. " +
              "This issue can occur when requesting many shuffle blocks. " +
              "The sends will be retried.")
        }

        // If we are still able to handle at least one `BufferSendState`, add any
        // others that also failed due back to the queue.
        addToContinueQueue(toTryAgain.toSeq)
      }

      serverStream.sync()

      // need to release at this point, we do this after the sync so
      // we are sure we actually copied everything to the bounce buffer
      bufferSendStates.foreach(_.releaseAcquiredToCatalog())

      bssBuffers.foreach { case (bufferSendState, buffersToSend) =>
        val peerExecutorId = bufferSendState.peerExecutorId
        val sendHeader = bufferSendState.getPeerBufferReceiveHeader
        // make sure we close the buffer slice
        withResource(buffersToSend) { _ =>
          // [Scala 2.13] The compiler does not seem to be able to do the implicit SAM
          // conversion after expanding the call in the method call below. So we have to define the 
          // callback here in a val and type it to TransactionCallback
          val txCallback: TransactionCallback = tx => withResource(tx) { bufferTx =>
            bufferTx.getStatus match {
              case TransactionStatus.Success =>
                logDebug(s"Done with the send for $bufferSendState with $buffersToSend")

                if (bufferSendState.hasMoreSends) {
                  // continue issuing sends.
                  logDebug(s"Buffer send state $bufferSendState is NOT done. " +
                    s"Still pending: ${pendingTransfersQueue.size}.")
                  addToContinueQueue(Seq(bufferSendState))
                } else {
                  val transferResponse = bufferSendState.getTransferResponse()

                  val requestTx = bufferSendState.getRequestTransaction
                  logDebug(s"Handling transfer request $requestTx for executor " +
                    s"$peerExecutorId with $buffersToSend")

                  // send the transfer response
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

                  // wake up the bssExec since bounce buffers became available
                  logDebug(s"Buffer send state " +
                    s"${TransportUtils.toHex(bufferSendState.getPeerBufferReceiveHeader)} " +
                    s"is done, closing. Still pending: ${pendingTransfersQueue.size}.")
                  bssExec.synchronized {
                    bufferSendState.close()
                    bssExec.notifyAll()
                  }
                }
              case _ =>
                // errored or cancelled
                logError(s"Error while sending buffers $bufferTx.")
                bssExec.synchronized {
                  bufferSendState.close()
                  bssExec.notifyAll()
                }
            }
          }

          serverConnection.send(peerExecutorId, MessageType.Buffer,
            // TODO: it may be nice to hide `sendHeader` in `Transaction`
            sendHeader, buffersToSend, txCallback)
        }
      }
    }
  }

  override def close(): Unit = {
    started = false
    bssExec.synchronized {
      bssExec.notifyAll()
    }
  }
}
