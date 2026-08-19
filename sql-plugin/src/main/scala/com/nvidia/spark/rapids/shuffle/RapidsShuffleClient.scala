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

import java.util.concurrent.{ConcurrentHashMap, Executor}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.{MetadataResponse, TableMeta, TransferState}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.storage.ShuffleBlockBatchId

/**
 * trait used by client consumers ([[RapidsShuffleIterator]]) to gather what the
 * expected number of batches (Tables) is, and the ids for each table as they are received.
 */
trait RapidsShuffleFetchHandler {
  /**
   * After a response for shuffle metadata is received, the expected number of columnar
   * batches is communicated back to the caller via this method.
   * @param expectedBatches - number of columnar batches
   */
  def start(expectedBatches: Int): Unit

  /**
   * Called when a buffer is received and has been handed off to the catalog.
   * @param bufferId - a tracked shuffle buffer id
   * @return a boolean that lets the caller know the batch was accepted (true), or
   *         rejected (false), in which case the caller should dispose of the batch.
   */
  def batchReceived(handle: RapidsShuffleHandle): Boolean

  /**
   * Called when the transport layer is not able to handle a fetch error for metadata
   * or buffer fetches.
   *
   * @param errorMessage - a string containing an error message
   */
  def transferError(errorMessage: String, throwable: Throwable = null): Unit

  /**
   * Called to get the task attempt ids that this shuffle handler is for.
   */
  def getTaskIds: Array[Long]
}

/**
 * A helper case class that describes a pending table. It allows
 * the transport to schedule/throttle these requests to fit within the maximum bytes in flight.
 * @param client client used to issue the requests
 * @param tableMeta shuffle metadata describing the table
 * @param handler a specific handler that is waiting for this batch
 */
case class PendingTransferRequest(client: RapidsShuffleClient,
                                  tableMeta: TableMeta,
                                  handler: RapidsShuffleFetchHandler) {
  val getLength: Long = tableMeta.bufferMeta.size()
}

/**
 * The client makes requests via a `ClientConnection` obtained from the [[RapidsShuffleTransport]].
 *
 * This class handles fetch requests from [[RapidsShuffleIterator]], turning them into
 * [[ShuffleMetadata]] messages, and shuffle `TransferRequest`s.
 *
 * Its counterpart is the [[RapidsShuffleServer]] on a specific peer executor, specified by
 * `connection`.
 *
 * @param connection a connection object against a remote executor
 * @param transport used to get metadata buffers and to work with the throttle mechanism
 * @param exec Executor used to handle tasks that take time, and should not be in the
 *             transport's thread
 * @param clientCopyExecutor Executors used to handle synchronous mem copies
 * @param catalog catalog to use to track received shuffle blocks
 */
class RapidsShuffleClient(
    val connection: ClientConnection,
    transport: RapidsShuffleTransport,
    exec: Executor,
    clientCopyExecutor: Executor,
    catalog: ShuffleReceivedBufferCatalog = GpuShuffleEnv.getReceivedCatalog)
      extends Logging with AutoCloseable {

  // these are handlers that are interested (live spark tasks) in peer failure handling
  private val liveHandlers =
    ConcurrentHashMap.newKeySet[RapidsShuffleFetchHandler]()

  object ShuffleClientOps {
    /**
     * When a metadata response is received, this event is issued to handle it.
     * @param tx the [[Transaction]] to be closed after consuming the response
     * @param shuffleRequests blocks to be requested
     * @param rapidsShuffleFetchHandler the handler (iterator) to callback to
     */
    case class HandleMetadataResponse(tx: Transaction,
                                      shuffleRequests: Seq[ShuffleBlockBatchId],
                                      rapidsShuffleFetchHandler: RapidsShuffleFetchHandler)

    /**
     * Used to have this client handle the enclosed [[BufferReceiveState]] asynchronously.
     *
     * Until [[bufferReceiveState]] completes, this event will continue to get posted.
     *
     * @param bufferReceiveState object containing the state of pending requests to the peer
     */
    case class IssueBufferReceives(bufferReceiveState: BufferReceiveState)

    /**
     * When a buffer is received, this event is posted to remove from the progress thread
     * the copy, and the callback into the iterator.
     *
     * @param tx live transaction for the buffer, to be closed after the buffer is handled
     * @param bufferReceiveState the object maintaining state for receives
     */
    case class HandleBounceBufferReceive(tx: Transaction,
                                         bufferReceiveState: BufferReceiveState)
  }

  import ShuffleClientOps._

  private[this] def handleOp(op: Any): Unit = {
    // functions we dispatch to must not throw
    op match {
      case HandleMetadataResponse(tx, shuffleRequests, rapidsShuffleFetchHandler) =>
        doHandleMetadataResponse(tx, shuffleRequests, rapidsShuffleFetchHandler)
      case IssueBufferReceives(bufferReceiveState) =>
        doIssueBufferReceives(bufferReceiveState)
      case HandleBounceBufferReceive(tx, bufferReceiveState) =>
        doHandleBounceBufferReceive(tx, bufferReceiveState)
    }
  }

  private[this] def asyncOrBlock(op: Any): Unit = {
    exec.execute(() => handleOp(op))
  }

  /**
   * Pushes a task onto the queue to be handled by the client's copy executor.
   *
   * @note - at this stage, tasks in this pool can block (it will grow as needed)
   *
   * @param op One of the case classes in [[ShuffleClientOps]]
   */
  private[this] def asyncOnCopyThread(op: Any): Unit = {
    clientCopyExecutor.execute(() => handleOp(op))
  }

  /**
   * Starts a fetch request for all the shuffleRequests, using `handler` to communicate
   * events back to the iterator.
   *
   * @param shuffleRequests blocks to fetch
   * @param handler iterator to callback to
   */
  def doFetch(shuffleRequests: Seq[ShuffleBlockBatchId],
              handler: RapidsShuffleFetchHandler): Unit = {
    try {
      NvtxRegistry.CLIENT_FETCH {
        require(shuffleRequests.nonEmpty, "Sending empty blockIds in the MetadataRequest?")

        val metaReq = new RefCountedDirectByteBuffer(
          ShuffleMetadata.buildShuffleMetadataRequest(shuffleRequests))

        logDebug(s"Requesting block_ids=[$shuffleRequests] from connection $connection, req: \n " +
            s"${ShuffleMetadata.printRequest(
              ShuffleMetadata.getMetadataRequest(metaReq.getBuffer()))}")

        // make request
        connection.request(MessageType.MetadataRequest, metaReq.acquire(), tx => {
          withResource(metaReq) { _ =>
            asyncOrBlock(HandleMetadataResponse(tx, shuffleRequests, handler))
          }
        })
      }
    } catch {
      case t: Throwable =>
        handler.transferError("Error occurred while requesting metadata", t)
    }
  }

  /**
   * Function to handle MetadataResponses, as a result of the [[HandleMetadataResponse]] event.
   *
   * @param tx live metadata response transaction to be closed in this handler
   * @param shuffleRequests blocks to fetch
   * @param handler iterator to callback to
   */
  private[this] def doHandleMetadataResponse(
      tx: Transaction,
      shuffleRequests: Seq[ShuffleBlockBatchId],
      handler: RapidsShuffleFetchHandler): Unit = {
    try {
      withResource(tx) { _ =>
        tx.getStatus match {
          case TransactionStatus.Success =>
            withResource(tx.releaseMessage()) { resp =>
              NvtxRegistry.CLIENT_HANDLE_META {
                try {
                  // start the receives
                  val metadataResponse =
                    ShuffleMetadata.getMetadataResponse(resp.getBuffer())

                  logDebug(s"Received from ${tx} response: \n:" +
                    s"${ShuffleMetadata.printResponse("received response", metadataResponse)}")

                  // signal to the handler how many batches are expected
                  handler.start(metadataResponse.tableMetasLength())

                  // queue up the receives
                  queueTransferRequests(metadataResponse, handler)
                } catch {
                  case t: Throwable =>
                    handler.transferError("Error occurred while handling metadata", t)
                }
              }
            }
          case _ =>
            handler.transferError(
              tx.getErrorMessage.getOrElse(s"Unsuccessful metadata request $tx"))
        }
      }
    } catch {
      case t: Throwable =>
        handler.transferError(s"Exception while handling metadata response $tx", t)
    }
  }

  /**
   * Used by the transport, to schedule receives. The requests are sent to the executor for this
   * client.
   * @param bufferReceiveState object tracking the state of pending TransferRequests
   */
  def issueBufferReceives(bufferReceiveState: BufferReceiveState): Unit = {
    doIssueBufferReceives(bufferReceiveState)
  }

  def handleBufferReceive(tx: Transaction, bufferReceiveState: BufferReceiveState): Unit = {
    asyncOnCopyThread(HandleBounceBufferReceive(tx, bufferReceiveState))
  }

  /**
   * Issues transfers requests (if the state of [[bufferReceiveState]] advances), or continue to
   * work a current request (continue receiving bounce buffer sized chunks from a larger receive).
   * @param bufferReceiveState object maintaining state of requests to be issued (current or
   *                           future). The requests included in this state object originated in
   *                           the transport's throttle logic.
   */
  private[shuffle] def doIssueBufferReceives(bufferReceiveState: BufferReceiveState): Unit = {
    try {
      logDebug(s"Adding ${connection.getPeerExecutorId} BRS " +
        s"${TransportUtils.toHex(bufferReceiveState.id)}")

      // send a transfer request to kick off receives
      sendTransferRequest(bufferReceiveState.id, bufferReceiveState)
    } catch {
      case t: Throwable =>
        withResource(bufferReceiveState) { _ =>
          bufferReceiveState.errorOccurred("Error issuing buffer receives", t)
        }
    }
  }

  /**
   * Sends the [[com.nvidia.spark.rapids.format.TransferRequest]] metadata message, to ask the
   * server to get started.
   * @param toIssue sequence of [[PendingTransferRequest]] we want included in the server
   *                transfers
   */
  private[this] def sendTransferRequest(id: Long, toIssue: BufferReceiveState): Unit = {
    val requestsToIssue = toIssue.getRequests
    logDebug(s"Sending a transfer request for ${TransportUtils.toHex(toIssue.id)}")

    val transferReq = new RefCountedDirectByteBuffer(
      ShuffleMetadata.buildTransferRequest(id, requestsToIssue.map { i =>
        i.tableMeta.bufferMeta().id()
      }))

    connection.request(MessageType.TransferRequest, transferReq.acquire(), withResource(_) { tx =>
      withResource(transferReq) { _ =>
        tx.getStatus match {
          case TransactionStatus.Success =>
            withResource(tx.releaseMessage()) { mtb =>
              // make sure all bufferTxs are still valid (e.g. resp says that they have STARTED)
              val transferResponse = ShuffleMetadata.getTransferResponse(mtb.getBuffer())
              (0 until transferResponse.responsesLength()).foreach(r => {
                val response = transferResponse.responses(r)
                if (response.state() != TransferState.STARTED) {
                  // we could either re-issue the request, cancelling and releasing memory
                  // or we could re-issue, and leave the old receive waiting
                  // for now, leaving the old receive waiting.
                  throw new IllegalStateException("NOT IMPLEMENTED")
                }
              })
            }
          case _ =>
            toIssue.errorOccurred(tx.getErrorMessage.getOrElse("TransferRequest failed"))
        }
      }
    })
  }

  /**
   * Feed into the throttle thread in the transport [[PendingTransferRequest]], to be
   * issued later via the [[doIssueBufferReceives]] method.
   *
   * @param metaResponse metadata response flat buffer
   * @param handler callback trait (the iterator implements this)
   */
  private def queueTransferRequests(metaResponse: MetadataResponse,
                                    handler: RapidsShuffleFetchHandler): Unit = {
    val allTables = metaResponse.tableMetasLength()
    logDebug(s"Queueing transfer requests for ${allTables} tables " +
      s"from ${connection.getPeerExecutorId}")

    val ptrs = new ArrayBuffer[PendingTransferRequest](allTables)
    (0 until allTables).foreach { i =>
      val tableMeta = ShuffleMetadata.copyTableMetaToHeap(metaResponse.tableMetas(i))
     
      // We check the uncompressedSize to make sure we don't request a 0-sized buffer
      // from a peer. We treat such a corner case as a degenerate batch
      if (tableMeta.bufferMeta() != null && tableMeta.bufferMeta().uncompressedSize() > 0) {
        ptrs += PendingTransferRequest(
          this,
          tableMeta,
          handler)
      } else {
        // Degenerate buffer (no device data) so no more data to request.
        // We need to trigger call in iterator, otherwise this batch is never handled.
        handler.batchReceived(track(null, tableMeta))
      }
    }

    if (ptrs.nonEmpty) {
      transport.queuePending(ptrs.toSeq)
    }
  }

  /**
   * Cancel pending requests for handler `handler` to the peer represented by this client.
   * @param handler instance to use to find requests to cancel
   * @note this currently only cancels pending requests that are queued in the transport,
   *       and not in flight.
   */
  def cancelPending(handler: RapidsShuffleFetchHandler): Unit = {
    transport.cancelPending(handler)
  }

  /**
   * This function handles data received in `bounceBuffers`. The data should be copied out
   * of the buffers, and the function should call into `bufferReceiveState` to advance its
   * state (consumeBuffers)
   * @param tx live transaction for these bounce buffers, it should be closed in this function
   * @param bufferReceiveState state management objects for live transfer requests
   */
  def doHandleBounceBufferReceive(tx: Transaction, 
      bufferReceiveState: BufferReceiveState): Unit = {
    try {
      withResource(tx) { _ =>
        tx.getStatus match {
          case TransactionStatus.Success =>
            NvtxRegistry.BUFFER_CALLBACK {
              // consume buffers, which will non empty for batches that are ready
              // to be handed off to the catalog
              val buffMetas = bufferReceiveState.consumeWindow()

              // the number of batches successfully received that the requesting iterator
              // rejected (limit case)
              var numBatchesRejected = 0

              // hand buffer off to the catalog
              buffMetas.foreach { consumed: ConsumedBatchFromBounceBuffer =>
                val handle = track(consumed.contigBuffer, consumed.meta)
                if (!consumed.handler.batchReceived(handle)) {
                  handle.close()
                  numBatchesRejected += 1
                }
                transport.doneBytesInFlight(consumed.contigBuffer.getLength)
              }

              if (numBatchesRejected > 0) {
                logDebug(s"Removed ${numBatchesRejected} batches that were received after " +
                  s"tasks completed.")
              }

              if (!bufferReceiveState.hasMoreBlocks) {
                logDebug(s"BufferReceiveState: " +
                  s"${TransportUtils.toHex(bufferReceiveState.id)} is DONE, closing.")
                bufferReceiveState.close()
              } else {
                logDebug(s"BufferReceiveState: " +
                  s"${TransportUtils.toHex(bufferReceiveState.id)} is NOT done, continuing.")
              }
            }
          case TransactionStatus.Error =>
            throw new IllegalStateException(s"Transaction errored with ${tx.getErrorMessage}")
          case TransactionStatus.Cancelled =>
            throw new IllegalStateException(s"Transaction cancelled")
         }
      }
    } catch {
      case t: Throwable =>
        withResource(bufferReceiveState) { _ =>
          bufferReceiveState.errorOccurred(
            s"Error while handling buffer receive for BRS: " +
            s"${TransportUtils.toHex(bufferReceiveState.id)}", t)
        }
    }
  }

  /**
   * Hands [[table]] and [[buffer]] to the device storage/catalog, obtaining an id that can be
   * used to look up the buffer from the catalog going (e.g. from the iterator)
   * @param buffer contiguous [[DeviceMemoryBuffer]] with the tables' data
   * @param meta [[TableMeta]] describing [[buffer]]
   * @return a [[RapidsShuffleHandle]] with a spillable and metadata
   */
  private[shuffle] def track(
      buffer: DeviceMemoryBuffer, meta: TableMeta): RapidsShuffleHandle = {
    if (buffer != null) {
      // add the buffer to the catalog so it is available for spill
      catalog.addBuffer(
        buffer,
        meta,
        SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
    } else {
      // no device data, just tracking metadata
      catalog.addDegenerateBatch(meta)
    }
  }

  override def close(): Unit = {
    logInfo(s"Closing pending requests for ${connection.getPeerExecutorId}")
    liveHandlers.forEach { handler =>
      logWarning(s"Signaling ${handler} that ${connection.getPeerExecutorId} errored")
      handler.transferError(s"Connection to ${connection.getPeerExecutorId} closed")
    }
    liveHandlers.clear()
    // pop all the pending buffer receives, and signal that there is an error
  }

  def registerPeerErrorListener(handler: RapidsShuffleFetchHandler): Unit = {
    // keep track of this `RapidsShuffleFetchHandler` as we will need to
    // signal it when there are failures for this peer
    liveHandlers.add(handler)
  }

  def unregisterPeerErrorListener(handler: RapidsShuffleFetchHandler): Unit = {
    logDebug(s"Unregister $handler from client for ${connection.getPeerExecutorId}")
    liveHandlers.remove(handler)
    // Make sure we remove the handler from the transport as well, otherwise
    // we could have a host memory leak: https://github.com/NVIDIA/spark-rapids/issues/7997
    // At this stage this should not cancel any requests because the task should have
    // completed, but it will also remove `handler` from internal tracking in `transport`.
    transport.cancelPending(handler)
  }
}
