/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle.ucx

import java.net._
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle.{ClientConnection, MemoryRegistrationCallback, MessageType, MetadataTransportBuffer, TransportBuffer, TransportUtils}
import org.openucx.jucx._
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.storage.RapidsStorageUtils
import org.apache.spark.storage.BlockManagerId

case class Rkeys(rkeys: Seq[ByteBuffer])

/**
 * A simple wrapper for an Active Message Id and a header. This pair
 * is used together when dealing with Active Messages, with `activeMessageId`
 * being a fire-and-forget registration with UCX, and `header` being a dynamic long
 * we continue to update (it contains the local executor id, and the transaction id).
 *
 * This allows us to send a request (with a header that the response handler knows about),
 * and for the request handler to echo back that header when it's done.
 */
case class UCXActiveMessage(activeMessageId: Int, header: Long, forceRndv: Boolean) {
  override def toString: String =
    UCX.formatAmIdAndHeader(activeMessageId, header)
}

/**
 * The UCX class wraps JUCX classes and handles all communication with UCX from other
 * parts of the shuffle code. It manages a `UcpContext` and `UcpWorker`, for the
 * local executor, and maintain a set of `UcpEndpoint` for peers.
 *
 * This class uses an extra TCP management connection to perform a handshake with remote peers,
 * this port should be distributed to peers by other means (e.g. via the `BlockManagerId`)
 *
 * @param transport transport instance for UCX
 * @param executor blockManagerId of the local executorId
 * @param rapidsConf rapids configuration
 */
class UCX(transport: UCXShuffleTransport, executor: BlockManagerId, rapidsConf: RapidsConf)
    extends AutoCloseable with Logging {

  private[this] val context = {
    val contextParams = new UcpParams()
      .requestTagFeature()
      .requestAmFeature()
    if (rapidsConf.shuffleUcxUseWakeup) {
      contextParams.requestWakeupFeature()
    }
    new UcpContext(contextParams)
  }

  logInfo(s"UCX context created")

  val localExecutorId: Long = executor.executorId.toLong

  // this object implements the transport-friendly interface for UCX
  private[this] val serverConnection = new UCXServerConnection(this, transport)

  // monotonically increasing counter that holds the txId (for debug purposes, at this stage)
  private[this] val txId = new AtomicLong(0L)

  private var worker: UcpWorker = _
  private var endpointManager: UcpEndpointManager = _
  @volatile private var initialized = false

  // set to true when executor is shutting down
  @volatile private var isShuttingDown = false

  // this is a monotonically increasing id, used for buffers
  private val uniqueIds = new AtomicLong(0)

  // event loop, used to call [[UcpWorker.progress]], and perform all UCX work
  private val progressThread = Executors.newFixedThreadPool(1,
    GpuDeviceManager.wrapThreadFactory(
      new ThreadFactoryBuilder()
        .setNameFormat("progress-thread-%d")
        .setDaemon(true)
        .build,
      null,
      () => RmmSpark.removeAllCurrentThreadAssociation()))

  // The pending queues are used to enqueue [[PendingReceive]] or [[PendingSend]], from executor
  // task threads and [[progressThread]] will hand them to the UcpWorker thread.
  private val workerTasks = new ConcurrentLinkedQueue[() => Unit]()

  // holds memory registered against UCX that should be de-register on exit (used for bounce
  // buffers)
  // NOTE: callers should hold the `registeredMemory` lock before modifying this array
  val registeredMemory = new ArrayBuffer[UcpMemory]

  // when this flag is set to true, an async call to `register` hasn't completed in
  // the worker thread. We need this to complete prior to getting the `rkeys`.
  private var pendingRegistration = false

  // There will be 1 entry in this map per UCX-registered Active Message. Presently
  // that means: 2 request active messages (Metadata and Transfer Request), and 2
  // response active messages (Metadata and Transfer response).
  private val amRegistrations = new ConcurrentHashMap[Int, ActiveMessageRegistration]()

  /**
   * Initializes the UCX context and local worker and starts up the worker progress thread.
   * UCX worker/endpoint relationship.
   */
  def init(): Unit = {
    synchronized {
      if (initialized) {
        throw new IllegalStateException("UCX already initialized")
      }

      var workerParams = new UcpWorkerParams()

      if (rapidsConf.shuffleUcxUseWakeup) {
        workerParams = workerParams
          .requestWakeupTagSend()
          .requestWakeupTagRecv()
      }

      worker = context.newWorker(workerParams)
      endpointManager = new UcpEndpointManager()
      logInfo(s"UCX Worker created")
      initialized = true
    }

    progressThread.execute(() => {
      // utility function to make all the progress possible in each iteration
      // this could change in the future to 1 progress call per loop, or be used
      // entirely differently once polling is figured out
      def drainWorker(): Unit = {
        withResource(new NvtxRange("UCX Draining Worker", NvtxColor.RED)) { _ =>
          while (worker.progress() > 0) {}
        }
      }

      while(initialized) {
        try {
          worker.progress()
          // else worker.progress returned 0
          if (rapidsConf.shuffleUcxUseWakeup) {
            drainWorker()
            if (workerTasks.isEmpty) {
              withResource(new NvtxRange("UCX Sleeping", NvtxColor.PURPLE)) { _ =>
                // Note that `waitForEvents` checks any events that have occurred, and will
                // return early in those cases. Therefore, `waitForEvents` is safe to be
                // called after `worker.signal()`, as it will wake up right away.
                worker.waitForEvents()
              }
            }
          }

          withResource(new NvtxRange("UCX Handling Tasks", NvtxColor.CYAN)) { _ =>
            // check initialized since on close we queue a "task" that sets initialized to false
            // to exit the progress loop, we don't want to execute any other tasks after that.
            while (!workerTasks.isEmpty && initialized) {
              val wt = workerTasks.poll()
              if (wt != null) {
                wt()
              }
            }
            worker.progress()
          }
        } catch {
          case t: Throwable =>
            logError("Exception caught in UCX progress thread. Continuing.", t)
        }
      }

      synchronized {
        logDebug("Exiting UCX progress thread.")
        Seq(endpointManager, worker, context).safeClose()
        worker = null
      }
    })
  }

  /**
   * Starts a TCP server to listen for external clients, returning with
   * what port it used.
   *
   * @param mgmtHost String the hostname to bind to
   * @return port bound
   */
  def startListener(mgmtHost: String): Int = {
    endpointManager.startListener(mgmtHost)
  }

  /**
   * This trait and next two implementations represent the mapping between an Active Message Id
   * and the callback that should be triggered when a message is received.
   *
   * There are three types of Active Messages we care about: requests, responses, and buffers.
   *
   * For requests:
   *   - `activeMessageId` for requests is the value of the `MessageType` enum, and it is
   *   set once when the `RapidsShuffleServer` is initialized, and no new request handlers
   *   are established.
   *
   *   - The Active Message header is handed to the request handler, via the transaction.
   *   The request handler needs to echo the header back for the response handler on the
   *   other side of the request.
   *
   *   - On a request, a callback is instantiated using `requestCallbackGen`, which creates
   *   a transaction each time. This is one way to handle several requests inbound to a server.
   *
   *   - useRndv: is set to false. We expect UCX to be able to use eager protocols or rndv
   *   at its leasure. `rapidsConf.shuffleUcxActiveMessagesForceRndv` can be used to force rndv.
   *
   * For responses:
   *   - `activeMessageId` for responses is the value of the `MessageType` enum with an extra
   *   bit flipped (see `UCXConnection.composeResponseAmId`). These are also set once, as requests
   *   are sent out.
   *
   *   - The Active Message header is used to pick the correct callback to call. In this case
   *   there could be several expected responses, for a single response `activeMessageId`, so the
   *   server echoes back our header so we can invoke the correct response callback.
   *
   *   - Each response received at the response activeMessageId, will be demuxed using the header:
   *   responseActiveMessageId1 -> [callbackForHeader1, callbackForHeader2, ..., callbackForHeaderN]
   *
   *   - useRndv: is set to false. We expect UCX to be able to use eager protocols or rndv
   *   at its leasure. `rapidsConf.shuffleUcxActiveMessagesForceRndv` can be used to force rndv.
   *
   * For buffers:
   *   - `activeMessageId` for buffers is the value of the `MessageType` enum (`MessageType.Buffer`)
   *
   *   - The Active Message header is used to pick the correct callback to call. Each header
   *   encodes the peer we expect to receive from and a monotonically inscreasing id per request.
   *
   *   - useRndv: is set to true. We prefer the zero-copy method since the buffer pointer
   *   provided to UCX could be on the GPU. An eager message would require us to issue an H2D in
   *   the handler.
   */
  trait ActiveMessageRegistration {
    val activeMessageId: Int
    def getCallback(header: Long): UCXAmCallback
    def useRndv: Boolean
  }

  class ReceiveActiveMessageRegistration(override val activeMessageId: Int, mask: Long)
      extends ActiveMessageRegistration {

    private[this] val handlers = new ConcurrentHashMap[Long, () => UCXAmCallback]()

    def addWildcardHeaderHandler(wildcardHdr: Long, cbGen: () => UCXAmCallback): Unit = {
      handlers.put(wildcardHdr, cbGen)
    }

    override def getCallback(header: Long): UCXAmCallback = {
      // callback is picked using a wildcard
      val cb = handlers.get(header & mask)
      if (cb == null) {
        throw new IllegalStateException(s"UCX Receive Active Message callback not found for " +
          s"${TransportUtils.toHex(header)} and mask ${TransportUtils.toHex(mask)}")
      }
      cb()
    }
    override def useRndv: Boolean = true
  }

  class RequestActiveMessageRegistration(override val activeMessageId: Int,
                                         requestCbGen: () => UCXAmCallback)
      extends ActiveMessageRegistration {

    def getCallback(header: Long): UCXAmCallback = requestCbGen()

    override def useRndv: Boolean = rapidsConf.shuffleUcxActiveMessagesForceRndv
  }

  class ResponseActiveMessageRegistration(override val activeMessageId: Int)
      extends ActiveMessageRegistration {

    private[this] val responseCallbacks = new ConcurrentHashMap[Long, UCXAmCallback]()

    def getCallback(header: Long): UCXAmCallback = {
      val cb = responseCallbacks.remove(header) // 1 callback per header
      require (cb != null,
        s"Failed to get a response Active Message callback for " +
          s"${UCX.formatAmIdAndHeader(activeMessageId, header)}")
      cb
    }

    def addResponseActiveMessageHandler(
        header: Long, responseCallback: UCXAmCallback): Unit = {
      val prior = responseCallbacks.putIfAbsent(header, responseCallback)
      require(prior == null,
        s"Invalid Active Message re-registration of response handler for " +
          s"${UCX.formatAmIdAndHeader(activeMessageId, header)}")
    }

    def removeResponseActiveMessageHandler(header: Long): Unit = {
      responseCallbacks.remove(header)
    }

    override def useRndv: Boolean = rapidsConf.shuffleUcxActiveMessagesForceRndv
  }

  /**
   * Register a response handler (clients will use this)
   *
   * @note This function will be called for each client, with the same `am.activeMessageId`
   * @param am UCXActiveMessage (id and header) to register a one time response for
   * @param responseCallback callback to handle a particular response
   */
  def registerResponseHandler(am: UCXActiveMessage, responseCallback: UCXAmCallback): Unit = {
    logDebug(s"Register Active Message $am response handler") 

    amRegistrations.computeIfAbsent(am.activeMessageId,
      _ => {
        val reg = new ResponseActiveMessageRegistration(am.activeMessageId)
        registerActiveMessage(reg)
        reg
      }) match {
      case reg: ResponseActiveMessageRegistration =>
        reg.addResponseActiveMessageHandler(am.header, responseCallback)
      case other =>
        throw new IllegalStateException(
          s"Attempted to add a response Active Message $am handler but got $other")
    }
  }

  /**
   * Unregister an active message handler (given by `am.header`). This is invoked
   * from request error handlers. On request failure (which is terminal for the transaction),
   * the response will never arrive, so this cleans up the response handler.
   *
   * @param am active message (id, header) that for the response handler to remove
   */
  def unregisterResponseHandler(am: UCXActiveMessage): Unit = {
    amRegistrations.computeIfPresent(am.activeMessageId,
      (_, msg) => {
        msg match {
          case reg: ResponseActiveMessageRegistration =>
            reg.removeResponseActiveMessageHandler(am.header)
            reg
          case other =>
            throw new IllegalStateException(
              s"Attempted to unregister a response Active Message $am handler but got $other")
        }
      })
  }

  /**
   * Register a request handler (the server will use this)
   * @note This function will be called once for the server for an `activeMessageId`
   * @param activeMessageId (up to 5 bits) used to register with UCX an Active Message
   * @param requestCallbackGen a function that instantiates a callback to handle
   *                           a particular request
   */
  def registerRequestHandler(activeMessageId: Int,
      requestCallbackGen: () => UCXAmCallback): Unit = {
    logDebug(s"Register Active Message ${TransportUtils.toHex(activeMessageId)} " +
      " request handler")
    val reg = new RequestActiveMessageRegistration(activeMessageId, requestCallbackGen)
    val oldReg = amRegistrations.putIfAbsent(activeMessageId, reg)
    require(oldReg == null,
      s"Tried to re-register a request handler for $activeMessageId")
    registerActiveMessage(reg)
  }

  /**
   * Register a receive (one way) message, this is used by the clients.
   * @param activeMessageId (up to 5 bits) used to register with UCX an Active Message
   * @param hdrMask - a long with bits set for those bits that should be masked to find
   *                  the proper callback.
   * @param hdrWildcard - a long with an id that should be associated with a specific callback.
   * @param receiveCallbackGen - a function that instantiates a callback to handle
   *                             a particular receive.
   */
  def registerReceiveHandler(activeMessageId: Int, hdrMask: Long, hdrWildcard: Long,
      receiveCallbackGen: () => UCXAmCallback): Unit = {
    logDebug(s"Register Active Message ${TransportUtils.toHex(activeMessageId)} " +
      s"mask ${TransportUtils.toHex(hdrMask)} " +
      s"wild ${TransportUtils.toHex(hdrWildcard)} request handler")
    amRegistrations.computeIfAbsent(activeMessageId,
      _ => {
        val reg = new ReceiveActiveMessageRegistration(activeMessageId, hdrMask)
        registerActiveMessage(reg)
        reg
      }) match {
      case reg: ReceiveActiveMessageRegistration =>
        reg.addWildcardHeaderHandler(hdrWildcard, receiveCallbackGen)
      case other =>
        throw new IllegalStateException(
          s"Attempted to add a receive Active Message handler to existing registration $other " +
            s"for ${UCX.formatAmIdAndHeader(activeMessageId, hdrWildcard)}")
    }
  }

  private def registerActiveMessage(reg: ActiveMessageRegistration): Unit = {
    onWorkerThreadAsync(() => {
      worker.setAmRecvHandler(reg.activeMessageId,
        (headerAddr, headerSize, amData: UcpAmData, _) => {
          if (headerSize != 8) {
            // this is a coding error, so I am just blowing up. It should never happen.
            throw new IllegalStateException(
              s"Received message with wrong header size $headerSize")
          } else {
            val header = UcxUtils.getByteBufferView(headerAddr, headerSize).getLong()
            val am = UCXActiveMessage(reg.activeMessageId, header, reg.useRndv)

            withResource(new NvtxRange("AM Receive", NvtxColor.YELLOW)) { _ =>
              logDebug(s"Active Message received: $am")
              val cb = reg.getCallback(header)

              if (amData.isDataValid) {
                require(!reg.useRndv,
                  s"Handling an eager Active Message, but expected rndv for: " +
                    s"amId ${TransportUtils.toHex(reg.activeMessageId)}")
                logDebug(s"Handling an EAGER active message receive $amData")
                val resp = UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength)

                // copy the data onto a buffer we own because it is going to be reused
                // in UCX
                cb.onMessageReceived(amData.getLength, header, {
                  case mtb: MetadataTransportBuffer =>
                    mtb.copy(resp)
                    cb.onSuccess(am, mtb)
                  case _ =>
                    cb.onError(am,
                      UCXError(0, "Received an eager message for non-metadata message"))
                })

                // we return OK telling UCX `amData` is ok to be closed, along with the eagerly
                // received data
                UcsConstants.STATUS.UCS_OK
              } else {
                // RNDV case: we get a direct buffer and UCX will fill it with data at `receive`
                // callback
                cb.onMessageReceived(amData.getLength, header, (resp: TransportBuffer) => {
                  logDebug(s"Receiving Active Message ${am} using data address " +
                    s"${TransportUtils.toHex(resp.getAddress())}")

                  // we must call `receive` on the `amData` object within the progress thread
                  onWorkerThreadAsync(() => {
                    val receiveAm = amData.receive(resp.getAddress(),
                      new UcxCallback {
                        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                          withResource(resp) { _ =>
                            withResource(amData) { _ =>
                              if (ucsStatus == UCX.UCS_ERR_CANCELED) {
                                logWarning(
                                  s"Cancelled Active Message " +
                                    s"${TransportUtils.toHex(reg.activeMessageId)}" +
                                    s" status=$ucsStatus, msg=$errorMsg")
                                cb.onCancel(am)
                              } else {
                                cb.onError(am, UCXError(ucsStatus, errorMsg))
                              }
                            }
                          }
                        }

                        override def onSuccess(request: UcpRequest): Unit = {
                          withResource(new NvtxRange("AM Success", NvtxColor.ORANGE)) { _ =>
                            withResource(amData) { _ =>
                              logDebug(s"Success with Active Message ${am} using data address " +
                                s"${TransportUtils.toHex(resp.getAddress())}")
                              cb.onSuccess(am, resp)
                            }
                          }
                        }
                      })
                    cb.onMessageStarted(receiveAm)
                  })
                })
                UcsConstants.STATUS.UCS_INPROGRESS
              }
            }
          }
        },
        // PERSISTENT_DATA allows the `amData` data structure to be valid after the callback returns
        // WHOLE_MSG ensures a single callback is invoked from UCX when the whole message
        //   arrives
        UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA |
          UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    })
  }

  /**
   * Send a message `am` to an executor given by `executorId`
   * with a cudf `MemoryBuffer` payload. The payload can be a GPU buffer.
   *
   * @param executorId long representing the peer executor
   * @param am active message (id, tag) for this message
   * @param data a cudf `MemoryBuffer`
   * @param cb callback to handle the active message state
   */
  def sendActiveMessage(executorId: Long, am: UCXActiveMessage,
      data: MemoryBuffer, cb: UcxCallback): Unit = {
    sendActiveMessage(
      executorId,
      am,
      data.getAddress,
      data.getLength,
      cb,
      isGpu = data.isInstanceOf[BaseDeviceMemoryBuffer])
  }

  /**
   * Send an active message `am` to an executor given by `executorId`
   * with a direct `ByteBuffer` payload.
   *
   * @param executorId long representing the peer executor
   * @param am active message (id, tag) for this message
   * @param data a direct `ByteBuffer`
   * @param cb callback to handle the active message state
   */
  def sendActiveMessage(executorId: Long, am: UCXActiveMessage,
      data: ByteBuffer, cb: UcxCallback): Unit = {
    sendActiveMessage(
      executorId,
      am,
      TransportUtils.getAddress(data),
      data.remaining(),
      cb,
      isGpu = false)
  }

  private def sendActiveMessage(executorId: Long, am: UCXActiveMessage,
      dataAddress: Long, dataSize: Long, cb: UcxCallback, isGpu: Boolean): Unit = {
    onWorkerThreadAsync(() => {
      val endpoint = endpointManager.getEndpointByExecutorId(executorId)
      if (endpoint == null) {
        cb.onError(-200,
          s"Trying to send a message to an executor that doesn't exist $executorId")
      } else {
        sendActiveMessage(endpoint, am, dataAddress, dataSize, cb, isGpu)
      }
    })
  }

  private def sendActiveMessage(ep: UcpEndpoint, am: UCXActiveMessage,
                                dataAddress: Long, dataSize: Long,
                                cb: UcxCallback, isGpu: Boolean): Unit = {
    val useRndv = am.forceRndv || rapidsConf.shuffleUcxActiveMessagesForceRndv

    // This isn't coming from the pool right now because it would be a bit of a
    // waste to get a larger hard-partitioned buffer just for 8 bytes.
    // TODO: since we no longer have metadata limits, the pool can be managed using the
    //   address-space allocator, so we should obtain this direct buffer from that pool
    val header = ByteBuffer.allocateDirect(UCX.ACTIVE_MESSAGE_HEADER_SIZE.toInt)
    header.putLong(am.header)
    header.rewind()

    val flags = if (useRndv) {
      UcpConstants.UCP_AM_SEND_FLAG_RNDV
    } else {
      0L /* AUTO */
    }

    val memType = if (isGpu) {
      UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA
    } else {
      UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
    }

    withResource(new NvtxRange("AM Send", NvtxColor.GREEN)) { _ =>
      ep.sendAmNonBlocking(
        am.activeMessageId,
        TransportUtils.getAddress(header),
        UCX.ACTIVE_MESSAGE_HEADER_SIZE,
        dataAddress,
        dataSize,
        flags,
        new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            cb.onSuccess(request)
            RapidsStorageUtils.dispose(header)
          }

          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            cb.onError(ucsStatus, errorMsg)
            RapidsStorageUtils.dispose(header)
          }
        }, memType)
    }
  }

  def getServerConnection: UCXServerConnection = serverConnection

  def cancel(request: UcpRequest): Unit = {
    onWorkerThreadAsync(() => {
      try {
        worker.cancelRequest(request)
      } catch {
        case e: Throwable =>
          logError("Error while cancelling UCX request: ", e)
      }
    })
  }

  def assignUniqueId(): Long = uniqueIds.incrementAndGet()

  /**
   * Connect to a remote UCX management port.
   *
   * @param peerMgmtHost management TCP host
   * @param peerMgmtPort management TCP port
   * @return Connection object representing this connection
   */
  def getConnection(peerExecutorId: Int,
      peerMgmtHost: String,
      peerMgmtPort: Int): ClientConnection = {
    endpointManager.getConnection(peerExecutorId, peerMgmtHost, peerMgmtPort)
  }

  def onWorkerThreadAsync(task: () => Unit): Unit = {
    workerTasks.add(task)
    if (rapidsConf.shuffleUcxUseWakeup) {
      withResource(new NvtxRange("UCX Signal", NvtxColor.RED)) { _ =>
        // take up the worker object lock to protect against another `.close`
        synchronized {
          if (worker != null) {
            worker.signal()
          }
        }
      }
    }
  }

  /**
   * Return rkeys (if we have registered memory)
   */
  private def localRkeys: Seq[ByteBuffer] = registeredMemory.synchronized {
    while (pendingRegistration) {
      registeredMemory.wait(100)
    }
    registeredMemory.map(_.getRemoteKeyBuffer).toSeq
  }

  /**
   * Register a set of `MemoryBuffers` against UCX.
   *
   * @param buffers to register
   * @param mmapCallback callback invoked when the memory map operation completes or fails
   */
  def register(buffers: Seq[MemoryBuffer], mmapCallback: MemoryRegistrationCallback): Unit =
    registeredMemory.synchronized {
      pendingRegistration = true

      onWorkerThreadAsync(() => {
        var error: Throwable = null
        registeredMemory.synchronized {
          try {
            buffers.foreach { buffer =>
              val mmapParam = new UcpMemMapParams()
                  .setAddress(buffer.getAddress)
                  .setLength(buffer.getLength)

              //note that this can throw, lets call back and let caller figure out how to handle
              try {
                val registered = context.memoryMap(mmapParam)
                registeredMemory += registered
              } catch {
                case t: Throwable =>
                  if (error == null) {
                    error = t
                  } else {
                    error.addSuppressed(t)
                  }
              }
            }
          } finally {
            mmapCallback(Option(error))
            pendingRegistration = false
            registeredMemory.notify()
          }
        }
      })
    }

  def getNextTransactionId: Long = txId.incrementAndGet()

  def shuttingDown(): Unit = {
    logWarning("UCX is shutting down")
    isShuttingDown = true
  }

  override def close(): Unit = {
    // put a UCX task in the progress thread. This will:
    // - signal the worker, so the task is executed
    // - tear down endpoints
    // - remove all active messages
    // - remove all memory registrations
    // - sets `initialized` to false, which means that no further
    //   tasks will get executed in the progress thread, the loop exits
    //   and we close the endpointManager, the worker, and the context.
    onWorkerThreadAsync(() => {
      amRegistrations.forEach { (activeMessageId, _) =>
        logDebug(s"Removing Active Message registration for " +
          s"${TransportUtils.toHex(activeMessageId)}")
        worker.removeAmRecvHandler(activeMessageId)
      }

      logInfo(s"De-registering UCX ${registeredMemory.size} memory buffers.")
      registeredMemory.synchronized {
        registeredMemory.foreach(_.deregister())
        registeredMemory.clear()
      }
      synchronized {
        initialized = false
        notifyAll()
        // exit the loop
      }
    })

    synchronized {
      while (initialized) {
        wait(100)
      }
    }

    progressThread.shutdown()
    if (!progressThread.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX progress thread failed to terminate correctly")
    }
  }

  private def makeClientConnection(peerExecutorId: Long): UCXClientConnection = {
    new UCXClientConnection(
      peerExecutorId,
      this,
      transport)
  }

  /**
   * An internal helper class to establish a `UcpListener` for incoming connections,
   * and handle the state of endpoints established due to a `UcpConnectionRequest` (inbound),
   * or initiated by us and made to a peer's listener (outbound).
   */
  class UcpEndpointManager
    extends UcpEndpointErrorHandler with UcpListenerConnectionHandler with AutoCloseable {

    // The TCP/RDMA connection manager for UCX
    private var listener: UcpListener = _

    // executorId -> ClientConnection
    private val connectionCache = new ConcurrentHashMap[Long, ClientConnection]()

    // executorId -> UcpEndpoint
    private val endpoints = new ConcurrentHashMap[Long, UcpEndpoint]()

    // UcpEndpoint -> executorId (needed for error handling, which gets only a `UcpEndpoint`)
    private val reverseLookupEndpoints = new ConcurrentHashMap[UcpEndpoint, Long]()

    private def epParams = new UcpEndpointParams()
      .setNoLoopbackMode() // prevent UCX from connecting to itself
      // enables `onError` callback
      .setPeerErrorHandlingMode()
      .setErrorHandler(this)

    /**
     * Get a `ClientConnection` after optionally connecting to a peer given by `peerExecutorId`,
     * `peerHost` and `peerPort`
     * @param peerExecutorId executorId for the peer
     * @param peerHost the peer listener is bound to
     * @param peerPort port the peer listener is bound to
     * @return
     */
    def getConnection(peerExecutorId: Long, peerHost: String, peerPort: Int): ClientConnection = {
      connectionCache.computeIfAbsent(peerExecutorId, _ => {
        logInfo(s"Creating connection for executorId $peerExecutorId")
        startEndpoint(
          peerExecutorId, peerHost, peerPort)
        makeClientConnection(peerExecutorId)
      })
    }

    /**
     * Initiate a connection to a peer listener
     * @param peerExecutorId executorId for the peer
     * @param peerHost the peer listener is bound to
     * @param peerPort port the peer listener is bound to
     * @return an instance of `UcpEndpoint`
     */
    private def startEndpoint(peerExecutorId: Long,
                              peerHost: String,
                              peerPort: Int): Unit = {
      onWorkerThreadAsync(() => {
        endpoints.computeIfAbsent(peerExecutorId, _ => {
          val sockAddr = new InetSocketAddress(peerHost, peerPort)
          val ep = worker.newEndpoint(epParams.setSocketAddress(sockAddr))
          logDebug(s"Initiator: created an endpoint $ep to $peerExecutorId")
          reverseLookupEndpoints.put(ep, peerExecutorId)
          ep
        })
      })
    }

    // UcpListenerConnectionHandler interface - called from progress thread
    // handles an incoming connection to our UCP Listener
    // TODO: in the future, this function may reject `ConnectionRequest`s
    //  given a peer id we already established a connection to:
    //  https://github.com/openucx/ucx/pull/6859
    override def onConnectionRequest(connectionRequest: UcpConnectionRequest): Unit = {
      logInfo(s"Got UcpListener request from ${connectionRequest.getClientAddress}")

      // accept it
      val ep = worker.newEndpoint(epParams.setConnectionRequest(connectionRequest))

      logInfo(s"Created ConnectionRequest endpoint $ep " +
        s"for ${connectionRequest.getClientAddress}")

      // Register a `Control` active message for a handshake response
      val responseAm = UCXActiveMessage(
        UCXConnection.composeResponseAmId(MessageType.Control), ep.getNativeId, false)

      registerResponseHandler(responseAm, new UCXAmCallback {
        override def onError(am: UCXActiveMessage, error: UCXError): Unit = {
          logError(s"Error detected while handshaking with peer $error with " +
            s"active message $am. Closing endpoint $ep")
          closeEndpointOnWorkerThread(ep)
        }

        override def onMessageStarted(receiveAm: UcpRequest): Unit = {}

        override def onSuccess(am: UCXActiveMessage, buff: TransportBuffer): Unit = {
          withResource(buff) { _ =>
            buff match {
              case mb: MetadataTransportBuffer =>
                val (peerExecId, peerRkeys) =
                  UCXConnection.unpackHandshake(mb.getBuffer())

                logDebug(s"Successful Control response $responseAm: " +
                  s"from executor $peerExecId with rkeys $peerRkeys using $ep")

                // since this could happen after a connection initiated from
                // us, we check to see if we want `ep`
                handleConnectedPeerFromRequest(peerExecId, ep, peerRkeys)
            }
          }
        }

        override def onCancel(am: UCXActiveMessage): Unit = {
          logWarning(s"Cancelled $am while handling handshake. Closing endpoint $ep")
          closeEndpointOnWorkerThread(ep)
        }

        override def onMessageReceived(size: Long, header: Long,
                                       finalizeCb: TransportBuffer => Unit): Unit = {
          finalizeCb(new MetadataTransportBuffer(
            transport.getDirectByteBuffer(size.toInt)))
        }
      })

      sendControlRequest(ep, responseAm)
    }

    // called from the progress thread
    private def handleConnectedPeerFromRequest(executorId: Long, newEp: UcpEndpoint,
        peerRkeys: Seq[ByteBuffer]): Unit = {
      // if another endpoint won, we keep both open
      val priorEp = endpoints.putIfAbsent(executorId, newEp)
      if (priorEp != null) {
        // if another endpoint won, open the peer rkeys, as it will be used
        // for sends
        peerRkeys.foreach(priorEp.unpackRemoteKey)
      }
      // always try to unpack on the new endpoint
      peerRkeys.foreach(newEp.unpackRemoteKey)
      reverseLookupEndpoints.put(newEp, executorId)
      logInfo(s"Established endpoint on ConnectionRequest for executor $executorId: $newEp")
    }

    // from progress thread
    def getEndpointByExecutorId(executorId: Long): UcpEndpoint = {
      endpoints.get(executorId)
    }

    private def closeEndpointOnWorkerThread(endpoint: UcpEndpoint): Unit = {
      onWorkerThreadAsync(() => {
        try {
          endpoint.closeNonBlockingFlush()
        } catch {
          case e: Throwable =>
            if (!isShuttingDown) {
              // don't print anything in this case, as it is known we are shutting down
              logError("Error while closing ep. Ignoring.", e)
            }
        }
      })
    }

    // UcpEndpointErrorHandler interface - called from progress thread
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      // if the endpoint is not in `reverseLookupEndpoints` we don't know what peer it was for
      if (reverseLookupEndpoints.containsKey(ucpEndpoint)) {
        val executorId = reverseLookupEndpoints.get(ucpEndpoint)
        if (!isShuttingDown) {
          val error = UCXError(errorCode, errorString)
          logError(s"UcpListener detected an error for executorId $executorId: " +
            s"$error")
        }
        reverseLookupEndpoints.remove(ucpEndpoint)
        val existingEp = endpoints.computeIfPresent(executorId, (_, ep) => {
          if (ep.getNativeId == ucpEndpoint.getNativeId) {
            if (!isShuttingDown) {
              logWarning(s"Removing endpoint $ep for $executorId")
            }
            null
          } else {
            ep // the endpoint we are actually using for this peer is not the one that errored
          }
        })

        // if we removed an endpoint from `endpoints`, the connection is no longer valid.
        if (existingEp == null) {
          connectionCache.computeIfPresent(executorId, (_, conn) => {
            transport.shutdownConnection(conn)
            if (!isShuttingDown) {
              logWarning(s"Removed stale client connection for $executorId")
            }
            null
          })
        }
        closeEndpointOnWorkerThread(ucpEndpoint)
      }
    }

    /**
     * Starts a UCP Listener, and returns the port used. Peers should create endpoints
     * to the port returned here.
     * @param host host to bind to
     * @return port peers should use to bind
     */
    def startListener(host: String): Int = {
      val ucpListenerParams =
        new UcpListenerParams()
          .setConnectionHandler(this)

      val maxRetries = SparkEnv.get.conf.getInt("spark.port.maxRetries", 16)
      val startPort = if (rapidsConf.shuffleUcxListenerStartPort != 0) {
        rapidsConf.shuffleUcxListenerStartPort
      } else {
        // TODO: remove this once ucx1.11 with random port selection would be released
        1024 + new SecureRandom().nextInt(65535 - 1024)
      }
      var attempt = 0
      while (listener == null && attempt < maxRetries) {
        val sockAddress = new InetSocketAddress(host, startPort + attempt)
        attempt += 1
        try {
          ucpListenerParams.setSockAddr(sockAddress)
          listener = worker.newListener(ucpListenerParams)
        } catch {
          case _: UcxException =>
            logDebug(s"Failed to bind UcpListener on $sockAddress. " +
              s"Attempt $attempt out of $maxRetries.")
            listener = null
        }
      }
      if (listener == null) {
        throw new BindException(s"Couldn't start UcpListener " +
          s"on port range $startPort-${startPort + maxRetries}")
      }
      logInfo(s"Started UcpListener on ${listener.getAddress}")

      startControlRequestHandler()
      listener.getAddress.getPort
    }

    private def startControlRequestHandler(): Unit = {
      val controlAmId = UCXConnection.composeRequestAmId(MessageType.Control)
      registerRequestHandler(controlAmId, () => new UCXAmCallback {
        override def onError(am: UCXActiveMessage, error: UCXError): Unit = {
          logWarning(s"Error while receiving handshake request. Ignoring.")
        }

        override def onMessageStarted(receiveAm: UcpRequest): Unit = {}

        override def onSuccess(requestAm: UCXActiveMessage,
                               buff: TransportBuffer): Unit = {
          withResource(buff) { _ =>
            buff match {
              case mb: MetadataTransportBuffer =>
                val (peerExecId, peerRkeys) =
                  UCXConnection.unpackHandshake(mb.getBuffer())

                // The request handler for `Control` is used by the initiator, therefore
                // this endpoint should exist by the time we get here.
                val existingEp = endpointManager.getEndpointByExecutorId(peerExecId)
                require(existingEp != null,
                  s"An endpoint to $peerExecId should exist, but could not be found")

                logDebug(s"Success receiving active message $requestAm " +
                  s"with endpoint $existingEp " +
                  s"peer executor $peerExecId, peer keys $peerRkeys")

                peerRkeys.foreach(existingEp.unpackRemoteKey)
                sendControlResponse(existingEp, requestAm)
            }
          }
        }

        override def onCancel(am: UCXActiveMessage): Unit = {
          logWarning(s"Cancelled $am. Ignoring.")
        }

        override def onMessageReceived(size: Long, header: Long,
                                       finalizeCb: TransportBuffer => Unit): Unit = {
          finalizeCb(new MetadataTransportBuffer(transport.getDirectByteBuffer(size.toInt)))
        }
      })
    }

    // called from progress thread - on ConnectionRequest
    private def sendControlRequest(ep: UcpEndpoint, responseAm: UCXActiveMessage): Unit = {
      val requestAm = UCXActiveMessage(
        UCXConnection.composeRequestAmId(MessageType.Control), ep.getNativeId, false)

      val handshakeMsg =
        UCXConnection.packHandshake(localExecutorId, localRkeys)

      onWorkerThreadAsync(() => {
        logDebug(s"Sending handshake: $ep, $requestAm, $handshakeMsg")
        sendActiveMessage(ep, requestAm,
          TransportUtils.getAddress(handshakeMsg), handshakeMsg.remaining(),
          new UcxCallback {
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              val error = UCXError(ucsStatus, errorMsg)
              logError(s"Error sending handshake header, " +
                s"error: $error active message: $requestAm handshake: $handshakeMsg")
              RapidsStorageUtils.dispose(handshakeMsg)

              // we cleanup the response active message handler, since the request errored
              // and we don't expect a response from the peer
              unregisterResponseHandler(responseAm)
            }

            override def onSuccess(request: UcpRequest): Unit = {
              logInfo(s"Success sending handshake header! $handshakeMsg")
              RapidsStorageUtils.dispose(handshakeMsg)
            }
          }, false)
      })
    }

    // called from progress thread - from ControlRequest handler
    private def sendControlResponse(ep: UcpEndpoint, requestAm: UCXActiveMessage): Unit = {
      // reply
      val handshakeMsg = UCXConnection.packHandshake(localExecutorId, localRkeys)
      val responseAmId = UCXConnection.composeResponseAmId(MessageType.Control)
      val responseAm = UCXActiveMessage(responseAmId, requestAm.header, false)
      val address = TransportUtils.getAddress(handshakeMsg)
      val len = handshakeMsg.remaining()

      onWorkerThreadAsync(() => {
        logDebug(s"Sending a reply to $ep to $responseAm")
        sendActiveMessage(ep, responseAm, address, len,
          new UcxCallback {
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              val error = UCXError(ucsStatus, errorMsg)
              logError(s"Error replying to sending handshake header, " +
                s"error: $error active message: $responseAm")
              RapidsStorageUtils.dispose(handshakeMsg)
            }

            override def onSuccess(request: UcpRequest): Unit = {
              // hs here to keep the reference
              logDebug(s"Success replying to sending handshake header $responseAm")
              RapidsStorageUtils.dispose(handshakeMsg)
            }
          }, false)
      })
    }

    override def close(): Unit = synchronized {
      reverseLookupEndpoints.forEach((ep, _) => ep.close())
      // all endpoints in `endpoints` are in `reverseLookupEndpoints`
      endpoints.clear()
      reverseLookupEndpoints.clear()
    }
  }

}

object UCX {
  // This is used to distinguish a cancelled request vs. other errors
  // as the callback is the same (onError)
  // from https://github.com/openucx/ucx/blob/master/src/ucs/type/status.h
  private val UCS_ERR_CANCELED = -16

  // We include a header with this size in our active messages
  private val ACTIVE_MESSAGE_HEADER_SIZE = 8L

  def formatAmIdAndHeader(activeMessageId: Int, header: Long) =
    s"[amId=${TransportUtils.toHex(activeMessageId)}, hdr=${TransportUtils.toHex(header)}]"
}
