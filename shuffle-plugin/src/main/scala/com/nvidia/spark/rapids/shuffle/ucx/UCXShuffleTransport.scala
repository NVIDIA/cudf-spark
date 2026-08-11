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

package com.nvidia.spark.rapids.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, CudaMemoryBuffer, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{GpuDeviceManager, HashedPriorityQueue, RapidsConf}
import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle._
import com.nvidia.spark.rapids.shuffle.{BounceBufferManager, BufferReceiveState, ClientConnection, PendingTransferRequest, RapidsShuffleClient, RapidsShuffleRequestHandler, RapidsShuffleServer, RapidsShuffleTransport, RefCountedDirectByteBuffer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.BlockManagerId

/**
 * UCXShuffleTransport is the UCX implementation for the `RapidsShuffleTransport`. It provides
 * a way to create a `RapidsShuffleServer` and one `RapidsShuffleClient` per peer, that are
 * able to send/receive via UCX.
 *
 * Additionally, this class maintains pools of memory used to limit the cost of memory
 * pinning and registration (bounce buffers), a metadata message pool for small flatbuffers used
 * to describe shuffled data, and implements a simple throttle mechanism to keep GPU memory
 * usage at bay by way of configuration settings.
 *
 * @param shuffleServerId `BlockManagerId` for this executor
 * @param rapidsConf      plugin configuration
 */
class UCXShuffleTransport(shuffleServerId: BlockManagerId, rapidsConf: RapidsConf)
  extends RapidsShuffleTransport
    with Logging {

  private[this] var inflightSize = 0L
  private[this] val inflightLimit = rapidsConf.shuffleTransportMaxReceiveInflightBytes
  private[this] val inflightMonitor = new Object
  private[this] var inflightStarted = true

  private[this] val shuffleMetadataPool = new DirectByteBufferPool(
    rapidsConf.shuffleMaxMetadataSize)

  private[this] val bounceBufferSize = rapidsConf.shuffleUcxBounceBuffersSize
  private[this] val deviceNumBuffers = rapidsConf.shuffleUcxDeviceBounceBuffersCount
  private[this] val hostNumBuffers = rapidsConf.shuffleUcxHostBounceBuffersCount

  private[this] var deviceSendBuffMgr: BounceBufferManager[BaseDeviceMemoryBuffer] = null
  private[this] var hostSendBuffMgr: BounceBufferManager[HostMemoryBuffer] = null
  private[this] var deviceReceiveBuffMgr: BounceBufferManager[BaseDeviceMemoryBuffer] = null

  private[this] val clients = new ConcurrentHashMap[Long, RapidsShuffleClient]()

  private[this] lazy val ucx = {
    logWarning("UCX Shuffle Transport Enabled")
    val ucxImpl = new UCX(this, shuffleServerId, rapidsConf)
    ucxImpl.init()

    initBounceBufferPools(bounceBufferSize,
      deviceNumBuffers, hostNumBuffers)

    // Perform transport (potentially IB) registration early
    // NOTE: on error we log and close things, which should fail other parts of the job in a bad
    // way in reality we should take a stab at lowering the requirement, and registering a smaller
    // buffer.
    val mgrs = Seq(deviceSendBuffMgr, deviceReceiveBuffMgr, hostSendBuffMgr)
    ucxImpl.register(mgrs.map(_.getRootBuffer()), ex => {
      if (ex.isDefined) {
        logError(s"Error registering bounce buffers", ex.get)
        ucxImpl.close()
      }
    })
    ucxImpl
  }

  private val altList = new HashedPriorityQueue[PendingTransferRequest](
    1000,
    (t: PendingTransferRequest, t1: PendingTransferRequest) => {
      java.lang.Long.compare(t.getLength, t1.getLength)
    })

  // access to this set must hold the `altList` lock
  val validHandlers =
    new mutable.HashSet[RapidsShuffleFetchHandler]()

  override def getDirectByteBuffer(size: Long): RefCountedDirectByteBuffer = {
    if (size > rapidsConf.shuffleMaxMetadataSize) {
      logWarning(s"Large metadata message size $size B, larger " +
        s"than ${rapidsConf.shuffleMaxMetadataSize} B. " +
        s"Consider setting ${RapidsConf.SHUFFLE_MAX_METADATA_SIZE.key} higher.")
      new RefCountedDirectByteBuffer(ByteBuffer.allocateDirect(size.toInt), None)
    } else {
      shuffleMetadataPool.getBuffer(size)
    }
  }

  /**
   * Initialize the bounce buffer pools that are to be used to send and receive data against UCX
   *
   * We have 2 pools for the send side, since buffers may come from spilled memory (host),
   * or device memory.
   *
   * We have 1 pool for the receive side, since all receives are targeted for the GPU.
   *
   * The size of buffers is the same for all pools, since send/receive sizes need to match. The
   * count can be set independently.
   *
   * @param bounceBufferSize the size for a single bounce buffer
   * @param deviceNumBuffers number of buffers to allocate for the device
   * @param hostNumBuffers   number of buffers to allocate for the host
   */
  def initBounceBufferPools(
      bounceBufferSize: Long,
      deviceNumBuffers: Int,
      hostNumBuffers: Int): Unit = {

    val deviceAllocator: Long => BaseDeviceMemoryBuffer = (size: Long) => {
      // CUDA async allocator is not compatible with GPUDirectRDMA, so need to use `cudaMalloc`.
      if (rapidsConf.rmmPool.equalsIgnoreCase("ASYNC")) {
        CudaMemoryBuffer.allocate(size)
      } else {
        DeviceMemoryBuffer.allocate(size)
      }
    }

    deviceSendBuffMgr =
      new BounceBufferManager[BaseDeviceMemoryBuffer](
        "device-send",
        bounceBufferSize,
        deviceNumBuffers,
        deviceAllocator)

    deviceReceiveBuffMgr =
      new BounceBufferManager[BaseDeviceMemoryBuffer](
        "device-receive",
        bounceBufferSize,
        deviceNumBuffers,
        deviceAllocator)

    hostSendBuffMgr =
      new BounceBufferManager[HostMemoryBuffer](
        "host-send",
        bounceBufferSize,
        hostNumBuffers,
        (size: Long) => HostMemoryBuffer.allocate(size))
  }

  def freeBounceBufferPools(): Unit = {
    Seq(deviceSendBuffMgr, deviceReceiveBuffMgr, hostSendBuffMgr).foreach(_.close())
  }

  private def getNumBounceBuffers(remaining: Long, totalRequired: Int): Int = {
    val numBuffers = (remaining + bounceBufferSize - 1) / bounceBufferSize
    Math.min(numBuffers, totalRequired).toInt
  }

  override def tryGetSendBounceBuffers(
      remaining: Long,
      totalRequired: Int): Seq[SendBounceBuffers] = {
    val numBuffs = getNumBounceBuffers(remaining, totalRequired)
    val deviceBuffer = tryAcquireBounceBuffers(deviceSendBuffMgr, numBuffs)
    if (deviceBuffer.nonEmpty) {
      val hostBuffer = tryAcquireBounceBuffers(hostSendBuffMgr, numBuffs)
      if (hostBuffer.nonEmpty) {
        deviceBuffer.zip(hostBuffer).map { case (d, h) =>
          SendBounceBuffers(d, Some(h))
        }
      } else {
        deviceBuffer.map(d => SendBounceBuffers(d, None))
      }
    } else {
      Seq.empty
    }
  }

  override def tryGetReceiveBounceBuffers(
      remaining: Long, totalRequired: Int): Seq[BounceBuffer] = {
    val numBuffs = getNumBounceBuffers(remaining, totalRequired)
    tryAcquireBounceBuffers(deviceReceiveBuffMgr, numBuffs)
  }

  private def tryAcquireBounceBuffers[T <: MemoryBuffer](
      bounceBuffMgr: BounceBufferManager[T],
      numBuffs: Integer): Seq[BounceBuffer] = {
    // if the # of buffers requested is more than what the pool has, we would deadlock
    // this ensures we only get as many buffers as the pool could possibly give us.
    val possibleNumBuffers = Math.min(bounceBuffMgr.numBuffers, numBuffs)
    val bounceBuffers =
      bounceBuffMgr.acquireBuffersNonBlocking(possibleNumBuffers)
    logTrace(s"Got ${bounceBuffers.size} bounce buffers from pool " +
      s"out of ${numBuffs} requested.")
    bounceBuffers
  }

  override def connect(peerBlockManagerId: BlockManagerId): ClientConnection = {
    val topo = peerBlockManagerId.topologyInfo
    val connection: ClientConnection = if (topo.isDefined) {
      val topoParts = topo.get.split("=")
      if (topoParts.size == 2 &&
          topoParts(0).equalsIgnoreCase(RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX)) {
        val peerExecutorId = peerBlockManagerId.executorId.toInt
        ucx.getConnection(peerExecutorId, peerBlockManagerId.host, topoParts(1).toInt)
      } else {
        // in the future this may create connections in other transports
        throw new IllegalStateException(s"Invalid block manager id for the rapids " +
          s"shuffle $peerBlockManagerId")
      }
    } else {
      throw new IllegalStateException(s"Invalid block manager id for the rapids " +
        s"shuffle $peerBlockManagerId")
    }

    connection
  }

  class CallerRunsAndLogs extends ThreadPoolExecutor.CallerRunsPolicy {
    override def rejectedExecution(
        runnable: Runnable,
        threadPoolExecutor: ThreadPoolExecutor): Unit = {

      logWarning(s"Rejected execution for ${threadPoolExecutor}, running in caller's thread.")
      super.rejectedExecution(runnable, threadPoolExecutor)
    }
  }

  // NOTE: this pool, as is, will add a new thread per task. This will likely change.
  private[this] val clientExecutor = {
    // We are using the TrampolineUtil to create a ThreadPoolExecutor even though we are
    // resetting most of the params that were set using the ThreadUtils class.
    // The reason is that this way we will get the Hadoop conf necessary for Unity Catalog
    val threadPoolExecutor = TrampolineUtil.newDaemonCachedThreadPool("ucx-shuffle-transport",
      rapidsConf.shuffleMaxClientThreads,
      rapidsConf.shuffleClientThreadKeepAliveTime)
    threadPoolExecutor.setCorePoolSize(1)
    threadPoolExecutor.setThreadFactory(
      GpuDeviceManager.wrapThreadFactory(
        threadPoolExecutor.getThreadFactory,
        null,
        () => RmmSpark.removeAllCurrentThreadAssociation()))
    // if we can't hand off because we are too busy, block the caller (in UCX's case,
    // the progress thread)
    threadPoolExecutor.setRejectedExecutionHandler(new CallerRunsAndLogs())
    threadPoolExecutor
  }

  // This executor handles any task that would block (e.g. wait for spill synchronously due to OOM)
  private[this] val clientCopyExecutor = Executors.newSingleThreadExecutor(
    GpuDeviceManager.wrapThreadFactory(new ThreadFactoryBuilder()
      .setNameFormat("shuffle-client-copy-thread-%d")
      .setDaemon(true)
      .build,
      null,
      () => RmmSpark.removeAllCurrentThreadAssociation()))

  override def makeClient(blockManagerId: BlockManagerId): RapidsShuffleClient = {
    val peerExecutorId = blockManagerId.executorId.toLong
    val clientConnection = connect(blockManagerId)
    clients.computeIfAbsent(peerExecutorId, _ => {
      val client = new RapidsShuffleClient(
        clientConnection,
        this,
        clientExecutor,
        clientCopyExecutor)
      clientConnection.registerReceiveHandler(MessageType.Buffer)
      client
    })
  }

  // NOTE: this is a single thread for the executor, nothing prevents us from having a pool here.
  // This will likely change.
  private[this] val serverExecutor = Executors.newSingleThreadExecutor(
    GpuDeviceManager.wrapThreadFactory(new ThreadFactoryBuilder()
      .setNameFormat(s"shuffle-server-conn-thread-${shuffleServerId.executorId}-%d")
      .setDaemon(true)
      .build,
      null,
      () => RmmSpark.removeAllCurrentThreadAssociation()))

  // This is used to queue up on the server all the [[BufferSendState]] as the server waits for
  // bounce buffers to become available (it is the equivalent of the transport's throttle, minus
  // the inflight limit)
  private[this] val bssExecutor = Executors.newSingleThreadExecutor(
    GpuDeviceManager.wrapThreadFactory(new ThreadFactoryBuilder()
      .setNameFormat(s"shuffle-server-bss-thread-%d")
      .setDaemon(true)
      .build,
      null,
      () => RmmSpark.removeAllCurrentThreadAssociation()))

  /**
   * Construct a server instance
   *
   * @param requestHandler used to get metadata info, and acquire tables used in the shuffle.
   * @return the server instance
   */
  override def makeServer(requestHandler: RapidsShuffleRequestHandler): RapidsShuffleServer = {
    new RapidsShuffleServer(
      this,
      ucx.getServerConnection,
      shuffleServerId,
      requestHandler,
      serverExecutor,
      bssExecutor,
      rapidsConf)
  }

  /**
   * Updates the inflightSize by adding the `neededAmount`
   * @param neededAmount amount of bytes needed.
   * @note This function is called only after a successful call to `wouldFitInFlightLimit`. It also
   *       calls `wouldFitInFlightLimit` as a sanity check.
   */
  private def markBytesInFlight(neededAmount: Long): Unit = inflightMonitor.synchronized {
    require(wouldFitInFlightLimit(neededAmount),
      s"Inflight limit can't allow this size $neededAmount of request")
    inflightSize = inflightSize + neededAmount
  }

  /**
   * Returns true if the neededAmount fits within the throttle, or if the throttle is at 0.
   * The "at 0" case helps in the case where we have buffer sizes that are greater than
   * inflightLimit
   * @param neededAmount amount of bytes needed
   * @return true if `neededAmount` would be allowed in the throttle
   */
  private def wouldFitInFlightLimit(neededAmount: Long): Boolean = inflightMonitor.synchronized {
    inflightSize + neededAmount <= inflightLimit || inflightSize == 0
  }

  /**
   * Decreases the inflight size.
   * @note We are holding onto the inflightMonitor lock here, which we use to update the limit
   *       in all cases.
   * @param bytesCompleted amount of bytes handled
   */
  override def doneBytesInFlight(bytesCompleted: Long): Unit = inflightMonitor.synchronized {
    inflightSize = inflightSize - bytesCompleted
    logDebug(s"Done with ${bytesCompleted} bytes inflight, " +
      s"new inflightSize is ${inflightSize}")
    inflightMonitor.notifyAll()
  }

  private[this] val exec = Executors.newSingleThreadExecutor(
    GpuDeviceManager.wrapThreadFactory(
      new ThreadFactoryBuilder()
        .setNameFormat(s"shuffle-transport-throttle-monitor")
        .setDaemon(true)
        .build,
      null,
      () => RmmSpark.removeAllCurrentThreadAssociation()))

  // helper class to hold transfer requests that have a bounce buffer
  // and should be ready to be handled by a `BufferReceiveState`
  class PerClientReadyRequests(val bounceBuffer: BounceBuffer) {
    val transferRequests = new ArrayBuffer[PendingTransferRequest]()
    var runningSize = 0L
    def addRequest(req: PendingTransferRequest): Unit = {
      transferRequests.append(req)
      runningSize += req.getLength
    }
  }

  private case class ClientAndBufferReceiveState(client: RapidsShuffleClient,
                                                 brs: BufferReceiveState)
  private val pendingBrs = new ConcurrentHashMap[Long, ClientAndBufferReceiveState]()

  def handleBufferReceive(size: Long, header: Long,
      finalizeCb: TransportBuffer => Unit): Unit = {
    logDebug(s"Handling: ${TransportUtils.toHex(header)} with size $size")
    val clientAndBrs = pendingBrs.get(header)
    require(clientAndBrs != null,
      s"Unknown header for a buffer receive: ${TransportUtils.toHex(header)}")
    clientAndBrs.brs.getBufferWhenReady(finalizeCb, size)
  }

  def handleBufferTransaction(tx: Transaction): Unit = {
    val header = tx.getHeader
    val clientAndBrs = pendingBrs.get(header)
    require(clientAndBrs != null,
      s"Unknown header for a buffer receive: ${TransportUtils.toHex(header)}")
    val client = clientAndBrs.client
    logDebug(s"Handling for peer ${client.connection.getPeerExecutorId}, " +
      s"handling ${TransportUtils.toHex(header)}, SUCCESS")
    client.handleBufferReceive(tx, clientAndBrs.brs)
  }

  def bufferReceiveStateComplete(header: Long): Unit = {
    val existing = pendingBrs.remove(header)
    require(existing != null,
      s"Unknown header ${TransportUtils.toHex(header)} for a `BufferReceiveState`")
  }

  exec.execute(() => {
    while (inflightStarted) {
      try {
        var req: PendingTransferRequest = null
        val requestsToHandle = new ArrayBuffer[PendingTransferRequest]()
        altList.synchronized {
          // pick up a request if ready, or wait until
          // a request is added to `altList`
          req = altList.poll()
          while (inflightStarted && req == null) {
            altList.wait(100)
            req = altList.poll()
          }
          // if we had 1 request, try to drain the rest
          while (req != null) {
            requestsToHandle.append(req)
            req = altList.poll()
          }
        }

        var requestIx = 0
        while (requestIx < requestsToHandle.size) {
          var hasBounceBuffers = true
          var fitsInFlight = true
          val perClientReq = mutable.Map[RapidsShuffleClient, PerClientReadyRequests]()
          var reqToHandle: PendingTransferRequest = null
          val putBack = new ArrayBuffer[PendingTransferRequest]()
          //NOTE: If the in-flight limit is high, we will run through every request
          // in the queue! This is an interim solution that will be fixed in future versions
          // to reduce the time spent here.
          while (requestIx < requestsToHandle.size && fitsInFlight) {
            reqToHandle = requestsToHandle(requestIx)
            if (wouldFitInFlightLimit(reqToHandle.getLength)) {
              val existingReq =
                perClientReq.get(reqToHandle.client)
              if (existingReq.isEmpty) {
                // need to get bounce buffers
                val bbs = tryGetReceiveBounceBuffers(1, 1)
                if (bbs.nonEmpty) {
                  markBytesInFlight(reqToHandle.getLength)
                  val perClientReadyRequests = new PerClientReadyRequests(bbs.head)
                  perClientReadyRequests.addRequest(reqToHandle)
                  perClientReq += reqToHandle.client -> perClientReadyRequests
                  requestIx += 1
                } else {
                  // TODO: make this a metric => "blocked while waiting on bounce buffers"
                  logTrace("Can't acquire bounce buffers for receive.")
                  hasBounceBuffers = false
                  putBack.append(reqToHandle)
                  requestIx += 1
                }
              } else if (existingReq.get.runningSize < bounceBufferSize) {
                // bounce buffers already acquired, and the requested amount so far
                // is less than 1 bounce buffer lengths, therefore the pending request
                // is added to the `PerClientReadyRequests`.
                markBytesInFlight(reqToHandle.getLength)
                existingReq.foreach(_.addRequest(reqToHandle))
                requestIx += 1
              } else {
                requestIx += 1
                putBack.append(reqToHandle)
              }
            } else {
              fitsInFlight = false
            }
          }

          // NOTE: because we skipped some indices above, we need to put these
          // back into the `altList`.
          // A _much_ better way of doing this would be to have separate
          // lists, one per client. This should be cleaned up later.
          altList.synchronized {
            putBack.foreach { pb =>
              // if this handler hasn't been invalidated, we can add the pending request back
              // like with NOTE above, when this is a queue per client, this gets refactored
              if (validHandlers.contains(pb.handler)) {
                altList.add(pb)
              }
            }
          }

          if (perClientReq.nonEmpty) {
            perClientReq.foreach { case (client, perClientRequests) =>
              // The transport uses this `brsId` as the active message header for this
              // `BufferReceiveState`
              val brsId = UCXConnection.composeBufferHeader(
                client.connection.getPeerExecutorId, ucx.assignUniqueId())
              val brs = new BufferReceiveState(brsId,
                perClientRequests.bounceBuffer,
                perClientRequests.transferRequests.toSeq,
                () => bufferReceiveStateComplete(brsId))
              pendingBrs.put(brs.id, ClientAndBufferReceiveState(client, brs))
              client.issueBufferReceives(brs)
            }
          } else if (!hasBounceBuffers) {
            deviceReceiveBuffMgr.synchronized {
              while (deviceReceiveBuffMgr.numFree() == 0){
                deviceReceiveBuffMgr.wait(100)
              }
            }
          } else if (putBack.isEmpty) {
            // then we must be waiting for the inflight limit
            inflightMonitor.synchronized {
              while (!wouldFitInFlightLimit(reqToHandle.getLength)) {
                inflightMonitor.wait(100)
              }
            }
          }
        }
      } catch {
        case t: Throwable =>
          logError("Error in the UCX throttle loop", t)
      }
    }
  })

  override def queuePending(reqs: Seq[PendingTransferRequest]): Unit =
    altList.synchronized {
      import scala.collection.JavaConverters._
      validHandlers.add(reqs.head.handler)
      altList.addAll(reqs.asJava)
      logDebug(s"THROTTLING ${altList.size} queued requests")
      altList.notifyAll()
    }

  override def cancelPending(handler: RapidsShuffleFetchHandler): Unit = {
    altList.synchronized {
      if (validHandlers.contains(handler)) {
        // This is expensive, but will be refactored with a queue per client.
        // As it stands, in the good case, it should be invoked once per task/peer,
        // on task completion, and `altList` should be empty, turning this into
        // mostly a noop.
        // When there are errors, we will get more invocations, specifically as `transferError`
        // is handled by `RapidsShuffleFetchHandler` and then later when the task finally
        // fails.
        if (!altList.isEmpty) {
          val it = altList.iterator()
          val toRemove = new ArrayBuffer[PendingTransferRequest]()
          while (it.hasNext) {
            val pending = it.next()
            if (pending.handler == handler) {
              toRemove.append(pending)
            }
          }
          if (toRemove.nonEmpty) {
            toRemove.foreach(altList.remove)
          }
        }
        // invalidate the handler
        validHandlers.remove(handler)
      }
    }
  }

  override def close(): Unit = {
    logInfo("UCX transport closing")
    ucx.shuttingDown()
    exec.shutdown()
    bssExecutor.shutdown()
    clientExecutor.shutdown()
    serverExecutor.shutdown()

    altList.synchronized {
      inflightStarted = false
      altList.notifyAll()
    }

    if (!exec.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Transport throttle failed to terminate correctly")
    }
    if (!clientExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Client failed to terminate correctly")
    }
    if (!serverExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Server main executor failed to terminate correctly")
    }
    if (!bssExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Server BufferSendState executor failed to terminate correctly")
    }

    ucx.close()
    freeBounceBufferPools()
  }

  /**
   * Removes state in the transport associated with `conn`
   * @param conn a connection to a remote executor
   */
  def shutdownConnection(conn: ClientConnection): Unit = {
    val client = clients.remove(conn.getPeerExecutorId)
    if (client != null) {
      client.close()
    }
  }
}
