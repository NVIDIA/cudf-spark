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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import ai.rapids.cudf.MemoryBuffer
import com.nvidia.spark.rapids.{NvtxRegistry, RapidsConf, ShimReflectionUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.storage.RapidsStorageUtils
import org.apache.spark.storage.BlockManagerId

trait TransactionCallback {
  def apply(tx: Transaction): Unit
}

trait MemoryRegistrationCallback {
  def apply(error: Option[Throwable] = None): Unit
}

/**
 * A server-side interface to the transport.
 *
 * The [[RapidsShuffleServer]] uses a [[ServerConnection]] to start the management port
 * in the transport (in order to allow for incoming connections)
 */
trait ServerConnection {
  /**
   * Starts a management port, bound to `host`, on an ephemeral port (returned)
   * @param host host to bind to
   * @return integer ephemeral port that was bound
   */
  def startManagementPort(host: String): Int

  /**
   * Registers a callback that will be called any type a `MessageType` message is
   * received by this `ServerConnection`
   * @param messageType see `MessageType` enum
   * @param cb triggered for a success or error on this request
   */
  def registerRequestHandler(messageType: MessageType.Value, cb: TransactionCallback): Unit

  /**
   * Send a message using a `ByteBuffer` (for metadata)
   * @param peerExecutorId - executor to send response to
   * @param messageType - type of message (likely `MetadataResponse` or `TransferResponse`)
   * @param header - a long that should match a request header, requester use this header
   *               to disambiguate messages
   * @param response - a direct `ByteBuffer` to be transmitted
   * @param cb callback to trigger once this respond completes
   * @return a [[Transaction]] that can be used to block while this transaction is not done
   */
  def send(peerExecutorId: Long,
    messageType: MessageType.Value,
    header: Long,
    response: ByteBuffer,
    cb: TransactionCallback): Transaction

  /**
   * Send a message using a `MemoryBuffer` (data)
   * @param peerExecutorId peer's executor id to target
   * @param messageType type of message (likely `Buffer`)
   * @param header - a long that should match a request header, requester use this header
   *               to disambiguate messages
   * @param buffer an `MemoryBuffer` for a buffer to send
   * @param cb callback to trigger once done
   * @return the [[Transaction]], which can be used to block wait for this send.
   */
  def send(peerExecutorId: Long,
    messageType: MessageType.Value,
    header: Long,
    buffer: MemoryBuffer,
    cb: TransactionCallback): Transaction
}

/**
 * Currently supported message types in the transport
 */
object MessageType extends Enumeration {
  /**
   * A message used during startup when establishing
   * a connection to a peer.
   */
  val Control = Value

  /**
   * A client will issue: `MetadataRequest`
   * A server will respond with: `MetadataResponse`
   */
  val MetadataRequest = Value

  /**
   * A client will issue: `TransferRequest`
   * A server will respond with: `TransferResponse`
   */
  val TransferRequest = Value

  /**
   * Messages with this type are shuffle blocks
   */
  val Buffer = Value
}

/**
 * Trait used by the clients to interact with the transport.
 */
trait ClientConnection {

  /**
   * This performs a request/response for a message of type `MessageType`. The response
   * `Transaction` on completion will call the callback (`cb`). The caller of `request`
   * must close, or consume the response in the transaction, otherwise we can leak.
   *
   * @param messageType value of the `MessageType` enum
   * @param request the populated request direct buffer `ByteBuffer`
   * @param cb callback to handle transaction status. If successful the memory described
   *           using "response" will hold the response as expected, otherwise its contents
   *           are not defined.
   * @return a transaction representing the request
   */
  def request(messageType: MessageType.Value, request: ByteBuffer,
    cb: TransactionCallback): Transaction

  /**
   * Register interest in messages of `messageType`.
   *
   * @note Currently `MessageType.Buffer` is the only supported message type.
   */
  def registerReceiveHandler(messageType: MessageType.Value): Unit

  /**
   * Get a long representing the executorId for the peer of this connection.
   * @return the executorId as a long
   */
  def getPeerExecutorId: Long
}

object TransactionStatus extends Enumeration {
  val NotStarted, InProgress, Complete, Success, Error, Cancelled = Value
}

/**
 * Case class representing stats for the a transaction
 * @param txTimeMs amount of time this [[Transaction]] took
 * @param sendSize amount of bytes sent
 * @param receiveSize amount of bytes received
 * @param sendThroughput send throughput in GB/sec
 * @param recvThroughput receive throughput in GB/sec
 */
case class TransactionStats(txTimeMs: Double,
                            sendSize: Long,
                            receiveSize: Long,
                            sendThroughput: Double,
                            recvThroughput: Double)

/**
 * TransportBuffer represents a buffer with an address and length.
 *
 * There are two implementations:
 *
 *  - `MetadataTransportBuffer`: This is host memory backing a flat buffer, used for
 *    block metadata and transfer requests.
 *
 *  - `CudfTransportBuffer`: This is data backing a cuDF `MemoryBuffer`, and can be on the
 *    host or device (sender side) and exclusively on the device (client side)
 */
trait TransportBuffer extends AutoCloseable {
  def getAddress(): Long
  def getLength(): Long
}

/**
 * MetadataTransportBuffer encapsulates a direct byte buffer used exclusively for metadata
 * transfers via the transport.
 */
class MetadataTransportBuffer(val dbb: RefCountedDirectByteBuffer) extends TransportBuffer {
  def copy(in: ByteBuffer): Unit = {
    val bb = dbb.getBuffer()
    bb.put(in)
    bb.rewind()
  }

  override def getAddress(): Long =
    TransportUtils.getAddress(dbb.getBuffer())

  override def getLength(): Long = dbb.getBuffer().remaining()

  def getBuffer(): ByteBuffer = dbb.getBuffer()

  override def close(): Unit = dbb.close()
}

/**
 * CudfTransportBuffer encapsulates a cuDF `MemoryBuffer` used for shuffle block transfers
 * via the transport
 */
class CudfTransportBuffer(mb: MemoryBuffer) extends TransportBuffer {
  override def getAddress(): Long = mb.getAddress

  override def getLength(): Long = mb.getLength

  def getMemoryBuffer: MemoryBuffer = mb

  override def close(): Unit = mb.close()
}

/**
 * This trait represents a shuffle "transaction", and it is specific to a transfer (or set of
 * transfers).
 *
 * It is useful in that it groups a set of sends and receives requires in order to carry an action
 * against a peer. It can be used to find statistics about the transfer (bytes send/received,
 * throughput),
 * and it also can be waited on, for blocking clients.
 *
 * NOTE: a Transaction is thread safe w.r.t. a connection's callback. Calling methods on the
 * transaction
 * outside of [[waitForCompletion]] produces undefined behavior.
 */
trait Transaction extends AutoCloseable {
  /**
   * Get the peer executor id if available
   * @note this can throw if the `Transaction` was not created due to an Active Message
   */
  def peerExecutorId(): Long

  /**
   * Return this transaction's header, for debug purposes
   */
  def getHeader: Long

  /**
   * Get the status this transaction is in. Callbacks use this to handle various transaction states
   * (e.g. success, error, etc.)
   */
  def getStatus: TransactionStatus.Value

  /**
   * Get error messages that could have occurred during the transaction.
   * @return returns an optional error message
   */
  def getErrorMessage: Option[String]

  /**
   * Get the statistics object (bytes sent/recv, tx time, and throughput are available)
   */
  def getStats: TransactionStats

  /**
   * Block until this transaction is completed.
   *
   * NOTE: not only does this include the transaction time, but it also includes the
   * code performed in the callback. If the callback would block, you could end up in a situation
   * of
   * deadlock.
   */
  def waitForCompletion(): Unit

  /**
   * Hands over a message (a host-side request or response at the moment)
   * that is held in the Transaction
   * @note The caller must call `close` on the returned message
   * @return a `TransportBuffer` instance
   */
  def releaseMessage(): MetadataTransportBuffer

  /**
   * For `Request` transactions, `respond` will be able to reply to a peer who issued
   * the request
   * @note this is only available for server-side transactions, and will throw
   *       if attempted from a client
   * @param response a direct ByteBuffer
   * @param cb triggered when the response succeds/fails
   * @return a `Transaction` object that can be used to wait for this response to complete
   */
  def respond(response: ByteBuffer, cb: TransactionCallback): Transaction
}

/**
 * This defines what a "transport" should support. The intention is to allow for
 * various transport implementations to exist, for different communication frameworks.
 *
 * It is an `AutoCloseable` and so the caller should close when the transport is no longer
 * needed.
 */
trait RapidsShuffleTransport extends AutoCloseable {

  /**
   * This function will connect (if not connected already) to a peer
   * described by `blockManagerId`. Connections are cached.
   *
   * @param blockManagerId the peer's block manager id
   * @return RapidsShuffleClient instance that can be used to interact with the peer
   */
  def makeClient(blockManagerId: BlockManagerId): RapidsShuffleClient

  /**
   * Connect to peer given a `BlockManagerId`
   *
   * This function is used during transport early start, if enabled.
   *
   * @param peerBlockManagerId the peer's block manager id
   * @return a client connection for the peer
   */
  def connect(peerBlockManagerId: BlockManagerId): ClientConnection

  /**
   * This function should only be needed once. The caller creates *a* server and it is used
   * for the duration of the process.
   * @param requestHandler used to get metadata info, and acquire tables used in the shuffle.
   * @return the server instance
   */
  def makeServer(requestHandler: RapidsShuffleRequestHandler): RapidsShuffleServer

  /**
   * Returns a wrapped buffer of size Long. The buffer may or may not be pooled.
   *
   * The caller should call .close() on the returned [[RefCountedDirectByteBuffer]]
   * when done.
   *
   * @param size size of buffer required
   * @return the ref counted buffer
   */
  def getDirectByteBuffer(size: Long): RefCountedDirectByteBuffer

  /**
   * (throttle) Adds a set of requests to be throttled as limits allowed.
   * @param reqs requests to add to the throttle queue
   */
  def queuePending(reqs: Seq[PendingTransferRequest]): Unit

  /**
   * Cancel requests that are waiting in the queue (not in-flight) for a specific
   * handler (if any), and unregister the handler.
   */
  def cancelPending(handler: RapidsShuffleFetchHandler): Unit

  /**
   * (throttle) Signals that `bytesCompleted` are done, allowing more requests through the
   * throttle.
   * @param bytesCompleted amount of bytes handled
   */
  def doneBytesInFlight(bytesCompleted: Long): Unit

  // Bounce Buffer Management

  /**
   * Get receive bounce buffers needed for a receive, limited by the amount of bytes
   * to be received, and a hard limit on the number of buffers set by the caller
   * using `totalRequired`.
   *
   * This function is non blocking. If it can't satisfy the bounce buffer request, an empty
   * sequence is returned.
   *
   * @param remaining amount of bytes remaining in the receive
   * @param totalRequired maximum no. of buffers that should be returned
   * @return a sequence of bounce buffers, or empty if the request can't be satisfied
   */
  def tryGetReceiveBounceBuffers(remaining: Long, totalRequired: Int): Seq[BounceBuffer]

  /**
   * Get send bounce buffers needed for a receive, limited by the amount of bytes
   * to be sent, and a hard limit on the number of buffers set by the caller
   * using `totalRequired`.
   *
   * This function is non blocking. If it can't satisfy the bounce buffer request, an empty
   * sequence is returned.
   *
   * @param remaining amount of bytes remaining in the receive
   * @param totalRequired maximum no. of buffers that should be returned
   * @return a sequence of send bounce buffers, or empty if the request can't be satisfied
   * @note the send bounce buffer object most likely includes both a device buffer and a host
   *       memory buffer, since sends can come from the device or the host.
   */
  def tryGetSendBounceBuffers(remaining: Long, totalRequired: Int): Seq[SendBounceBuffers]
}

/**
 * A pool of direct byte buffers, sized to be `bufferSize`.
 * This is a controlled leak at the moment, there is no reclaiming of buffers.
 *
 * NOTE: this is used for metadata messages.
 *
 * @param bufferSize the size of direct `ByteBuffer` to allocate.
 */
class DirectByteBufferPool(bufferSize: Long) extends Logging {
  val buffers = new ConcurrentLinkedQueue[ByteBuffer]()
  val high = new AtomicInteger(0)

  def getBuffer(size: Long): RefCountedDirectByteBuffer = {
    if (size > bufferSize) {
      throw new IllegalStateException(s"Buffers of size $bufferSize are the only ones supported, " +
        s"asked for $size")
    }
    val buff = buffers.poll()
    if (buff == null) {
      high.incrementAndGet()
      logDebug(s"Allocating new direct buffer, high watermark = $high")
      new RefCountedDirectByteBuffer(ByteBuffer.allocateDirect(bufferSize.toInt), Option(this))
    } else {
      buff.clear()
      // Reset endianness to BIG_ENDIAN, as it could have changed depending on the consumer
      // (i.e. flat buffers force byte order to be LITTLE_ENDIAN, but pool consumers could be
      //  things like handshake messages that don't use flat buffers).
      // Since this is a pool of ByteBuffer, and it can be used by different consumers,
      // the choice of BIG_ENDIAN as default follows the JVM's default byte order for
      // newly created ByteBuffer objects:
      // https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html#order--
      buff.order(ByteOrder.BIG_ENDIAN)
      new RefCountedDirectByteBuffer(buff, Option(this))
    }
  }

  def releaseBuffer(buff: RefCountedDirectByteBuffer): Boolean = {
    logDebug(s"Free direct buffers ${buffers.size()}")
    buff.getBuffer().clear()
    buffers.offer(buff.getBuffer())
  }
}

/**
 * [[RefCountedDirectByteBuffer]] is a simple wrapper on top of a `ByteBuffer` that has been
 * allocated in direct mode.
 *
 * The pool is used to return the `ByteBuffer` to be reused, but not all of these buffers
 * are pooled (hence the argument is optional)
 *
 * The user should always close a [[RefCountedDirectByteBuffer]]. The close could hard destroy
 * the buffer, or return the object to the pool
 *
 * @param bb buffer to wrap
 * @param pool optional pool
 */
class RefCountedDirectByteBuffer(
    bb: ByteBuffer,
    pool: Option[DirectByteBufferPool] = None) extends AutoCloseable {
  assert(bb.isDirect, "Only direct buffers are supported")

  var refCount: Int = 0

  var closed: Boolean = false

  /**
   * Adds one to the ref count. Caller should call .close() when done
   * @return wrapped buffer
   */
  def acquire(): ByteBuffer = synchronized {
    refCount = refCount + 1
    bb
  }

  /**
   * Peeks into the wrapped buffer, without changing the ref count.
   * @return wrapped buffer
   */
  def getBuffer(): ByteBuffer = bb

  def isClosed: Boolean = synchronized { closed }

  /**
   * Decrements the ref count. If the ref count reaches 0, the buffer is
   * either returned to the (optional) pool or destroyed.
   */
  override def close(): Unit = synchronized {
    if (closed) {
      throw new IllegalStateException("Close called too many times!")
    }
    refCount = refCount - 1
    if (refCount <= 0) {
      if (pool.isDefined) {
        pool.get.releaseBuffer(this)
      } else {
        unsafeDestroy() // not pooled, should disappear
      }
    }
    closed = true
  }

  /**
   * Destroys the direct byte buffer forcefully, rather than wait for GC
   * to do it later. This helps with fragmentation issues with nearly depleted
   * native heap.
   */
  def unsafeDestroy(): Unit = synchronized {
    RapidsStorageUtils.dispose(bb)
  }

  override def toString: String = {
    s"RefCountedDirectByteBuffer[bb=$bb, ref_count=$refCount]"
  }
}

/**
 * A set of util functions used throughout
 */
object TransportUtils {
  val addressMethod = {
    val ret = Class.forName("sun.nio.ch.DirectBuffer")
      .getDeclaredMethod("address")
    ret.setAccessible(true)
    ret
  }


  def toHex(value: Long): String = {
    f"0x$value%016X"
  }

  def toHex(value: Int): String = {
    f"0x$value%08X"
  }

  def copyBuffer(src: ByteBuffer, dst: ByteBuffer, size: Int): Unit = {
    NvtxRegistry.TRANSPORT_COPY_BUFFER.push()
    try {
      val ro = src.asReadOnlyBuffer()
      ro.limit(ro.position() + size) // make sure we only copy size bytes
      // copy from position to remaining = (limit - position)
      dst.put(ro) // bulk put
    } finally {
      NvtxRegistry.TRANSPORT_COPY_BUFFER.pop()
    }
  }

  def getAddress(byteBuffer: ByteBuffer): Long = synchronized {
    require(byteBuffer.isDirect, "Only direct ByteBuffers supported in getAddress")
    val address = addressMethod.invoke(byteBuffer).asInstanceOf[Long]
    address + byteBuffer.position()
  }

  def timeDiffMs(startTimeMs: Long): Long = {
    System.currentTimeMillis() - startTimeMs
  }
}

object RapidsShuffleTransport extends Logging {
  /**
   * Used in `BlockManagerId`s when returning a map status after a shuffle write to
   * let the readers know what TCP port to use to establish a transport connection.
   */
  val BLOCK_MANAGER_ID_TOPO_PREFIX: String = "rapids"

  /**
   * Returns an instance of `RapidsShuffleTransport`.
   * @note the single current implementation is `UCXShuffleTransport`.
   * @param shuffleServerId this is the original `BlockManagerId` that Spark has for this
   *                        executor
   * @param rapidsConf instance of `RapidsConf`
   * @return a transport instance to be used to create a server and clients.
   */
  def makeTransport(shuffleServerId: BlockManagerId,
                    rapidsConf: RapidsConf): RapidsShuffleTransport = {
    val transportClass = try {
      ShimReflectionUtils.loadClass(rapidsConf.shuffleTransportClassName)
    } catch {
      case classNotFoundException: ClassNotFoundException =>
        logError(s"Unable to find RapidsShuffleTransport class " +
          s"${rapidsConf.shuffleTransportClassName}. Please ensure the appropriate jars have " +
          s"been added to the classpath.", classNotFoundException)
        throw classNotFoundException
    }
    try {
      val ctr = transportClass.getConstructors()(0)
      ctr.newInstance(shuffleServerId, rapidsConf).asInstanceOf[RapidsShuffleTransport]
    } catch {
      case t: Throwable =>
        logError(s"Found class for ${rapidsConf.shuffleTransportClassName}, but failed " +
          s"to instantiate", t)
        throw t
    }
  }
}
