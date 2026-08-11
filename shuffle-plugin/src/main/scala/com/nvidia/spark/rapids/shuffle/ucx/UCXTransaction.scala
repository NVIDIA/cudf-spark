/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.shuffle.{MessageType, MetadataTransportBuffer, Transaction, TransactionCallback, TransactionStats, TransactionStatus, TransportBuffer, TransportUtils}
import org.openucx.jucx.ucp.UcpRequest

import org.apache.spark.internal.Logging

/**
 * Helper enum to describe transaction types supported in UCX
 * request = is a send and a receive, with the callback happening after the receive
 * send = is a send of 1 or more messages (callback at the end)
 * receive is a receive of 1 or more messages (callback at the end)
 */
private[ucx] object UCXTransactionType extends Enumeration {
  val Request, Send, Receive = Value
}

private[ucx] class UCXTransaction(conn: UCXConnection, val txId: Long)
  extends Transaction with Logging {

  // Active Messages: header used to disambiguate responses for a request
  private var header: Option[Long] = None

  // Type of request this transaction is handling, used to simplify the `respond` method
  private var messageType: Option[MessageType.Value] = None

  // various threads can access the status during the course of a Transaction
  // the UCX progress thread, client/server pools, and the executor task thread
  @volatile private[this] var status = TransactionStatus.NotStarted
  private[this] var errorMessage: Option[String] = None
  private[this] val pending = new AtomicLong(0L)
  private[this] var total: Long = 0L

  def decrementPendingAndGet: Long = pending.decrementAndGet

  override def getStatus: TransactionStatus.Value = status

  // This holds all the registered buffers. It is how we know when are done
  // (when completed == registered)
  private val pendingMessages = new ConcurrentLinkedQueue[UcpRequest]()

  private var hadError: Boolean = false

  private var txCallback: TransactionStatus.Value => Unit = _

  // Start and end times used for metrics
  private var start: Long = 0
  private var end: Long = 0

  // Track how much was sent and received/ also for metrics
  private[this] val receiveSize = new AtomicLong(0L)
  private[this] val sendSize = new AtomicLong(0L)

  def incrementSendSize(size: Long) = sendSize.addAndGet(size)
  def incrementReceiveSize(size: Long) = receiveSize.addAndGet(size)

  private var closed: Boolean = false

  def isClosed: Boolean = closed

  // This is the condition variable used to determine when the transaction is complete.
  private[this] val lock = new ReentrantLock()
  private[this] val notComplete = lock.newCondition()

  private[this] var transactionType: UCXTransactionType.Value = _

  override def getErrorMessage: Option[String] = errorMessage

  override def toString: String = {
    s"Transaction(" +
      s"txId=$txId, " +
      s"type=$transactionType, " +
      s"connection=${conn.toString}, " +
      s"status=$status, " +
      s"errorMessage=$errorMessage, " +
      s"totalMessages=$total, " +
      s"pending=$pending)"
  }

  /**
   * TODO: Note that this does not handle a timeout. We still need to do this, the version of UCX
   *  we build against can cancel messages.
   */
  def waitForCompletion(): Unit = {
    while (status != TransactionStatus.Complete &&
      status != TransactionStatus.Error) {
      logInfo(s"Waiting for status to become complete! $this")
      val condRange = new NvtxRange("Conditional wait", NvtxColor.PURPLE)
      try {
        lock.lock()
        try {
          if (status != TransactionStatus.Complete &&
            status != TransactionStatus.Error) {
            notComplete.await(1000, TimeUnit.MILLISECONDS)
          }
        } finally {
          lock.unlock()
        }
      } finally {
        condRange.close()
      }
    }
    logInfo(s"Leaving waitForCompletion $this")
  }

  /**
   * Internal function to register a callback against the callback service
   *
   * @param cb callback function to call using the callbackService.
   */
  private def registerCb(cb: TransactionCallback): Unit = {
    txCallback =
      (newStatus: TransactionStatus.Value) => {
        // NOTE: at this point, we are in the UCX progress thread.
        if (callbackCalled) {
          // if it was an otherwise good transaction (don't clobber an error already set)
          if (status != TransactionStatus.Cancelled && !hadError) {
            hadError = true
            errorMessage = Some(s"Callback called multiple times: $this")
            status = TransactionStatus.Error
          }
          // else, it's cancelled or errored. We could get a lot of calls here from the progress
          // thread, this drops spurious callbacks in the floor as we already communicated to the
          // user about the cancel or the error
        } else {
          callbackCalled = true
          if (newStatus == TransactionStatus.Success) {
            // Success is not a terminal state. It indicates that
            // from the UCX perspective the transfer was successful and not an error.
            // In the caller's callback, make sure that the transaction is marked as Success,
            // else handle it (retry, or let Spark know about it so it can re-schedule).
            //
            // This is set before we call the callback function. Once that callback is done,
            // we mark the request Complete (signaling the condition variable `notComplete`)
            // which will unblock a thread that has called `waitForCompletion`.
            status = newStatus
            stop()
          } else {
            hadError = true
          }

          if (isClosed || status == TransactionStatus.Complete ||
              status == TransactionStatus.NotStarted) {
            logError(s"Transaction $this has invalid status on callback.")
            status = TransactionStatus.Error
          }

          // call a user-defined callback, from the progress thread.
          try {
            cb(this)
          } catch {
            case e: Throwable =>
              logError(s"Detected an exception from user code. Dropping: ", e)
              signalStatus(TransactionStatus.Error)
          }
        }
      }
  }

  def registerPendingMessage(request: UcpRequest): Unit = {
    pendingMessages.offer(request)
  }

  /**
   * Internal function to kick off a [[Transaction]]
   *
   * @param txType     a transaction type to be used for debugging purposes
   * @param numPending number of messages we expect to see sent/received
   * @param cb         callback to call when done/errored
   */
  private[ucx] def start(
      txType: UCXTransactionType.Value,
      numPending: Long,
      cb: TransactionCallback): Unit = {

    if (start != 0) {
      throw new IllegalStateException("Transaction can't be started twice!")
    }
    if (closed) {
      throw new IllegalStateException("Transaction already closed!!")
    }
    transactionType = txType
    start = System.nanoTime
    registerCb(cb)
    status = TransactionStatus.InProgress
    total = numPending
    if (numPending == 0) {
      throw new IllegalStateException(s"Can't have an empty transaction $this")
    }
    pending.set(numPending)
  }

  private def signalStatus(newStatus: TransactionStatus.Value): Unit = {
    lock.lock()
    try {
      status = newStatus
      notComplete.signal()
    } finally {
      lock.unlock()
    }
    conn.removeTransaction(this)
  }

  override def close(): Unit = {
    if (closed) {
      throw new IllegalStateException(s"Trying to close transaction ${this} too many times.")
    }
    try {
      // this becomes non-null when there are exceptions while closing
      var ex: Throwable = null
      pendingMessages.forEach(msg => {
        try {
          if (hadError) {
            logWarning(s"Issuing a message cancel $msg for transaction $this")
            if (!msg.isCompleted) {
              conn.cancel(msg)
            }
          } else {
            // a close was called, and there was no error, so we expect messages
            // to be complete
            if (!msg.isCompleted) {
              hadError = true
            }
          }
          // close any active message we may have
          activeMessageData.foreach(_.close())
          activeMessageData = None
        } catch {
          case t: Throwable =>
            if (ex == null) {
              ex = new Throwable
            }
            ex.addSuppressed(t)
        }
      })
      if (ex != null) {
        logError("UCX error seen while closing transaction", ex)
        throw ex
      }
    } finally {
      closed = true
      signalStatus(if (hadError) {
        TransactionStatus.Error
      } else {
        TransactionStatus.Success
      })
    }
  }

  def stop(): Unit  = {
    end = System.nanoTime()
  }

  def getStats: TransactionStats = {
    if (end == 0) {
      throw new IllegalStateException("Transaction not stopped, can't get stats")
    }
    val diff: Double = (end - start)/1000000.0D
    val sendThroughput: Double = (sendSize.get()/1024.0D/1024.0D/1024.0D) / (diff / 1000.0D)
    val recvThroughput: Double = (receiveSize.get()/1024.0D/1024.0D/1024.0D) / (diff / 1000.0D)
    TransactionStats(diff, sendSize.get(), receiveSize.get(), sendThroughput, recvThroughput)
  }

  var callbackCalled: Boolean = false

  private var activeMessageData: Option[TransportBuffer] = None

  override def respond(response: ByteBuffer,
                       cb: TransactionCallback): Transaction = {
    logDebug(s"Responding to ${peerExecutorId} at ${TransportUtils.toHex(this.getHeader)} " +
      s"with ${response}")

    conn match {
      case serverConnection: UCXServerConnection =>
        serverConnection.send(peerExecutorId(), messageType.get, this.getHeader, response, cb)
      case _ =>
        throw new IllegalStateException("Tried to respond using a client connection. " +
          "This is not supported.")
    }
  }

  def complete(status: TransactionStatus.Value,
               messageType: Option[MessageType.Value] = None,
               header: Option[Long] = None,
               message: Option[TransportBuffer] = None,
               errorMessage: Option[String] = None): Unit = {
    setHeader(header)
    setActiveMessageData(message)
    setMessageType(messageType)
    setErrorMessage(errorMessage)
    setHeader(header)
    txCallback(status)
  }

  def completeWithError(errorMsg: String): Unit = {
    complete(TransactionStatus.Error,
      errorMessage = Option(errorMsg))
  }

  def completeCancelled(messageType: MessageType.Value, hdr: Long): Unit = {
    complete(TransactionStatus.Cancelled,
      messageType = Option(messageType),
      header = Option(hdr))
  }

  def completeWithSuccess(messageType: MessageType.Value,
      hdr: Option[Long],
      message: Option[TransportBuffer]): Unit = {
    complete(TransactionStatus.Success,
      messageType = Option(messageType),
      header = hdr,
      message = message)
  }

  // Reference count is not updated here. The caller is responsible to close
  private[ucx] def setActiveMessageData(data: Option[TransportBuffer]): Unit = {
    activeMessageData = data
  }

  // Reference count is not updated here. The caller is responsible to close
  override def releaseMessage(): MetadataTransportBuffer = {
    val msg = activeMessageData.get
    activeMessageData = None
    msg match {
      case mtb: MetadataTransportBuffer => mtb
      case _ =>
        throw new IllegalStateException(s"Expected a metadata buffer, but got ${msg}")
    }
  }

  private[ucx] def setHeader(id: Option[Long]): Unit = header = id

  override def getHeader: Long = {
    require(header.nonEmpty,
      "Attempted to get an Active Message header, but it was not set!")
    header.get
  }

  private[ucx] def setMessageType(msgType: Option[MessageType.Value]): Unit = {
    messageType = msgType
  }

  private[ucx] def setErrorMessage(errorMsg: Option[String]): Unit = {
    errorMessage = errorMsg
  }

  override def peerExecutorId(): Long =
    UCXConnection.extractExecutorId(getHeader)
}

