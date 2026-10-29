/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, LinkedBlockingQueue,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DeviceMemoryBuffer, Rmm}
import com.nvidia.spark.rapids.{LogCaptureUtils, MetaUtils, RapidsShuffleHandle,
  ShuffleMetadata, TestUtils}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle._
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, contains, isNull}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.openucx.jucx.UcxCallback

import org.apache.spark.FetchFailed
import org.apache.spark.shuffle.rapids.{RapidsShuffleFetchFailedException,
  ShuffleOutputGoneException}
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId}

/**
 * What a requester does when the server answers its request with an error reply, and when its
 * reader is done before any reply comes. The reply reaches the client through the real UCX
 * client connection and transaction, with only UCX mocked, because the transaction's callback
 * wrapper drops whatever the client's callback throws. The suite is in the ucx package so that
 * it can answer a request through the response handler the connection registers, as UCX does.
 */
class RapidsShuffleErrorReplySuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L

  private val reason = "the shuffle output it asked for is gone"

  private val metadataResponseAmId = UCXConnection.composeResponseAmId(MessageType.MetadataRequest)

  /**
   * A client of executor 2 whose requests the test answers, over a spied connection when
   * `spied`, so that a test can see its transactions removed.
   */
  private class Requester(spied: Boolean = false) {
    val ucx: UCX = mock[UCX]
    private val txIds = new AtomicLong()
    when(ucx.getNextTransactionId).thenAnswer(_ => txIds.incrementAndGet())
    private val unanswered = new LinkedBlockingQueue[UCXAmCallback]()
    doAnswer { invocation =>
      unanswered.add(invocation.getArgument[UCXAmCallback](1))
      null
    }.when(ucx).registerResponseHandler(any(), any())

    val connection: UCXClientConnection = {
      val real = new UCXClientConnection(2L, ucx, mock[UCXShuffleTransport])
      if (spied) spy(real) else real
    }

    val client = new RapidsShuffleClient(
      connection, mockTransport, mockExecutor, mockCopyExecutor, mockCatalog)

    /** Answers the oldest request still waiting for a response, as UCX delivers one. */
    def answer(response: ByteBuffer): Unit = {
      val callback = unanswered.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(callback != null, "the client sent no request to answer")
      callback.onSuccess(UCXActiveMessage(0, 0L, forceRndv = false),
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(response)))
    }

    /**
     * Registers the responses of this client's later requests as UCX does, in the registration it
     * returns, so that a reply finds what its header holds, a withdrawal retires the header, and
     * a release frees it.
     */
    def registerResponses(): UCX#ResponseActiveMessageRegistration = {
      val responses = metadataResponses()
      doAnswer { invocation =>
        java.lang.Boolean.valueOf(responses.addResponseActiveMessageHandler(
          invocation.getArgument[UCXActiveMessage](0).header,
          invocation.getArgument[UCXAmCallback](1)))
      }.when(ucx).registerResponseHandler(any(), any())
      doAnswer { invocation =>
        java.lang.Boolean.valueOf(responses.retire(
          invocation.getArgument[UCXActiveMessage](0).header,
          invocation.getArgument[UCXAmCallback](1)))
      }.when(ucx).retireResponseHandler(any(), any())
      doAnswer { invocation =>
        responses.release(
          invocation.getArgument[UCXActiveMessage](0).header,
          invocation.getArgument[UCXAmCallback](1))
        null
      }.when(ucx).releaseResponseHandler(any(), any())
      responses
    }

    /** The response and callback that this client's latest request registered. */
    def lastRegistered: (UCXActiveMessage, UCXAmCallback) = {
      val response = ArgumentCaptor.forClass(classOf[UCXActiveMessage])
      val callback = ArgumentCaptor.forClass(classOf[UCXAmCallback])
      verify(ucx, atLeastOnce()).registerResponseHandler(response.capture(), callback.capture())
      (response.getValue, callback.getValue)
    }

    /** The active message of this client's latest request, and its send's callback. */
    def lastSent: (UCXActiveMessage, UcxCallback) = {
      val request = ArgumentCaptor.forClass(classOf[UCXActiveMessage])
      val send = ArgumentCaptor.forClass(classOf[UcxCallback])
      verify(ucx, atLeastOnce()).sendActiveMessage(
        anyLong(), request.capture(), any[ByteBuffer](), send.capture())
      (request.getValue, send.getValue)
    }
  }

  private def tableMeta(tableId: Int, size: Long): TableMeta =
    MetaUtils.buildTableMeta(tableId, size, ByteBuffer.allocate(8), 100)

  private def reader(): RapidsShuffleFetchHandler = {
    val handler = mock[RapidsShuffleFetchHandler]
    when(handler.getTaskIds).thenReturn(Array[Long](1))
    handler
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The test thread plays the requester's copy thread, which consumeWindow marks as a
    // shuffle thread. A thread dedicated to a task, as the base suite leaves it, cannot be one.
    RmmSpark.removeAllCurrentThreadAssociation()
  }

  /** Runs `body` with a receive bounce buffer of `size` bytes, closing it if nothing did. */
  private def withBounceBuffer(size: Long)(body: BounceBuffer => Unit): Unit = {
    val bounceBuffer = getBounceBuffer(size)
    try {
      body(bounceBuffer)
    } finally {
      if (!bounceBuffer.isClosed) {
        bounceBuffer.close()
      }
    }
  }

  /**
   * Runs `body` with a receive state for `requests`, spied unless `spied` is false, its bounce
   * buffer of `size` bytes, and a count of the times the state released its transport entry.
   */
  private def withReceiveState(size: Long, requests: Seq[PendingTransferRequest],
      spied: Boolean = true)(
      body: (BufferReceiveState, BounceBuffer, AtomicInteger) => Unit): Unit = {
    val closes = new AtomicInteger()
    withBounceBuffer(size) { bounceBuffer =>
      val state = new BufferReceiveState(123L, bounceBuffer, requests,
        () => closes.incrementAndGet())
      body(if (spied) spy(state) else state, bounceBuffer, closes)
    }
  }

  private def receivedWindow(): Transaction = {
    val tx = mock[Transaction]
    when(tx.getStatus).thenReturn(TransactionStatus.Success)
    tx
  }

  test("an error reply before the first window ends the receive state once") {
    val requester = new Requester
    val (first, second) = (reader(), reader())
    // two more buffers than an error message names, alternating between two readers
    val requests = (1 to 7).map { id =>
      PendingTransferRequest(requester.client, tableMeta(id, 100L * id),
        if (id % 2 == 1) first else second)
    }
    withReceiveState(4096, requests) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(1 to 7, reason))

      // each reader with a request in the state fails once, told of the first few buffers
      Seq(first, second).foreach { handler =>
        verify(handler, times(1)).transferError(
          contains(s"buffers 1, 2, 3, 4, 5 and 2 more: $reason"), isNull())
      }
      // the receive pool slot, the in-flight bytes and the transport's entry are returned
      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      verify(mockTransport, times(1)).doneBytesInFlight(2800L)
    }
  }

  test("an error reply with a window still to consume ends the state once it is consumed") {
    val requester = new Requester
    val (delivered, failed) = (reader(), reader())
    // the first window holds the first table and half of the second
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), delivered),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), failed))
    withReceiveState(1024, requests) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      // the first window arrives, and UCX receives it into the slot
      state.getBufferWhenReady(_ => (), 1024)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      // UCX can still be writing the window, so the slot stays with the state
      assert(!bounceBuffer.isClosed)
      assertResult(0)(closes.get)

      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)

      verify(delivered, times(1)).batchReceived(any())
      verify(delivered, never()).transferError(any(), any())
      verify(failed, times(1)).transferError(contains(reason), isNull())
      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      // the delivered table's bytes come back with its batch, the failed one's with the reply
      verify(mockTransport, times(1)).doneBytesInFlight(512L)
      verify(mockTransport, times(1)).doneBytesInFlight(1024L)
    }
  }

  test("a second server failure while the first waits for a window is logged and ignored") {
    val requester = new Requester
    val (delivered, failed) = (reader(), reader())
    // the first window holds the first table and half of the second
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), delivered),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), failed))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      // no production path fails a request twice, so the test calls serverFailed itself
      val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[BufferReceiveState].getName)) {
        assert(state.serverFailed("the first failure").isEmpty)
        assert(state.serverFailed("the second failure").isEmpty)
      }

      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)

      verify(failed, times(1)).transferError(contains("the first failure"), isNull())
      verify(failed, never()).transferError(contains("the second failure"), any())
      assertResult(1)(logs.count(_.contains("the second failure")))
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
    }
  }

  test("a reader that throws while a deferred error reply ends the state is logged, and the " +
      "state closes once") {
    val requester = new Requester
    val (delivered, failed) = (reader(), reader())
    doThrow(new IllegalStateException("reader failed in test"))
      .when(failed).transferError(any(), any())
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), delivered),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), failed))
    withReceiveState(1024, requests) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      val windowReceived = receivedWindow()

      // the copy thread consumes the window, then ends the state the server failed
      val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[RapidsShuffleClient].getName)) {
        requester.client.doHandleBounceBufferReceive(windowReceived, state)
      }

      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      assertResult(1)(logs.count(_.startsWith("Error while ending BRS")))
    }
  }

  test("a window whose transaction fails to close, with a deferred error reply, closes the " +
      "state once") {
    val requester = new Requester
    val (delivered, failed) = (reader(), reader())
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), delivered),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), failed))
    withReceiveState(1024, requests) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      val windowReceived = receivedWindow()
      doThrow(new IllegalStateException("closing the window failed in test"))
        .when(windowReceived).close()

      // the window's own failure path closes the state, so the server's failure ends nothing
      requester.client.doHandleBounceBufferReceive(windowReceived, state)

      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
    }
  }

  test("a last window whose transaction fails to close releases its state once") {
    val requester = new Requester
    val requests = Seq(PendingTransferRequest(requester.client, tableMeta(1, 512), reader()))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 512)
      val windowReceived = receivedWindow()
      doThrow(new IllegalStateException("closing the window failed in test"))
        .when(windowReceived).close()

      // the last window closes the state, and then its failed close sends the state through
      // the window handler's catch, which closes it again
      requester.client.doHandleBounceBufferReceive(windowReceived, state)

      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
    }
  }

  test("an error reply after the requester ended the state itself releases nothing again") {
    val requester = new Requester
    val (first, second) = (reader(), reader())
    // the window cannot be handed to its reader, so the copy thread ends the state itself
    when(first.batchReceived(any())).thenThrow(new IllegalStateException("reader failed in test"))
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 1024), first),
      PendingTransferRequest(requester.client, tableMeta(2, 512), second))
    withReceiveState(1024, requests) { (state, _, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)
      assertResult(1)(closes.get)

      // the server, which does not know, fails the request's next window
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      verify(state, times(1)).close()
      assertResult(1)(closes.get)
    }
  }

  test("a receive state whose bounce buffer fails to close still releases the rest") {
    val requester = new Requester
    // the first window holds the first table and half of the second, which stays partial
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), reader()),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), reader()))
    val closes = new AtomicInteger()
    val slot = DeviceMemoryBuffer.allocate(1024)
    val bounceBuffer = new BounceBuffer(slot) {
      override def free(bb: BounceBuffer): Unit = {
        slot.close()
        throw new IllegalStateException("returning the bounce buffer failed in test")
      }
    }
    val state = new BufferReceiveState(123L, bounceBuffer, requests,
      () => closes.incrementAndGet())
    requester.client.doIssueBufferReceives(state)
    state.getBufferWhenReady(_ => (), 1024)
    requester.client.doHandleBounceBufferReceive(receivedWindow(), state)

    val allocated = Rmm.getTotalBytesAllocated
    val thrown = intercept[IllegalStateException](state.close())
    assertResult("returning the bounce buffer failed in test")(thrown.getMessage)
    // the slot and the partial table's buffer are freed, and the transport's entry removed
    assertResult(allocated - 2048)(Rmm.getTotalBytesAllocated)
    assertResult(1)(closes.get)
  }

  test("a transfer request whose send task throws fails its readers") {
    val requester = new Requester
    val (first, second) = (reader(), reader())
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 1000), first),
      PendingTransferRequest(requester.client, tableMeta(2, 2000), second))
    withBounceBuffer(4096) { bounceBuffer =>
      requester.client.doIssueBufferReceives(
        new BufferReceiveState(123L, bounceBuffer, requests, () => ()))
      val send = ArgumentCaptor.forClass(classOf[UcxCallback])
      verify(requester.ucx, times(1)).sendActiveMessage(
        anyLong(), any[UCXActiveMessage](), any[ByteBuffer](), send.capture())

      // the request's task, on the progress thread, cannot allocate the send's header
      val oom = new OutOfMemoryError("Direct buffer memory")
      intercept[OutOfMemoryError](UCX.sendOrReport(send.getValue)(_ => throw oom))

      // Each reader is told at once. The receive state stays, as it does whenever a transfer
      // request's transaction ends without success.
      Seq(first, second).foreach { handler =>
        verify(handler, times(1)).transferError(contains("Direct buffer memory"), isNull())
      }
      // and the response that will not come is no longer waited for
      verify(requester.ucx, times(1)).unregisterResponseHandler(any())
      // by its header alone, since only a metadata request retires or releases its response
      verify(requester.ucx, never()).retireResponseHandler(any(), any())
      verify(requester.ucx, never()).releaseResponseHandler(any(), any())
    }
  }

  test("a window that fails after starting a partial table closes that table's buffer once") {
    val requester = new Requester
    // the first window holds the first table and starts the second
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), reader()),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), reader()))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      // the next window's receive waits for this one to be consumed, then fails to start
      state.getBufferWhenReady(
        _ => throw new IllegalStateException("receiving the next window failed in test"), 512)

      val allocated = Rmm.getTotalBytesAllocated
      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)

      // what the window allocated is freed once, and so is the slot, so the total drops by the
      // slot's 1024 bytes
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      assertResult(allocated - 1024)(Rmm.getTotalBytesAllocated)
    }
  }

  test("a window that fails copying into a new partial table closes that table's buffer once") {
    val requester = new Requester
    val failed = reader()
    // the table is larger than a window, so the first window starts it in a new buffer
    val requests = Seq(PendingTransferRequest(requester.client, tableMeta(1, 2048), failed))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      val partial = mock[DeviceMemoryBuffer]
      val copyFailure = new IllegalStateException("copying the window failed in test")
      doThrow(copyFailure)
        .when(partial).copyFromDeviceBufferAsync(anyLong(), any(), anyLong(), anyLong(), any())
      val windowReceived = receivedWindow()

      // consumeWindow allocates the table's buffer itself, so Rmm hands it the mock, on this
      // thread only
      val allocs = mockStatic(classOf[Rmm], (invocation: InvocationOnMock) =>
        invocation.getMethod.getName match {
          case "alloc" if invocation.getArgument[Long](0) == 2048L => partial
          case _ => invocation.callRealMethod()
        })
      try {
        requester.client.doHandleBounceBufferReceive(windowReceived, state)
      } finally {
        allocs.close()
      }

      // the copy's failure reaches the reader, and only the state's close closes the buffer
      verify(failed, times(1)).transferError(any(), ArgumentMatchers.same(copyFailure))
      verify(partial, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
    }
  }

  test("a window consumed after its state closed fails before it allocates") {
    val requester = new Requester
    val (first, second) = (reader(), reader())
    // the first window's hand-off fails after the second window's receive started
    when(first.batchReceived(any())).thenThrow(new IllegalStateException("reader failed in test"))
    // the first window holds table 1, and the second starts table 2
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 1024), first),
      PendingTransferRequest(requester.client, tableMeta(2, 2048), second))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      // the second window's receive waits for the first window to be consumed
      state.getBufferWhenReady(_ => (), 1024)
      // the window handler's catch closes the state with the second window still to consume
      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)
      assert(bounceBuffer.isClosed)

      val allocated = Rmm.getTotalBytesAllocated
      requester.client.doHandleBounceBufferReceive(receivedWindow(), state)

      // the second window allocates nothing, and its reader is told of the closed state
      assertResult(allocated)(Rmm.getTotalBytesAllocated)
      verify(first, times(1)).transferError(any(), any())
      val errors = ArgumentCaptor.forClass(classOf[Throwable])
      verify(second, times(2)).transferError(any(), errors.capture())
      assert(errors.getValue.getMessage.contains("is closed"))
      assertResult(1)(closes.get)
    }
  }

  test("a window that fails after completing a partial table closes that table's buffer") {
    val requester = new Requester
    // the first window starts table 1, and the second completes it and holds all of table 2
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 1536), reader()),
      PendingTransferRequest(requester.client, tableMeta(2, 512), reader()),
      PendingTransferRequest(requester.client, tableMeta(3, 512), reader()))
    withReceiveState(1024, requests, spied = false) { (state, bounceBuffer, closes) =>
      requester.client.doIssueBufferReceives(state)
      val windowReceived = receivedWindow()
      state.getBufferWhenReady(_ => (), 1024)
      requester.client.doHandleBounceBufferReceive(windowReceived, state)
      state.getBufferWhenReady(_ => (), 1024)
      // the third window's receive waits for the second to be consumed, then fails to start
      state.getBufferWhenReady(
        _ => throw new IllegalStateException("receiving the next window failed in test"), 512)

      val allocated = Rmm.getTotalBytesAllocated
      requester.client.doHandleBounceBufferReceive(windowReceived, state)

      // the first table's 1536 bytes and the slot's 1024 are freed, and the second table's
      // buffer is allocated and freed in the window
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      assertResult(allocated - 2560)(Rmm.getTotalBytesAllocated)
    }
  }

  test("a reader waiting on a transfer that the server fails gets a fetch failure") {
    val requester = new Requester
    val bounceBuffers = new ArrayBuffer[BounceBuffer]()
    val closes = new AtomicInteger()
    when(mockTransport.makeClient(any())).thenReturn(requester.client)
    // as the transport's throttle does, issuing every queued request in one receive state
    val states = new ArrayBuffer[BufferReceiveState]()
    doAnswer { invocation =>
      val bounceBuffer = getBounceBuffer(4096)
      bounceBuffers += bounceBuffer
      val state = spy(new BufferReceiveState(123L, bounceBuffer,
        invocation.getArgument[Seq[PendingTransferRequest]](0), () => closes.incrementAndGet()))
      states += state
      requester.client.issueBufferReceives(state)
      null
    }.when(mockTransport).queuePending(any())

    val taskId = 1L
    // far longer than the test takes, but short enough that a reader that times out shows it
    val timeoutSeconds = 10L
    val blocks: collection.Seq[(BlockId, Long, Int)] =
      RapidsShuffleTestHelper.getShuffleBlocks.toSeq
    val iterator = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), mockConf, mockTransport,
      Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])](
        (RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), blocks)),
      testMetricsUpdater, Array.empty, taskId, mockCatalog, timeoutSeconds)
    val failure = new AtomicReference[Throwable]()
    val readerThread = new Thread(() => {
      RmmSpark.currentThreadIsDedicatedToTask(taskId)
      try {
        iterator.next()
      } catch {
        case t: Throwable => failure.set(t)
      }
    }, "shuffle-reader")
    readerThread.setDaemon(true)
    try {
      readerThread.start()
      requester.answer(
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 1000), tableMeta(2, 2000))))
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(1, 2), reason))
      readerThread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))

      assert(failure.get.isInstanceOf[RapidsShuffleFetchFailedException],
        s"the reader ended with ${failure.get}")
      assert(failure.get.getMessage.contains(reason))
      assertResult(1)(states.size)
      verify(states.head, times(1)).close()
      assert(bounceBuffers.forall(_.isClosed))
      assertResult(1)(closes.get)
    } finally {
      RmmSpark.taskDone(taskId)
      bounceBuffers.filterNot(_.isClosed).foreach(_.close())
    }
  }

  /**
   * A clock that a server's OOM backoff advances and a reader waits on. When the clock passes
   * the end of the reader's wait, `advance` returns only once the reader has given up, so that
   * whatever the server does next reaches the reader after its wait ended, as with a real clock.
   */
  private class SharedClock {
    private[this] var now = 0L
    private[this] var waitEnds = Long.MaxValue
    private[this] var waiting = false

    def nanos: Long = synchronized(now)

    def advance(nanos: Long): Unit = synchronized {
      now += nanos
      notifyAll()
      val giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      while (waiting && now >= waitEnds && System.nanoTime() < giveUpAt) {
        wait(10)
      }
    }

    /** Waits on this clock until `poll` returns a result, for at most `timeoutNanos`. */
    def await[R](timeoutNanos: Long)(poll: => Option[R]): Option[R] = {
      val ends = synchronized {
        waitEnds = now + timeoutNanos
        waiting = true
        notifyAll()
        waitEnds
      }
      try {
        var result = poll
        while (result.isEmpty && nanos < ends) {
          synchronized(wait(10))
          result = poll
        }
        result
      } finally {
        synchronized {
          waiting = false
          notifyAll()
        }
      }
    }

    /** Blocks until a reader is waiting on this clock. */
    def awaitReader(): Unit = synchronized {
      val giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      while (!waiting && System.nanoTime() < giveUpAt) {
        wait(10)
      }
      assert(waiting, "the reader is not waiting")
    }
  }

  test("a reader waiting through a persistent OOM gets the error reply before its wait ends") {
    // The server and the reader wait the same length, spark.network.timeout, on one clock. The
    // reader starts waiting before its request reaches the server, and every attempt to
    // prepare the request's buffers runs out of GPU memory.
    val timeoutSeconds = 30L
    val clock = new SharedClock
    val requester = new Requester
    val bounceBuffers = new ArrayBuffer[BounceBuffer]()
    val states = new ArrayBuffer[BufferReceiveState]()
    when(mockTransport.makeClient(any())).thenReturn(requester.client)
    doAnswer { invocation =>
      val bounceBuffer = getBounceBuffer(4096)
      bounceBuffers += bounceBuffer
      val state = spy(new BufferReceiveState(123L, bounceBuffer,
        invocation.getArgument[Seq[PendingTransferRequest]](0), () => ()))
      states += state
      requester.client.issueBufferReceives(state)
      null
    }.when(mockTransport).queuePending(any())

    // the server's side: the requested buffers, which never fit in GPU memory
    val requestHandler = mock[RapidsShuffleRequestHandler]
    Seq(1 -> 1000L, 2 -> 2000L).foreach { case (id, size) =>
      val handle = mock[SpillableDeviceBufferHandle]
      when(handle.sizeInBytes).thenReturn(size)
      when(handle.materialize()).thenAnswer(_ =>
        throw new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(id)))
        .thenReturn(RapidsShuffleHandle(handle, tableMeta(id, size)))
    }
    val sendBuffer = mock[SendBounceBuffers]
    val bounceBuffer = mock[BounceBuffer]
    val bounceMemory = mock[DeviceMemoryBuffer]
    when(bounceBuffer.buffer).thenReturn(bounceMemory)
    when(bounceMemory.getLength).thenReturn(4096L)
    when(sendBuffer.bounceBufferSize).thenReturn(4096)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(bounceBuffer)
    when(mockTransport.tryGetSendBounceBuffers(anyLong(), anyInt())).thenReturn(Seq(sendBuffer))

    // the request as it reaches the server, whose reply goes to the requester when it is sent
    val replySentAt = new AtomicLong(-1L)
    val replied = new CountDownLatch(1)
    val request = mock[Transaction]
    when(request.peerExecutorId()).thenReturn(1L)
    when(request.getStatus).thenReturn(TransactionStatus.Success)
    when(request.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, Seq(1, 2)))))
    doAnswer { invocation =>
      replySentAt.set(clock.nanos)
      // a copy, as the requester's own receive buffer would be
      val sent = invocation.getArgument[ByteBuffer](0)
      val reply = ByteBuffer.allocateDirect(sent.remaining())
      reply.put(sent.duplicate())
      reply.flip()
      requester.answer(reply)
      val responseTx = mock[Transaction]
      when(responseTx.getStatus).thenReturn(TransactionStatus.Success)
      invocation.getArgument[TransactionCallback](1)(responseTx)
      // the requester handled the reply on its copy thread, here inline, and released its state
      replied.countDown()
      responseTx
    }.when(request).respond(any(), any())

    val serverConnection = mock[ServerConnection]
    val requestCallbacks = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    doAnswer { invocation =>
      requestCallbacks.put(invocation.getArgument[MessageType.Value](0),
        invocation.getArgument[TransactionCallback](1))
      null
    }.when(serverConnection).registerRequestHandler(any(), any())
    val sendLoop = Executors.newSingleThreadExecutor()
    val server = new RapidsShuffleServer(mockTransport, serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), requestHandler, mockExecutor,
      sendLoop, mockConf) {
      override private[shuffle] def currentTimeNanos(): Long = clock.nanos
      override private[shuffle] def oomRetryTimeoutNanos: Long =
        TimeUnit.SECONDS.toNanos(timeoutSeconds)
      override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit =
        clock.advance(TimeUnit.MILLISECONDS.toNanos(backoffMillis))
    }

    val taskId = 1L
    val blocks: collection.Seq[(BlockId, Long, Int)] =
      RapidsShuffleTestHelper.getShuffleBlocks.toSeq
    val iterator = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), mockConf, mockTransport,
      Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])](
        (RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), blocks)),
      testMetricsUpdater, Array.empty, taskId, mockCatalog, timeoutSeconds) {
      override def pollForResult(timeoutSeconds: Long): Option[ShuffleClientResult] =
        clock.await(TimeUnit.SECONDS.toNanos(timeoutSeconds))(super.pollForResult(0))
    }
    val failure = new AtomicReference[Throwable]()
    val readerThread = new Thread(() => {
      RmmSpark.currentThreadIsDedicatedToTask(taskId)
      try {
        iterator.next()
      } catch {
        case t: Throwable => failure.set(t)
      }
    }, "shuffle-reader")
    readerThread.setDaemon(true)
    try {
      server.start()
      readerThread.start()
      requester.answer(
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 1000), tableMeta(2, 2000))))
      clock.awaitReader()
      // the request reaches the server a second after the reader started waiting
      clock.advance(TimeUnit.SECONDS.toNanos(1))
      requestCallbacks.get(MessageType.TransferRequest)(request)
      readerThread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      // This waits for the reply, which comes even if the reader gave up first, so a failed assert
      // below can say when it came. The reader can also wake before the receive state closes.
      assert(replied.await(awaitSeconds, TimeUnit.SECONDS), "the server never replied")

      assert(failure.get.isInstanceOf[RapidsShuffleFetchFailedException],
        s"the reader ended with ${failure.get}, and the server replied at " +
          s"${TimeUnit.NANOSECONDS.toMillis(replySentAt.get)} ms of a " +
          s"${timeoutSeconds * 1000} ms wait")
      assert(failure.get.getMessage.contains(
        RapidsShuffleServer.failureReason(BufferSendState.OomExpired)))
      // the server kept retrying for most of the reader's wait before it gave up
      assert(replySentAt.get >= TimeUnit.SECONDS.toNanos(timeoutSeconds) / 2)
      assertResult(1)(states.size)
      verify(states.head, times(1)).close()
      assert(bounceBuffers.forall(_.isClosed))
    } finally {
      server.close()
      sendLoop.shutdownNow()
      RmmSpark.taskDone(taskId)
      bounceBuffers.filterNot(_.isClosed).foreach(_.close())
    }
  }

  /** A copy of what `buffer` has left, in a direct buffer, as UCX delivers a message. */
  private def directCopy(buffer: ByteBuffer): ByteBuffer = {
    val copy = ByteBuffer.allocateDirect(buffer.remaining())
    copy.put(buffer.duplicate())
    copy.flip()
    copy
  }

  /**
   * What a reader ends with when the server's lookup for its metadata request throws `failure`.
   */
  private def readerOfFailedMetadataRequest(failure: Throwable): Throwable =
    readerOfMetadataRequest(_ => throw failure)

  /**
   * What a reader of the helper's three blocks ends with when the server looks up each block of
   * its metadata request with `lookup`. The reader waits 5 seconds, far longer than the test
   * takes, so a reader that timed out shows it.
   */
  private def readerOfMetadataRequest(
      lookup: ShuffleBlockBatchId => Seq[TableMeta]): Throwable = {
    val requester = new Requester
    when(mockTransport.makeClient(any())).thenReturn(requester.client)
    // the requests the client sends, which the test hands to the server
    val sent = new LinkedBlockingQueue[ByteBuffer]()
    doAnswer { invocation =>
      sent.add(directCopy(invocation.getArgument[ByteBuffer](2)))
      null
    }.when(requester.ucx).sendActiveMessage(
      anyLong(), any[UCXActiveMessage](), any[ByteBuffer](), any[UcxCallback]())

    val requestHandler = mock[RapidsShuffleRequestHandler]
    when(requestHandler.getShuffleBufferMetas(any())).thenAnswer { invocation =>
      lookup(invocation.getArgument[ShuffleBlockBatchId](0))
    }
    val serverConnection = mock[ServerConnection]
    val requestCallbacks = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    doAnswer { invocation =>
      requestCallbacks.put(invocation.getArgument[MessageType.Value](0),
        invocation.getArgument[TransactionCallback](1))
      null
    }.when(serverConnection).registerRequestHandler(any(), any())
    // metadata requests run on the test's thread, and no transfer request comes
    val server = new RapidsShuffleServer(mockTransport, serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), requestHandler, mockExecutor,
      mockBssExecutor, mockConf)
    server.start()
    // the request as it reaches the server, whose reply goes to the requester when it is sent
    val request = mock[Transaction]
    when(request.peerExecutorId()).thenReturn(1L)
    when(request.getStatus).thenReturn(TransactionStatus.Success)
    doAnswer { invocation =>
      requester.answer(directCopy(invocation.getArgument[ByteBuffer](0)))
      val responseTx = mock[Transaction]
      when(responseTx.getStatus).thenReturn(TransactionStatus.Success)
      invocation.getArgument[TransactionCallback](1)(responseTx)
      responseTx
    }.when(request).respond(any(), any())

    val taskId = 1L
    val timeoutSeconds = 5L
    val blocks: collection.Seq[(BlockId, Long, Int)] =
      RapidsShuffleTestHelper.getShuffleBlocks.toSeq
    val iterator = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), mockConf, mockTransport,
      Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])](
        (RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), blocks)),
      testMetricsUpdater, Array.empty, taskId, mockCatalog, timeoutSeconds)
    val thrown = new AtomicReference[Throwable]()
    val readerThread = new Thread(() => {
      RmmSpark.currentThreadIsDedicatedToTask(taskId)
      try {
        iterator.next()
      } catch {
        case t: Throwable => thrown.set(t)
      }
    }, "shuffle-reader")
    readerThread.setDaemon(true)
    try {
      readerThread.start()
      val metadataRequest = sent.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(metadataRequest != null, "the reader sent no metadata request")
      when(request.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(metadataRequest)))
      requestCallbacks.get(MessageType.MetadataRequest)(request)
      readerThread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      thrown.get
    } finally {
      RmmSpark.taskDone(taskId)
    }
  }

  test("a reader's fetch failure names the block whose lookup failed") {
    // the server finds the request's first block, then fails to find its second
    val (missing, _, missingMapIndex) = RapidsShuffleTestHelper.getShuffleBlocks(1)
    val gone = "unknown shuffle block shuffle_2_2_2"
    val thrown = readerOfMetadataRequest { blockId =>
      if (blockId == missing) throw new ShuffleOutputGoneException(gone) else Seq(tableMeta(1, 100))
    }
    assert(thrown.isInstanceOf[RapidsShuffleFetchFailedException],
      s"the reader ended with $thrown")
    // Spark unregisters by name the map index that the fetch failure carries
    val failure = thrown.asInstanceOf[RapidsShuffleFetchFailedException].toTaskFailedReason
      .asInstanceOf[FetchFailed]
    assertResult((missing.shuffleId, missing.mapId, missingMapIndex, missing.startReduceId))(
      (failure.shuffleId, failure.mapId, failure.mapIndex, failure.reduceId))
    assertResult("2")(failure.bmAddress.executorId)
    assert(thrown.getMessage.contains("Executor 2 failed the metadata request: " +
      s"${RapidsShuffleServer.failureReason(BufferSendState.MissingOutput)}: $gone"))
  }

  test("a reader whose metadata request the server fails gets a fetch failure") {
    val gone = "unknown shuffle block shuffle_1_1_1"
    val thrown = readerOfFailedMetadataRequest(new ShuffleOutputGoneException(gone))
    assert(thrown.isInstanceOf[RapidsShuffleFetchFailedException],
      s"the reader ended with $thrown")
    assert(thrown.getMessage.contains("Executor 2 failed the metadata request: " +
      s"${RapidsShuffleServer.failureReason(BufferSendState.MissingOutput)}: $gone"))
  }

  test("a reader whose metadata request fails for a reason other than gone output gets a " +
      "fetch failure") {
    // with no message, the reason names the failure's class
    val thrown = readerOfFailedMetadataRequest(new NoSuchElementException())
    assert(thrown.isInstanceOf[RapidsShuffleFetchFailedException],
      s"the reader ended with $thrown")
    assert(thrown.getMessage.contains("Executor 2 failed the metadata request: " +
      s"${RapidsShuffleServer.failureReason(BufferSendState.Failed)}: " +
      classOf[NoSuchElementException].getName))
  }

  /** UCX's registration of metadata responses. It uses only its own map, so a mock UCX holds it. */
  private def metadataResponses(): UCX#ResponseActiveMessageRegistration = {
    val ucx = mock[UCX]
    new ucx.ResponseActiveMessageRegistration(metadataResponseAmId)
  }

  /** Sends `handler`'s metadata request, returning the response and callback it registered. */
  private def fetchMetadata(requester: Requester,
      handler: RapidsShuffleFetchHandler): (UCXActiveMessage, UCXAmCallback) = {
    requester.client.registerPeerErrorListener(handler)
    requester.client.doFetch(Seq(ShuffleBlockBatchId(1, 7L, 2, 2)), handler)
    requester.lastRegistered
  }

  /**
   * Delivers `reply` for `response` as UCX's receive handler does: it takes what the response's
   * header holds, and hands the reply only to a request's callback.
   */
  private def deliver(responses: UCX#ResponseActiveMessageRegistration,
      response: UCXActiveMessage, reply: ByteBuffer): Unit = {
    val callback = responses.getCallback(response.header)
    if (callback != null && (callback ne UCX.RetiredResponse)) {
      callback.onSuccess(response,
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(directCopy(reply))))
    }
  }

  test("a metadata request stops waiting for its response once its reader is done") {
    val requester = new Requester(spied = true)
    when(requester.ucx.retireResponseHandler(any(), any())).thenReturn(true)
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)

    // the reader's task ends, as after its wait timed out, and no response has come
    requester.client.unregisterPeerErrorListener(handler)

    // it withdraws its own request: the callback it registered, then the transaction
    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
    verify(requester.connection, times(1)).removeTransaction(any())
    verify(handler, never()).transferError(any(), any())
  }

  test("a metadata request whose response took its callback first keeps its transaction") {
    val requester = new Requester(spied = true)
    // the progress thread took the callback for the response just before the task ended
    when(requester.ucx.retireResponseHandler(any(), any())).thenReturn(false)
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)

    requester.client.unregisterPeerErrorListener(handler)

    // the response's handling owns the transaction, and closes it
    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
    verify(requester.connection, never()).removeTransaction(any())
  }

  test("a reply handled before doFetch records its request leaves the header free") {
    val requester = new Requester(spied = true)
    val responses = requester.registerResponses()
    // the reply is handled before request returns, so before doFetch records the request
    doAnswer { invocation =>
      val header = invocation.getArgument[UCXActiveMessage](1).header
      deliver(responses, UCXActiveMessage(metadataResponseAmId, header, forceRndv = false),
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 100))))
      null
    }.when(requester.ucx).sendActiveMessage(
      anyLong(), any[UCXActiveMessage](), any[ByteBuffer](), any[UcxCallback]())
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)
    verify(handler, times(1)).start(1)

    requester.client.unregisterPeerErrorListener(handler)

    // the reader's end withdraws the stale entry, which finds the header free and retires
    // nothing, and the reply's handling already closed the transaction
    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
    verify(requester.connection, times(1)).removeTransaction(any())
    verify(handler, never()).transferError(any(), any())
    assert(!responses.addResponseActiveMessageHandler(response.header, mock[UCXAmCallback]))
  }

  test("a metadata request is withdrawn before the transport cancels its reader, which can " +
      "throw") {
    val requester = new Requester
    when(requester.ucx.retireResponseHandler(any(), any())).thenReturn(true)
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)
    doThrow(new IllegalStateException("cancelling failed in test"))
      .when(mockTransport).cancelPending(handler)

    intercept[IllegalStateException](requester.client.unregisterPeerErrorListener(handler))

    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
  }

  test("a reader that ends withdraws only its own metadata request from a shared client") {
    val requester = new Requester(spied = true)
    val responses = requester.registerResponses()
    val (ended, running) = (reader(), reader())
    val (endedResponse, endedCallback) = fetchMetadata(requester, ended)
    val (runningResponse, runningCallback) = fetchMetadata(requester, running)

    requester.client.unregisterPeerErrorListener(ended)

    verify(requester.ucx, times(1)).retireResponseHandler(any(), any())
    verify(requester.ucx, times(1)).retireResponseHandler(endedResponse, endedCallback)
    verify(requester.connection, times(1)).removeTransaction(any())
    // the running reader's request still waits, in the client and in UCX, until its own end
    requester.client.unregisterPeerErrorListener(running)
    verify(requester.ucx, times(1)).retireResponseHandler(runningResponse, runningCallback)
    verify(requester.connection, times(2)).removeTransaction(any())
    assert(responses.getCallback(runningResponse.header) eq UCX.RetiredResponse)
  }

  test("a reader whose client closed still withdraws its metadata request when it ends") {
    val requester = new Requester(spied = true)
    when(requester.ucx.retireResponseHandler(any(), any())).thenReturn(true)
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)
    // a peer error closes the client, which fails its readers and leaves their requests
    requester.client.close()
    verify(handler, times(1)).transferError(any(), any())

    requester.client.unregisterPeerErrorListener(handler)

    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
    verify(requester.connection, times(1)).removeTransaction(any())
  }

  test("a metadata request whose send fails retires its response, not removes it") {
    val requester = new Requester
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)
    val (_, send) = requester.lastSent

    send.onError(-1, "the send failed in test")

    // the send may have reached the peer before it failed, so a reply may still come
    verify(requester.ucx, times(1)).retireResponseHandler(response, callback)
    verify(requester.ucx, never()).unregisterResponseHandler(any())
    verify(handler, times(1)).transferError(contains("the send failed in test"), isNull())
  }

  test("a metadata request whose send never left the executor releases its response") {
    val requester = new Requester
    val handler = reader()
    val (response, callback) = fetchMetadata(requester, handler)
    val (_, send) = requester.lastSent

    send.onError(UCX.NO_ENDPOINT, "the send never left in test")

    // no reply can come, so the header is freed
    verify(requester.ucx, times(1)).releaseResponseHandler(response, callback)
    verify(requester.ucx, never()).retireResponseHandler(any(), any())
    verify(requester.ucx, never()).unregisterResponseHandler(any())
    verify(handler, times(1)).transferError(contains("the send never left in test"), isNull())
  }

  test("a metadata request whose send fails after its withdrawal is closed once") {
    val requester = new Requester
    val responses = requester.registerResponses()
    val handler = reader()
    val (response, _) = fetchMetadata(requester, handler)
    val (_, send) = requester.lastSent
    requester.client.unregisterPeerErrorListener(handler)
    verify(requester.ucx, times(1)).retireResponseHandler(any(), any())

    // The send's callback releases the request buffer and the transaction, which the withdrawal
    // leaves to it. A second release of either would be logged, or would reach the reader.
    val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[UCXTransaction].getName)) {
      send.onError(-1, "the send failed in test")
    }
    verify(handler, times(1)).transferError(any(), any())
    verify(handler, times(1)).transferError(contains("the send failed in test"), isNull())
    assert(!logs.exists(_.contains("Detected an exception from user code")))
    // the reply may still come, so the withdrawn request's header stays held
    assert(responses.getCallback(response.header) eq UCX.RetiredResponse)
  }

  test("a withdrawn metadata request whose send then never left frees its header") {
    val requester = new Requester
    val responses = requester.registerResponses()
    val handler = reader()
    val (response, _) = fetchMetadata(requester, handler)
    val (_, send) = requester.lastSent
    requester.client.unregisterPeerErrorListener(handler)
    verify(requester.ucx, times(1)).retireResponseHandler(any(), any())

    send.onError(UCX.SEND_TASK_FAILED, "the send never left in test")

    // no reply can come, so the marker goes, and a newer request can take the header
    assert(!responses.addResponseActiveMessageHandler(response.header, mock[UCXAmCallback]))
  }

  test("a late reply to a withdrawn metadata request does not reach a newer request") {
    val requester = new Requester
    val responses = requester.registerResponses()
    // The first request draws transaction 5. The next draws 5 + 2^32, whose low 32 bits, and so
    // its header, repeat the first's, as once the executor has run 2^32 transactions.
    val wrap = 1L << 32
    when(requester.ucx.getNextTransactionId).thenReturn(5L, 5L + wrap, 6L + wrap, 7L + wrap)

    // the old reader's request goes unanswered, and its task ends
    val old = reader()
    val (oldResponse, _) = fetchMetadata(requester, old)
    requester.client.unregisterPeerErrorListener(old)

    // a newer reader's request draws the transaction id whose low 32 bits repeat
    val current = reader()
    val (currentResponse, currentCallback) = fetchMetadata(requester, current)
    verify(current, never()).transferError(any(), any())
    // and the request carries the header its response registered, which the server echoes
    assertResult(currentResponse.header)(requester.lastSent._1.header)

    // the old reply arrives now
    deliver(responses, oldResponse, ShuffleMetadata.buildMetaResponse(
      Seq(tableMeta(11, 100), tableMeta(12, 100), tableMeta(13, 100))))

    // the newer reader gets none of the old tables, and its own request still waits: once its
    // reader is done, the withdrawal finds its callback under the header that it registered
    verify(current, never()).start(anyInt())
    verify(current, never()).transferError(any(), any())
    requester.client.unregisterPeerErrorListener(current)
    verify(requester.ucx, times(1)).retireResponseHandler(currentResponse, currentCallback)
    assert(responses.getCallback(currentResponse.header) eq UCX.RetiredResponse)
  }

  test("a withdrawn request's response header holds a marker until its reply takes it") {
    val responses = metadataResponses()
    val (withdrawn, newer) = (mock[UCXAmCallback], mock[UCXAmCallback])
    responses.addResponseActiveMessageHandler(1L, withdrawn)
    assert(responses.retire(1L, withdrawn))
    // a newer request that draws the header is refused it, and the marker stays
    assert(responses.addResponseActiveMessageHandler(1L, newer))
    // the reply takes the marker, once, which frees the header
    assert(responses.getCallback(1L) eq UCX.RetiredResponse)
    assert(responses.getCallback(1L) == null)
    assert(!responses.addResponseActiveMessageHandler(1L, newer))
  }

  test("a withdrawal that loses to the reply leaves a newer request's callback") {
    val responses = metadataResponses()
    val (answered, newer) = (mock[UCXAmCallback], mock[UCXAmCallback])
    responses.addResponseActiveMessageHandler(1L, answered)
    assert(responses.getCallback(1L) eq answered)
    responses.addResponseActiveMessageHandler(1L, newer)
    // a header that a live request holds still refuses another
    intercept[IllegalArgumentException](
      responses.addResponseActiveMessageHandler(1L, mock[UCXAmCallback]))

    assert(!responses.retire(1L, answered))
    assert(responses.getCallback(1L) eq newer)
  }

  test("a removal by header alone frees a live header and leaves a withdrawn one held") {
    val responses = metadataResponses()
    val (live, withdrawn) = (mock[UCXAmCallback], mock[UCXAmCallback])
    responses.addResponseActiveMessageHandler(1L, live)
    responses.addResponseActiveMessageHandler(2L, withdrawn)
    responses.retire(2L, withdrawn)

    responses.removeResponseActiveMessageHandler(1L)
    responses.removeResponseActiveMessageHandler(2L)

    assert(responses.getCallback(2L) eq UCX.RetiredResponse)
    assert(responses.getCallback(1L) == null)
  }

  test("a release frees its own request's header, held by its callback or its marker") {
    val responses = metadataResponses()
    val (sent, withdrawn, newer) = (mock[UCXAmCallback], mock[UCXAmCallback], mock[UCXAmCallback])
    responses.addResponseActiveMessageHandler(1L, sent)
    responses.addResponseActiveMessageHandler(2L, withdrawn)
    assert(responses.retire(2L, withdrawn))
    responses.addResponseActiveMessageHandler(3L, newer)

    responses.release(1L, sent)
    responses.release(2L, withdrawn)
    // a release by a request that does not hold the header leaves its holder
    responses.release(3L, sent)

    assert(responses.getCallback(3L) eq newer)
    assert(responses.getCallback(1L) == null)
    assert(responses.getCallback(2L) == null)
  }

  // past the task attempt ids that other suites register completion callbacks under, since
  // ScalableTaskCompletion keys a map that the whole JVM shares by that id
  private val taskAttempts = new AtomicLong(1000L)

  /**
   * Runs `body` with a reader of one block from each of `clients`' peers, built while a task
   * context is set, as in a task, so that it registers its cleaner. The context's
   * `markTaskComplete` ends the task, and nothing stays registered for it after `body`.
   */
  private def withReaderInTask(clients: RapidsShuffleClient*)(
      body: (RapidsShuffleIterator, MockTaskContext) => Unit): Unit = {
    val taskAttemptId = taskAttempts.incrementAndGet()
    val context = new MockTaskContext(taskAttemptId, partitionId = 0)
    val blocksByAddress = clients.zipWithIndex.map { case (client, i) =>
      val peer = RapidsShuffleTestHelper.makeMockBlockManager(s"${i + 2}", s"${i + 2}")
      when(mockTransport.makeClient(peer)).thenReturn(client)
      (peer, Seq[(BlockId, Long, Int)]((ShuffleBlockBatchId(1, i, 1, 1), 100L, i)))
    }
    TestUtils.withTaskContext(context) {
      body(new RapidsShuffleIterator(
        RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), mockConf, mockTransport,
        blocksByAddress.toArray[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
        testMetricsUpdater, Array.empty, taskAttemptId, mockCatalog, timeoutSeconds = 5L),
        context)
    }
  }

  test("a task that ends with one of two peers answered withdraws the other's metadata " +
      "request") {
    val (answering, silent) = (new Requester, new Requester)
    val answered = answering.registerResponses()
    silent.registerResponses()
    withReaderInTask(answering.client, silent.client) { (iterator, task) =>
      iterator.start()
      // one peer's reply comes, with a table to transfer
      val (answeredResponse, _) = answering.lastRegistered
      deliver(answered, answeredResponse,
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 100))))
      val queued = ArgumentCaptor.forClass(classOf[Seq[PendingTransferRequest]])
      verify(mockTransport, times(1)).queuePending(queued.capture())

      // the task succeeds before the other peer answers, as when a limit stops reading
      task.markTaskComplete()

      val (unanswered, unansweredCallback) = silent.lastRegistered
      verify(silent.ucx, times(1)).retireResponseHandler(unanswered, unansweredCallback)
      verify(answering.ucx, never()).retireResponseHandler(any(), any())
      // and the answered peer's reader reports itself done, which the transport's admit refuses
      assert(queued.getValue.head.handler.isDone)
    }
  }

  test("a reader's cleaner reaches every peer when a step throws, and rethrows the first " +
      "failure") {
    val (first, second) = (mock[RapidsShuffleClient], mock[RapidsShuffleClient])
    withReaderInTask(first, second) { (iterator, task) =>
      iterator.start()
      val cancelFailed = new IllegalStateException("cancelling failed in test")
      val unregisterFailed = new IllegalStateException("unregistering failed in test")
      doThrow(cancelFailed).when(first).cancelPending(any())
      // the same instance again, as a preallocated JVM error can be, which cannot suppress itself
      doThrow(cancelFailed).when(second).cancelPending(any())
      doThrow(unregisterFailed).when(first).unregisterPeerErrorListener(any())

      val thrown = intercept[IllegalStateException](task.markTaskComplete())

      assert(thrown eq cancelFailed)
      assertResult(Seq(unregisterFailed))(thrown.getSuppressed.toSeq)
      Seq(first, second).foreach { client =>
        verify(client, times(1)).unregisterPeerErrorListener(any())
      }
    }
  }

  test("a reader's cleaner reaches every peer when closing a batch it received throws") {
    val (first, second) = (mock[RapidsShuffleClient], mock[RapidsShuffleClient])
    withReaderInTask(first, second) { (iterator, task) =>
      iterator.start()
      // the first peer's reader received two batches that the task never took
      val handler = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
      verify(first).doFetch(any(), handler.capture())
      val (received, alsoReceived) = (mock[RapidsShuffleHandle], mock[RapidsShuffleHandle])
      val closeFailed = new IllegalStateException("closing a batch failed in test")
      doThrow(closeFailed).when(received).close()
      handler.getValue.start(2)
      assert(handler.getValue.batchReceived(received))
      assert(handler.getValue.batchReceived(alsoReceived))

      val thrown = intercept[IllegalStateException](task.markTaskComplete())

      assert(thrown eq closeFailed)
      Seq(first, second).foreach { client =>
        verify(client, times(1)).cancelPending(any())
        verify(client, times(1)).unregisterPeerErrorListener(any())
      }
      // the first batch's throwing close did not skip the second's
      verify(alsoReceived, times(1)).close()
    }
  }

  test("a reader's cleaner reaches every peer when its hasNext check throws") {
    val client = mock[RapidsShuffleClient]
    withReaderInTask(client) { (iterator, task) =>
      iterator.start()
      val handler = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
      verify(client).doFetch(any(), handler.capture())
      handler.getValue.start(1)
      assert(handler.getValue.batchReceived(mock[RapidsShuffleHandle]))
      assert(iterator.pollForResult(0).isDefined)
      assert(!iterator.hasNext)
      // a second start for a drained reader unbalances its counts
      handler.getValue.start(1)

      val thrown = intercept[IllegalStateException](task.markTaskComplete())

      assert(thrown.getMessage.contains("were expected"))
      verify(client, times(1)).cancelPending(any())
      verify(client, times(1)).unregisterPeerErrorListener(any())
    }
  }

  test("a reader's cleaner restores an interrupt that it suppressed, after its last step") {
    val (first, second) = (mock[RapidsShuffleClient], mock[RapidsShuffleClient])
    try {
      withReaderInTask(first, second) { (iterator, task) =>
        iterator.start()
        val cancelFailed = new IllegalStateException("cancelling failed in test")
        doThrow(cancelFailed).when(first).cancelPending(any())
        // an interrupted wait in a later step, which doThrow cannot stub for a Scala method
        doAnswer(_ => throw new InterruptedException("interrupted in test"))
          .when(second).cancelPending(any())
        var interruptedAtLastStep: Option[Boolean] = None
        doAnswer { _ =>
          interruptedAtLastStep = Some(Thread.currentThread().isInterrupted)
          null
        }.when(second).unregisterPeerErrorListener(any())

        val thrown = intercept[IllegalStateException](task.markTaskComplete())

        assert(thrown eq cancelFailed)
        assert(thrown.getSuppressed.exists(_.isInstanceOf[InterruptedException]))
        // restored once every step ran, so that it cut none short
        assertResult(Some(false))(interruptedAtLastStep)
        assert(Thread.currentThread().isInterrupted)
      }
    } finally {
      // Clears the status the cleaner restored, which would fail the tests after this one, also
      // when the test fails early and withTaskContext's reset runs the cleaner.
      Thread.interrupted()
    }
  }
}
