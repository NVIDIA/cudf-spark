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
  ShuffleMetadata}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle._
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, contains, isNull}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.openucx.jucx.UcxCallback

import org.apache.spark.shuffle.rapids.RapidsShuffleFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * What a requester does when the server answers its transfer request with an error reply. The
 * reply reaches the client through the real UCX client connection and transaction, with only
 * UCX mocked, because the transaction's callback wrapper drops whatever the client's callback
 * throws. The suite is in the ucx package so that it can answer a request through the response
 * handler the connection registers, as UCX does.
 */
class RapidsShuffleErrorReplySuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L

  private val reason = "the shuffle output it asked for is gone"

  /** A client of executor 2 whose requests the test answers. */
  private class Requester {
    val ucx: UCX = mock[UCX]
    private val txIds = new AtomicLong()
    when(ucx.getNextTransactionId).thenAnswer(_ => txIds.incrementAndGet())
    private val unanswered = new LinkedBlockingQueue[UCXAmCallback]()
    doAnswer { invocation =>
      unanswered.add(invocation.getArgument[UCXAmCallback](1))
      null
    }.when(ucx).registerResponseHandler(any(), any())

    val client = new RapidsShuffleClient(
      new UCXClientConnection(2L, ucx, mock[UCXShuffleTransport]),
      mockTransport, mockExecutor, mockCopyExecutor, mockCatalog)

    /** Answers the oldest request still waiting for a response, as UCX delivers one. */
    def answer(response: ByteBuffer): Unit = {
      val callback = unanswered.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(callback != null, "the client sent no request to answer")
      callback.onSuccess(UCXActiveMessage(0, 0L, forceRndv = false),
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(response)))
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
}
