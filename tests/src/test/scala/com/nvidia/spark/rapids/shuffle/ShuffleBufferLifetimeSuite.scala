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

package com.nvidia.spark.rapids.shuffle

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, CudaFatalException, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Rmm, Table}
import com.nvidia.spark.rapids.{RapidsShuffleHandle, ShuffleBufferCatalog, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TransferState
import com.nvidia.spark.rapids.spill.{SpillableDeviceBufferHandle, SpillFramework}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import org.apache.spark.storage.ShuffleBlockId

/**
 * Deterministic interleavings of `ShuffleBufferCatalog.unregisterShuffle`, and of the server's
 * close, with the UCX shuffle server's send path.
 *
 * Each test records what the requester receives, whether the server's send loop survives and
 * serves the next request, how each send state ends, and what happens to the catalog's device
 * buffer and to the send's bounce buffer and request transaction, with exact counts. The
 * outcomes are logged with `info` and then compared, in one assertion, against what the server
 * does, so a change to the ownership of these buffers shows up as the list of outcomes it
 * changed rather than as the first failed check. A request the server fails gets an error reply
 * naming the tables it will not send, unless its own receive failed, a data send failed, the
 * server closed, or the failure ends the send loop.
 */
class ShuffleBufferLifetimeSuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L

  /** Outcomes of one interleaving, logged as observed and compared at the end of the test. */
  private class Outcomes {
    private val observed = new ArrayBuffer[(String, String)]()

    def apply(key: String, value: Any): Unit = {
      observed += key -> String.valueOf(value)
      info(s"$key = $value")
    }

    def check(expected: (String, Any)*): Unit = {
      val want = expected.map { case (k, v) => k -> String.valueOf(v) }
      val got = observed.toMap
      val wrong = want.collect {
        case (k, v) if !got.get(k).contains(v) =>
          s"$k: expected <$v>, observed <${got.getOrElse(k, "nothing")}>"
      }
      val unexpected = (got.keySet -- want.map(_._1)).toList.sorted
      assert(wrong.isEmpty && unexpected.isEmpty,
        (wrong ++ unexpected.map(k => s"$k: observed <${got(k)}>, not expected"))
          .mkString("outcomes differ from the code under test:\n", "\n", ""))
    }
  }

  private def describe(t: Throwable): String =
    if (t == null) "none" else s"${t.getClass.getSimpleName}: ${t.getMessage}"

  private def attempt(body: => Unit): Throwable = {
    try {
      body
      null
    } catch {
      case t: Throwable => t
    }
  }

  private def calls(mockObject: AnyRef, method: String): Int = {
    var n = 0
    val it = mockingDetails(mockObject).getInvocations.iterator()
    while (it.hasNext) {
      if (it.next().getMethod.getName == method) {
        n += 1
      }
    }
    n
  }

  /** Runs `body`, then every cleanup it registered, last registered first. */
  private def withCleanup[T](body: ArrayBuffer[() => Unit] => T): T = {
    val cleanups = new ArrayBuffer[() => Unit]()
    try {
      body(cleanups)
    } finally {
      cleanups.reverse.foreach(c => attempt(c()))
    }
  }

  /** A buffer in the catalog, the test's own reference to its device memory, and its bytes. */
  private case class Cached(
      blockId: ShuffleBlockId,
      tableId: Int,
      device: DeviceMemoryBuffer,
      bytes: HostMemoryBuffer) extends AutoCloseable {
    def size: Long = device.getLength

    /** References to the device memory other than the test's own. */
    def otherRefs: Int = device.getRefCount - 1

    override def close(): Unit = {
      device.close()
      bytes.close()
    }
  }

  private def contiguousTable(first: Int, numRows: Int): ContiguousTable = {
    withResource(ColumnVector.fromInts((first until first + numRows).toArray: _*)) { cv =>
      withResource(new Table(cv)) { table =>
        table.contiguousSplit()(0)
      }
    }
  }

  /** Adds a 1000-row batch whose values start at `first`, so that no two buffers match. */
  private def cache(catalog: ShuffleBufferCatalog, blockId: ShuffleBlockId, first: Int,
      cleanups: ArrayBuffer[() => Unit]): Cached = {
    val ct = contiguousTable(first, 1000)
    val (device, bytes) = closeOnExcept(ct) { _ =>
      val buffer = ct.getBuffer
      val host = closeOnExcept(HostMemoryBuffer.allocate(buffer.getLength)) { host =>
        host.copyFromDeviceBuffer(buffer)
        host
      }
      buffer.incRefCount()
      (buffer, host)
    }
    val cached = closeOnExcept(device) { _ =>
      closeOnExcept(bytes) { _ =>
        Cached(blockId, catalog.addContiguousTable(blockId, ct, -1).tableId, device, bytes)
      }
    }
    cleanups += (() => cached.close())
    cached
  }

  private def newCatalog(cleanups: ArrayBuffer[() => Unit], shuffleIds: Int*)
      : ShuffleBufferCatalog = {
    val catalog = new ShuffleBufferCatalog()
    shuffleIds.foreach(catalog.registerShuffle)
    cleanups += (() => shuffleIds.filter(catalog.hasActiveShuffle)
      .foreach(catalog.unregisterShuffle))
    catalog
  }

  /** What the catalog and the spill store hold, and the other references to each buffer. */
  private def holding(catalog: ShuffleBufferCatalog, cached: Cached*): String = {
    val (handles, tables, listed) = catalog.bookkeepingSizes
    val tracked = SpillFramework.stores.deviceStore.numHandles
    s"catalog=($handles,$tables,$listed) spillStore=$tracked" +
      s" deviceRefs=${cached.map(_.otherRefs).mkString(",")}"
  }

  private def sameBytes(a: HostMemoryBuffer, b: HostMemoryBuffer): Boolean = {
    a.getLength == b.getLength && {
      val (x, y) = (new Array[Byte](a.getLength.toInt), new Array[Byte](b.getLength.toInt))
      a.getBytes(x, 0, 0, x.length)
      b.getBytes(y, 0, 0, y.length)
      java.util.Arrays.equals(x, y)
    }
  }

  /**
   * The request handler `RapidsShuffleInternalManagerBase` gives the server, with a pause and a
   * failing lookup.
   */
  private class CatalogRequestHandler(catalog: ShuffleBufferCatalog)
      extends CatalogShuffleRequestHandler(catalog) {
    @volatile var pauseAfterLookupOf: Option[Int] = None
    @volatile var failLookupOf: Option[Int] = None
    @volatile var resumedInTime = false
    /** Handles to return for a table id in place of the catalog's, such as failing ones. */
    val replaced = new ConcurrentHashMap[Int, RapidsShuffleHandle]()
    val lookedUp = new CountDownLatch(1)
    val resume = new CountDownLatch(1)

    override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
      if (failLookupOf.contains(tableId)) {
        throw new IllegalStateException(s"the lookup of table $tableId failed in test")
      }
      val handle = Option(replaced.get(tableId)).getOrElse(super.getShuffleHandle(tableId))
      if (pauseAfterLookupOf.contains(tableId)) {
        lookedUp.countDown()
        resumedInTime = resume.await(awaitSeconds, TimeUnit.SECONDS)
      }
      handle
    }
  }

  private case class Sent(bytes: HostMemoryBuffer, callback: TransactionCallback)

  /** The requester's side of the transport: keeps what the server sends and completes it. */
  private class Peer(cleanups: ArrayBuffer[() => Unit]) {
    val connection: ServerConnection = mock[ServerConnection]
    val sends = new LinkedBlockingQueue[Sent]()
    /** Runs at the start of every data send, with the peer's executor id; it may throw. */
    @volatile var beforeSend: (Long => Unit) = (_: Long) => ()
    private val dataSendCount = new AtomicInteger()
    private val copies = new ConcurrentLinkedQueue[HostMemoryBuffer]()
    private val handlers = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    cleanups += (() => copies.forEach(_.close()))

    when(connection.send(any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]()))
      .thenAnswer { invocation =>
        beforeSend(invocation.getArgument[Long](0))
        val buffer = invocation.getArgument[MemoryBuffer](3)
        val copy = HostMemoryBuffer.allocate(buffer.getLength)
        copies.add(copy)
        copy.copyFromDeviceBuffer(buffer.asInstanceOf[DeviceMemoryBuffer])
        dataSendCount.incrementAndGet()
        sends.put(Sent(copy, invocation.getArgument[TransactionCallback](4)))
        mock[Transaction]
      }
    doAnswer { invocation =>
      handlers.put(invocation.getArgument[MessageType.Value](0),
        invocation.getArgument[TransactionCallback](1))
      null
    }.when(connection).registerRequestHandler(any(), any())

    def dataSends: Int = dataSendCount.get

    def request(messageType: MessageType.Value, tx: Transaction): Unit =
      handlers.get(messageType)(tx)

    def nextSend(): Sent = {
      val sent = sends.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(sent != null, "the server never sent the data it was asked for")
      sent
    }

    /**
     * Completes `sent` with `status`. A success is when the server continues the send or
     * replies. A failed send reports InProgress, as the UCX transport does.
     */
    def complete(sent: Sent, status: TransactionStatus.Value = TransactionStatus.Success)
        : Unit = {
      val done = mock[Transaction]
      when(done.getStatus).thenReturn(status)
      sent.callback(done)
    }

    def completeAll(status: TransactionStatus.Value = TransactionStatus.Success): Unit = {
      var sent = sends.poll()
      while (sent != null) {
        complete(sent, status)
        sent = sends.poll()
      }
    }
  }

  // counted down when a request transaction is closed, which can happen on the loop's thread
  private val closedRequests = new ConcurrentHashMap[Transaction, CountDownLatch]()
  // the tables each request transaction asks for, in order
  private val requestedTables = new ConcurrentHashMap[Transaction, Seq[Int]]()

  private def transferRequest(peerId: Long, tableIds: Seq[Int]): Transaction = {
    val tx = mock[Transaction]
    val closed = new CountDownLatch(1)
    closedRequests.put(tx, closed)
    requestedTables.put(tx, tableIds)
    doAnswer { _ =>
      closed.countDown()
      null
    }.when(tx).close()
    when(tx.peerExecutorId()).thenReturn(peerId)
    when(tx.getStatus).thenReturn(TransactionStatus.Success)
    // the message can be taken once, as from a UCX transaction
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(peerId, tableIds))))
      .thenThrow(new NoSuchElementException("None.get"))
    tx
  }

  private def awaitClosed(tx: Transaction): Unit =
    assert(closedRequests.get(tx).await(awaitSeconds, TimeUnit.SECONDS),
      "the server never closed the request transaction")

  /**
   * What the requester got: "none", "sent" for a completed request's transfer response, or an
   * error reply, with each table the server will not send shown by its place in the request.
   */
  private def reply(tx: Transaction): String = {
    val replies = new ArrayBuffer[String]()
    val it = mockingDetails(tx).getInvocations.iterator()
    while (it.hasNext) {
      val invocation = it.next()
      if (invocation.getMethod.getName == "respond") {
        val response = ShuffleMetadata.getTransferResponse(invocation.getArgument[ByteBuffer](0))
        val error = response.error()
        replies += (if (error == null) {
          "sent"
        } else {
          val requested = requestedTables.get(tx)
          val failed = (0 until response.responsesLength()).map { i =>
            val failedTable = response.responses(i)
            val place = requested.indexOf(failedTable.bufferId()) + 1
            if (failedTable.state() == TransferState.FAILED) s"$place" else s"$place not failed"
          }
          s"error for ${failed.mkString(",")} of ${requested.size}: ${error.reason()}"
        })
      }
    }
    if (replies.isEmpty) "none" else replies.mkString("; ")
  }

  private val goneReply = RapidsShuffleServer.failureReason(BufferSendState.MissingOutput)
  private val failedReply = RapidsShuffleServer.failureReason(BufferSendState.Failed)

  private def txCloses(tx: Transaction): Int = calls(tx, "close")

  private def ended(state: BufferSendState): String =
    state.endedWith.map(o => s"ended: $o").getOrElse("live")

  // how many times each bounce buffer the suite made was returned; a second return shows as 2
  private val bounceReturns = new ConcurrentHashMap[SendBounceBuffers, AtomicInteger]()

  /** A send bounce buffer that counts its returns. */
  private def countedBounceBuffer(size: Long): SendBounceBuffers = {
    val device = DeviceMemoryBuffer.allocate(size)
    val returns = new AtomicInteger()
    val bounce = SendBounceBuffers(new BounceBuffer(device) {
      override def free(bb: BounceBuffer): Unit = device.close()
      override def close(): Unit = {
        returns.incrementAndGet()
        super.close()
      }
    }, None)
    bounceReturns.put(bounce, returns)
    bounce
  }

  private def bounceState(bounce: SendBounceBuffers): String = bounceReturns.get(bounce).get match {
    case 0 => "not returned"
    case 1 => "returned"
    case n => s"returned $n times"
  }

  private def sendBounceBuffer(size: Long, cleanups: ArrayBuffer[() => Unit]): SendBounceBuffers = {
    val bounce = countedBounceBuffer(size)
    cleanups += (() => if (!bounce.deviceBounceBuffer.isClosed) bounce.close())
    bounce
  }

  private def newServer(handler: RapidsShuffleRequestHandler, peer: Peer): RapidsShuffleServer = {
    val server = new RapidsShuffleServer(mockTransport, peer.connection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "server"), handler,
      new ImmediateExecutor, mockBssExecutor, mockConf)
    server.start()
    server
  }

  /** A server whose send loop runs on its own thread, as `UCXShuffleTransport` runs it. */
  private class LoopServer(handler: RapidsShuffleRequestHandler, bounceBufferSize: Long,
      cleanups: ArrayBuffer[() => Unit]) {
    val peer = new Peer(cleanups)
    private val failure = new AtomicReference[Throwable]()
    private val died = new CountDownLatch(1)
    // the production thread factory sets no handler, so an executor's default one gets this
    private val threads = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, "test-shuffle-server-bss")
        thread.setDaemon(true)
        thread.setUncaughtExceptionHandler { (_: Thread, t: Throwable) =>
          failure.set(t)
          died.countDown()
        }
        thread
      }
    }
    // counted down when the loop's Runnable returns normally, as it does after close
    private val exited = new CountDownLatch(1)
    private val bssExec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](), threads) {
      override def afterExecute(r: Runnable, t: Throwable): Unit = if (t == null) {
        exited.countDown()
      }
    }
    private val bounceBuffers = new ConcurrentLinkedQueue[SendBounceBuffers]()
    /** Makes the next request for bounce buffers throw. */
    @volatile var failNextBounceRequest = false
    private val transport = mock[RapidsShuffleTransport]
    when(transport.tryGetSendBounceBuffers(any(), any())).thenAnswer { _ =>
      if (failNextBounceRequest) {
        failNextBounceRequest = false
        throw new IllegalStateException("the bounce buffer pool failed")
      }
      val bounce = countedBounceBuffer(bounceBufferSize)
      bounceBuffers.add(bounce)
      Seq(bounce)
    }
    val server = new RapidsShuffleServer(transport, peer.connection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "server"), handler,
      new ImmediateExecutor, bssExec, mockConf)
    server.start()
    cleanups += { () =>
      server.close()
      bssExec.shutdown()
      bssExec.awaitTermination(awaitSeconds, TimeUnit.SECONDS)
      bounceBuffers.forEach(b => if (!b.deviceBounceBuffer.isClosed) b.close())
    }

    def requestTransfer(tx: Transaction): Unit = peer.request(MessageType.TransferRequest, tx)

    /** Waits until the loop sends data or dies, and returns the send if there was one. */
    def awaitSendOrDeath(): Option[Sent] = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      var sent: Sent = null
      while (sent == null && died.getCount > 0 && System.nanoTime() < deadline) {
        sent = peer.sends.poll(10, TimeUnit.MILLISECONDS)
      }
      Option(sent)
    }

    /** Asks for `tableId` from another peer and reports whether the loop served it. */
    def nextRequestGets(tableId: Int): String = {
      val next = transferRequest(3L, Seq(tableId))
      requestTransfer(next)
      // nothing else polls the request queue once the loop is gone, so there is nothing to wait for
      Option(peer.sends.poll(if (died.getCount == 0) 0 else awaitSeconds, TimeUnit.SECONDS))
        .foreach(peer.complete(_))
      s"${reply(next)}, read by the server: ${calls(next, "releaseMessage") > 0}"
    }

    def loop: String = {
      if (died.getCount == 0) {
        s"exited, pool threads running it: ${bssExec.getActiveCount}: ${describe(failure.get)}"
      } else if (exited.getCount == 0) {
        "exited normally"
      } else {
        "running"
      }
    }

    /** Waits until the loop has returned or died, as it does once the server closed. */
    def awaitExit(): Unit = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      while (exited.getCount > 0 && died.getCount > 0 && System.nanoTime() < deadline) {
        exited.await(10, TimeUnit.MILLISECONDS)
      }
    }

    def bounceBuffersTaken: Int = bounceBuffers.size

    /** How many bounce buffers taken here were never returned, and how many more than once. */
    def bounceBuffersNotReturned: String = {
      var never = 0
      var again = 0
      bounceBuffers.forEach { b =>
        val n = bounceReturns.get(b).get
        if (n == 0) never += 1 else if (n > 1) again += 1
      }
      if (again == 0) s"$never" else s"$never, and $again returned more than once"
    }
  }

  // Send path, one step at a time without the send loop.

  test("T1 send: unregister after the send state looks the buffer up, before its first window") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bounce = sendBounceBuffer(cached.size, cleanups)
      val bss = new BufferSendState(tx, bounce, handler)
      catalog.unregisterShuffle(1)
      outcomes("buffer after unregister", holding(catalog, cached))

      outcomes("server throws", describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      outcomes("send state", ended(bss))
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffer at the end", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "server throws" -> "none",
      "send state" -> "ended: MissingOutput",
      "data sends" -> 0,
      "requester gets" -> s"error for 1 of 1: $goneReply",
      "request transaction closes" -> 1,
      "send bounce buffer" -> "returned",
      "buffer at the end" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T1b send: a co-batched send state of a closed buffer ends in the pass that fails it") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      val goneTx = transferRequest(2L, Seq(gone.tableId))
      val liveTx = transferRequest(3L, Seq(live.tableId))
      val goneBounce = sendBounceBuffer(gone.size, cleanups)
      val goneState = new BufferSendState(goneTx, goneBounce, handler)
      val liveState = new BufferSendState(liveTx, sendBounceBuffer(live.size, cleanups), handler)
      catalog.unregisterShuffle(1)

      outcomes("batch of both throws",
        describe(attempt(server.doHandleTransferRequest(Seq(goneState, liveState)))))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      outcomes("closed buffer's send state", ended(goneState))
      outcomes("closed buffer's bounce buffer", bounceState(goneBounce))
      outcomes("data sends", peer.dataSends)
      peer.sends.forEach(s => outcomes("live data matches", sameBytes(s.bytes, live.bytes)))
      peer.completeAll()
      outcomes("live requester gets", reply(liveTx))
      outcomes("live send state", ended(liveState))
      outcomes("closed buffer's requester gets", reply(goneTx))
      outcomes("closed buffer's request transaction closes", txCloses(goneTx))
      outcomes("sends in flight at the end", server.inFlightCount)
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "batch of both throws" -> "none",
      "states re-queued" -> 0,
      "closed buffer's send state" -> "ended: MissingOutput",
      "closed buffer's bounce buffer" -> "returned",
      "data sends" -> 1,
      "live data matches" -> true,
      "live requester gets" -> "sent",
      "live send state" -> "ended: Completed",
      "closed buffer's requester gets" -> s"error for 1 of 1: $goneReply",
      "closed buffer's request transaction closes" -> 1,
      "sends in flight at the end" -> 0,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "device bytes still allocated" -> 0)
  }

  test("T2 send: unregister between the windows of a two-window send") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      // a bounce buffer the size of one buffer makes one window per buffer
      val bounce = sendBounceBuffer(first.size, cleanups)
      val bss = new BufferSendState(tx, bounce, handler)

      outcomes("first window throws", describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      val window1 = peer.nextSend()
      outcomes("first window matches", sameBytes(window1.bytes, first.bytes))
      peer.complete(window1)
      outcomes("more to send", bss.hasMoreSends)
      catalog.unregisterShuffle(1)
      outcomes("buffers after unregister", holding(catalog, first, second))

      outcomes("second window throws",
        describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      outcomes("send state", ended(bss))
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffers at the end", holding(catalog, first, second))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window throws" -> "none",
      "first window matches" -> true,
      "more to send" -> true,
      "buffers after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "second window throws" -> "none",
      "send state" -> "ended: MissingOutput",
      "data sends" -> 1,
      "requester gets" -> s"error for 2 of 2: $goneReply",
      "request transaction closes" -> 1,
      "send bounce buffer" -> "returned",
      "buffers at the end" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "device bytes still allocated" -> 0)
  }

  test("T2b send: unregister after a window that ends inside the second buffer") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      // the first window holds the first buffer and the start of the second
      val bounce = sendBounceBuffer(first.size + second.size / 2, cleanups)
      val bss = new BufferSendState(tx, bounce, handler)

      outcomes("first window throws", describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      peer.complete(peer.nextSend())
      catalog.unregisterShuffle(1)
      outcomes("second window throws",
        describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      outcomes("send state", ended(bss))
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("send bounce buffer", bounceState(bounce))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window throws" -> "none",
      "second window throws" -> "none",
      "send state" -> "ended: MissingOutput",
      "data sends" -> 1,
      "requester gets" -> s"error for 2 of 2: $goneReply",
      "request transaction closes" -> 1,
      "send bounce buffer" -> "returned",
      "device bytes still allocated" -> 0)
  }

  test("T5 send: unregister after a window materialized the buffer, before its release") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bss = new BufferSendState(tx, sendBounceBuffer(cached.size, cleanups), handler)
      withResource(bss) { _ =>
        withResource(bss.getBufferToSend()) { window =>
          catalog.unregisterShuffle(1)
          outcomes("buffer after unregister", holding(catalog, cached))
          Cuda.DEFAULT_STREAM.sync()
          withResource(HostMemoryBuffer.allocate(window.getLength)) { copy =>
            copy.copyFromDeviceBuffer(window.asInstanceOf[DeviceMemoryBuffer])
            outcomes("window matches", sameBytes(copy, cached.bytes))
          }
          bss.releaseAcquiredToCatalog()
          outcomes("buffer after the window's release", holding(catalog, cached))
        }
        outcomes("more to send", bss.hasMoreSends)
      }
      outcomes("buffer after the send state closes", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=1",
      "window matches" -> true,
      "buffer after the window's release" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "more to send" -> false,
      "buffer after the send state closes" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T7 send: a send state that looked the buffer up and is never driven") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bss = new BufferSendState(tx, sendBounceBuffer(cached.size, cleanups), handler)
      catalog.unregisterShuffle(1)
      outcomes("buffer while the send state is abandoned", holding(catalog, cached))
      bss.close()
      outcomes("buffer once the send state closes", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer while the send state is abandoned" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "buffer once the send state closes" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T8 send: the transport reports a data send as failed, on a first and on a last window") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      // two windows, of which the first fails
      val twoTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      val twoBounce = sendBounceBuffer(first.size, cleanups)
      val two = new BufferSendState(twoTx, twoBounce, handler)
      // one window, which fails
      val oneTx = transferRequest(3L, Seq(first.tableId))
      val oneBounce = sendBounceBuffer(first.size, cleanups)
      val one = new BufferSendState(oneTx, oneBounce, handler)

      server.doHandleTransferRequest(Seq(two, one))
      outcomes("data sends", peer.dataSends)
      outcomes("bounce buffers before the callbacks",
        s"${bounceState(twoBounce)}, ${bounceState(oneBounce)}")
      outcomes("sends in flight before the callbacks", server.inFlightCount)
      peer.completeAll(TransactionStatus.InProgress)
      outcomes("sends in flight after the callbacks", server.inFlightCount)
      outcomes("first window's send state", ended(two))
      outcomes("last window's send state", ended(one))
      outcomes("requesters get", s"${reply(twoTx)}, ${reply(oneTx)}")
      outcomes("request transactions close", s"${txCloses(twoTx)}, ${txCloses(oneTx)}")
      outcomes("bounce buffers after the callbacks",
        s"${bounceState(twoBounce)}, ${bounceState(oneBounce)}")
      outcomes("states re-queued", calls(server, "addToContinueQueue"))

      val nextTx = transferRequest(4L, Seq(second.tableId))
      val next = new BufferSendState(nextTx, sendBounceBuffer(second.size, cleanups), handler)
      server.doHandleTransferRequest(Seq(next))
      peer.completeAll()
      outcomes("next request gets", reply(nextTx))
      outcomes("buffers at the end", holding(catalog, first, second))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "data sends" -> 2,
      "bounce buffers before the callbacks" -> "not returned, not returned",
      "sends in flight before the callbacks" -> 2,
      "sends in flight after the callbacks" -> 0,
      "first window's send state" -> "ended: SendFailed",
      "last window's send state" -> "ended: SendFailed",
      "requesters get" -> "none, none",
      "request transactions close" -> "1, 1",
      "bounce buffers after the callbacks" -> "returned, returned",
      "states re-queued" -> 0,
      "next request gets" -> "sent",
      "buffers at the end" -> "catalog=(2,2,2) spillStore=2 deviceRefs=1,1",
      "device bytes still allocated" -> 0)
  }

  test("T9 send: the server closes while sends' callbacks are pending") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      def newState(peerId: Long, tableIds: Int*): (Transaction, SendBounceBuffers,
          BufferSendState) = {
        val tx = transferRequest(peerId, tableIds)
        val bounce = sendBounceBuffer(first.size, cleanups)
        (tx, bounce, new BufferSendState(tx, bounce, handler))
      }
      val (lastTx, lastBounce, last) = newState(2L, first.tableId)
      val (earlierTx, earlierBounce, earlier) = newState(3L, first.tableId, second.tableId)
      val (failedTx, failedBounce, failed) = newState(4L, second.tableId)
      server.doHandleTransferRequest(Seq(last, earlier, failed))
      // the sends go out in the order of the batch
      val (lastSend, earlierSend, failedSend) = (peer.nextSend(), peer.nextSend(), peer.nextSend())

      server.close()
      outcomes("bounce buffers before the callbacks",
        Seq(lastBounce, earlierBounce, failedBounce).map(bounceState).mkString(", "))
      outcomes("sends in flight before the callbacks", server.inFlightCount)
      peer.complete(lastSend)
      peer.complete(earlierSend)
      peer.complete(failedSend, TransactionStatus.InProgress)
      outcomes("send states", Seq(last, earlier, failed).map(ended).mkString(", "))
      outcomes("requesters get", Seq(lastTx, earlierTx, failedTx).map(reply).mkString(", "))
      outcomes("request transactions close",
        Seq(lastTx, earlierTx, failedTx).map(txCloses).mkString(", "))
      outcomes("bounce buffers after the callbacks",
        Seq(lastBounce, earlierBounce, failedBounce).map(bounceState).mkString(", "))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      outcomes("sends in flight after the callbacks", server.inFlightCount)
      outcomes("data sends", peer.dataSends)
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "bounce buffers before the callbacks" -> "not returned, not returned, not returned",
      "sends in flight before the callbacks" -> 3,
      "sends in flight after the callbacks" -> 0,
      "send states" -> "ended: ServerClosed, ended: ServerClosed, ended: SendFailed",
      "requesters get" -> "none, none, none",
      "request transactions close" -> "1, 1, 1",
      "bounce buffers after the callbacks" -> "returned, returned, returned",
      // the earlier window's callback offers the state, and gets it back from the closed server
      "states re-queued" -> 1,
      "data sends" -> 3,
      "device bytes still allocated" -> 0)
  }

  test("T10 send: the transport closes before a send's callback arrives") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      def newState(peerId: Long): (Transaction, SendBounceBuffers, BufferSendState) = {
        val tx = transferRequest(peerId, Seq(cached.tableId))
        val bounce = sendBounceBuffer(cached.size, cleanups)
        (tx, bounce, new BufferSendState(tx, bounce, handler))
      }

      // the progress thread terminated, so nothing can read the bounce buffer any more
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val (tx, bounce, state) = newState(2L)
      server.doHandleTransferRequest(Seq(state))
      val pending = peer.nextSend()
      server.close()
      outcomes("sends in flight before the sweep", server.inFlightCount)
      server.releaseAbandoned(transportStopped = true)
      outcomes("sends in flight after the sweep", server.inFlightCount)
      outcomes("abandoned send state", ended(state))
      outcomes("abandoned bounce buffer", bounceState(bounce))
      outcomes("abandoned request transaction closes", txCloses(tx))
      peer.complete(pending)
      outcomes("after a late callback", s"${ended(state)}, closes ${txCloses(tx)}, " +
        s"reply ${reply(tx)}")
      val (afterTx, afterBounce, after) = newState(3L)
      server.doHandleTransferRequest(Seq(after))
      outcomes("a send after the sweep", s"${ended(after)}, ${bounceState(afterBounce)}, " +
        s"closes ${txCloses(afterTx)}, data sends ${peer.dataSends}")

      // the progress thread did not terminate, so the transport may still read it
      val stuckPeer = new Peer(cleanups)
      val stuckServer = newServer(handler, stuckPeer)
      val (stuckTx, stuckBounce, stuck) = newState(4L)
      stuckServer.doHandleTransferRequest(Seq(stuck))
      stuckPeer.nextSend()
      stuckServer.close()
      stuckServer.releaseAbandoned(transportStopped = false)
      outcomes("send state the transport may still read",
        s"${ended(stuck)}, ${bounceState(stuckBounce)}, closes ${txCloses(stuckTx)}, " +
          s"in flight ${stuckServer.inFlightCount}")
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "sends in flight before the sweep" -> 1,
      "sends in flight after the sweep" -> 0,
      "abandoned send state" -> "ended: ServerClosed",
      "abandoned bounce buffer" -> "returned",
      "abandoned request transaction closes" -> 1,
      "after a late callback" -> "ended: ServerClosed, closes 1, reply none",
      "a send after the sweep" -> "ended: ServerClosed, returned, closes 1, data sends 1",
      "send state the transport may still read" -> "live, not returned, closes 0, in flight 0",
      "device bytes still allocated" -> 0)
  }

  test("T14 send: a send that throws ends only its own state, and the pass goes on") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val third = cache(catalog, block, 10000, cleanups)
      val fourth = cache(catalog, block, 15000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      // two windows, still going when the other send throws
      val goingTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      val goingBounce = sendBounceBuffer(first.size, cleanups)
      val going = new BufferSendState(goingTx, goingBounce, handler)
      // one window, whose send throws before it queues anything
      val throwingTx = transferRequest(3L, Seq(third.tableId))
      val throwingBounce = sendBounceBuffer(third.size, cleanups)
      val throwing = new BufferSendState(throwingTx, throwingBounce, handler)
      // one window, sent after the one that threw
      val laterTx = transferRequest(4L, Seq(fourth.tableId))
      val laterBounce = sendBounceBuffer(fourth.size, cleanups)
      val later = new BufferSendState(laterTx, laterBounce, handler)
      val firstWindowDelivered = new CountDownLatch(1)
      peer.beforeSend = peerId => if (peerId == 3L) {
        assert(firstWindowDelivered.await(awaitSeconds, TimeUnit.SECONDS))
        throw new IllegalStateException("the send failed before it queued anything")
      }

      val passFailure = new AtomicReference[Throwable]()
      val pass = new Thread(() => passFailure.set(
        attempt(server.doHandleTransferRequest(Seq(going, throwing, later)))), "send-pass")
      pass.setDaemon(true)
      pass.start()
      cleanups += { () =>
        firstWindowDelivered.countDown()
        pass.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      }
      // the first window's callback puts its state back on the queue while the pass still runs
      peer.complete(peer.nextSend())
      firstWindowDelivered.countDown()
      pass.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      outcomes("pass throws", describe(passFailure.get))
      outcomes("throwing send's state", ended(throwing))
      outcomes("throwing send's bounce buffer", bounceState(throwingBounce))
      outcomes("throwing send's request transaction closes", txCloses(throwingTx))
      outcomes("throwing send's requester gets", reply(throwingTx))
      outcomes("co-batched state after the pass", ended(going))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      // only the later state's window is in flight now
      outcomes("sends in flight after the pass", server.inFlightCount)
      val laterWindow = peer.nextSend()
      outcomes("later window matches", sameBytes(laterWindow.bytes, fourth.bytes))
      peer.complete(laterWindow)
      outcomes("later state", s"${ended(later)}, reply ${reply(laterTx)}, " +
        s"closes ${txCloses(laterTx)}, bounce ${bounceState(laterBounce)}")

      // as the loop's next pass does with the re-queued state
      server.doHandleTransferRequest(Seq(going))
      val window2 = peer.nextSend()
      outcomes("second window matches", sameBytes(window2.bytes, second.bytes))
      outcomes("co-batched bounce buffer before its last callback", bounceState(goingBounce))
      peer.complete(window2)
      outcomes("co-batched state", ended(going))
      outcomes("co-batched requester gets", reply(goingTx))
      outcomes("co-batched request transaction closes", txCloses(goingTx))
      outcomes("co-batched bounce buffer", bounceState(goingBounce))
      outcomes("sends in flight at the end", server.inFlightCount)
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "pass throws" -> "none",
      "throwing send's state" -> "ended: Failed",
      "throwing send's bounce buffer" -> "returned",
      "throwing send's request transaction closes" -> 1,
      "throwing send's requester gets" -> s"error for 1 of 1: $failedReply",
      "co-batched state after the pass" -> "live",
      "states re-queued" -> 1,
      "sends in flight after the pass" -> 1,
      "later window matches" -> true,
      "later state" -> "ended: Completed, reply sent, closes 1, bounce returned",
      "second window matches" -> true,
      "co-batched bounce buffer before its last callback" -> "not returned",
      "co-batched state" -> "ended: Completed",
      "co-batched requester gets" -> "sent",
      "co-batched request transaction closes" -> 1,
      "co-batched bounce buffer" -> "returned",
      "sends in flight at the end" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T15 send: a removed buffer and a closed one the catalog still holds, sending nothing") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val held = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      val goneTx = transferRequest(2L, Seq(gone.tableId))
      val goneBounce = sendBounceBuffer(gone.size, cleanups)
      val goneState = new BufferSendState(goneTx, goneBounce, handler)
      val heldTx = transferRequest(3L, Seq(held.tableId))
      val heldBounce = sendBounceBuffer(held.size, cleanups)
      val heldState = new BufferSendState(heldTx, heldBounce, handler)
      catalog.unregisterShuffle(1)
      // closed without the catalog letting go of it, as executor teardown closes handles
      val heldHandle = catalog.getShuffleBufferHandle(held.tableId).spillable
      heldHandle.close()
      outcomes("the catalog's check for the closed buffer",
        catalog.isShuffleBufferRemoved(held.tableId, heldHandle))

      outcomes("pass throws",
        describe(attempt(server.doHandleTransferRequest(Seq(goneState, heldState)))))
      outcomes("send states", s"${ended(goneState)}, ${ended(heldState)}")
      outcomes("request transactions close", s"${txCloses(goneTx)}, ${txCloses(heldTx)}")
      // the pass calls finish on the removed buffer's state a second time, with no second reply
      outcomes("requesters get", s"${reply(goneTx)}, ${reply(heldTx)}")
      outcomes("bounce buffers", s"${bounceState(goneBounce)}, ${bounceState(heldBounce)}")
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      outcomes("data sends", peer.dataSends)
      // the first outcome stays, and a state releases its resources once
      outcomes("a second finish ends the state", goneState.finish(BufferSendState.Failed))
      goneState.close()
      outcomes("after a second finish and a close", s"${ended(goneState)}, " +
        s"closes ${txCloses(goneTx)}, bounce ${bounceState(goneBounce)}")
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "the catalog's check for the closed buffer" -> false,
      "pass throws" -> "none",
      "send states" -> "ended: MissingOutput, ended: Failed",
      "request transactions close" -> "1, 1",
      "requesters get" -> s"error for 1 of 1: $goneReply, error for 1 of 1: $failedReply",
      "bounce buffers" -> "returned, returned",
      "states re-queued" -> 0,
      "data sends" -> 0,
      "a second finish ends the state" -> false,
      "after a second finish and a close" -> "ended: MissingOutput, closes 1, bounce returned",
      "device bytes still allocated" -> 0)
  }

  test("a bounce buffer pool hands out nothing once it is closed") {
    val pool = new BounceBufferManager[DeviceMemoryBuffer]("test", 1024, 2,
      size => DeviceMemoryBuffer.allocate(size))
    pool.acquireBuffersNonBlocking(1).foreach(_.close())
    pool.close()
    assert(pool.acquireBuffersNonBlocking(1).isEmpty)
  }

  // Send path through the send loop, on a thread of its own.

  test("T3 send loop: unregister after the metadata reply, before the transfer request") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), gone.size, cleanups)
      outcomes("a request before the unregister gets", loop.nextRequestGets(live.tableId))

      catalog.unregisterShuffle(1)
      val tx = transferRequest(2L, Seq(gone.tableId))
      loop.requestTransfer(tx)
      awaitClosed(tx)
      outcomes("send loop", loop.loop)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "a request before the unregister gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "requester gets" -> s"error for 1 of 1: $goneReply",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "data sends" -> 2,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T3b send loop: unregister between the lookups of a two-buffer request") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 10000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, first.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      // the end-to-end runs: the request's first lookup succeeds, then its shuffle goes
      handler.pauseAfterLookupOf = Some(first.tableId)
      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(tx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      catalog.unregisterShuffle(1)
      handler.resume.countDown()
      awaitClosed(tx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("send loop", loop.loop)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, first, second, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "send loop" -> "running",
      "requester gets" -> s"error for 1,2 of 2: $goneReply",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "data sends" -> 1,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0,1",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T3c send loop: a lookup that fails for another reason, and a request whose receive " +
      "failed") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val live = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val failing = cache(catalog, ShuffleBlockId(1, 2L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      handler.failLookupOf = Some(failing.tableId)
      val loop = new LoopServer(handler, live.size, cleanups)

      val failedLookupTx = transferRequest(2L, Seq(live.tableId, failing.tableId))
      loop.requestTransfer(failedLookupTx)
      awaitClosed(failedLookupTx)
      // a request whose receive was cancelled arrives with its header but no message
      val cancelledTx = transferRequest(3L, Seq(live.tableId))
      when(cancelledTx.getStatus).thenReturn(TransactionStatus.InProgress)
      when(cancelledTx.releaseMessage()).thenThrow(new NoSuchElementException("None.get"))
      loop.requestTransfer(cancelledTx)
      awaitClosed(cancelledTx)
      outcomes("failed lookup's requester gets", reply(failedLookupTx))
      outcomes("cancelled request",
        s"reply ${reply(cancelledTx)}, closes ${txCloses(cancelledTx)}")
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      notReturned = loop.bounceBuffersNotReturned
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "failed lookup's requester gets" -> s"error for 1,2 of 2: $failedReply",
      "cancelled request" -> "reply none, closes 1",
      "next request gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T1L send loop: unregister while the loop is paused after the lookup") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, gone.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      handler.pauseAfterLookupOf = Some(gone.tableId)
      val tx = transferRequest(2L, Seq(gone.tableId))
      loop.requestTransfer(tx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      catalog.unregisterShuffle(1)
      outcomes("buffer after unregister", holding(catalog, gone))
      handler.resume.countDown()
      awaitClosed(tx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0",
      "requester gets" -> s"error for 1 of 1: $goneReply",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "data sends" -> 1,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T4 send loop: unregister between windows, then the loop continues the send alone") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 10000, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), first.size, cleanups)

      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(tx)
      val window1 = loop.peer.nextSend()
      outcomes("first window matches", sameBytes(window1.bytes, first.bytes))
      catalog.unregisterShuffle(1)
      outcomes("buffers after unregister", holding(catalog, first, second))
      // delivering the first window puts the send state back on the loop's queue
      loop.peer.complete(window1)
      awaitClosed(tx)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, first, second, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window matches" -> true,
      "buffers after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0",
      "requester gets" -> s"error for 2 of 2: $goneReply",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "data sends" -> 2,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0,1",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T11 send loop: a transfer request that arrives after the server closed") {
    val outcomes = new Outcomes
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), cached.size, cleanups)
      loop.server.close()
      val tx = transferRequest(2L, Seq(cached.tableId))
      loop.requestTransfer(tx)
      outcomes("request transaction closes", txCloses(tx))
      outcomes("requester gets", reply(tx))
      outcomes("read by the server", calls(tx, "releaseMessage") > 0)
      outcomes("bounce buffers taken", loop.bounceBuffersTaken)
      loop.awaitExit()
      outcomes("send loop", loop.loop)
    }
    outcomes.check(
      "request transaction closes" -> 1,
      "requester gets" -> "none",
      "read by the server" -> false,
      "bounce buffers taken" -> 0,
      "send loop" -> "exited normally")
  }

  test("T12 send loop: what is still queued when the server closes ends as the loop exits") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val paused = cache(catalog, ShuffleBlockId(1, 2L, 0), 10000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, first.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      val queuedTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(queuedTx)
      val window1 = loop.peer.nextSend()
      // keep the loop busy in another request's lookup while the first state is re-queued
      handler.pauseAfterLookupOf = Some(paused.tableId)
      val pausedTx = transferRequest(3L, Seq(paused.tableId))
      loop.requestTransfer(pausedTx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      loop.peer.complete(window1)
      // a request that arrives while the loop is busy, which it never gets to
      val pendingTx = transferRequest(4L, Seq(first.tableId))
      loop.requestTransfer(pendingTx)
      // the loop is still paused, so close gives up waiting for it
      loop.server.close()
      outcomes("close returned while the loop was paused", handler.resume.getCount == 1)
      handler.resume.countDown()
      // the pass in progress still sends the request it was building
      val pausedSend = loop.awaitSendOrDeath()
      outcomes("the paused request's window is sent", pausedSend.isDefined)
      awaitClosed(queuedTx)
      awaitClosed(pendingTx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("queued requester gets", reply(queuedTx))
      outcomes("queued request transaction closes", txCloses(queuedTx))
      outcomes("pending request", s"reply ${reply(pendingTx)}, closes ${txCloses(pendingTx)}, " +
        s"read by the server: ${calls(pendingTx, "releaseMessage") > 0}")
      pausedSend.foreach(loop.peer.complete(_))
      outcomes("paused requester gets", reply(pausedTx))
      outcomes("paused request transaction closes", txCloses(pausedTx))
      outcomes("data sends", loop.peer.dataSends)
      outcomes("bounce buffers taken", loop.bounceBuffersTaken)
      loop.awaitExit()
      outcomes("send loop", loop.loop)
      notReturned = loop.bounceBuffersNotReturned
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "close returned while the loop was paused" -> true,
      "the paused request's window is sent" -> true,
      "queued requester gets" -> "none",
      "queued request transaction closes" -> 1,
      "pending request" -> "reply none, closes 1, read by the server: false",
      "paused requester gets" -> "none",
      "paused request transaction closes" -> 1,
      "data sends" -> 2,
      "bounce buffers taken" -> 2,
      "send loop" -> "exited normally",
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T13 send loop: a send state queued after it ended is skipped") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, cached.size, cleanups)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bounce = sendBounceBuffer(cached.size, cleanups)
      val state = spy(new BufferSendState(tx, bounce, handler))
      state.finish(BufferSendState.Failed)
      // counted down when the loop checks the state it polled from the queue
      val checked = new CountDownLatch(1)
      doAnswer { invocation =>
        checked.countDown()
        invocation.callRealMethod()
      }.when(state).isTerminal
      loop.server.addToContinueQueue(Seq(state))
      outcomes("the loop checked the ended state", checked.await(awaitSeconds, TimeUnit.SECONDS))
      outcomes("next request gets", loop.nextRequestGets(cached.tableId))
      outcomes("ended state prepared again", calls(state, "getBufferToSend"))
      outcomes("ended state", ended(state))
      outcomes("ended state's request transaction closes", txCloses(tx))
      outcomes("ended state's requester gets", reply(tx))
      outcomes("ended state's bounce buffer", bounceState(bounce))
      outcomes("data sends", loop.peer.dataSends)
      outcomes("send loop", loop.loop)
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "the loop checked the ended state" -> true,
      "next request gets" -> "sent, read by the server: true",
      "ended state prepared again" -> 0,
      "ended state" -> "ended: Failed",
      "ended state's request transaction closes" -> 1,
      "ended state's requester gets" -> "none",
      "ended state's bounce buffer" -> "returned",
      "data sends" -> 1,
      "send loop" -> "running",
      "device bytes still allocated" -> 0)
  }

  test("T16 send loop: a failure while gathering sends ends only what that pass built") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = ""
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val built = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val next = cache(catalog, ShuffleBlockId(1, 2L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, built.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      // pause while the pass builds one request's state, then fail its next bounce buffer request
      handler.pauseAfterLookupOf = Some(built.tableId)
      val builtTx = transferRequest(2L, Seq(built.tableId))
      loop.requestTransfer(builtTx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      val nextTx = transferRequest(3L, Seq(next.tableId))
      loop.requestTransfer(nextTx)
      loop.failNextBounceRequest = true
      handler.resume.countDown()
      awaitClosed(builtTx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("built request", s"reply ${reply(builtTx)}, closes ${txCloses(builtTx)}")
      // the request whose bounce buffer request failed is served in a later pass
      loop.awaitSendOrDeath().foreach { sent =>
        outcomes("next request's data matches", sameBytes(sent.bytes, next.bytes))
        loop.peer.complete(sent)
      }
      outcomes("next requester gets", reply(nextTx))
      outcomes("send loop", loop.loop)
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "built request" -> s"reply error for 1 of 1: $failedReply, closes 1",
      "next request's data matches" -> true,
      "next requester gets" -> "sent",
      "send loop" -> "running",
      "data sends" -> 1,
      "send bounce buffers not returned" -> "0",
      "device bytes still allocated" -> 0)
  }

  test("T17 send loop: a fatal CUDA error still ends the loop, so the executor exits") {
    val outcomes = new Outcomes
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      // its constructors are not public
      val constructor =
        classOf[CudaFatalException].getDeclaredConstructor(classOf[String], classOf[Int])
      constructor.setAccessible(true)
      val fatal = constructor.newInstance("an illegal memory access in test", Int.box(700))
      val broken = mock[SpillableDeviceBufferHandle]
      val meta = catalog.getShuffleBufferHandle(cached.tableId).tableMeta
      when(broken.sizeInBytes).thenReturn(meta.bufferMeta().size())
      when(broken.materialize()).thenThrow(fatal)
      handler.replaced.put(cached.tableId, RapidsShuffleHandle(broken, meta))
      val loop = new LoopServer(handler, cached.size, cleanups)
      val tx = transferRequest(2L, Seq(cached.tableId))
      loop.requestTransfer(tx)
      outcomes("data sent", loop.awaitSendOrDeath().isDefined)
      outcomes("send loop", loop.loop)
      outcomes("request transaction closes", txCloses(tx))
      outcomes("requester gets", reply(tx))
      outcomes("send bounce buffers not returned", loop.bounceBuffersNotReturned)
    }
    outcomes.check(
      "data sent" -> false,
      // the failed preparation ends the loop at once, rather than as a batch with nothing to send
      "send loop" -> ("exited, pool threads running it: 0: RapidsShuffleSendPrepareException: " +
        "Error while copying to bounce buffer for executor 2 and header 0x0000000000000002"),
      "request transaction closes" -> 1,
      "requester gets" -> "none",
      "send bounce buffers not returned" -> "0")
  }
}
