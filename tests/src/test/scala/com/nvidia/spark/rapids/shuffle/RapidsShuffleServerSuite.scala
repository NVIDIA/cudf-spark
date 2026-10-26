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

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, ExecutionException, Executor,
  Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, CudaFatalException, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{LogCaptureUtils, MetaUtils, RapidsShuffleHandle,
  ShuffleBufferCatalog, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.shuffle.ucx.{UCX, UCXActiveMessage, UCXServerConnection,
  UCXShuffleTransport}
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockedConstruction}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.openucx.jucx.UcxCallback

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.rapids.RapidsShuffleSendPrepareException
import org.apache.spark.sql.rapids.RapidsShuffleInternalManagerBase
import org.apache.spark.storage.ShuffleBlockBatchId

class MockRapidsShuffleRequestHandler(mockBuffers: Seq[RapidsShuffleHandle])
    extends RapidsShuffleRequestHandler with AutoCloseable {
  var acquiredTables = Seq[Int]()
  override def getShuffleBufferMetas(
      shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta] = {
    throw new NotImplementedError("getShuffleBufferMetas")
  }

  override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
    acquiredTables = acquiredTables :+ tableId
    mockBuffers(tableId)
  }

  override def close(): Unit = {
    // a removeShuffle action would likewise remove handles
    mockBuffers.foreach(_.close())
  }
}

class RapidsShuffleServerSuite extends RapidsShuffleTestHelper {

  def setupMocks(deviceBuffers: Seq[DeviceMemoryBuffer]): MockRapidsShuffleRequestHandler = {
    val mockBuffers = deviceBuffers.map { deviceBuffer =>
      withResource(HostMemoryBuffer.allocate(deviceBuffer.getLength)) { hostBuff =>
        fillBuffer(hostBuff)
        deviceBuffer.copyFromHostBuffer(hostBuff)
        val mockMeta = RapidsShuffleTestHelper.mockTableMeta(100000)
        RapidsShuffleHandle(SpillableDeviceBufferHandle(deviceBuffer), mockMeta)
      }
    }
    new MockRapidsShuffleRequestHandler(mockBuffers)
  }

  class MockBlockWithSize(override val size: Long) extends BlockWithSize {}

  def compareRanges(
      bounceBuffer: SendBounceBuffers,
      receiveBlocks: Seq[(BlockRange[MockBlockWithSize], DeviceMemoryBuffer)]): Unit = {
    var bounceBuffOffset = 0L
    receiveBlocks.foreach { case (range, deviceBuff) =>
      val deviceBounceBuff = bounceBuffer.deviceBounceBuffer.buffer
      withResource(deviceBounceBuff.slice(bounceBuffOffset, range.rangeSize())) { bbSlice =>
        bounceBuffOffset = bounceBuffOffset + range.rangeSize()
        withResource(HostMemoryBuffer.allocate(bbSlice.getLength)) { hostCopy =>
          hostCopy.copyFromDeviceBuffer(bbSlice.asInstanceOf[DeviceMemoryBuffer])
          withResource(deviceBuff.slice(range.rangeStart, range.rangeSize())) { origSlice =>
            assert(areBuffersEqual(hostCopy, origSlice))
          }
        }
      }
    }
  }

  /** Mocked send bounce buffers of 1024 bytes whose slices are `slice`. */
  private def makeSendBuffer(slice: DeviceMemoryBuffer): SendBounceBuffers = {
    val sendBuffer = mock[SendBounceBuffers]
    val bounceBuffer = mock[BounceBuffer]
    val bounceMemory = mock[DeviceMemoryBuffer]
    when(bounceBuffer.buffer).thenReturn(bounceMemory)
    when(bounceMemory.getLength).thenReturn(1024L)
    when(bounceMemory.slice(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong()))
      .thenReturn(slice)
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(bounceBuffer)
    sendBuffer
  }

  /** A transfer request for one table. */
  private def makeTx(tableId: Int): Transaction = makeTx(Seq(tableId))

  /** A transfer request for several tables. */
  private def makeTx(tableIds: Seq[Int]): Transaction = {
    val tx = mock[Transaction]
    // the message can be taken once, as from a UCX transaction
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, tableIds))))
      .thenThrow(new NoSuchElementException("None.get"))
    tx
  }

  /** A handle whose materialization answers with `materialize`. */
  private def makeHandle(tableMeta: TableMeta, materialize: () => DeviceMemoryBuffer)
      : RapidsShuffleHandle = {
    val handle = mock[SpillableDeviceBufferHandle]
    when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
    when(handle.materialize()).thenAnswer(_ => materialize())
    RapidsShuffleHandle(handle, tableMeta)
  }

  /**
   * Runs `body` with a request handler that serves tables `tableIds`, whose buffers materialize
   * with `materialize`.
   */
  private def withTables(tableIds: Int*)(materialize: () => DeviceMemoryBuffer)(
      body: RapidsShuffleRequestHandler => Unit): Unit =
    withRequestHandler(tableIds, materialize, removed = false, body)

  /**
   * Runs `body` with a request handler whose tables `tableIds` are gone. Each fails to
   * materialize as a deleted spill file does and is reported removed, so that its request ends
   * with an error reply.
   */
  private def withGoneTables(tableIds: Int*)(body: RapidsShuffleRequestHandler => Unit): Unit =
    withRequestHandler(tableIds,
      () => throw new FileNotFoundException("spill file deleted in test"), removed = true, body)

  /** As `withTables`, and with `removed`, `isShuffleBufferRemoved` is true for each table. */
  private def withRequestHandler(
      tableIds: Seq[Int],
      materialize: () => DeviceMemoryBuffer,
      removed: Boolean,
      body: RapidsShuffleRequestHandler => Unit): Unit = {
    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      tableIds.foreach { id =>
        val handle = makeHandle(tableMeta, materialize)
        when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(id))).thenReturn(handle)
        if (removed) {
          when(requestHandler.isShuffleBufferRemoved(ArgumentMatchers.eq(id),
            ArgumentMatchers.eq(handle))).thenReturn(true)
        }
      }
      body(requestHandler)
    }
  }

  /** Answers with the buffer the real method builds, spied, after recording it in `built`. */
  private def recordBuilt(
      built: ArrayBuffer[RefCountedDirectByteBuffer]): Answer[RefCountedDirectByteBuffer] =
    new Answer[RefCountedDirectByteBuffer] {
      override def answer(invocation: InvocationOnMock): RefCountedDirectByteBuffer = {
        val buffer = spy(invocation.callRealMethod().asInstanceOf[RefCountedDirectByteBuffer])
        built.synchronized(built += buffer)
        buffer
      }
    }

  /**
   * Mockito answers the mocked send-loop executor's `wait()` at once, without releasing its
   * monitor, so a close that waits for a reply being queued would hang. A test that needs that
   * wait builds its own server on a real executor, as `withGoneStateOnSendLoop` does. It does not
   * spy that server, since a spy's close would not stop the send loop, which runs on the original.
   */
  private def newServer(requestHandler: RapidsShuffleRequestHandler,
      serverConnection: ServerConnection): RapidsShuffleServer = {
    spy(new RapidsShuffleServer(
      mockTransport,
      serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
      requestHandler,
      mockExecutor,
      mockBssExecutor,
      mockConf))
  }

  test("sending tables that fit within one bounce buffer") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(10, 1000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 10).map(_ => DeviceMemoryBuffer.allocate(1000))
      val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(1000))
      withResource(setupMocks(deviceBuffers)) { handler =>
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          assert(bss.hasMoreSends)
          withResource(bss.getBufferToSend()) { mb =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assertResult(10000)(mb.getLength)
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("sending tables that require two bounce buffer lengths") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(20, 1000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 20).map(_ => DeviceMemoryBuffer.allocate(1000))
      val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(1000))
      val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
      withResource(setupMocks(deviceBuffers)) { handler =>
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assert(bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }

          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("sending buffers larger than bounce buffer") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(20, 10000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 20).map(_ => DeviceMemoryBuffer.allocate(123000))
      withResource(setupMocks(deviceBuffers)) { handler =>
        val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(123000))
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          (0 until 246).foreach { _ =>
            withResource(bss.getBufferToSend()) { _ =>
              val receiveBlocks = receiveWindow.next()
              compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
              bss.releaseAcquiredToCatalog()
            }
          }
          assert(!bss.hasMoreSends)
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("when a send fails, we un-acquire buffers that are currently being sent") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    withResource(DeviceMemoryBuffer.allocate(123)) { buff =>
      when(mockDeviceBounceBuffer.buffer).thenReturn(buff)
      when(mockSendBuffer.bounceBufferSize).thenReturn(buff.getLength)
      when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
      when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

      when(mockTransport.tryGetSendBounceBuffers(any(), any()))
        .thenReturn(Seq(mockSendBuffer))

      val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1))
      when(mockTransaction.getStatus)
        .thenReturn(TransactionStatus.Success)
        .thenReturn(TransactionStatus.Error)
      when(mockTransaction.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

      val mockServerConnection = mock[ServerConnection]
      val ac = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(mockServerConnection.send(
        any(), any(), any(), any[MemoryBuffer](), ac.capture())).thenReturn(mockTransaction)

      val mockRequestHandler = mock[RapidsShuffleRequestHandler]

      val bb = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
        val testHandle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(456))
        val rapidsBuffer = RapidsShuffleHandle(testHandle, tableMeta)
        when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
          .thenReturn(rapidsBuffer)

        val server = new RapidsShuffleServer(
          mockTransport,
          mockServerConnection,
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
          mockRequestHandler,
          mockExecutor,
          mockBssExecutor,
          mockConf)

        server.start()

        val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler)
        server.doHandleTransferRequest(Seq(bss))
        val cb = ac.getValue.asInstanceOf[TransactionCallback]
        cb(mockTransaction)
        server.doHandleTransferRequest(Seq(bss)) //Error
        cb(mockTransaction)
        // bounce buffers are freed
        verify(mockSendBuffer, times(1)).close()
        // acquire once at the beginning, and closed at the end
        verify(mockRequestHandler, times(1))
          .getShuffleHandle(ArgumentMatchers.eq(1))
        withResource(rapidsBuffer.spillable.materialize()) { dmb =>
          // refcount=2 because it was on the device, and we +1 to materialize.
          // but it shows no leaks.
          assertResult(2)(dmb.getRefCount)
        }
      }
    }
  }

  test("when we fail to prepare a send, end the request if nothing can be handled") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    val mockDeviceMemoryBuffer = mock[DeviceMemoryBuffer]
    when(mockDeviceBounceBuffer.buffer).thenReturn(mockDeviceMemoryBuffer)
    when(mockSendBuffer.bounceBufferSize).thenReturn(1024)
    when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
    when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

    when(mockTransport.tryGetSendBounceBuffers(any(), any()))
      .thenReturn(Seq(mockSendBuffer))

    val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1, 2))
    when(mockTransaction.getStatus)
      .thenReturn(TransactionStatus.Success)
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

    val mockServerConnection = mock[ServerConnection]
    val mockRequestHandler = mock[RapidsShuffleRequestHandler]

    val bb = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
      val mockHandle = mock[SpillableDeviceBufferHandle]
      val mockHandleThatThrows = mock[SpillableDeviceBufferHandle]
      val mockMaterialized = mock[DeviceMemoryBuffer]
      when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(mockHandle.materialize()).thenAnswer(_ => mockMaterialized)

      when(mockHandleThatThrows.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      val ex = new IllegalStateException("something happened")
      when(mockHandleThatThrows.materialize()).thenThrow(ex)

      val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
      val rapidsBufferThatThrows = RapidsShuffleHandle(mockHandleThatThrows, tableMeta)

      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(rapidsBuffer)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(rapidsBufferThatThrows)

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mockServerConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        mockRequestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      server.start()

      val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null)
      // if nothing else can be handled, the request ends rather than the send loop
      server.doHandleTransferRequest(Seq(bss))
      assertResult(Some(BufferSendState.Failed))(bss.endedWith)

      // since nothing could be handled, we don't try again
      verify(server, times(0)).addToContinueQueue(any())

      // bounce buffers are freed, and the request transaction is closed, once
      verify(mockSendBuffer, times(1)).close()
      verify(mockTransaction, times(1)).close()

      verify(mockRequestHandler, times(1))
        .getShuffleHandle(ArgumentMatchers.eq(1))

      // the spillable that materialized we need to close
      verify(mockMaterialized, times(1)).close()
    }
  }

  test("when GPU OOM prevents all sends, stop the request when its OOM retries end") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    val mockDeviceMemoryBuffer = mock[DeviceMemoryBuffer]
    val mockServerConnection = mock[ServerConnection]
    when(mockDeviceBounceBuffer.buffer).thenReturn(mockDeviceMemoryBuffer)
    when(mockSendBuffer.bounceBufferSize).thenReturn(1024)
    when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
    when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

    val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1, 2))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

    val mockRequestHandler = mock[RapidsShuffleRequestHandler]
    val bb = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
      val mockHandle = mock[SpillableDeviceBufferHandle]
      val mockHandleThatThrows = mock[SpillableDeviceBufferHandle]
      val mockMaterialized = mock[DeviceMemoryBuffer]
      when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(mockHandle.materialize()).thenReturn(mockMaterialized)
      when(mockHandleThatThrows.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      val oom = new OutOfMemoryError("GPU allocation failed in test")
      when(mockHandleThatThrows.materialize()).thenThrow(oom)
      val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
      val rapidsBufferThatThrows = RapidsShuffleHandle(mockHandleThatThrows, tableMeta)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(rapidsBuffer)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(rapidsBufferThatThrows)

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mockServerConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        mockRequestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 1L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null,
        requestArrivalNanos = 0L)
      assertResult(10L)(server.oomRetryBackoffMillis(1))
      assertResult(20L)(server.oomRetryBackoffMillis(2))
      assertResult(100L)(server.oomRetryBackoffMillis(5))
      assertResult(100L)(server.oomRetryBackoffMillis(1000))
      server.doHandleTransferRequest(Seq(bss))

      verify(server, times(1)).addToContinueQueue(Seq(bss))
      verify(server, times(1)).waitBeforeOomRetry(10L)
      verify(mockSendBuffer, times(0)).close()

      nowNanos = 1L
      server.doHandleTransferRequest(Seq(bss))
      verify(server, times(1)).addToContinueQueue(Seq(bss))
      verify(server, times(1)).waitBeforeOomRetry(10L)
      verify(mockHandle, times(2)).materialize()
      verify(mockHandleThatThrows, times(2)).materialize()
      verify(mockMaterialized, times(2)).close()
      verify(mockSendBuffer, times(1)).close()
      assertResult(Some(BufferSendState.OomExpired))(bss.endedWith)
      verify(mockTransaction, times(1)).close()
      val reply = ArgumentCaptor.forClass(classOf[ByteBuffer])
      verify(mockTransaction, times(1)).respond(reply.capture(), any())
      assertResult(RapidsShuffleServer.failureReason(BufferSendState.OomExpired))(
        ShuffleMetadata.getTransferResponse(reply.getValue).error().reason())
    }
  }

  test("successful preparation restarts the OOM backoff and keeps the request's retry deadline") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    val deviceMemoryBuffer = mock[DeviceMemoryBuffer]
    val bufferSlice = mock[DeviceMemoryBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(deviceMemoryBuffer)
    when(deviceMemoryBuffer.getLength).thenReturn(1024L)
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)
    when(deviceMemoryBuffer.slice(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong()))
      .thenReturn(bufferSlice)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = mock[SpillableDeviceBufferHandle]
      val materialized = mock[DeviceMemoryBuffer]
      when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(handle.materialize()).thenReturn(materialized)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(handle, tableMeta))

      withResource(new BufferSendState(
        mockTransaction, sendBuffer, requestHandler, null, requestArrivalNanos = 0L)) { bss =>
        assertResult(Some(1))(bss.recordOomAndGetRetryAttempt(5L, 10L))
        assertResult(Some(2))(bss.recordOomAndGetRetryAttempt(6L, 10L))
        withResource(bss.getBufferToSend()) { _ => }

        // A new OOM episode backs off from its first attempt again. If resetOomRetryAttempts
        // were a no-op or its successful preparation call site were removed, this would be the
        // third attempt.
        assertResult(Some(1))(bss.recordOomAndGetRetryAttempt(9L, 10L))
        // The request's retries still end at the deadline counted from its arrival, since a
        // reader that started waiting before the request arrived saw none of this progress.
        assertResult(None)(bss.recordOomAndGetRetryAttempt(10L, 10L))
      }
      // a reader's wait less 10 seconds, or less half of it if it is shorter than 20 seconds
      assertResult(TimeUnit.SECONDS.toNanos(110))(
        RapidsShuffleServer.oomRetryNanos(TimeUnit.SECONDS.toNanos(120)))
      assertResult(TimeUnit.SECONDS.toNanos(10))(
        RapidsShuffleServer.oomRetryNanos(TimeUnit.SECONDS.toNanos(20)))
      assertResult(TimeUnit.SECONDS.toNanos(5))(
        RapidsShuffleServer.oomRetryNanos(TimeUnit.SECONDS.toNanos(10)))
    }
  }

  test("an expired OOM window does not close a co-batched send with a fresh window") {
    def makeSendBuffer(name: String): SendBounceBuffers = {
      val sb = mock[SendBounceBuffers](name)
      val dbb = mock[BounceBuffer]
      when(dbb.buffer).thenReturn(mock[DeviceMemoryBuffer])
      when(sb.bounceBufferSize).thenReturn(1024)
      when(sb.hostBounceBuffer).thenReturn(None)
      when(sb.deviceBounceBuffer).thenReturn(dbb)
      sb
    }
    val sendBufferA = makeSendBuffer("sendBufferA")
    val sendBufferB = makeSendBuffer("sendBufferB")

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val oomHandle = mock[SpillableDeviceBufferHandle]
      when(oomHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(oomHandle.materialize())
        .thenThrow(new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(oomHandle, tableMeta))

      def makeTx(peerExecutorId: Long): Transaction = {
        val tx = mock[Transaction]
        when(tx.peerExecutorId()).thenReturn(peerExecutorId)
        when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
          new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, Seq(1)))))
        tx
      }

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mock[ServerConnection],
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 10L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val bssA = new BufferSendState(makeTx(1L), sendBufferA, requestHandler, null,
        requestArrivalNanos = 0L)
      server.doHandleTransferRequest(Seq(bssA))
      verify(server, times(1)).addToContinueQueue(Seq(bssA))

      nowNanos = 10L
      val bssB = new BufferSendState(makeTx(2L), sendBufferB, requestHandler, null,
        requestArrivalNanos = 10L)
      server.doHandleTransferRequest(Seq(bssA, bssB))

      verify(sendBufferA, times(1)).close()
      verify(sendBufferB, times(0)).close()
      verify(server, times(1)).addToContinueQueue(Seq(bssB))
      assertResult(Some(BufferSendState.OomExpired))(bssA.endedWith)
      assertResult(None)(bssB.endedWith)
    }
  }

  test("progress by a co-batched send does not reset another send state's OOM window") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(mock[DeviceMemoryBuffer])
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val oomHandle = mock[SpillableDeviceBufferHandle]
      when(oomHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(oomHandle.materialize())
        .thenThrow(new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(oomHandle, tableMeta))

      val successfulState = mock[BufferSendState]
      val successfulBuffer = mock[MemoryBuffer]
      when(successfulState.hasMoreSends).thenReturn(true)
      when(successfulState.getBufferToSend()).thenReturn(successfulBuffer)

      val serverConnection = mock[ServerConnection]
      when(serverConnection.send(
        any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]()))
        .thenReturn(mock[Transaction])

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        serverConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 10L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val oomState = new BufferSendState(
        mockTransaction, sendBuffer, requestHandler, null, requestArrivalNanos = 0L)
      server.doHandleTransferRequest(Seq(oomState))
      verify(server, times(1)).addToContinueQueue(Seq(oomState))

      nowNanos = 10L
      server.doHandleTransferRequest(Seq(oomState, successfulState))

      verify(sendBuffer, times(1)).close()
      verify(server, times(1)).addToContinueQueue(any())
    }
  }

  test("when we fail to prepare a send, re-queue the request if anything can be handled") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    withResource(DeviceMemoryBuffer.allocate(123)) { buff =>
      when(mockDeviceBounceBuffer.buffer).thenReturn(buff)
      when(mockSendBuffer.bounceBufferSize).thenReturn(buff.getLength)
      when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
      when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

      when(mockTransport.tryGetSendBounceBuffers(any(), any()))
          .thenReturn(Seq(mockSendBuffer))

      val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1))
      when(mockTransaction.getStatus)
          .thenReturn(TransactionStatus.Success)
      when(mockTransaction.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

      val tr2 = ShuffleMetadata.buildTransferRequest(0, Seq(2))
      val mockTransaction2 = mock[Transaction]
      when(mockTransaction2.getStatus)
          .thenReturn(TransactionStatus.Success)
      when(mockTransaction2.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr2)))

      val mockServerConnection = mock[ServerConnection]
      val ac = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(mockServerConnection.send(
        any(), any(), any(), any[MemoryBuffer](), ac.capture())).thenReturn(mockTransaction)

      val mockRequestHandler = mock[RapidsShuffleRequestHandler]

      def makeMockBuffer(tableId: Int, bb: ByteBuffer, error: Boolean): RapidsShuffleHandle  = {
        val tableMeta = MetaUtils.buildTableMeta(tableId, 456, bb, 100)
        val rapidsBuffer = if (error) {
          val mockHandle = mock[SpillableDeviceBufferHandle]
          val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
          when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
          // mock an error with the copy
          when(rapidsBuffer.spillable.materialize())
            .thenAnswer(_ => {
              throw new IOException("mmap failed in test")
            })
          rapidsBuffer
        } else {
          val testHandle = spy(SpillableDeviceBufferHandle(spy(DeviceMemoryBuffer.allocate(456))))
          RapidsShuffleHandle(testHandle, tableMeta)
        }
        when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(tableId)))
          .thenAnswer(_ => rapidsBuffer)
        rapidsBuffer
      }

      val bb = ByteBuffer.allocateDirect(123)
      val bb2 = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        withResource(new RefCountedDirectByteBuffer(bb2)) { _ =>
          val rapidsHandle = makeMockBuffer(1, bb, error = true)
          val rapidsHandle2 = makeMockBuffer(2, bb2, error = false)

          val server = spy(new RapidsShuffleServer(
            mockTransport,
            mockServerConnection,
            RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
            mockRequestHandler,
            mockExecutor,
            mockBssExecutor,
            mockConf))

          server.start()

          val bssFailed = new BufferSendState(
            mockTransaction, mockSendBuffer, mockRequestHandler)

          val bssSuccess = spy(new BufferSendState(
            mockTransaction2, mockSendBuffer, mockRequestHandler))

          var callCount = 0
          doAnswer { _ =>
            callCount += 1
            // send 1 buffer length
            if (callCount > 1){
              false
            } else {
              true
            }
          }.when(bssSuccess).hasMoreSends

          // if something else can be handled we don't throw, and re-queue
          server.doHandleTransferRequest(Seq(bssFailed, bssSuccess))

          val cb = ac.getValue.asInstanceOf[TransactionCallback]
          cb(mockTransaction)

          verify(server, times(1)).addToContinueQueue(any())

          // the bounce buffer is freed 1 time for `bssSuccess`, but not for `bssFailed`
          verify(mockSendBuffer, times(1)).close()

          // we obtained the handles once, we don't need to get them again
          verify(mockRequestHandler, times(1))
              .getShuffleHandle(ArgumentMatchers.eq(1))
          verify(mockRequestHandler, times(1))
              .getShuffleHandle(ArgumentMatchers.eq(2))
          // this handle fails to materialize
          verify(rapidsHandle.spillable, times(1)).materialize()

          // this handle materializes, so make sure we close it
          verify(rapidsHandle2.spillable, times(1)).materialize()
          withResource(rapidsHandle2.spillable.materialize()) { dmb =>
            // refcount=2 because it was on the device, and we +1 to materialize.
            // but it shows no leaks.
            assertResult(2)(dmb.getRefCount)
          }
        }
      }
    }
  }

  test("when a buffer is gone from the catalog, end its request instead of retrying it") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(mock[DeviceMemoryBuffer])
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = mock[SpillableDeviceBufferHandle]
      when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      // a disk-spilled buffer whose spill file the unregister deleted
      when(handle.materialize()).thenAnswer(_ => {
        throw new FileNotFoundException("spill file deleted in test")
      })
      val rapidsHandle = RapidsShuffleHandle(handle, tableMeta)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(rapidsHandle)
      when(requestHandler.isShuffleBufferRemoved(ArgumentMatchers.eq(1),
        ArgumentMatchers.eq(rapidsHandle))).thenReturn(true)

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mock[ServerConnection],
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      val bss = new BufferSendState(mockTransaction, sendBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(bss))

      assertResult(Some(BufferSendState.MissingOutput))(bss.endedWith)
      verify(server, times(0)).addToContinueQueue(any())
      verify(sendBuffer, times(1)).close()
      verify(mockTransaction, times(1)).close()
    }
  }

  test("a slice that fails to close after its send leaves its state to the send's callback") {
    // the slice of the state that is sent fails to close once its send returned
    val sentSlice = mock[DeviceMemoryBuffer]
    doThrow(new IllegalStateException("slice close failed in test")).when(sentSlice).close()
    val sentBuffer = makeSendBuffer(sentSlice)
    val retriedBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val sentTx = makeTx(1)
    val retriedTx = makeTx(2)

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val sentHandle = mock[SpillableDeviceBufferHandle]
      when(sentHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(sentHandle.materialize()).thenReturn(mock[DeviceMemoryBuffer])
      // fails to prepare for a reason that is not classified, so it would be retried
      val retriedHandle = mock[SpillableDeviceBufferHandle]
      when(retriedHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(retriedHandle.materialize()).thenAnswer(_ => {
        throw new IOException("mmap failed in test")
      })
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(sentHandle, tableMeta))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(RapidsShuffleHandle(retriedHandle, tableMeta))

      val serverConnection = mock[ServerConnection]
      val callback = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(serverConnection.send(
        any(), any(), any(), any[MemoryBuffer](), callback.capture())).thenReturn(mock[Transaction])

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        serverConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      val sent = new BufferSendState(sentTx, sentBuffer, requestHandler, null)
      val retried = new BufferSendState(retriedTx, retriedBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(sent, retried))

      // the failure came after the send returned, so the transport may still read the buffer
      assertResult(None)(sent.endedWith)
      verify(sentBuffer, times(0)).close()
      assertResult(1)(server.inFlightCount)
      // the state that had not been sent ends in that pass, and is never queued to retry
      assertResult(Some(BufferSendState.Failed))(retried.endedWith)
      verify(retriedBuffer, times(1)).close()
      verify(retriedTx, times(1)).close()
      verify(server, times(0)).addToContinueQueue(any())

      val done = mock[Transaction]
      when(done.getStatus).thenReturn(TransactionStatus.Success)
      callback.getValue.asInstanceOf[TransactionCallback](done)
      assertResult(Some(BufferSendState.Completed))(sent.endedWith)
      verify(sentTx, times(1)).respond(any(), any())
      verify(sentBuffer, times(1)).close()
      verify(sentTx, times(1)).close()
      assertResult(0)(server.inFlightCount)
    }
  }

  test("a fatal error from a send's callback stays in the UCX callback wrapper, logged once") {
    // A callback's releases make no CUDA call, so the error is injected through the request's
    // close. The send goes through the real UCX server connection and transaction: only UCX is
    // mocked, to capture the completion callback it would call on its progress thread.
    val fatal = cudaFatal()
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val requestTx = makeTx(1)
    doThrow(fatal).when(requestTx).close()

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = makeHandle(tableMeta, () => mock[DeviceMemoryBuffer])
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(handle)
      val ucx = mock[UCX]
      val connection = spy(new UCXServerConnection(ucx, mock[UCXShuffleTransport]))
      var sendTx: Transaction = null
      doAnswer(new Answer[Transaction] {
        override def answer(invocation: InvocationOnMock): Transaction = {
          sendTx = invocation.callRealMethod().asInstanceOf[Transaction]
          sendTx
        }
      }).when(connection).send(anyLong(), any[MessageType.Value](), anyLong(),
        any[MemoryBuffer](), any[TransactionCallback]())
      val server = newServer(requestHandler, connection)

      val state = new BufferSendState(requestTx, sendBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(state))
      val completion = ArgumentCaptor.forClass(classOf[UcxCallback])
      verify(ucx, times(1)).sendActiveMessage(
        anyLong(), any[UCXActiveMessage](), any[MemoryBuffer](), completion.capture())
      val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[BufferSendState].getName)) {
        // returns normally: the error does not reach the progress thread's own catch
        completion.getValue.onSuccess(null)
      }

      assertResult(TransactionStatus.Error)(sendTx.getStatus)
      // the state still ended once, and released what it held
      assertResult(Some(BufferSendState.Completed))(state.endedWith)
      verify(requestTx, times(1)).respond(any(), any())
      verify(sendBuffer, times(1)).close()
      verify(requestTx, times(1)).close()
      assertResult(0)(server.inFlightCount)
      // logged once, by the wrapper that contains it
      assertResult(1)(logs.count(_.startsWith("Detected an exception from user code")))
      assertResult(0)(logs.count(_.startsWith(releaseErrorLog)))
    }
  }

  test("a data send whose send task throws ends its state, which leaves the in-flight set") {
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val requestTx = makeTx(1)
    withTables(1)(() => mock[DeviceMemoryBuffer]) { requestHandler =>
      // the real UCX server connection and transaction, with UCX mocked to capture the callback
      // that the send's task gets
      val ucx = mock[UCX]
      val server = newServer(requestHandler,
        new UCXServerConnection(ucx, mock[UCXShuffleTransport]))
      val state = new BufferSendState(requestTx, sendBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(state))
      val send = ArgumentCaptor.forClass(classOf[UcxCallback])
      verify(ucx, times(1)).sendActiveMessage(
        anyLong(), any[UCXActiveMessage](), any[MemoryBuffer](), send.capture())
      assertResult(1)(server.inFlightCount)
      assertResult(None)(state.endedWith)

      // the task, on the progress thread, cannot allocate the send's header
      val oom = new OutOfMemoryError("Direct buffer memory")
      intercept[OutOfMemoryError](UCX.sendOrReport(send.getValue)(_ => throw oom))

      assertResult(Some(BufferSendState.SendFailed))(state.endedWith)
      assertResult(0)(server.inFlightCount)
      verify(sendBuffer, times(1)).close()
      verify(requestTx, times(1)).close()
      // a failed data send gets no reply
      verify(requestTx, never()).respond(any(), any())
    }
  }

  test("an error reply whose send task throws is released once") {
    val ucx = mock[UCX]
    val connection = new UCXServerConnection(ucx, mock[UCXShuffleTransport])
    val requestTx = makeTx(1)
    doAnswer(new Answer[Transaction] {
      override def answer(invocation: InvocationOnMock): Transaction =
        connection.send(2L, MessageType.TransferRequest, 0L,
          invocation.getArgument[ByteBuffer](0), invocation.getArgument[TransactionCallback](1))
    }).when(requestTx).respond(any(), any())

    withGoneTables(1) { requestHandler =>
      val server = newServer(requestHandler, mock[ServerConnection])
      val replies = new ArrayBuffer[RefCountedDirectByteBuffer]()
      val state = spy(new BufferSendState(
        requestTx, makeSendBuffer(mock[DeviceMemoryBuffer]), requestHandler, null))
      doAnswer(recordBuilt(replies)).when(state).getErrorResponse(any())

      server.doHandleTransferRequest(Seq(state))
      val send = ArgumentCaptor.forClass(classOf[UcxCallback])
      verify(ucx, times(1)).sendActiveMessage(
        anyLong(), any[UCXActiveMessage](), any[ByteBuffer](), send.capture())
      assertResult(1)(replies.size)
      verify(replies.head, never()).close()

      // the reply's task, on the progress thread, cannot allocate the send's header
      val oom = new OutOfMemoryError("Direct buffer memory")
      intercept[OutOfMemoryError](UCX.sendOrReport(send.getValue)(_ => throw oom))

      assertResult(Some(BufferSendState.MissingOutput))(state.endedWith)
      verify(replies.head, times(1)).close()
      verify(requestTx, times(1)).close()
    }
  }

  private val releaseErrorLog = "Error while releasing a BufferSendState"

  /** Runs `body`, returning what was logged while it ran with its result. */
  private def withLogs[T](body: => T): (Array[String], T) = {
    var result: Option[T] = None
    val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[BufferSendState].getName)) {
      result = Some(body)
    }
    (logs, result.get)
  }

  private def cudaFatal(): CudaFatalException = {
    // its constructors are not public
    val constructor =
      classOf[CudaFatalException].getDeclaredConstructor(classOf[String], classOf[Int])
    constructor.setAccessible(true)
    constructor.newInstance("an illegal memory access in test", Int.box(700))
  }

  /**
   * Three states that all prepare: the first one's two buffers, the second one's buffer and the
   * third one's. Releasing the first state's first buffer throws an ordinary error, which starts
   * the pass's cleanup, and releasing the second state's buffer, during that cleanup, throws
   * `secondReleaseError`.
   */
  private def cleanupAfterFailure(secondReleaseError: Throwable)
      : (Throwable, Seq[BufferSendState], Seq[SendBounceBuffers], Seq[Transaction],
        Seq[DeviceMemoryBuffer], ServerConnection) = {
    val firstRelease = mock[DeviceMemoryBuffer]
    doThrow(new IllegalStateException("release failed in test")).when(firstRelease).close()
    val otherRelease = mock[DeviceMemoryBuffer]
    val secondRelease = mock[DeviceMemoryBuffer]
    doThrow(secondReleaseError).when(secondRelease).close()
    val thirdRelease = mock[DeviceMemoryBuffer]
    val buffers = Seq.fill(3)(makeSendBuffer(mock[DeviceMemoryBuffer]))
    val txs = Seq(makeTx(Seq(1, 3)), makeTx(2), makeTx(4))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handles = Seq(1 -> firstRelease, 3 -> otherRelease, 2 -> secondRelease,
        4 -> thirdRelease).map { case (id, buffer) => id -> makeHandle(tableMeta, () => buffer) }
      handles.foreach { case (id, handle) =>
        when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(id))).thenReturn(handle)
      }
      val serverConnection = mock[ServerConnection]
      val server = newServer(requestHandler, serverConnection)
      val states = buffers.zip(txs).map { case (buffer, tx) =>
        new BufferSendState(tx, buffer, requestHandler, null)
      }
      val failure = try {
        server.doHandleTransferRequest(states)
        null
      } catch {
        case t: Throwable => t
      }
      (failure, states, buffers, txs, Seq(firstRelease, otherRelease, secondRelease, thirdRelease),
        serverConnection)
    }
  }

  private def assertEndedOnce(
      states: Seq[BufferSendState],
      buffers: Seq[SendBounceBuffers],
      txs: Seq[Transaction],
      releases: Seq[DeviceMemoryBuffer],
      serverConnection: ServerConnection): Unit = {
    states.foreach(state => assertResult(Some(BufferSendState.Failed))(state.endedWith))
    buffers.foreach(buffer => verify(buffer, times(1)).close())
    txs.foreach(tx => verify(tx, times(1)).close())
    // every materialized buffer is released once, including the one after a release that threw
    releases.foreach(release => verify(release, times(1)).close())
    verify(serverConnection, times(0)).send(
      any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]())
  }

  test("a fatal CUDA error found while cleaning up after another failure still ends the loop") {
    val fatal = cudaFatal()
    val (logs, (failure, states, buffers, txs, releases, serverConnection)) =
      withLogs(cleanupAfterFailure(fatal))
    assert(failure.isInstanceOf[IllegalStateException])
    assertResult("release failed in test")(failure.getMessage)
    assert(failure.getSuppressed.contains(fatal))
    assert(!Thread.interrupted())
    assertEndedOnce(states, buffers, txs, releases, serverConnection)
    // only the state that ended before the fatal error surfaced was answered
    verify(txs.head, times(1)).respond(any(), any())
    txs.tail.foreach(tx => verify(tx, never()).respond(any(), any()))
    // rethrown, so left to whoever ends the loop with it to log
    assertResult(0)(logs.count(_.startsWith(releaseErrorLog)))
  }

  test("an ordinary failure while cleaning up after another one does not end the loop") {
    val (logs, (failure, states, buffers, txs, releases, serverConnection)) =
      withLogs(cleanupAfterFailure(new IllegalStateException("second release failed in test")))
    assert(failure == null)
    assertEndedOnce(states, buffers, txs, releases, serverConnection)
    // the loop goes on, so every requester is answered
    txs.foreach(tx => verify(tx, times(1)).respond(any(), any()))
    // contained, so logged where it is contained
    assertResult(1)(logs.count(_.startsWith(releaseErrorLog)))
  }

  test("a fatal CUDA error from the recovery synchronization still ends the loop") {
    val fatal = cudaFatal()
    val buffers = Seq.fill(2)(makeSendBuffer(mock[DeviceMemoryBuffer]))
    val txs = Seq(makeTx(1), makeTx(2))
    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      // both fail to prepare for an ordinary reason, so the pass fails fast before its own sync
      val handles = Seq(1, 2).map { id =>
        id -> makeHandle(tableMeta, () => throw new IOException("mmap failed in test"))
      }
      handles.foreach { case (id, handle) =>
        when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(id))).thenReturn(handle)
      }
      val serverConnection = mock[ServerConnection]
      val server = newServer(requestHandler, serverConnection)
      val stream = mock[Cuda.Stream]
      doThrow(fatal).when(stream).sync()
      doReturn(stream, Nil: _*).when(server).serverStream

      val states = buffers.zip(txs).map { case (buffer, tx) =>
        new BufferSendState(tx, buffer, requestHandler, null)
      }
      val failure = intercept[IllegalStateException] {
        server.doHandleTransferRequest(states)
      }

      assert(failure.getMessage.startsWith("Unable to prepare any sends."))
      assert(failure.getSuppressed.contains(fatal))
      assert(!Thread.interrupted())
      verify(stream, times(1)).sync()
      states.foreach(state => assertResult(Some(BufferSendState.Failed))(state.endedWith))
      buffers.foreach(buffer => verify(buffer, times(1)).close())
      txs.foreach(tx => verify(tx, times(1)).close())
      // the loop ends, so the requesters hear of it when their connections close
      txs.foreach(tx => verify(tx, never()).respond(any(), any()))
      verify(server, times(0)).addToContinueQueue(any())
    }
  }

  test("an interrupted preparation ends the loop and keeps the thread's interrupt") {
    val healthySlice = mock[DeviceMemoryBuffer]
    val healthyBuffer = makeSendBuffer(healthySlice)
    val interruptedBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val healthyTx = makeTx(1)
    val interruptedTx = makeTx(2)

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val materialized = mock[DeviceMemoryBuffer]
      val healthyHandle = makeHandle(tableMeta, () => materialized)
      // as a disk-spilled buffer does when its wait for a host spill bounce buffer is interrupted
      val interruptedHandle =
        makeHandle(tableMeta, () => throw new InterruptedException("in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(healthyHandle)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(2))).thenReturn(interruptedHandle)
      val serverConnection = mock[ServerConnection]
      val server = newServer(requestHandler, serverConnection)

      val healthy = new BufferSendState(healthyTx, healthyBuffer, requestHandler, null)
      val interrupted = new BufferSendState(interruptedTx, interruptedBuffer, requestHandler, null)
      var interruptKept = false
      val failure = try {
        intercept[RapidsShuffleSendPrepareException] {
          server.doHandleTransferRequest(Seq(healthy, interrupted))
        }
      } finally {
        // cleared here, so that a failure above cannot leave this thread interrupted
        interruptKept = Thread.interrupted()
      }

      assert(interruptKept)
      assert(failure.getCause.isInstanceOf[InterruptedException])
      assertResult(Some(BufferSendState.Failed))(healthy.endedWith)
      assertResult(Some(BufferSendState.Failed))(interrupted.endedWith)
      verify(healthyBuffer, times(1)).close()
      verify(interruptedBuffer, times(1)).close()
      // the healthy state's window was prepared but never sent, so the pass closes its slice
      verify(healthySlice, times(1)).close()
      Seq(healthyTx, interruptedTx).foreach(tx => verify(tx, never()).respond(any(), any()))
      verify(server, times(0)).addToContinueQueue(any())
      verify(serverConnection, times(0)).send(
        any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]())
    }
  }

  test("an interrupted OOM backoff ends the request and keeps the thread's interrupt") {
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val tx = makeTx(1)
    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = makeHandle(tableMeta,
        () => throw new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(handle)
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mock[ServerConnection],
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def oomRetryTimeoutNanos: Long = Long.MaxValue
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit =
          throw new InterruptedException("backoff interrupted in test")
      })

      val bss = new BufferSendState(tx, sendBuffer, requestHandler, null)
      var interruptKept = false
      try {
        intercept[InterruptedException] {
          server.doHandleTransferRequest(Seq(bss))
        }
      } finally {
        interruptKept = Thread.interrupted()
      }

      assert(interruptKept)
      assertResult(Some(BufferSendState.Failed))(bss.endedWith)
      verify(sendBuffer, times(1)).close()
      verify(tx, times(1)).close()
      verify(tx, never()).respond(any(), any())
      verify(server, times(0)).addToContinueQueue(any())
    }
  }

  test("an error reply is released and logged once, whether its send fails or cannot start") {
    // The first reply goes through the real UCX server connection and transaction, with only UCX
    // mocked, so that its failed send reaches the reply's callback as UCX reports it.
    val ucx = mock[UCX]
    val connection = new UCXServerConnection(ucx, mock[UCXShuffleTransport])
    val sendFailsTx = makeTx(1)
    doAnswer(new Answer[Transaction] {
      override def answer(invocation: InvocationOnMock): Transaction =
        connection.send(2L, MessageType.TransferRequest, 0L,
          invocation.getArgument[ByteBuffer](0), invocation.getArgument[TransactionCallback](1))
    }).when(sendFailsTx).respond(any(), any())
    val cannotStartTx = makeTx(2)
    doThrow(new IllegalStateException("respond failed in test"))
      .when(cannotStartTx).respond(any(), any())
    val txs = Seq(sendFailsTx, cannotStartTx)

    withGoneTables(1, 2) { requestHandler =>
      val server = newServer(requestHandler, mock[ServerConnection])
      val replies = new ArrayBuffer[RefCountedDirectByteBuffer]()
      val states = txs.map { tx =>
        val state = spy(new BufferSendState(
          tx, makeSendBuffer(mock[DeviceMemoryBuffer]), requestHandler, null))
        doAnswer(recordBuilt(replies)).when(state).getErrorResponse(any())
        state
      }

      val logs = LogCaptureUtils.captureLogsFrom(
          Seq(classOf[BufferSendState].getName, classOf[UCXServerConnection].getName)) {
        server.doHandleTransferRequest(states)
        val completion = ArgumentCaptor.forClass(classOf[UcxCallback])
        verify(ucx, times(1)).sendActiveMessage(
          anyLong(), any[UCXActiveMessage](), any[ByteBuffer](), completion.capture())
        completion.getValue.onError(-1, "the peer is gone in test")
      }

      states.foreach(state =>
        assertResult(Some(BufferSendState.MissingOutput))(state.endedWith))
      assertResult(2)(replies.size)
      replies.foreach(reply => verify(reply, times(1)).close())
      txs.foreach(tx => verify(tx, times(1)).close())
      // the reply's callback threw nothing into the transport's callback wrapper
      assertResult(0)(logs.count(_.startsWith("Detected an exception from user code")))
      // the failed send is logged by the connection that saw it fail, and only there
      assertResult(1)(logs.count(_.contains("the peer is gone in test")))
      assertResult(1)(logs.count(_.startsWith("Unable to send an error reply")))
    }
  }

  private val awaitSeconds = 30L

  /**
   * Starts a server whose send loop runs on a thread of its own, and hands it a transfer request
   * as the transport would. It closes the server while the request handler holds the request's
   * lookup or preparation. The held work fails only once the close's bounded wait for the loop
   * has run out and the transport has closed. By then UCX's progress thread runs no more sends.
   * @param held counted down by the request handler once it holds the send loop
   * @param mayFail counted down once the held lookup or preparation may fail
   */
  private def failAfterClose(
      requestHandler: RapidsShuffleRequestHandler,
      held: CountDownLatch,
      mayFail: CountDownLatch): (Transaction, SendBounceBuffers) = {
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    when(mockTransport.tryGetSendBounceBuffers(anyLong(), anyInt())).thenReturn(Seq(sendBuffer))
    val request = makeTx(1)
    when(request.getStatus).thenReturn(TransactionStatus.Success)
    val serverConnection = mock[ServerConnection]
    val requestCallbacks = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    doAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit =
        requestCallbacks.put(invocation.getArgument[MessageType.Value](0),
          invocation.getArgument[TransactionCallback](1))
    }).when(serverConnection).registerRequestHandler(any(), any())
    val sendLoop = Executors.newSingleThreadExecutor()
    val server = new RapidsShuffleServer(mockTransport, serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"), requestHandler, mockExecutor,
      sendLoop, mockConf)
    try {
      server.start()
      requestCallbacks.get(MessageType.TransferRequest)(request)
      assert(held.await(awaitSeconds, TimeUnit.SECONDS), "the send loop never took the request")
      server.close()
      // as the transport does once UCX's progress thread has stopped
      server.releaseAbandoned(transportStopped = true)
      mayFail.countDown()
      verify(request, timeout(TimeUnit.SECONDS.toMillis(awaitSeconds))).close()
    } finally {
      mayFail.countDown()
      sendLoop.shutdownNow()
    }
    (request, sendBuffer)
  }

  test("a lookup that fails after the server closed gets no error reply") {
    val (held, mayFail) = (new CountDownLatch(1), new CountDownLatch(1))
    val requestHandler = mock[RapidsShuffleRequestHandler]
    when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenAnswer { _ =>
      held.countDown()
      mayFail.await(awaitSeconds, TimeUnit.SECONDS)
      throw new IllegalStateException("lookup failed in test")
    }
    val (request, sendBuffer) = failAfterClose(requestHandler, held, mayFail)
    // nothing would send a reply, or release it and its transaction
    verify(request, never()).respond(any(), any())
    verify(request, times(1)).close()
    verify(sendBuffer, times(1)).close()
  }

  test("a preparation that fails after the server closed gets no error reply") {
    val (held, mayFail) = (new CountDownLatch(1), new CountDownLatch(1))
    withTables(1)(() => {
      held.countDown()
      mayFail.await(awaitSeconds, TimeUnit.SECONDS)
      throw new IllegalStateException("preparation failed in test")
    }) { requestHandler =>
      val (request, sendBuffer) = failAfterClose(requestHandler, held, mayFail)
      verify(request, never()).respond(any(), any())
      verify(request, times(1)).close()
      verify(sendBuffer, times(1)).close()
    }
  }

  /**
   * Runs `body` with two spied send states, for the requests `txs` of tables 1 and 2, whose
   * buffers materialize with `materialize`. `body` gets the states and their request handler.
   */
  private def withTwoStates(txs: Seq[Transaction], materialize: () => DeviceMemoryBuffer)(
      body: (Seq[BufferSendState], RapidsShuffleRequestHandler) => Unit): Unit = {
    withTables(1, 2)(materialize) { requestHandler =>
      val states = txs.map { tx =>
        spy(new BufferSendState(tx, makeSendBuffer(mock[DeviceMemoryBuffer]), requestHandler, null))
      }
      body(states, requestHandler)
    }
  }

  /** The first state is answered before the server closed, and the second builds no reply. */
  private def assertOnlyFirstAnswered(states: Seq[BufferSendState], txs: Seq[Transaction])
      : Unit = {
    verify(states.head, times(1)).getErrorResponse(any())
    verify(txs.head, times(1)).respond(any(), any())
    verify(states(1), never()).getErrorResponse(any())
    verify(txs(1), never()).respond(any(), any())
    txs.foreach(tx => verify(tx, times(1)).close())
  }

  test("states whose OOM retries end as the server closes build no reply after it closed") {
    val txs = Seq(makeTx(1), makeTx(2))
    withTwoStates(txs, () => throw new OutOfMemoryError("GPU allocation failed in test")) {
      (states, requestHandler) =>
        val server = new RapidsShuffleServer(mockTransport, mock[ServerConnection],
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"), requestHandler, mockExecutor,
          mockBssExecutor, mockConf) {
          // each OOM is past its request's retry deadline
          override private[shuffle] def oomRetryTimeoutNanos: Long = 0L
        }
        // the server closes once the first state's request has closed, and with no send loop
        // running, its close returns at once
        doAnswer(_ => server.close()).when(txs.head).close()
        server.doHandleTransferRequest(states)
        states.foreach(state => assertResult(Some(BufferSendState.OomExpired))(state.endedWith))
        assertOnlyFirstAnswered(states, txs)
    }
  }

  test("states ended by a failure to gather sends build no reply after the server closed") {
    val (holding, mayResume) = (new CountDownLatch(1), new CountDownLatch(1))
    val txs = Seq(makeTx(1), makeTx(2))
    // closing the first state's request holds the send loop until the server has closed
    doAnswer(_ => {
      holding.countDown()
      mayResume.await(awaitSeconds, TimeUnit.SECONDS)
    }).when(txs.head).close()
    withTwoStates(txs, () => mock[DeviceMemoryBuffer]) { (states, requestHandler) =>
      // a new request makes the loop ask for bounce buffers while both states wait to continue
      when(mockTransport.tryGetSendBounceBuffers(anyLong(), anyInt()))
        .thenThrow(new IllegalStateException("no bounce buffers in test"))
      val request = makeTx(3)
      val serverConnection = mock[ServerConnection]
      val requestCallbacks = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
      doAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit =
          requestCallbacks.put(invocation.getArgument[MessageType.Value](0),
            invocation.getArgument[TransactionCallback](1))
      }).when(serverConnection).registerRequestHandler(any(), any())
      // the send loop starts once the states and the request are queued, so its first pass
      // sees all three
      val (queued, loopExited) = (new CountDownLatch(1), new CountDownLatch(1))
      val loopThread = Executors.newSingleThreadExecutor()
      val sendLoop: Executor = loop => loopThread.execute(() => {
        try {
          queued.await(awaitSeconds, TimeUnit.SECONDS)
          loop.run()
        } finally {
          loopExited.countDown()
        }
      })
      try {
        val server = new RapidsShuffleServer(mockTransport, serverConnection,
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"), requestHandler, mockExecutor,
          sendLoop, mockConf)
        try {
          server.start()
          server.addToContinueQueue(states)
          requestCallbacks.get(MessageType.TransferRequest)(request)
          queued.countDown()
          assert(holding.await(awaitSeconds, TimeUnit.SECONDS),
            "the send loop never ended the first state")
          server.close()
        } finally {
          mayResume.countDown()
        }
        assert(loopExited.await(awaitSeconds, TimeUnit.SECONDS), "the send loop never exited")
      } finally {
        loopThread.shutdownNow()
      }
      states.foreach(state => assertResult(Some(BufferSendState.Failed))(state.endedWith))
      assertOnlyFirstAnswered(states, txs)
      // the loop closes the request it never served as it exits
      verify(request, never()).respond(any(), any())
      verify(request, times(1)).close()
    }
  }

  test("a reply the gate turns away is closed once, even when its close throws") {
    val reply = spy(new RefCountedDirectByteBuffer(ByteBuffer.allocateDirect(8)))
    doAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        invocation.callRealMethod()
        throw new IllegalStateException("closing the reply failed in test")
      }
    }).when(reply).close()
    val shut = new BufferSendState.ReplyGate {
      override def queueIfOpen(queue: => Unit): Boolean = false
    }
    BufferSendState.sendErrorReply(makeTx(1), shut, reply)
    verify(reply, times(1)).close()
  }

  test("an error reply that runs out of memory as it is built is not sent, and the loop goes on") {
    val requestTx = makeTx(1)
    withGoneTables(1) { requestHandler =>
      val server = newServer(requestHandler, mock[ServerConnection])
      val state = spy(new BufferSendState(
        requestTx, makeSendBuffer(mock[DeviceMemoryBuffer]), requestHandler, null))
      doThrow(new OutOfMemoryError("Cannot reserve 1024 bytes of direct buffer memory in test"))
        .when(state).getErrorResponse(any())

      // an OutOfMemoryError that escapes a test aborts the suite, so this one is caught
      var escaped: Option[OutOfMemoryError] = None
      val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[BufferSendState].getName)) {
        try {
          server.doHandleTransferRequest(Seq(state))
        } catch {
          case oom: OutOfMemoryError => escaped = Some(oom)
        }
      }

      assertResult(None)(escaped)
      verify(state, times(1)).getErrorResponse(any())
      assertResult(1)(logs.count(_.startsWith("Unable to send an error reply")))
      assertResult(Some(BufferSendState.MissingOutput))(state.endedWith)
      verify(requestTx, never()).respond(any(), any())
      verify(requestTx, times(1)).close()
    }
  }

  /**
   * Runs `body` with a server whose send loop runs on a thread of its own, and a send state for
   * table 1, whose buffer is gone, so that the loop ends it with an error reply. `body` gets the
   * server, the state, and the error replies the state builds, which it reads under their lock.
   */
  private def withGoneStateOnSendLoop(requestTx: Transaction, sendBuffer: SendBounceBuffers)(
      body: (RapidsShuffleServer, BufferSendState, ArrayBuffer[RefCountedDirectByteBuffer]) =>
        Unit): Unit = {
    withGoneTables(1) { requestHandler =>
      val replies = new ArrayBuffer[RefCountedDirectByteBuffer]()
      val state = spy(new BufferSendState(requestTx, sendBuffer, requestHandler, null))
      doAnswer(recordBuilt(replies)).when(state).getErrorResponse(any())
      val sendLoop = Executors.newSingleThreadExecutor()
      try {
        val server = new RapidsShuffleServer(mockTransport, mock[ServerConnection],
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"), requestHandler, mockExecutor,
          sendLoop, mockConf)
        body(server, state, replies)
      } finally {
        sendLoop.shutdownNow()
      }
    }
  }

  test("an error reply decided before the server closed is not queued after it closed") {
    // Releasing the state's bounce buffers, after its reply was decided and before it is
    // queued, holds the send loop until the server has closed.
    val (holding, mayResume) = (new CountDownLatch(1), new CountDownLatch(1))
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    doAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        holding.countDown()
        mayResume.await(awaitSeconds, TimeUnit.SECONDS)
      }
    }).when(sendBuffer).close()
    val requestTx = makeTx(1)
    withGoneStateOnSendLoop(requestTx, sendBuffer) { (server, state, replies) =>
      try {
        server.addToContinueQueue(Seq(state))
        assert(holding.await(awaitSeconds, TimeUnit.SECONDS), "the send loop never ended the state")
        server.close()
        // as the transport does once UCX's progress thread has stopped, after which nothing
        // queued for it runs. It ends nothing here, since the state never went in flight.
        server.releaseAbandoned(transportStopped = true)
      } finally {
        mayResume.countDown()
      }
      verify(requestTx, timeout(TimeUnit.SECONDS.toMillis(awaitSeconds))).close()
      // the reply, decided before close, never reached the stopped transport and is released
      verify(requestTx, never()).respond(any(), any())
      replies.synchronized {
        assertResult(1)(replies.size)
        verify(replies.head, times(1)).close()
      }
      assertResult(Some(BufferSendState.MissingOutput))(state.endedWith)
      verify(sendBuffer, times(1)).close()
      verify(requestTx, times(1)).close()
    }
  }

  /**
   * Closes `server` on a thread of its own while `mayQueue` holds a reply that the server let
   * through in the middle of being queued. Checks that the close waits for it, then releases it
   * and checks that the close returns. `reply` names the reply in the failure message.
   */
  private def assertCloseWaitsForQueueing(
      server: RapidsShuffleServer,
      mayQueue: CountDownLatch,
      reply: String): Unit = {
    val closer = Executors.newSingleThreadExecutor()
    val closed = new CountDownLatch(1)
    try {
      closer.execute(() => {
        server.close()
        closed.countDown()
      })
      // longer than close's bounded wait for the send loop
      assert(!closed.await(2, TimeUnit.SECONDS),
        s"close returned while $reply it let through was still being queued")
    } finally {
      mayQueue.countDown()
      closer.shutdown()
    }
    try {
      assert(closed.await(awaitSeconds, TimeUnit.SECONDS), "close never returned")
    } finally {
      // a close still waiting is interrupted only once the reply may be queued
      closer.shutdownNow()
    }
  }

  test("close waits for an error reply that is being queued") {
    val (queueing, mayQueue) = (new CountDownLatch(1), new CountDownLatch(1))
    val requestTx = makeTx(1)
    // the reply's send holds the send loop while the transport queues it
    doAnswer(new Answer[Transaction] {
      override def answer(invocation: InvocationOnMock): Transaction = {
        queueing.countDown()
        mayQueue.await(awaitSeconds, TimeUnit.SECONDS)
        null
      }
    }).when(requestTx).respond(any(), any())
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withGoneStateOnSendLoop(requestTx, sendBuffer) { (server, state, replies) =>
      server.addToContinueQueue(Seq(state))
      assert(queueing.await(awaitSeconds, TimeUnit.SECONDS), "the reply was never sent")
      assertCloseWaitsForQueueing(server, mayQueue, "an error reply")
      val callback = ArgumentCaptor.forClass(classOf[TransactionCallback])
      verify(requestTx, times(1)).respond(any(), callback.capture())
      // the queued reply is the transport's, and its callback releases it
      callback.getValue.apply(mock[Transaction])
      replies.synchronized {
        assertResult(1)(replies.size)
        verify(replies.head, times(1)).close()
      }
      verify(sendBuffer, times(1)).close()
      verify(requestTx, timeout(TimeUnit.SECONDS.toMillis(awaitSeconds))).close()
    }
  }

  /** Closes `server` on a thread of its own, so that a close that never returns fails. */
  private def assertCloseReturns(server: RapidsShuffleServer, why: String): Unit = {
    val closer = Executors.newSingleThreadExecutor()
    val closed = new CountDownLatch(1)
    try {
      closer.execute(() => {
        server.close()
        closed.countDown()
      })
      assert(closed.await(awaitSeconds, TimeUnit.SECONDS), why)
    } finally {
      closer.shutdownNow()
    }
  }

  test("close does not wait for an error reply whose send threw") {
    val requestTx = makeTx(1)
    doThrow(new IllegalStateException("respond failed in test"))
      .when(requestTx).respond(any(), any())
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withGoneStateOnSendLoop(requestTx, sendBuffer) { (server, state, replies) =>
      server.addToContinueQueue(Seq(state))
      verify(requestTx, timeout(TimeUnit.SECONDS.toMillis(awaitSeconds))).close()
      verify(requestTx, times(1)).respond(any(), any())
      assertCloseReturns(server, "close kept waiting for an error reply whose send threw")
      replies.synchronized {
        assertResult(1)(replies.size)
        verify(replies.head, times(1)).close()
      }
    }
  }

  test("close does not wait for an error reply whose send threw a fatal error") {
    val requestTx = makeTx(1)
    // the send loop ends on it, and the reply is left alone, since the send may be queued
    doThrow(new OutOfMemoryError("Java heap space in test"))
      .when(requestTx).respond(any(), any())
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withGoneStateOnSendLoop(requestTx, sendBuffer) { (server, state, replies) =>
      server.addToContinueQueue(Seq(state))
      verify(requestTx, timeout(TimeUnit.SECONDS.toMillis(awaitSeconds))).respond(any(), any())
      assertCloseReturns(server,
        "close kept waiting for an error reply whose send threw a fatal error")
      replies.synchronized {
        assertResult(1)(replies.size)
        verify(replies.head, never()).close()
      }
    }
  }

  /**
   * Runs `body` with a server whose send loop runs on a thread of its own and whose one send
   * state, for table 1, has sent its only window and waits for that send's completion. `prepare`
   * stubs the spied state before its send. `body` gets the server, the state, and `complete`,
   * which completes the send successfully on a thread of its own, as UCX's progress thread does.
   */
  private def withStateAwaitingCompletion(requestTx: Transaction, sendBuffer: SendBounceBuffers)(
      prepare: BufferSendState => Unit)(
      body: (RapidsShuffleServer, BufferSendState, () => java.util.concurrent.Future[_]) =>
        Unit): Unit = {
    withTables(1)(() => mock[DeviceMemoryBuffer]) { requestHandler =>
      val serverConnection = mock[ServerConnection]
      val callback = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(serverConnection.send(
        any(), any(), any(), any[MemoryBuffer](), callback.capture())).thenReturn(mock[Transaction])
      // close waits on the send loop executor's monitor, and a mocked executor's wait() returns
      // at once, so the loop runs on a real one
      val sendLoop = Executors.newSingleThreadExecutor()
      val progressThread = Executors.newSingleThreadExecutor()
      try {
        val server = new RapidsShuffleServer(mockTransport, serverConnection,
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"), requestHandler, mockExecutor,
          sendLoop, mockConf)
        val state = spy(new BufferSendState(requestTx, sendBuffer, requestHandler, null))
        prepare(state)
        server.doHandleTransferRequest(Seq(state))
        assert(!state.hasMoreSends)
        val done = mock[Transaction]
        when(done.getStatus).thenReturn(TransactionStatus.Success)
        body(server, state, () => progressThread.submit(new Runnable {
          override def run(): Unit = callback.getValue.apply(done)
        }))
      } finally {
        progressThread.shutdownNow()
        sendLoop.shutdownNow()
      }
    }
  }

  test("a transfer response decided before the server closed is not queued after it closed") {
    val (holding, mayResume) = (new CountDownLatch(1), new CountDownLatch(1))
    val responses = new ArrayBuffer[RefCountedDirectByteBuffer]()
    val requestTx = makeTx(1)
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withStateAwaitingCompletion(requestTx, sendBuffer) { state =>
      // building the response, after the server checked that it is open, holds the progress
      // thread until the server has closed
      doAnswer(new Answer[RefCountedDirectByteBuffer] {
        override def answer(invocation: InvocationOnMock): RefCountedDirectByteBuffer = {
          holding.countDown()
          mayResume.await(awaitSeconds, TimeUnit.SECONDS)
          recordBuilt(responses).answer(invocation)
        }
      }).when(state).getTransferResponse()
    } { (server, state, complete) =>
      val completed = complete()
      try {
        assert(holding.await(awaitSeconds, TimeUnit.SECONDS), "the send never completed")
        server.close()
        // as the transport does once UCX's progress thread has stopped. It ends nothing here,
        // since the send's callback already took the state out of flight.
        server.releaseAbandoned(transportStopped = true)
      } finally {
        mayResume.countDown()
      }
      completed.get(awaitSeconds, TimeUnit.SECONDS)
      // the response never reached the stopped transport, and is released
      verify(requestTx, never()).respond(any(), any())
      responses.synchronized {
        assertResult(1)(responses.size)
        verify(responses.head, times(1)).close()
      }
      assertResult(Some(BufferSendState.ServerClosed))(state.endedWith)
      verify(sendBuffer, times(1)).close()
      verify(requestTx, times(1)).close()
    }
  }

  test("close waits for a transfer response that is being queued") {
    val (queueing, mayQueue) = (new CountDownLatch(1), new CountDownLatch(1))
    val requestTx = makeTx(1)
    // the response's send holds the progress thread while the transport queues it
    doAnswer(new Answer[Transaction] {
      override def answer(invocation: InvocationOnMock): Transaction = {
        queueing.countDown()
        mayQueue.await(awaitSeconds, TimeUnit.SECONDS)
        null
      }
    }).when(requestTx).respond(any(), any())
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withStateAwaitingCompletion(requestTx, sendBuffer)(_ => ()) { (server, state, complete) =>
      val completed = complete()
      assert(queueing.await(awaitSeconds, TimeUnit.SECONDS), "the response was never sent")
      assertCloseWaitsForQueueing(server, mayQueue, "a transfer response")
      completed.get(awaitSeconds, TimeUnit.SECONDS)
      verify(requestTx, times(1)).respond(any(), any())
      assertResult(Some(BufferSendState.Completed))(state.endedWith)
      verify(requestTx, times(1)).close()
    }
  }

  /** Records each transfer response that `state` builds in `responses`, spied. */
  private def recordTransferResponses(
      responses: ArrayBuffer[RefCountedDirectByteBuffer])(state: BufferSendState): Unit =
    doAnswer(recordBuilt(responses)).when(state).getTransferResponse()

  test("a transfer response whose send throws is closed once, and close does not wait for it") {
    val responses = new ArrayBuffer[RefCountedDirectByteBuffer]()
    val requestTx = makeTx(1)
    doThrow(new IllegalStateException("respond failed in test"))
      .when(requestTx).respond(any(), any())
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withStateAwaitingCompletion(requestTx, sendBuffer)(recordTransferResponses(responses)) {
      (server, state, complete) =>
        val thrown = intercept[ExecutionException] {
          complete().get(awaitSeconds, TimeUnit.SECONDS)
        }
        assertResult("respond failed in test")(thrown.getCause.getMessage)
        responses.synchronized {
          assertResult(1)(responses.size)
          verify(responses.head, times(1)).close()
        }
        assertResult(Some(BufferSendState.Failed))(state.endedWith)
        verify(sendBuffer, times(1)).close()
        verify(requestTx, times(1)).close()
        assertCloseReturns(server, "close kept waiting for a transfer response whose send threw")
    }
  }

  test("a transfer response is closed once, by its send's callback") {
    val responses = new ArrayBuffer[RefCountedDirectByteBuffer]()
    val requestTx = makeTx(1)
    val sendBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    withStateAwaitingCompletion(requestTx, sendBuffer)(recordTransferResponses(responses)) {
      (server, state, complete) =>
        complete().get(awaitSeconds, TimeUnit.SECONDS)
        assertResult(Some(BufferSendState.Completed))(state.endedWith)
        val callback = ArgumentCaptor.forClass(classOf[TransactionCallback])
        verify(requestTx, times(1)).respond(any(), callback.capture())
        responses.synchronized {
          assertResult(1)(responses.size)
          verify(responses.head, never()).close()
        }
        val sent = mock[Transaction]
        when(sent.getStatus).thenReturn(TransactionStatus.Success)
        callback.getValue.apply(sent)
        responses.synchronized(verify(responses.head, times(1)).close())
        verify(sent, times(1)).close()
        assertCloseReturns(server, "close kept waiting after a transfer response was sent")
    }
  }

  test("the transport closes even when the server's close is interrupted") {
    val server = mock[AutoCloseable]
    val interrupted = new InterruptedException("stop interrupted in test")
    doThrow(interrupted).when(server).close()
    val transport = mock[RapidsShuffleTransport]
    val thrown = intercept[InterruptedException](
      RapidsShuffleTransport.closeServerThenTransport(Some(server), Some(transport)))
    // the caller still sees the interrupt
    assert(thrown eq interrupted)
    verify(transport, times(1)).close()
  }

  /**
   * Runs `body` on an executor's shuffle manager in UCX mode, whose SparkEnv and catalog are
   * mocks. The manager builds the configured transport class by reflection, and a constructor
   * mock makes that transport serve `server`. `body` gets the transport once `stop` built it.
   */
  private def withExecutorManager(server: RapidsShuffleServer)(
      body: (RapidsShuffleInternalManagerBase, () => UCXShuffleTransport) => Unit): Unit = {
    val conf = new SparkConf(loadDefaults = false).set("spark.rapids.shuffle.mode", "UCX")
    withResource(mockConstruction(classOf[UCXShuffleTransport],
        (transport: UCXShuffleTransport, _: MockedConstruction.Context) =>
          when(transport.makeServer(any())).thenReturn(server))) { transports =>
      val manager = new RapidsShuffleInternalManagerBase(conf, isDriver = false) {
        override protected lazy val env: SparkEnv = mock[SparkEnv](RETURNS_DEEP_STUBS)
        override protected def getCatalogOrThrow: ShuffleBufferCatalog =
          mock[ShuffleBufferCatalog]
      }
      body(manager, () => {
        assert(transports.constructed().size() == 1)
        transports.constructed().get(0)
      })
    }
  }

  private def assertClosedOnceInOrder(
      server: RapidsShuffleServer,
      transport: UCXShuffleTransport): Unit = {
    verify(server, times(1)).close()
    verify(transport, times(1)).close()
    // the transport's close stops UCX, so it comes after the server's
    val closes = inOrder(server, transport)
    closes.verify(server).close()
    closes.verify(transport).close()
  }

  test("the shuffle manager's stop closes the transport when the server's close is interrupted") {
    val server = mock[RapidsShuffleServer]
    val interrupted = new InterruptedException("stop interrupted in test")
    // doThrow refuses a checked exception that the Scala method does not declare
    doAnswer(_ => throw interrupted).when(server).close()
    withExecutorManager(server) { (manager, transport) =>
      val thrown = intercept[InterruptedException](manager.stop())
      assert(thrown eq interrupted)
      assertClosedOnceInOrder(server, transport())
    }
  }

  test("the shuffle manager's stop closes the server, then the transport") {
    val server = mock[RapidsShuffleServer]
    withExecutorManager(server) { (manager, transport) =>
      manager.stop()
      assertClosedOnceInOrder(server, transport())
    }
  }

  test("a missing-output warning names the first requests and counts the rest") {
    def request(executorId: Long): (Long, Long) = (executorId, executorId << 8)
    def named(executorId: Long): String =
      s"executor $executorId header ${TransportUtils.toHex(executorId << 8)}"
    assertResult(s"${named(1)}, ${named(2)}")(
      RapidsShuffleServer.describeRequests(Seq(request(1), request(2))))
    assertResult((1L to 5L).map(named).mkString(", ") + " and 2 more")(
      RapidsShuffleServer.describeRequests((1L to 7L).map(request)))
  }
}
