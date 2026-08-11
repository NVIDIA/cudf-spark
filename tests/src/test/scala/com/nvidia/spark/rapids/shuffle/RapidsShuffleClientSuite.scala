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

package com.nvidia.spark.rapids.shuffle

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.ShuffleMetadata
import com.nvidia.spark.rapids.format.{BufferMeta, TableMeta}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.storage.ShuffleBlockBatchId

class RapidsShuffleClientSuite extends RapidsShuffleTestHelper {
  def prepareBufferReceiveState(
      tableMeta: TableMeta,
      bounceBuffer: BounceBuffer): BufferReceiveState = {
    val ptr = PendingTransferRequest(client, tableMeta, mockHandler)
    spy(new BufferReceiveState(123L, bounceBuffer, Seq(ptr), () => {}))
  }

  def prepareBufferReceiveState(
      tableMetas: Seq[TableMeta],
      bounceBuffer: BounceBuffer): BufferReceiveState = {

    val ptrs = tableMetas.map { tm =>
      PendingTransferRequest(client, tm, mockHandler)
    }

    spy(new BufferReceiveState(123L, bounceBuffer, ptrs, () => {}))
  }

  def verifyTableMeta(expected: TableMeta, actual: TableMeta): Unit = {
    assertResult(expected.rowCount())(actual.rowCount())
    assertResult(expected.packedMetaAsByteBuffer())(actual.packedMetaAsByteBuffer())
    verifyBufferMeta(expected.bufferMeta, actual.bufferMeta)
  }

  def verifyBufferMeta(expected: BufferMeta, actual: BufferMeta): Unit = {
    assertResult(expected.id)(actual.id)
    assertResult(expected.size)(actual.size)
    assertResult(expected.uncompressedSize)(actual.uncompressedSize)
    assertResult(expected.codecBufferDescrsLength)(actual.codecBufferDescrsLength)
    (0 until expected.codecBufferDescrsLength).foreach { i =>
      val expectedDescr = expected.codecBufferDescrs(i)
      val actualDescr = actual.codecBufferDescrs(i)
      assertResult(expectedDescr.codec)(actualDescr.codec)
      assertResult(expectedDescr.compressedOffset)(actualDescr.compressedOffset)
      assertResult(expectedDescr.compressedSize)(actualDescr.compressedSize)
      assertResult(expectedDescr.uncompressedOffset)(actualDescr.uncompressedOffset)
      assertResult(expectedDescr.uncompressedSize)(actualDescr.uncompressedSize)
    }
  }

  test("successful metadata fetch") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
    val contigBuffSize = 100000
    val numBatches = 3
    val (tableMetas, response) =
      RapidsShuffleTestHelper.mockMetaResponse(mockTransaction, contigBuffSize, numBatches)

    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))

    // initialize metadata fetch
    client.doFetch(shuffleRequests.map(_._1).toSeq, mockHandler)

    // the connection saw one request (for metadata)
    assertResult(1)(mockConnection.requests)

    // upon a successful response, the `start()` method in the fetch handler
    // will be called with 3 expected batches
    verify(mockHandler, times(1)).start(ArgumentMatchers.eq(numBatches))

    // the transport will receive 3 pending requests (for buffers) for queuing
    val ac = ArgumentCaptor.forClass(classOf[Seq[PendingTransferRequest]])
    verify(mockTransport, times(1)).queuePending(ac.capture())
    val ptrs = ac.getValue.asInstanceOf[Seq[PendingTransferRequest]]
    assertResult(numBatches)(ptrs.size)

    // we check their metadata below
    (0 until numBatches).foreach { t =>
      val expected = tableMetas(t)
      val tm = ptrs(t).tableMeta
      verifyTableMeta(expected, tm)
    }

    assert(response.dbb.isClosed)
  }

  test("successful degenerate metadata fetch") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
    val numBatches = 3

    RapidsShuffleTestHelper.mockDegenerateMetaResponse(mockTransaction, numBatches)

    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))

    // initialize metadata fetch
    client.doFetch(shuffleRequests.map(_._1).toSeq, mockHandler)

    // the connection saw one request (for metadata)
    assertResult(1)(mockConnection.requests)

    // upon a successful response, the `start()` method in the fetch handler
    // will be called with 3 expected batches
    verify(mockHandler, times(1)).start(ArgumentMatchers.eq(numBatches))

    // nothing gets queued to be received since it's just metadata
    verify(mockTransport, times(0)).queuePending(any())

    // ensure our handler (iterator) received 3 batches
    verify(mockHandler, times(numBatches)).batchReceived(any())
  }

  private def doTestErrorOrCancelledMetadataFetch(status: TransactionStatus.Value): Unit = {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))

    when(mockTransaction.getStatus).thenReturn(status)
    when(mockTransaction.getErrorMessage).thenReturn(Some("Error/cancel occurred"))

    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks

    client.doFetch(shuffleRequests.map(_._1).toSeq, mockHandler)

    assertResult(1)(mockConnection.requests)

    // upon an errored response, the start handler will not be called
    verify(mockHandler, times(0)).start(any())

    // but the error handler will
    verify(mockHandler, times(1)).transferError(anyString(), isNull())

    // the transport will receive no pending requests (for buffers) for queuing
    verify(mockTransport, times(0)).queuePending(any())

    // this is not called since a message implies success
    verify(mockTransaction, times(0)).releaseMessage()
  }

  test("errored metadata fetch is handled") {
    doTestErrorOrCancelledMetadataFetch(TransactionStatus.Error)
  }

  test("cancelled metadata fetch is handled") {
    doTestErrorOrCancelledMetadataFetch(TransactionStatus.Cancelled)
  }

  test("exception in metadata fetch escalates to handler"){
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenThrow(new RuntimeException("test exception"))
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks

    client.doFetch(shuffleRequests.map(_._1).toSeq, mockHandler)

    assertResult(1)(mockConnection.requests)

    // upon an errored response, the start handler will not be called
    verify(mockHandler, times(0)).start(any())

    // but the error handler will, and there will be an exception along.
    verify(mockHandler, times(1))
        .transferError(anyString(), any[RuntimeException]())

    // the transport will receive no pending requests (for buffers) for queuing
    verify(mockTransport, times(0)).queuePending(any())
  }

  test("successful buffer fetch") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 25001
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)

    // 10000 in bytes ~ 2500 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we need 10 buffer-lengths to receive all of 25000 rows,
    // the extra one adds 1 receive. Note that each receive is 1 buffer, so that is
    // 11 receives expected.
    val sizePerBuffer = 10000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 11

    withResource(HostMemoryBuffer.allocate(100032)) { refHostBuffer =>
      var count = 0
      (0 until refHostBuffer.getLength.toInt)
          .foreach { off =>
            refHostBuffer.setByte(off, count.toByte)
            count = count + 1
            if (count >= sizePerBuffer) {
              count = 0
            }
          }

      closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
        val db = bounceBuffer.buffer.asInstanceOf[DeviceMemoryBuffer]
        withResource(refHostBuffer.slice(0, sizePerBuffer)) { slice =>
          db.copyFromHostBuffer(slice)
        }
        val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

        assert(brs.hasMoreBlocks)

        // Kick off receives
        client.doIssueBufferReceives(brs)

        (0 until expectedReceives).foreach { _ =>
          assert(brs.hasMoreBlocks)
          client.doHandleBounceBufferReceive(mockTransaction, brs)
        }

        assert(!brs.hasMoreBlocks)

        // we would perform 1 request to issue a `TransferRequest`, so the server can start.
        verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

        // we will hand off a `DeviceMemoryBuffer` to the catalog
        val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
        val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
        verify(client, times(1)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
        verifyTableMeta(tableMeta, tmCaptor.getValue.asInstanceOf[TableMeta])
        verify(mockCatalog, times(1))
            .addBuffer(dmbCaptor.capture(), any(), any())

        val receivedBuff = dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer]
        assertResult(tableMeta.bufferMeta().size())(receivedBuff.getLength)

        withResource(HostMemoryBuffer.allocate(receivedBuff.getLength)) { hostBuff =>
          hostBuff.copyFromDeviceBuffer(receivedBuff)
          (0 until numRows).foreach { r =>
            assertResult(refHostBuffer.getByte(r))(hostBuff.getByte(r))
          }
        }

        // after closing, we should have freed our bounce buffers.
        assert(bounceBuffer.isClosed)
      }
    }
  }

  test("successful buffer fetch - but handler rejected it") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    when(mockHandler.batchReceived(any())).thenReturn(false) // reject incoming batches

    val numRows = 100
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
    val sizePerBuffer = 10000
    val expectedReceives = 1
    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

      assert(brs.hasMoreBlocks)
      // Kick off receives
      client.doIssueBufferReceives(brs)

      (0 until expectedReceives).foreach { _ =>
        assert(brs.hasMoreBlocks)
        client.doHandleBounceBufferReceive(mockTransaction, brs)
      }

      // If transactions are successful, we should have completed the receive
      assert(!brs.hasMoreBlocks)

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(1)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      verifyTableMeta(tableMeta, tmCaptor.getValue.asInstanceOf[TableMeta])
      verify(mockCatalog, times(1))
          .addBuffer(dmbCaptor.capture(), any(), any())

      val receivedBuff = dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer]
      assertResult(tableMeta.bufferMeta().size())(receivedBuff.getLength)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  test("successful buffer fetch multi-buffer") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 500
    val tableMetas =
      (0 until 5).map {
        _ => RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
      }

    // 20000 in bytes ~ 5000 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we can pack 5 device receives into a single bounce buffer
    val sizePerBuffer = 20000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 1

    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMetas, bounceBuffer)

      assert(brs.hasMoreBlocks)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      (0 until expectedReceives).foreach { _ =>
        assert(brs.hasMoreBlocks)
        client.doHandleBounceBufferReceive(mockTransaction, brs)
      }

      val totalExpectedSize = tableMetas.map(tm => tm.bufferMeta().size()).sum

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(5)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      tableMetas.zipWithIndex.foreach { case (tm, ix) =>
        verifyTableMeta(tm, tmCaptor.getAllValues().get(ix).asInstanceOf[TableMeta])
      }

      verify(mockCatalog, times(5))
          .addBuffer(dmbCaptor.capture(), any(), any())

      assertResult(totalExpectedSize)(
        dmbCaptor.getAllValues().toArray().map(_.asInstanceOf[DeviceMemoryBuffer].getLength).sum)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  test("successful buffer fetch multi-buffer, larger than a single bounce buffer") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 500
    val tableMetas =
      (0 until 20).map {
        _ => RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
      }

    // 20000 in bytes ~ 5000 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we can pack 5 device receives into a single bounce buffer
    // we have 20 bounce buffers, so we expect in this case 3 receives.
    val sizePerBuffer = 20000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 3

    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMetas, bounceBuffer)

      assert(brs.hasMoreBlocks)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      (0 until expectedReceives).foreach { _ =>
        assert(brs.hasMoreBlocks)
        client.doHandleBounceBufferReceive(mockTransaction, brs)
      }

      val totalExpectedSize = tableMetas.map(tm => tm.bufferMeta().size()).sum

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(20)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      tableMetas.zipWithIndex.foreach { case (tm, ix) =>
        verifyTableMeta(tm, tmCaptor.getAllValues().get(ix).asInstanceOf[TableMeta])
      }

      verify(mockCatalog, times(20))
          .addBuffer(dmbCaptor.capture(), any(), any())

      assertResult(totalExpectedSize)(
        dmbCaptor.getAllValues().toArray().map(_.asInstanceOf[DeviceMemoryBuffer].getLength).sum)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  private def doTestErrorOrCancelledBufferFetch(status: TransactionStatus.Value): Unit = {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    when(mockTransaction.getStatus).thenReturn(status)
    when(mockTransaction.getErrorMessage).thenReturn(Some(s"Status is: ${status}"))

    val numRows = 100000
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)

    // error condition, so it doesn't matter much what we set here, only the first
    // receive will happen
    val sizePerBuffer = numRows * 4 / 10
    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

      assert(brs.hasMoreBlocks)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      assert(brs.hasMoreBlocks)

      client.doHandleBounceBufferReceive(mockTransaction, brs)

      // Errored transaction. Therefore we should not be done
      assert(brs.hasMoreBlocks)

      // We should have called `transferError` in the `RapidsShuffleFetchHandler` and not
      // pass a throwable, since this was a transport-level exception we caught
      verify(mockHandler, times(10)).transferError(any(), isNull())

      // we would have issued 1 request to issue a `TransferRequest` for the server to start
      verify(mockConnection, times(1)).request(any(), any(), any())

      // ensure we closed the BufferReceiveState => releasing the bounce buffers
      assert(bounceBuffer.isClosed)
    }
  }

  test("errored buffer fetch is handled") {
    doTestErrorOrCancelledBufferFetch(TransactionStatus.Error)
  }

  test("cancelled buffer fetch is handled") {
    doTestErrorOrCancelledBufferFetch(TransactionStatus.Cancelled)
  }


  def makeRequest(numRows: Long): (PendingTransferRequest, HostMemoryBuffer, TableMeta) = {
    val ptr = mock[PendingTransferRequest]
    val mockTable = RapidsShuffleTestHelper.mockTableMeta(numRows)
    when(ptr.getLength).thenReturn(mockTable.bufferMeta().size())
    when(ptr.tableMeta).thenReturn(mockTable)
    when(ptr.handler).thenReturn(mockHandler)
    val buff = HostMemoryBuffer.allocate(mockTable.bufferMeta().size())
    fillBuffer(buff)
    (ptr, buff, mockTable)
  }

  def checkBuffer(
      source: HostMemoryBuffer,
      mockTable: TableMeta,
      consumed: ConsumedBatchFromBounceBuffer): Unit = {
    assertResult(mockTable.bufferMeta().size())(consumed.contigBuffer.getLength)
    assert(areBuffersEqual(source, consumed.contigBuffer))
  }

  class MockBlock(val hmb: HostMemoryBuffer) extends BlockWithSize {
    override def size: Long = hmb.getLength
  }

  case class ReceivedBufferWindow(
      blocksInWindow: Int,
      materializedBatches: Int,
      bytesInWindow: Long,
      isLast: Boolean)

  def endToEndTest(buff: BounceBuffer,
                   expected: Seq[ReceivedBufferWindow],
                   ptrBuffs: Seq[(PendingTransferRequest, HostMemoryBuffer, TableMeta)]): Unit = {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockHandler.getTaskIds).thenReturn(Array[Long](1))
    withResource(ptrBuffs.map(_._2)) { sources =>
      withResource(new BufferReceiveState(123, buff, ptrBuffs.map(_._1), () => {})) { br =>
        val blocks = sources.map(x => new MockBlock(x))
        val sendWindow = new WindowedBlockIterator[MockBlock](blocks, 1000)

        expected.foreach {
          case ReceivedBufferWindow(inWindow, materialized, bytesInWindow, isLast) =>
            br.getBufferWhenReady(tb => {
              val cb = tb.asInstanceOf[CudfTransportBuffer]
              val bb = cb.getMemoryBuffer.asInstanceOf[DeviceMemoryBuffer]
              val ranges = sendWindow.next()
              assertResult(inWindow)(ranges.size)
              var offset = 0L

              ranges.foreach(r => {
                bb.copyFromHostBuffer(offset, r.block.hmb, r.rangeStart, r.rangeSize())
                offset = offset + r.rangeSize()
              })

              val consumed = br.consumeWindow()
              assertResult(materialized)(consumed.size)
              ranges.zip(consumed).foreach {
                case (range: BlockRange[MockBlock], c: ConsumedBatchFromBounceBuffer) =>
                  checkBuffer(range.block.hmb, c.meta, c)
                  c.contigBuffer.close()
              }
              assertResult(!isLast)(br.hasMoreBlocks)
            }, bytesInWindow)
        }
      }
    }
  }

  test("one request spanning several bounce buffer lengths") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(1000)
      val ptrBuffs = rowCounts.map(makeRequest(_))
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ReceivedBufferWindow]()

      // 4 buffer lengths
      (0 until 4).foreach { _ =>
        expected.append(
          ReceivedBufferWindow(
            blocksInWindow = 1,
            materializedBatches = 0,
            bytesInWindow = math.min(1000, remaining),
            isLast = false))
        remaining = remaining - 1000
      }

      // then a last one to finish it off
      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected.toSeq, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("three requests within a bounce buffer") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(40, 50, 60)
      val ptrBuffs = rowCounts.map(makeRequest(_))
      val remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ReceivedBufferWindow]()
      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 3,
          materializedBatches = 3,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected.toSeq, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("three requests spanning two bounce buffer lengths") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(40, 180, 100)
      val ptrBuffs = rowCounts.map(makeRequest(_))
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ReceivedBufferWindow]()
      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 3,
          materializedBatches = 2,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected.toSeq, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("two requests larger than the bounce buffer length") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(300, 300)
      val ptrBuffs = rowCounts.map(makeRequest(_))
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ReceivedBufferWindow]()
      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 1,
          materializedBatches = 0,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 2,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ReceivedBufferWindow(
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected.toSeq, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test("on endpoint failure the iterator is notified if it is registered") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val metaResp = ShuffleMetadata.buildMetaResponse(Seq.empty)
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(metaResp)))

    client.registerPeerErrorListener(mockHandler)
    client.doFetch(Seq(ShuffleBlockBatchId(1,2,3,4)), mockHandler)
    client.close()

    // error expected at the iterator
    verify(mockHandler, times(1)).transferError(any(), any())
  }

  test("on endpoint failure the iterator is not notified if it is done (unregistered)") {
    // This test inherits from RmmSparkRetrySuiteBase, but it sets up Rmm wrong for the current
    // thread, so we have to fix it for all of the tests
    RmmSpark.removeAllCurrentThreadAssociation()
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val metaResp = ShuffleMetadata.buildMetaResponse(Seq.empty)
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(metaResp)))

    client.registerPeerErrorListener(mockHandler)
    client.doFetch(Seq(ShuffleBlockBatchId(1,2,3,4)), mockHandler)
    // tell the client that we are done (task finished or we actually consumed)
    client.unregisterPeerErrorListener(mockHandler)
    client.close()

    // error not expected at the iterator
    verify(mockHandler, times(0)).transferError(any(), any())
  }
}
