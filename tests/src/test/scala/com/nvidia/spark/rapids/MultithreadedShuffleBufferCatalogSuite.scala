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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

class MultithreadedShuffleBufferCatalogSuite
    extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  test("registered shuffles should be active") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    assertResult(false)(catalog.hasActiveShuffle(123))
    catalog.registerShuffle(123)
    assertResult(true)(catalog.hasActiveShuffle(123))
    catalog.unregisterShuffle(123)
    assertResult(false)(catalog.hasActiveShuffle(123))
  }

  test("addPartition and hasData") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)

    val blockId = ShuffleBlockId(1, 0L, 0)
    assertResult(false)(catalog.hasData(blockId))

    catalog.addPartition(1, 0L, 0, handle, 0, 100)
    assertResult(true)(catalog.hasData(blockId))

    catalog.unregisterShuffle(1)
    verify(handle).close()
  }

  test("getMergedBuffer returns correct data") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4, 5))

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle, 0, 5)

    val buffer = catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    assertResult(5)(buffer.size())

    val byteBuffer = buffer.nioByteBuffer()
    assertResult(1)(byteBuffer.get(0))
    assertResult(5)(byteBuffer.get(4))

    catalog.unregisterShuffle(1)
  }

  test("getMergedBatchBuffer returns correct data for multiple partitions") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    catalog.registerShuffle(1)
    // Add 3 partitions from the same handle
    catalog.addPartition(1, 0L, 0, handle, 0, 3)   // bytes 0-2
    catalog.addPartition(1, 0L, 1, handle, 3, 3)   // bytes 3-5
    catalog.addPartition(1, 0L, 2, handle, 6, 4)   // bytes 6-9

    // Request batch containing partitions 0, 1, 2
    val batchId = ShuffleBlockBatchId(1, 0L, 0, 3)
    val buffer = catalog.getMergedBatchBuffer(batchId)
    assertResult(10)(buffer.size())

    catalog.unregisterShuffle(1)
  }

  test("unregisterShuffle closes all handles") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle1 = createMockHandle()
    val handle2 = createMockHandle()

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle1, 0, 100)
    catalog.addPartition(1, 1L, 0, handle2, 0, 100)

    catalog.unregisterShuffle(1)

    verify(handle1).close()
    verify(handle2).close()
  }

  test("unregisterShuffle closes each handle only once") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)
    // Same handle used for multiple partitions
    catalog.addPartition(1, 0L, 0, handle, 0, 100)
    catalog.addPartition(1, 0L, 1, handle, 100, 100)
    catalog.addPartition(1, 0L, 2, handle, 200, 100)

    catalog.unregisterShuffle(1)

    // Should only be closed once
    verify(handle, times(1)).close()
  }

  test("unregisterShuffle handles close exception gracefully") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()
    // Use RuntimeException since close() doesn't declare checked exceptions
    doThrow(new RuntimeException("Test exception")).when(handle).close()

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle, 0, 100)

    // Should not throw
    catalog.unregisterShuffle(1)

    verify(handle).close()
  }

  test("getMergedBuffer throws for non-existent block") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)

    assertThrows[IllegalArgumentException] {
      catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    }

    catalog.unregisterShuffle(1)
  }

  test("getMergedBatchBuffer throws for non-existent blocks") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)

    assertThrows[IllegalArgumentException] {
      catalog.getMergedBatchBuffer(ShuffleBlockBatchId(1, 0L, 0, 3))
    }

    catalog.unregisterShuffle(1)
  }

  test("empty partitions are skipped") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)

    // Adding partition with length 0 should be skipped
    catalog.addPartition(1, 0L, 0, handle, 0, 0)
    assertResult(false)(catalog.hasData(ShuffleBlockId(1, 0L, 0)))

    // Adding partition with length > 0 should work
    catalog.addPartition(1, 0L, 1, handle, 0, 100)
    assertResult(true)(catalog.hasData(ShuffleBlockId(1, 0L, 1)))

    catalog.unregisterShuffle(1)
  }

  test("multiple batches for same partition are accumulated") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle1 = createMockHandleWithData(Array[Byte](1, 2, 3))
    val handle2 = createMockHandleWithData(Array[Byte](4, 5, 6))

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle1, 0, 3)
    catalog.addPartition(1, 0L, 0, handle2, 0, 3)  // Same partition, different batch

    val buffer = catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    assertResult(6)(buffer.size())  // Should contain data from both batches

    catalog.unregisterShuffle(1)
  }

  private def createMockHandle(): SpillablePartialFileHandle = {
    val handle = mock[SpillablePartialFileHandle]
    handle
  }

  private def createMockHandleWithData(data: Array[Byte]): SpillablePartialFileHandle = {
    val handle = mock[SpillablePartialFileHandle]
    when(handle.readAt(anyLong(), any[Array[Byte]](), anyInt(), anyInt()))
      .thenAnswer(invocation => {
        val position = invocation.getArgument[Long](0)
        val bytes = invocation.getArgument[Array[Byte]](1)
        val offset = invocation.getArgument[Int](2)
        val length = invocation.getArgument[Int](3)
        val actualLength = math.min(length, (data.length - position).toInt)
        if (actualLength <= 0) {
          -1
        } else {
          System.arraycopy(data, position.toInt, bytes, offset, actualLength)
          actualLength
        }
      })
    handle
  }
}

