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

package com.nvidia.spark.rapids.shuffle

import java.util.NoSuchElementException

import org.mockito.Mockito._

class WindowedBlockIteratorSuite extends RapidsShuffleTestHelper {
  test ("empty iterator throws on next") {
    val wbi = new WindowedBlockIterator[BlockWithSize](Seq.empty, 1024)
    assertResult(false)(wbi.hasNext)
    assertThrows[NoSuchElementException](wbi.next)
  }

  test ("1-byte+ ranges are allowed, but 0-byte or negative ranges are not") {
    assertResult(1)(BlockRange(null, 123, 124).rangeSize())
    assertResult(2)(BlockRange(null, 123, 125).rangeSize())
    assertThrows[IllegalArgumentException](BlockRange(null, 123, 123))
    assertThrows[IllegalArgumentException](BlockRange(null, 123, 122))
  }

  test ("0-byte blocks are not allowed") {
    val block = mock[BlockWithSize]
    when(block.size).thenReturn(0)
    assertThrows[IllegalArgumentException](
      new WindowedBlockIterator[BlockWithSize](Seq(block), 1024))
  }

  test ("1024 1-byte blocks all fit in 1 1024-byte window") {
    val mockBlocks = (0 until 1024).map { _ =>
      val block = mock[BlockWithSize]
      when(block.size).thenReturn(1)
      block
    }
    val wbi = new WindowedBlockIterator[BlockWithSize](mockBlocks, 1024)
    assertResult(true)(wbi.hasNext)
    val blockRange = wbi.next()
    assertResult(1024)(blockRange.size)
    blockRange.foreach { br =>
      assertResult(1)(br.rangeSize())
      assertResult(0)(br.rangeStart)
      assertResult(1)(br.rangeEnd)
    }
    assertResult(false)(wbi.hasNext)
    assertThrows[NoSuchElementException](wbi.next)
  }

  test ("a block larger than the window is split between calls to next") {
    val block = mock[BlockWithSize]
    when(block.size).thenReturn(2049)

    val wbi = new WindowedBlockIterator[BlockWithSize](Seq(block), 1024)
    assertResult(true)(wbi.hasNext)
    val blockRanges = wbi.next()
    assertResult(1)(blockRanges.size)

    val blockRange = blockRanges.head
    assertResult(1024)(blockRange.rangeSize())
    assertResult(0)(blockRange.rangeStart)
    assertResult(1024)(blockRange.rangeEnd)
    assertResult(true)(wbi.hasNext)

    val blockRangesMiddle = wbi.next()
    val blockRangeMiddle = blockRangesMiddle.head
    assertResult(1024)(blockRangeMiddle.rangeSize())
    assertResult(1024)(blockRangeMiddle.rangeStart)
    assertResult(2048)(blockRangeMiddle.rangeEnd)
    assertResult(true)(wbi.hasNext)

    val blockRangesLastByte = wbi.next()
    val blockRangeLastByte = blockRangesLastByte.head
    assertResult(1)(blockRangeLastByte.rangeSize())
    assertResult(2048)(blockRangeLastByte.rangeStart)
    assertResult(2049)(blockRangeLastByte.rangeEnd)

    assertResult(false)(wbi.hasNext)
    assertThrows[NoSuchElementException](wbi.next)
  }

  test ("a block fits entirely, but a subsequent block doesn't") {
    val block = mock[BlockWithSize]
    when(block.size).thenReturn(1000)

    val block2 = mock[BlockWithSize]
    when(block2.size).thenReturn(1000)

    val wbi = new WindowedBlockIterator[BlockWithSize](Seq(block, block2), 1024)
    assertResult(true)(wbi.hasNext)
    val blockRanges = wbi.next()
    assertResult(2)(blockRanges.size)

    val Seq(firstBlock, secondBlock) = blockRanges

    assertResult(1000)(firstBlock.rangeSize())
    assertResult(0)(firstBlock.rangeStart)
    assertResult(1000)(firstBlock.rangeEnd)
    assertResult(true)(wbi.hasNext)

    assertResult(24)(secondBlock.rangeSize())
    assertResult(0)(secondBlock.rangeStart)
    assertResult(24)(secondBlock.rangeEnd)
    assertResult(true)(wbi.hasNext)

    val blockRangesLastByte = wbi.next()
    val blockRangeLastByte = blockRangesLastByte.head
    assertResult(976)(blockRangeLastByte.rangeSize())
    assertResult(24)(blockRangeLastByte.rangeStart)
    assertResult(1000)(blockRangeLastByte.rangeEnd)

    assertResult(false)(wbi.hasNext)
    assertThrows[NoSuchElementException](wbi.next)
  }
}
