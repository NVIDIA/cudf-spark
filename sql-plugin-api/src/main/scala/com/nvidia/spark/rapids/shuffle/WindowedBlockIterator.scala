/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

// Helper trait that callers can use to add blocks to the iterator
// as long as they can provide a size
trait BlockWithSize {
  /**
   * Abstract method to return the size in bytes of this block
   * @return Long - size in bytes
   */
  def size: Long
}

/**
 * Specifies a start and end range of bytes for a block.
 * @param block - a BlockWithSize instance
 * @param rangeStart - byte offset for the start of the range (inclusive)
 * @param rangeEnd - byte offset for the end of the range (exclusive)
 * @tparam T - the specific type of `BlockWithSize`
 */
case class BlockRange[T <: BlockWithSize](
    block: T, rangeStart: Long, rangeEnd: Long) {
  require(rangeStart < rangeEnd,
    s"Instantiated a BlockRange with invalid boundaries: $rangeStart to $rangeEnd")

  /**
   * Returns the size of this range in bytes
   * @return - Long - size in bytes
   */
  def rangeSize(): Long = rangeEnd - rangeStart

  def isComplete(): Boolean = rangeEnd == block.size
}

/**
 * Given a set of blocks, this iterator returns BlockRanges
 * of such blocks that fit `windowSize`. The ranges are just logical
 * chunks of the blocks, so this class performs no memory management or copying.
 *
 * If a block is too large for the window, the block will be
 * returned in `next()` until the full block can be covered.
 *
 * For example, given a block that is 4 window-sizes in length:
 * block = [sb1, sb2, sb3, sb4]
 *
 * The window will return on `next()` four "sub-blocks", governed by `windowSize`:
 * window.next() // sb1
 * window.next() // sb2
 * window.next() // sb3
 * window.next() // sb4
 *
 * If blocks are smaller than the `windowSize`, they will be packed:
 * block1 = [b1]
 * block2 = [b2]
 * window.next() // [b1, b2]
 *
 * A mix of both scenarios above is possible:
 * block1 = [sb11, sb12, sb13] // where sb13 is smaller than window length
 * block2 = [b2]
 *
 * window.next() // sb11
 * window.next() // sb12
 * window.next() // [sb13, b2]
 *
 * @param blocks - sequence of blocks to manage
 * @param windowSize - the size (in bytes) that block ranges should fit
 * @tparam T - the specific type of `BlockWithSize`
 * @note this class does not own `transferBlocks`
 * @note this class is not thread safe
 */
class WindowedBlockIterator[T <: BlockWithSize](blocks: Seq[T], windowSize: Long)
    extends Iterator[Seq[BlockRange[T]]] {

  require(windowSize > 0, s"Invalid window size specified $windowSize")

  private case class BlockWindow(start: Long, size: Long) {
    val end = start + size // exclusive end offset
    def move(): BlockWindow = {
      BlockWindow(start + size, size)
    }
  }

  // start the window at byte 0
  private[this] var window = BlockWindow(0, windowSize)
  private[this] var done = false

  // helper class that captures the start/end byte offset
  // for `block` on creation
  private case class BlockWithOffset[T <: BlockWithSize](
      block: T, startOffset: Long, endOffset: Long)

  private[this] val blocksWithOffsets = {
    var lastOffset = 0L
    blocks.map { block =>
      require(block.size > 0, "Invalid 0-byte block")
      val startOffset = lastOffset
      val endOffset = startOffset + block.size
      lastOffset = endOffset // for next block
      BlockWithOffset(block, startOffset, endOffset)
    }
  }

  // the last block index that made it into a window, which
  // is an index into the `blocksWithOffsets` sequence
  private[this] var lastSeenBlock = 0

  case class BlocksForWindow(lastBlockIndex: Option[Int],
      blockRanges: Seq[BlockRange[T]],
      hasMoreBlocks: Boolean)

  private def getBlocksForWindow(
      window: BlockWindow,
      startingBlock: Int): BlocksForWindow = {
    val blockRangesInWindow = new ArrayBuffer[BlockRange[T]]()
    var continue = true
    var thisBlock = startingBlock
    var lastBlockIndex: Option[Int] = None
    while (continue && thisBlock < blocksWithOffsets.size) {
      val b = blocksWithOffsets(thisBlock)
      // if at least 1 byte fits within the window, this block should be included
      if (window.start < b.endOffset && window.end > b.startOffset) {
        var rangeStart = window.start - b.startOffset
        if (rangeStart < 0) {
          rangeStart = 0
        }
        var rangeEnd = window.end - b.startOffset
        if (window.end >= b.endOffset) {
          rangeEnd = b.endOffset - b.startOffset
        }
        blockRangesInWindow.append(BlockRange[T](b.block, rangeStart, rangeEnd))
        lastBlockIndex = Some(thisBlock)
      } else {
        // skip this block, unless it's before our window starts
        continue = b.endOffset <= window.start
      }
      thisBlock = thisBlock + 1
    }
    val lastBlock = blockRangesInWindow.last
    BlocksForWindow(lastBlockIndex,
      blockRangesInWindow.toSeq,
      !continue || !lastBlock.isComplete())
  }

  def next(): Seq[BlockRange[T]] = {
    if (!hasNext) {
      throw new NoSuchElementException(s"BounceBufferWindow $window has been exhausted.")
    }

    val blocksForWindow = getBlocksForWindow(window, lastSeenBlock)
    lastSeenBlock = blocksForWindow.lastBlockIndex.getOrElse(0)

    if (blocksForWindow.hasMoreBlocks) {
      window = window.move()
    } else {
      done = true
    }

    blocksForWindow.blockRanges
  }

  override def hasNext: Boolean = !done && blocksWithOffsets.nonEmpty
}
