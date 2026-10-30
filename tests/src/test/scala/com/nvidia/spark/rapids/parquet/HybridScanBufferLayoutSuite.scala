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

package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.ByteRange
import com.nvidia.spark.rapids.parquet.HybridScanParquetPartitionReader.{hostBufferLayout, HostBufferLayout}
import org.scalatest.funsuite.AnyFunSuite

/**
 * The layout is what makes the hybrid reader's IO coalesce: ranges adjacent in the file have to
 * be adjacent in the host buffer, or `coalesceReads` sees nothing to merge and the read is split
 * into one request per column chunk.
 */
class HybridScanBufferLayoutSuite extends AnyFunSuite {

  private def layoutOf(ranges: (Long, Long)*): (Array[ByteRange], HostBufferLayout) = {
    val byteRanges = ranges.map { case (offset, size) => new ByteRange(offset, size) }.toArray
    (byteRanges, hostBufferLayout(byteRanges))
  }

  /** Whether the two ranges, named in request order, occupy one unbroken span of the buffer. */
  private def isContiguous(
      layout: HostBufferLayout,
      ranges: Array[ByteRange],
      first: Int,
      second: Int): Boolean = {
    layout.offsets(first) + ranges(first).size() == layout.offsets(second)
  }

  test("no ranges need no buffer") {
    val (_, layout) = layoutOf()
    assert(layout.totalSize == 0)
    assert(layout.fileOrder.isEmpty)
    assert(layout.offsets.isEmpty)
  }

  test("one range starts at the beginning of the buffer") {
    val (_, layout) = layoutOf((100L, 30L))
    assert(layout.totalSize == 30)
    assert(layout.offsets.toSeq == Seq(0L))
    assert(layout.fileOrder.toSeq == Seq(0))
  }

  test("the buffer holds exactly the bytes asked for") {
    val (_, layout) = layoutOf((0L, 10L), (50L, 25L), (200L, 5L))
    assert(layout.totalSize == 40)
  }

  test("ranges adjacent in the file are adjacent in the buffer") {
    val (ranges, layout) = layoutOf((0L, 10L), (10L, 20L), (30L, 5L))
    assert(isContiguous(layout, ranges, 0, 1))
    assert(isContiguous(layout, ranges, 1, 2))
  }

  test("ranges adjacent in the file are adjacent in the buffer whatever order they arrive in") {
    // Nothing requires the reader to list ranges in file order, even when the chunks they name are
    // neighbors.
    val (ranges, layout) = layoutOf((30L, 5L), (0L, 10L), (10L, 20L))
    assert(layout.fileOrder.toSeq == Seq(1, 2, 0))
    assert(isContiguous(layout, ranges, 1, 2))
    assert(isContiguous(layout, ranges, 2, 0))
    assert(layout.offsets.toSeq == Seq(30L, 0L, 10L))
  }

  test("a gap in the file is not a gap in the buffer") {
    val (ranges, layout) = layoutOf((0L, 10L), (1000L, 20L))
    assert(isContiguous(layout, ranges, 0, 1))
    assert(layout.totalSize == 30)
  }

  test("every range gets its own span of the buffer") {
    val (ranges, layout) = layoutOf((500L, 8L), (0L, 16L), (100L, 4L), (64L, 32L))
    val spans = layout.fileOrder.map { i =>
      (layout.offsets(i), layout.offsets(i) + ranges(i).size())
    }
    spans.zip(spans.tail).foreach { case ((_, prevEnd), (nextStart, _)) =>
      assert(prevEnd == nextStart)
    }
    assert(spans.head._1 == 0)
    assert(spans.last._2 == layout.totalSize)
  }

  test("an empty range takes no space and leaves its neighbors adjacent") {
    val (ranges, layout) = layoutOf((0L, 10L), (10L, 0L), (10L, 20L))
    assert(layout.totalSize == 30)
    assert(isContiguous(layout, ranges, 0, 2))
  }
}
