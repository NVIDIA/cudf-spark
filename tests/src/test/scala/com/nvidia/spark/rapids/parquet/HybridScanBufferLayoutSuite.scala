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
import com.nvidia.spark.rapids.parquet.HybridScanPipeline.{deviceBufferLayout, hostBufferLayout, DeviceBufferLayout, GDS_ALIGNMENT, HostBufferLayout}
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

  private val largeFile = 1L << 40

  private def byteRanges(ranges: Seq[(Long, Long)]): Array[ByteRange] =
    ranges.map { case (offset, size) => new ByteRange(offset, size) }.toArray

  private def deviceLayoutOf(ranges: (Long, Long)*): DeviceBufferLayout =
    deviceBufferLayout(byteRanges(ranges), largeFile)

  private def readsOf(layout: DeviceBufferLayout): Seq[(Long, Long, Long)] =
    layout.reads.map(r => (r.getInputOffset, r.getLength, r.getOutputOffset))

  private def fileByte(position: Long): Byte = (position % 251).toByte

  /**
   * Issue the layout's reads against a file of `fileSize` bytes into a buffer, and check that
   * every read stays in the file and the buffer and that every range's slice holds its bytes.
   */
  private def assertSlicesHoldTheirBytes(ranges: Seq[(Long, Long)], fileSize: Long): Unit = {
    val requested = byteRanges(ranges)
    val layout = deviceBufferLayout(requested, fileSize)
    val buffer = new Array[Byte](layout.totalSize.toInt)
    layout.reads.foreach { r =>
      assert(r.getInputOffset >= 0 && r.getInputOffset + r.getLength <= fileSize, readsOf(layout))
      assert(r.getOutputOffset >= 0 && r.getOutputOffset + r.getLength <= layout.totalSize)
      (0L until r.getLength).foreach { j =>
        buffer((r.getOutputOffset + j).toInt) = fileByte(r.getInputOffset + j)
      }
    }
    requested.indices.foreach { i =>
      val r = requested(i)
      val slice = buffer.slice(layout.offsets(i).toInt, (layout.offsets(i) + r.size()).toInt)
      assert(slice.toSeq == (r.offset() until r.offset() + r.size()).map(fileByte), s"range $i")
    }
  }

  test("no ranges need no device buffer") {
    val layout = deviceLayoutOf()
    assert(layout.totalSize == 0)
    assert(layout.reads.isEmpty)
  }

  test("a range is read as the whole blocks it touches and sliced at its offset in them") {
    val layout = deviceLayoutOf((5000L, 100L))
    assert(readsOf(layout) == Seq((GDS_ALIGNMENT, GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(904L))
    assert(layout.totalSize == GDS_ALIGNMENT)
  }

  test("a range crossing a block boundary reads every block it touches") {
    val layout = deviceLayoutOf((4000L, 200L))
    assert(readsOf(layout) == Seq((0L, 2 * GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(4000L))
  }

  test("ranges far apart in the file get their own aligned spans") {
    val layout = deviceLayoutOf((9000L, 10L), (10L, 10L), (20L, 5L))
    assert(readsOf(layout) == Seq(
      (0L, GDS_ALIGNMENT, 0L),
      (2 * GDS_ALIGNMENT, GDS_ALIGNMENT, GDS_ALIGNMENT)))
    assert(layout.offsets.toSeq == Seq(GDS_ALIGNMENT + 808L, 10L, 20L))
    assert(layout.totalSize == 2 * GDS_ALIGNMENT)
  }

  test("ranges that only share a block are read once") {
    val layout = deviceLayoutOf((3000L, 10L), (100L, 10L))
    assert(readsOf(layout) == Seq((0L, GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(3000L, 100L))
  }

  test("ranges in touching blocks are read together") {
    val layout = deviceLayoutOf((100L, 10L), (4200L, 10L))
    assert(readsOf(layout) == Seq((0L, 2 * GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(100L, 4200L))
  }

  test("a range nested in an earlier one does not shorten its span") {
    val layout = deviceLayoutOf((100L, 5000L), (200L, 10L))
    assert(readsOf(layout) == Seq((0L, 2 * GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(100L, 200L))
  }

  test("overlapping and repeated ranges each slice their own bytes") {
    assertSlicesHoldTheirBytes(
      Seq((100L, 5000L), (200L, 10L), (4000L, 2000L), (200L, 10L), (5990L, 20L), (9000L, 1L)),
      largeFile)
  }

  test("an empty range is not read") {
    val layout = deviceLayoutOf((0L, 10L), (10L, 0L), (10L, 20L))
    assert(readsOf(layout) == Seq((0L, GDS_ALIGNMENT, 0L)))
    assert(layout.offsets.toSeq == Seq(0L, 0L, 10L))
  }

  test("every read is aligned when the file ends on a block boundary") {
    val layout = deviceBufferLayout(
      byteRanges(Seq((500L, 8L), (0L, 16L), (12345L, 6789L), (2 * GDS_ALIGNMENT, 100L))),
      8 * GDS_ALIGNMENT)
    layout.reads.foreach { r =>
      assert(r.getInputOffset % GDS_ALIGNMENT == 0, readsOf(layout))
      assert(r.getLength % GDS_ALIGNMENT == 0, readsOf(layout))
      assert(r.getOutputOffset % GDS_ALIGNMENT == 0, readsOf(layout))
    }
  }

  test("the last partial block of the file is a read of its own") {
    val layout = deviceBufferLayout(byteRanges(Seq((100L, 9900L))), 10000L)
    assert(readsOf(layout) == Seq(
      (0L, 2 * GDS_ALIGNMENT, 0L),
      (2 * GDS_ALIGNMENT, 10000L - 2 * GDS_ALIGNMENT, 2 * GDS_ALIGNMENT)))
    assert(layout.totalSize == 3 * GDS_ALIGNMENT)
  }

  test("a file smaller than a block is read in one partial read") {
    val layout = deviceBufferLayout(byteRanges(Seq((10L, 20L), (500L, 100L))), 600L)
    assert(readsOf(layout) == Seq((0L, 600L, 0L)))
  }

  test("a range past the end of the file is rejected") {
    assertThrows[IllegalArgumentException] {
      deviceBufferLayout(byteRanges(Seq((100L, 50L), (4990L, 11L))), 5000L)
    }
  }

  test("slices hold their bytes when the reads stop at the end of the file") {
    assertSlicesHoldTheirBytes(Seq((4500L, 500L), (100L, 50L), (4990L, 10L)), 5000L)
    assertSlicesHoldTheirBytes(Seq((100L, 9900L), (8200L, 1800L)), 10000L)
  }
}
