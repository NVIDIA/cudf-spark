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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class InputFileNameBatchSlicerSuite extends RmmSparkRetrySuiteBase {
  private val Limit = 1000L + (1L << 20) // 1000 string bytes beyond the headroom

  private def intBatch(start: Int, n: Int): ColumnarBatch = {
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(start until start + n: _*), IntegerType)),
      n)
  }

  private def values(b: ColumnarBatch): Seq[Int] = {
    withResource(b.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { h =>
      (0 until b.numRows()).map(h.getInt)
    }
  }

  test("maxRowsPerBatch boundaries") {
    val path = 135
    val max = InputFileNameBatchSlicer.maxRowsPerBatch(path)
    assert(max.toLong * path <= Int.MaxValue)
    assert((max + 1L) * path > Int.MaxValue - (1L << 20))
    assert(InputFileNameBatchSlicer.maxRowsPerBatch(0) > 0)
    assertResult(1)(InputFileNameBatchSlicer.maxRowsPerBatch(Int.MaxValue))
    assertResult(1)(InputFileNameBatchSlicer.maxRowsPerBatch(4096, 100L))
  }

  test("batch at the limit passes through, one more row is sliced") {
    val max = InputFileNameBatchSlicer.maxRowsPerBatch(10, Limit)
    val b = intBatch(0, max)
    val same = new InputFileNameSlicingIterator(Iterator(b), () => 10, Limit)
    withResource(same.next()) { out => assert(out eq b) }
    assert(!same.hasNext)

    val split = new InputFileNameSlicingIterator(Iterator(intBatch(0, max + 1)), () => 10, Limit)
    val sizes = scala.collection.mutable.ArrayBuffer[Int]()
    while (split.hasNext) withResource(split.next())(o => sizes += o.numRows())
    assertResult(Seq(max, 1))(sizes.toSeq)
  }

  test("slices are ordered and path is captured once per source batch") {
    var path = 100 // 100 bytes -> ~10 rows per slice with Limit
    val max1 = InputFileNameBatchSlicer.maxRowsPerBatch(100, Limit)
    val max2 = InputFileNameBatchSlicer.maxRowsPerBatch(500, Limit)
    val n1 = max1 * 2 + 3
    val n2 = max2 + 1
    val src = Iterator(intBatch(0, n1), intBatch(n1, n2))
    val iter = new InputFileNameSlicingIterator(
      new Iterator[ColumnarBatch] {
        def hasNext: Boolean = src.hasNext
        def next(): ColumnarBatch = {
          val b = src.next()
          path = if (b.numRows() == n1) 100 else 500
          b
        }
      }, () => path, Limit)
    val sizes = scala.collection.mutable.ArrayBuffer[Int]()
    val vals = scala.collection.mutable.ArrayBuffer[Int]()
    while (iter.hasNext) {
      withResource(iter.next()) { o => sizes += o.numRows(); vals ++= values(o) }
    }
    assertResult(Seq(max1, max1, 3, max2, 1))(sizes.toSeq)
    assertResult(0 until n1 + n2)(vals.toSeq)
  }

  test("zero column batches are sliced by row count") {
    val iter = new InputFileNameSlicingIterator(
      Iterator(new ColumnarBatch(Array.empty, 25)), () => 100, 1000L + (1L << 20))
    val sizes = scala.collection.mutable.ArrayBuffer[Int]()
    while (iter.hasNext) withResource(iter.next()) { o =>
      assertResult(0)(o.numCols())
      sizes += o.numRows()
    }
    assertResult(Seq(10, 10, 5))(sizes.toSeq)
  }

  test("source is not advanced while slices are pending") {
    var pulled = 0
    val src = new Iterator[ColumnarBatch] {
      def hasNext: Boolean = pulled < 2
      def next(): ColumnarBatch = { pulled += 1; intBatch(0, 25) }
    }
    val iter = new InputFileNameSlicingIterator(src, () => 100, Limit)
    withResource(iter.next())(_ => ())
    assertResult(1)(pulled)
    withResource(iter.next())(_ => ())
    assert(iter.hasNext)
    assertResult(1)(pulled)
    withResource(iter.next())(_ => ())
    assertResult(1)(pulled)
    assert(iter.hasNext) // the second source batch
    withResource(iter.next())(_ => ())
    assertResult(2)(pulled)
  }

  private def deviceHandles: Int = SpillFramework.stores.deviceStore.numHandles

  test("abandoned iterator releases its pending batch") {
    val before = deviceHandles
    val iter = new InputFileNameSlicingIterator(
      Iterator(intBatch(0, 25)), () => 100, Limit)
    withResource(iter.next())(_ => ())
    assertResult(before + 1)(deviceHandles) // the rest of the source batch is pending
    iter.close()
    assertResult(before)(deviceHandles)
    iter.close() // idempotent
    assertResult(before)(deviceHandles)
  }

  test("exhausted iterator holds nothing") {
    val before = deviceHandles
    val iter = new InputFileNameSlicingIterator(
      Iterator(intBatch(0, 25)), () => 100, Limit)
    while (iter.hasNext) withResource(iter.next())(_ => ())
    assertResult(before)(deviceHandles)
  }

  test("slicing survives injected retry OOM") {
    val iter = new InputFileNameSlicingIterator(
      Iterator(intBatch(0, 25)), () => 100, Limit)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val vals = scala.collection.mutable.ArrayBuffer[Int]()
    while (iter.hasNext) withResource(iter.next())(o => vals ++= values(o))
    assertResult(0 until 25)(vals.toSeq)
  }
}
