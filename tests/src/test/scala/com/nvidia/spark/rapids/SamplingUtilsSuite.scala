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

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

class SamplingUtilsSuite extends RmmSparkRetrySuiteBase {
  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val attr = AttributeReference(ref.name, ref.dataType, ref.nullable)()
  private val sorter = new GpuSorter(Seq(SortOrder(ref, Ascending)), Array(attr),
    Map.empty[String, GpuMetric])
  private val converter = GpuColumnarToRowExec.makeIteratorFunc(sorter.projectedBatchSchema,
    NoopMetric, NoopMetric, NoopMetric, NoopMetric)

  private def rowsOnly(numRows: Int): ColumnarBatch =
    new ColumnarBatch(Array.empty[SparkColumnVector], numRows)

  private def batch(values: Seq[Int]): ColumnarBatch =
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(values: _*), IntegerType)), values.size)

  private def checkSample(sample: Array[InternalRow], count: Int, inputRows: Int): Unit = {
    val values = sample.map(_.getInt(0))
    assert(values.length === count)
    assert(values.distinct.length === count)
    assert(values.forall(v => v >= 0 && v < inputRows))
  }

  test("reservoirSampleAndCount selects the requested number of distinct rows") {
    for {
      rows <- Seq(0, 1, 2, 3, 100, 101)
      count <- Seq(0, rows / 2, math.max(0, rows / 2 - 1),
        math.min(rows, rows / 2 + 1), math.max(0, rows - 1), rows).distinct
    } {
      withClue(s"rows=$rows count=$count: ") {
        val (sample, numRows) = SamplingUtils.reservoirSampleAndCount(
          Iterator.single(batch(0 until rows)), count, sorter, converter, 7L)
        assert(numRows === rows.toLong)
        checkSample(sample, count, rows)
      }
    }
  }

  test("randomResample maintains the requested fraction across batches") {
    val sample = SamplingUtils.randomResample(
      (0 until 300).grouped(100).map(batch), 0.6, sorter, converter, 7L)
    checkSample(sample, 180, 300)
  }

  test("randomResample preserves fixed-seed results across batches and fraction boundaries") {
    // Expected rows were captured from the implementation before shortening resource lifetimes.
    val expectedSamples = Seq(
      0.0 -> Seq.empty[Int],
      0.25 -> Seq(18, 19, 9, 12, 0, 3),
      0.75 -> Seq(16, 17, 20, 21, 22, 23, 8, 10, 11, 13, 14, 15, 1, 2, 4, 5, 6, 7),
      1.0 -> ((16 until 24) ++ (8 until 16) ++ (0 until 8)))
    for ((fraction, expected) <- expectedSamples) {
      val sample = SamplingUtils.randomResample(
        (0 until 24).grouped(8).map(batch), fraction, sorter, converter, 7L)
      checkSample(sample, (24 * fraction).toInt, 24)
      assert(sample.map(_.getInt(0)).sorted.toSeq === expected.sorted)
    }
  }

  test("reservoirSampleAndCount preserves fixed-seed results with and without dropping rows") {
    for (count <- Seq(0, 4, 32)) {
      val (sample, numRows) = SamplingUtils.reservoirSampleAndCount(
        (0 until 24).grouped(8).map(batch), count, sorter, converter, 7L)
      assert(numRows === 24L)
      checkSample(sample, math.min(count, 24), 24)
      // Set iteration order can differ between Scala versions; the same seed must be repeatable.
      val (repeated, repeatedNumRows) = SamplingUtils.reservoirSampleAndCount(
        (0 until 24).grouped(8).map(batch), count, sorter, converter, 7L)
      assert(repeatedNumRows === numRows)
      assert(sample.map(_.getInt(0)).toSeq === repeated.map(_.getInt(0)).toSeq)
    }
  }

  for (resample <- Seq(false, true)) {
    test(s"sampling releases both inputs when concatenation fails: resample=$resample") {
      val allocatedBefore = Rmm.getTotalBytesAllocated
      val handlesBefore = SpillFramework.stores.deviceStore.numHandles
      val input = (0 until 16).grouped(8).zipWithIndex.map { case (values, index) =>
        val cb = batch(values)
        if (index == 1) {
          // Selecting every row requires no allocation. Fail the following concatenation.
          RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
        }
        cb
      }
      val error = intercept[GpuRetryOOM] {
        if (resample) {
          SamplingUtils.randomResample(input, 1.0, sorter, converter, 7L)
        } else {
          SamplingUtils.reservoirSampleAndCount(input, 32, sorter, converter, 7L)
        }
      }
      assert(error.getStackTrace.exists(frame =>
        frame.getClassName == "ai.rapids.cudf.Table" && frame.getMethodName == "concatenate"))
      assert(SpillFramework.stores.deviceStore.numHandles === handlesBefore)
      assert(Rmm.getTotalBytesAllocated === allocatedBefore)
    }
  }

  test("reservoirSampleAndCount retains the requested rows from the previous sample") {
    val (sample, numRows) = SamplingUtils.reservoirSampleAndCount(
      Iterator(0 until 1000, 1000 until 1100).map(batch), 100, sorter, converter, 7L)
    assert(numRows === 1100L)
    checkSample(sample, 100, 1100)
  }

  test("reservoirSampleAndCount counts rows-only batches and samples nothing") {
    val (sample, numRows) = SamplingUtils.reservoirSampleAndCount(
      Seq(4000, 1, 0, 123).iterator.map(rowsOnly), 100, null, null, 0L)
    assert(sample.isEmpty)
    // The 0 is deliberate: isRowsOnly tests the column count only.
    assert(numRows === 4124)
  }

  test("reservoirSampleAndCount on an empty input returns no rows") {
    val (sample, numRows) = SamplingUtils.reservoirSampleAndCount(
      Iterator.empty, 100, null, null, 0L)
    assert(sample.isEmpty)
    assert(numRows === 0)
  }

  test("randomResample samples nothing from rows-only batches") {
    val sample = SamplingUtils.randomResample(
      Seq(4000, 1, 0, 123).iterator.map(rowsOnly), 0.1, null, null, 0L)
    assert(sample.isEmpty)
  }
}
