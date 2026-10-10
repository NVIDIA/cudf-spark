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
