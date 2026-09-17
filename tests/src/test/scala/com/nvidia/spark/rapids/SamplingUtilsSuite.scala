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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

/** `sorter` and `converter` are null: with no sample collected, both samplers return early. */
class SamplingUtilsSuite extends AnyFunSuite {
  private def rowsOnly(numRows: Int): ColumnarBatch =
    new ColumnarBatch(Array.empty[SparkColumnVector], numRows)

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
