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

class GpuColumnVectorSuite extends AnyFunSuite {
  /** Allocates nothing on the GPU, so the tests below need no CUDA context. */
  private def rowsOnly(numRows: Int): ColumnarBatch =
    new ColumnarBatch(Array.empty[SparkColumnVector], numRows)

  test("from(ColumnarBatch) names both counts when the batch has rows but no columns") {
    val e = intercept[IllegalArgumentException] {
      GpuColumnVector.from(rowsOnly(1234))
    }
    assert(e.getMessage.contains("numRows=1234"))
    assert(e.getMessage.contains("numCols=0"))
  }

  test("from(ColumnarBatch) rejects a batch with neither rows nor columns") {
    // The column count alone decides: a 0-row, 0-column batch fails today for the same reason
    // and must keep failing.
    val e = intercept[IllegalArgumentException] {
      GpuColumnVector.from(rowsOnly(0))
    }
    assert(e.getMessage.contains("numRows=0"))
    assert(e.getMessage.contains("numCols=0"))
  }
}
