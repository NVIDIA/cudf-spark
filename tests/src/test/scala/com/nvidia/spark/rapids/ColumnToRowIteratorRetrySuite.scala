/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnToRowIteratorRetrySuite extends RmmSparkRetrySuiteBase {

  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val attrs = Seq(AttributeReference(ref.name, ref.dataType, ref.nullable)())
  private val NUM_ROWS = 50

  private def buildBatch: ColumnarBatch = {
    val ints = 0 until NUM_ROWS
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), NUM_ROWS)
  }

  test("Accelerated columnar to row with retry OOM") {
    val aCol2RowIter = new AcceleratedColumnarToRowIterator(
      attrs,
      Iterator(buildBatch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    var numRows = 0
    aCol2RowIter.foreach { _ =>
      numRows += 1
    }
    assertResult(NUM_ROWS)(numRows)
  }

  test("Accelerated columnar_to_row with split and retry OOM") {
    val aCol2RowIter = new AcceleratedColumnarToRowIterator(
      attrs,
      Iterator(buildBatch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    var numRows = 0
    aCol2RowIter.foreach { _ =>
      numRows += 1
    }
    assertResult(NUM_ROWS)(numRows)
  }

}
