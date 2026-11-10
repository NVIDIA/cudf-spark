/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
import org.apache.spark.sql.types.{IntegerType, StringType}
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

  // The first host allocation of a batch is in the copy of the converted rows to the host.
  test("Accelerated columnar to row with CPU retry OOM during the host copy") {
    val aCol2RowIter = new AcceleratedColumnarToRowIterator(
      attrs,
      Iterator(buildBatch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 0)
    assertResult((0 until NUM_ROWS).toList)(aCol2RowIter.map(_.getInt(0)).toList)
  }

  test("Accelerated columnar to row with CPU split and retry OOM during the host copy") {
    val aCol2RowIter = new AcceleratedColumnarToRowIterator(
      attrs,
      Iterator(buildBatch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 0)
    assertResult((0 until NUM_ROWS).toList)(aCol2RowIter.map(_.getInt(0)).toList)
  }

  // A STRING column takes the generic conversion kernel, whose rows have variable lengths.
  test("Accelerated columnar to row with CPU split and retry OOM during the host copy, " +
      "STRING") {
    val values = Seq(null, "", "a", "\u65e5\u672c\u8a9e", "caf\u00e9", "\ud83d\ude42x",
      "longer than one word")
    val strs = (0 until NUM_ROWS).map(i => values(i % values.length))
    val strAttrs = attrs :+ AttributeReference("s", StringType, nullable = true)()
    val batch = new ColumnarBatch(Array(
      GpuColumnVector.from(ColumnVector.fromInts(0 until NUM_ROWS: _*), IntegerType),
      GpuColumnVector.from(ColumnVector.fromStrings(strs: _*), StringType)), NUM_ROWS)
    val aCol2RowIter = new AcceleratedColumnarToRowIterator(
      strAttrs,
      Iterator(batch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 0)
    val rows = aCol2RowIter.map { r =>
      (r.getInt(0), if (r.isNullAt(1)) null else r.getUTF8String(1).toString)
    }.toList
    assertResult((0 until NUM_ROWS).zip(strs).toList)(rows)
  }

}
