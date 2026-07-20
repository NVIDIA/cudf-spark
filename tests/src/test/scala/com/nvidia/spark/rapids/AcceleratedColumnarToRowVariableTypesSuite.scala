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

import java.math.{BigDecimal => JBigDecimal}

import ai.rapids.cudf.ColumnVector

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * End-to-end coverage for the AcceleratedColumnarToRow path on types that were previously
 * forced to fall back to the row-by-row CPU iterator: DECIMAL128 (precision > 18) and STRING.
 * These exercise the matching CudfUnsafeRow decoders and the spark-rapids-jni row_conversion
 * fixes for the variable-width path.
 */
class AcceleratedColumnarToRowVariableTypesSuite extends RmmSparkRetrySuiteBase {

  private def attr(name: String, dt: DataType): AttributeReference =
    AttributeReference(name, dt, nullable = true)()

  private def runIter(attrs: Seq[AttributeReference], batch: ColumnarBatch) = {
    val iter     = new AcceleratedColumnarToRowIterator(
      attrs,
      Iterator(batch),
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    val toUnsafe = UnsafeProjection.create(attrs, attrs)
    iter.map(toUnsafe(_).copy()).toList
  }

  test("DECIMAL128 round trip through AcceleratedColumnarToRow") {
    val dt = DecimalType(30, 4)
    // Pick values that exceed the DECIMAL64 range so the kernel actually exercises the
    // 16-byte path that was broken before spark-rapids-jni#4586's default-branch fix.
    val values = Seq(
      new JBigDecimal("12345678901234567890.1234"),
      new JBigDecimal("-99999999999999999999.9999"),
      new JBigDecimal("0.0001"))
    val cv = ColumnVector.decimalFromBigInt(-dt.scale,
      values.map(_.unscaledValue): _*)
    val batch = new ColumnarBatch(
      Array(GpuColumnVector.from(cv, dt)), values.size)

    val rows = runIter(Seq(attr("d", dt)), batch)
    assertResult(values.size)(rows.size)
    rows.zip(values).foreach { case (r, expected) =>
      val actual = r.getDecimal(0, dt.precision, dt.scale)
      assertResult(expected)(actual.toJavaBigDecimal)
    }
  }

  test("STRING round trip through AcceleratedColumnarToRow") {
    val values = Seq("hello", "", "αβγ ✓", "this is a longer string that crosses 8 bytes")
    val cv     = ColumnVector.fromStrings(values: _*)
    val batch  = new ColumnarBatch(
      Array(GpuColumnVector.from(cv, StringType)), values.size)

    val rows = runIter(Seq(attr("s", StringType)), batch)
    assertResult(values.size)(rows.size)
    rows.zip(values).foreach { case (r, expected) =>
      assertResult(expected)(r.getUTF8String(0).toString)
    }
  }

  test("Mixed STRING + INT round trip through AcceleratedColumnarToRow") {
    val ints    = Seq(1, 2, 3, 4)
    val strings = Seq("a", "bb", "ccc", "dddd")
    val intCv   = ColumnVector.fromInts(ints: _*)
    val strCv   = ColumnVector.fromStrings(strings: _*)
    val batch   = new ColumnarBatch(Array(
      GpuColumnVector.from(intCv, IntegerType),
      GpuColumnVector.from(strCv, StringType)),
      ints.size)

    val attrs = Seq(attr("i", IntegerType), attr("s", StringType))
    val rows  = runIter(attrs, batch)
    assertResult(ints.size)(rows.size)
    rows.zip(ints.zip(strings)).foreach { case (r, (expectedInt, expectedStr)) =>
      assertResult(expectedInt)(r.getInt(0))
      assertResult(expectedStr)(r.getUTF8String(1).toString)
    }
  }
}
