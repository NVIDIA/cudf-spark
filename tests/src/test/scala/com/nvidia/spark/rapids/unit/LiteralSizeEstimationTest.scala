/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.unit

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuScalar, GpuUnitTests, PreProjectSplitIterator}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** These tests only cover nested type literals for the PreProjectSplitIterator case */
class LiteralSizeEstimationTest extends GpuUnitTests {
  private val numRows = 1000

  private def testLiteralSizeEstimate(lit: Any, litType: DataType): Unit = {
    val col = withResource(GpuScalar.from(lit, litType))(ColumnVector.fromScalar(_, numRows))
    val actualSize = withResource(col)(_.getDeviceMemorySize)
    val estimatedSize = PreProjectSplitIterator.calcSizeForLiteral(lit, litType, numRows,
      lit == null, Some(1))
    assertResult(actualSize)(estimatedSize)
  }

  test("estimate the array(int) literal size") {
    val litType = ArrayType(IntegerType, true)
    val lit = ArrayData.toArrayData(Array(null, 1, 2, null))
    testLiteralSizeEstimate(lit, litType)
  }

  test("estimate the array(string) literal size") {
    val litType = ArrayType(StringType, true)
    val lit = ArrayData.toArrayData(
      Array(null, UTF8String.fromString("s1"), UTF8String.fromString("s2")))
    testLiteralSizeEstimate(lit, litType)
  }

  test("estimate the array(array(array(int))) literal size") {
    val litType = ArrayType(ArrayType(ArrayType(IntegerType, true), true), true)
    val nestedElem1 = ArrayData.toArrayData(Array(null, 1, 2, null))
    val nestedElem2 = ArrayData.toArrayData(Array(null))
    val nestedElem3 = ArrayData.toArrayData(Array())
    val elem1 = ArrayData.toArrayData(Array(nestedElem1, null))
    val elem2 = ArrayData.toArrayData(Array(nestedElem2, null, nestedElem3))
    val lit = ArrayData.toArrayData(Array(null, elem1, null, elem2, null))
    testLiteralSizeEstimate(lit, litType)
  }

  test("estimate the array(array(array(string))) literal size") {
    val litType = ArrayType(ArrayType(ArrayType(StringType, true), true), true)
    val nestedElem1 = ArrayData.toArrayData(
      Array(null, UTF8String.fromString("s1"), UTF8String.fromString("s2")))
    val nestedElem2 = ArrayData.toArrayData(Array(null))
    val nestedElem3 = ArrayData.toArrayData(Array())
    val elem1 = ArrayData.toArrayData(Array(nestedElem1, null))
    val elem2 = ArrayData.toArrayData(Array(nestedElem2, null, nestedElem3))
    val lit = ArrayData.toArrayData(Array(null, elem1, null, elem2, null))
    testLiteralSizeEstimate(lit, litType)
  }

  test("estimate the struct(int, string) literal size") {
    val litType = StructType(Seq(
      StructField("int1", IntegerType),
      StructField("string2", StringType)
    ))
    // null
    testLiteralSizeEstimate(InternalRow(null, null), litType)
    // normal case
    testLiteralSizeEstimate(InternalRow(1, UTF8String.fromString("s1")), litType)
  }

  test("estimate the struct(int, array(string)) literal size") {
    val litType = StructType(Seq(
        StructField("int1", IntegerType),
        StructField("string2", ArrayType(StringType, true))
    ))
    testLiteralSizeEstimate(InternalRow(null, null), litType)
    val arrayLit = ArrayData.toArrayData(
      Array(null, UTF8String.fromString("s1"), UTF8String.fromString("s2")))
    // normal case
    testLiteralSizeEstimate(InternalRow(1, arrayLit), litType)
  }

  test("estimate the list(struct(int, array(string))) literal size") {
    val litType = ArrayType(
      StructType(Seq(
        StructField("int1", IntegerType),
        StructField("string2", ArrayType(StringType, true))
      )), true)
    val arrayLit = ArrayData.toArrayData(
      Array(null, UTF8String.fromString("a1"), UTF8String.fromString("a2")))
    val elem1 = InternalRow(1, arrayLit)
    val elem2 = InternalRow(null, null)
    val lit = ArrayData.toArrayData(Array(null, elem1, elem2))
    testLiteralSizeEstimate(lit, litType)
  }

  test("estimate the map(int, array(string)) literal size") {
    val litType = MapType(IntegerType, ArrayType(StringType, true), true)
    val arrayLit = ArrayData.toArrayData(
      Array(null, UTF8String.fromString("s1"), UTF8String.fromString("s2")))
    val valueLit = ArrayData.toArrayData(Array(null, arrayLit))
    val keyLit = ArrayData.toArrayData(Array(1, 2))
    val lit = new ArrayBasedMapData(keyLit, valueLit)
    testLiteralSizeEstimate(lit, litType)
  }
}
