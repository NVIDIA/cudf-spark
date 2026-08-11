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
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, GpuExpression, GpuLiteral, GpuProjectExec, GpuTieredProject, GpuUnitTests, PreProjectSplitIterator}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.{GpuCreateArray, GpuCreateMap, GpuCreateNamedStruct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * These tests cover only 3 complex type creators for the size estimation in
 * PreProjectSplitIterator. They are
 *     "CreateArray", "CreateMap", "CreateNamedStruct"
 */
class ComplexCreatorSizeEstimationTest extends GpuUnitTests {

  private val intColBound = GpuBoundReference(0, IntegerType, true)(ExprId(0), "int")
  private val strColBound = GpuBoundReference(1, StringType, true)(ExprId(1), "string")
  private val strNoNullColBound = GpuBoundReference(2, StringType, false)(ExprId(1), "string")

  private def inputBatch(): ColumnarBatch = {
    val intCol = GpuColumnVector.from(
      ColumnVector.fromBoxedInts(1, 2, null, 3, 4), IntegerType)
    val strCol = GpuColumnVector.from(
      ColumnVector.fromStrings(null, "", "s1", "s2", "s3"), StringType)
    val strNoNullCol = GpuColumnVector.from(
      ColumnVector.fromStrings("k1", "k2", "k3", "k4", "k5"), StringType)
    new ColumnarBatch(Array(intCol, strCol, strNoNullCol), 5)
  }

  private def testExprsSizeEstimate(boundList: Seq[GpuExpression]): Unit = {
    withResource(inputBatch()) { inCb =>
      val actualSize = withResource(GpuProjectExec.project(inCb, boundList)) { proCb =>
        GpuColumnVector.getTotalDeviceMemoryUsed(proCb)
      }
      val estimatedSize = PreProjectSplitIterator.calcMinOutputSize(inCb,
        GpuTieredProject(Seq(boundList)))
      assertResult(actualSize)(estimatedSize)
    }
  }

  test("estimate CreateArray(int, 1) size") {
    val projections = Seq(
      GpuCreateArray(Seq(intColBound, GpuLiteral(1)), true)
    )
    testExprsSizeEstimate(projections)
  }

  test("estimate CreateArray(string, 's1') size") {
    val projections = Seq(
      GpuCreateArray(Seq(strColBound, GpuLiteral("s1")), true)
    )
    testExprsSizeEstimate(projections)
  }

  test("estimate CreateNamedStruct(string, 's1', int, 1) size") {
    val projections = Seq(
      GpuCreateNamedStruct(Seq(
        GpuLiteral("_c1"), strColBound,
        GpuLiteral("_c2"), GpuLiteral("s1"),
        GpuLiteral("_c3"), intColBound,
        GpuLiteral("_c4"), GpuLiteral(1)))
    )
    testExprsSizeEstimate(projections)
  }

  test("estimate CreateMap(string, int, 's1', 1) size") {
    val projections = Seq(
      GpuCreateMap(Seq(strNoNullColBound, intColBound, GpuLiteral("s1"), GpuLiteral(1)))
    )
    testExprsSizeEstimate(projections)
  }
}
