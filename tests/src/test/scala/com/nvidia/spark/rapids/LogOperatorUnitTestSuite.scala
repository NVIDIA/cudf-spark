/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.rapids.{GpuAdd, GpuLog, GpuLogarithm}
import org.apache.spark.sql.types.{DataTypes, StructType}

class LogOperatorUnitTestSuite extends GpuExpressionTestSuite {

  private val schema = FuzzerUtils.createSchema(Seq(DataTypes.DoubleType))
  private val childExpr: GpuBoundReference =
    GpuBoundReference(0, DataTypes.DoubleType, nullable = false)(NamedExpression.newExprId, "input")

  test("log") {

    val expectedFun = (d: Double) => {
      if (d <= 0d) {
        None
      } else {
        Some(StrictMath.log(d))
      }
    }

    checkEvaluateGpuUnaryMathExpression(GpuLog(childExpr), expectedFun, schema)
  }

  test("log1p") {

    val expectedFun = (d: Double) => {
      if (d + 1d <= 0d) {
        None
      } else {
        Some(StrictMath.log1p(d))
      }
    }

    checkEvaluateGpuUnaryMathExpression(GpuLog(GpuAdd(childExpr, GpuLiteral(1d,
      DataTypes.DoubleType), false)()), expectedFun, schema)
  }

  test("log2") {

    val expectedFun = (d: Double) => {
      if (d <= 0d) {
        None
      } else {
        Some(StrictMath.log(d) / StrictMath.log(2))
      }
    }

    checkEvaluateGpuUnaryMathExpression(GpuLogarithm(childExpr, GpuLiteral(2d,
      DataTypes.DoubleType)), expectedFun, schema)
  }

  test("log10") {

    val expectedFun = (d: Double) => {
      if (d <= 0d) {
        None
      } else {
        Some(StrictMath.log(d) / StrictMath.log(10))
      }
    }

    checkEvaluateGpuUnaryMathExpression(GpuLogarithm(childExpr, GpuLiteral(10d,
      DataTypes.DoubleType)), expectedFun, schema)
  }

  test("log with variable base") {

    val base = Math.PI

    val expectedFun = (d: Double) => {
      if (d <= 0d) {
        None
      } else {
        Some(StrictMath.log(d) / StrictMath.log(base))
      }
    }

    checkEvaluateGpuUnaryMathExpression(GpuLogarithm(childExpr, GpuLiteral(base,
      DataTypes.DoubleType)), expectedFun, schema)
  }

  private def checkEvaluateGpuUnaryMathExpression(inputExpr: GpuExpression,
    expectedFun: Double => Option[Double],
    schema: StructType): Unit = {

    val fun = (input: Any) => {
      if (input == null) {
        null
      } else {
        expectedFun(input.asInstanceOf[Double])
      }
    }

    super.checkEvaluateGpuUnaryExpression(inputExpr, DataTypes.DoubleType, DataTypes.DoubleType,
      fun, schema)
  }
}

