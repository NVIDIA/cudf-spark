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

/*** spark-rapids-shim-json-lines
{"spark": "403"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{FQSuiteName, GpuColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DoubleType

class MathExpressionsSuite extends AnyFunSuite with FQSuiteName {
  import HyperbolicMathExpressions._

  private val log2 = StrictMath.log(2.0)
  private val epsilon = 1e-12

  test("acosh matches Spark-compatible boundary behavior") {
    val inputs = Seq(
      LARGE_ACOSH,
      LARGE_ACOSH - 1.0,
      Math.nextDown(LARGE_ACOSH),
      1.0,
      0.999,
      0.5,
      Double.PositiveInfinity)

    assertDoubleSeqEquals(evaluate(inputs)(acosh), inputs.map(expectedAcosh))
  }

  test("asinh matches Spark-compatible boundary behavior") {
    val justBelowLarge = Math.nextDown(LARGE_ASINH)
    val inputs = Seq(
      LARGE_ASINH,
      -LARGE_ASINH,
      justBelowLarge,
      -justBelowLarge,
      0.0,
      1.0,
      -1.0,
      Double.PositiveInfinity,
      Double.NegativeInfinity)

    assertDoubleSeqEquals(evaluate(inputs)(asinh), inputs.map(expectedAsinh))
  }

  test("GpuAcosh doColumnar matches Spark-compatible boundary behavior") {
    val inputs = Seq(
      LARGE_ACOSH,
      LARGE_ACOSH - 1.0,
      Math.nextDown(LARGE_ACOSH),
      1.0,
      0.999,
      Double.PositiveInfinity)

    assertDoubleSeqEquals(
      evaluateGpu(inputs)(GpuAcosh(Literal(0.0)).doColumnar),
      inputs.map(expectedAcosh))
  }

  test("GpuAsinh doColumnar matches Spark-compatible boundary behavior") {
    val justBelowLarge = Math.nextDown(LARGE_ASINH)
    val inputs = Seq(
      LARGE_ASINH,
      -LARGE_ASINH,
      justBelowLarge,
      -justBelowLarge,
      0.0,
      Double.PositiveInfinity,
      Double.NegativeInfinity)

    assertDoubleSeqEquals(
      evaluateGpu(inputs)(GpuAsinh(Literal(0.0)).doColumnar),
      inputs.map(expectedAsinh))
  }

  private def evaluate(inputs: Seq[Double])(
      op: ColumnVector => ColumnVector): Seq[Double] = {
    withResource(ColumnVector.fromDoubles(inputs: _*)) { input =>
      withResource(op(input)) { result =>
        collect(result)
      }
    }
  }

  private def evaluateGpu(inputs: Seq[Double])(
      op: GpuColumnVector => ColumnVector): Seq[Double] = {
    withResource(GpuColumnVector.from(ColumnVector.fromDoubles(inputs: _*), DoubleType)) {
      input =>
        withResource(op(input)) { result =>
          collect(result)
        }
    }
  }

  private def collect(result: ColumnVector): Seq[Double] = {
    withResource(result.copyToHost()) { host =>
      val rowCount = result.getRowCount.toInt
      (0 until rowCount).map(i => host.getDouble(i.toLong))
    }
  }

  private def expectedAcosh(x: Double): Double = {
    if (x < 1.0) {
      Double.NaN
    } else if (x >= LARGE_ACOSH) {
      StrictMath.log(x) + log2
    } else {
      StrictMath.log(x + StrictMath.sqrt(x * x - 1.0))
    }
  }

  private def expectedAsinh(x: Double): Double = {
    if (Math.abs(x) >= LARGE_ASINH) {
      Math.signum(x) * (StrictMath.log(Math.abs(x)) + log2)
    } else {
      StrictMath.log(x + StrictMath.sqrt(x * x + 1.0))
    }
  }

  private def assertDoubleSeqEquals(actual: Seq[Double], expected: Seq[Double]): Unit = {
    actual.zip(expected).foreach { case (a, e) =>
      if (e.isNaN) {
        assert(a.isNaN)
      } else if (e.isInfinite || e == 0.0) {
        assert(a === e)
      } else {
        assert(Math.abs(a - e) <= Math.abs(e) * epsilon)
      }
    }
  }
}
