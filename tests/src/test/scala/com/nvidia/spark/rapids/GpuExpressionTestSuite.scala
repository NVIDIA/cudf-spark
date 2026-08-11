/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructType}

abstract class GpuExpressionTestSuite extends SparkQueryCompareTestSuite {

  /**
   * Evaluate the GpuExpression and compare the results to the provided function.
   *
   * @param inputExpr GpuExpression under test
   * @param expectedFun Function that produces expected results
   * @param schema Schema to use for generated data
   * @param rowCount Number of rows of random to generate
   * @param comparisonFunc Optional function to compare results
   * @param maxFloatDiff Maximum acceptable difference between expected and actual results
   */
  def checkEvaluateGpuUnaryExpression[T, U](inputExpr: GpuExpression,
    inputType: DataType,
    outputType: DataType,
    expectedFun: T => Option[U],
    schema: StructType,
    rowCount: Int = 50,
    seed: Long = 0,
    comparisonFunc: Option[(U, U) => Boolean] = None,
    maxFloatDiff: Double = 0.00001): Unit = {

    // generate batch
    withResource(FuzzerUtils.createColumnarBatch(schema, rowCount, seed = seed)) { batch =>
      // evaluate expression
      withResource(inputExpr.columnarEval(batch).asInstanceOf[GpuColumnVector]) { result =>
        // bring gpu data onto host
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostInput =>
          withResource(result.copyToHost()) { hostResult =>
            // compare results
            assert(result.getRowCount == rowCount)
            for (i <- 0 until result.getRowCount.toInt) {
              val inputValue = getAs(hostInput, i, inputType)
              val actualOption: Option[U] = getAs(hostResult, i, outputType).map(_.asInstanceOf[U])
              val expectedOption: Option[U] =
                inputValue.flatMap(v => expectedFun(v.asInstanceOf[T]))
              (expectedOption, actualOption) match {
                case (Some(expected), Some(actual)) if comparisonFunc.isDefined =>
                  if (!comparisonFunc.get(expected, actual)) {
                    throw new IllegalStateException(s"Expected: $expected. Actual: $actual. " +
                      s"Input value: $inputValue")
                  }
                case (Some(expected), Some(actual)) =>
                  if (!compare(expected, actual, maxFloatDiff)) {
                    throw new IllegalStateException(s"Expected: $expected. Actual: $actual. " +
                      s"Input value: $inputValue")
                  }
                case (None, None) =>
                case _ => throw new IllegalStateException(s"Expected: $expectedOption. " +
                  s"Actual: $actualOption. Input value: $inputValue")
              }
            }
          }
        }
      }
    }
  }

  /**
   * Evaluate the GpuBinaryExpression and compare the results to the provided function.
   *
   * @param inputExpr GpuBinaryExpression under test
   * @param expectedFun Function that produces expected results
   * @param schema Schema to use for generated data
   * @param rowCount Number of rows of random to generate
   * @param comparisonFunc Optional function to compare results
   * @param maxFloatDiff Maximum acceptable difference between expected and actual results
   */
  def checkEvaluateGpuBinaryExpression[L, R, U](inputExpr: GpuExpression,
                                                leftType: DataType,
                                                rightType: DataType,
                                                outputType: DataType,
                                                expectedFun: (L, R) => Option[U],
                                                schema: StructType,
                                                rowCount: Int = 50,
                                                seed: Long = 0,
                                                nullable: Boolean = false,
                                                comparisonFunc: Option[(U, U) => Boolean] = None,
                                                maxFloatDiff: Double = 0.00001): Unit = {

    // generate batch
    withResource(FuzzerUtils.createColumnarBatch(schema, rowCount, seed = seed)) { batch =>
      // evaluate expression
      withResource(inputExpr.columnarEval(batch).asInstanceOf[GpuColumnVector]) { result =>
        // bring gpu data onto host
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { leftInput =>
          withResource(batch.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { rightInput =>
            withResource(result.copyToHost()) { hostResult =>
              // compare results
              assert(result.getRowCount == rowCount)
              for (i <- 0 until result.getRowCount.toInt) {
                val lValue = getAs(leftInput, i, leftType)
                val rValue = getAs(rightInput, i, rightType)
                val actualOption: Option[U] =
                  getAs(hostResult, i, outputType).map(_.asInstanceOf[U])
                val expectedOption: Option[U] = if (!nullable) {
                  lValue.flatMap(l => rValue.flatMap(r =>
                    expectedFun(l.asInstanceOf[L], r.asInstanceOf[R])))
                } else {
                  expectedFun(lValue.orNull.asInstanceOf[L], rValue.orNull.asInstanceOf[R])
                }
                (expectedOption, actualOption) match {
                  case (Some(expected), Some(actual)) if comparisonFunc.isDefined =>
                    if (!comparisonFunc.get(expected, actual)) {
                      throw new IllegalStateException(s"Expected: $expected. Actual: $actual. " +
                        s"Left value: $lValue, Right value: $rValue")
                    }
                  case (Some(expected), Some(actual)) =>
                    if (!compare(expected, actual, maxFloatDiff)) {
                      throw new IllegalStateException(s"Expected: $expected. Actual: $actual. " +
                        s"Left value: $lValue, Right value: $rValue")
                    }
                  case (None, None) =>
                  case _ => throw new IllegalStateException(s"Expected: $expectedOption. " +
                    s"Actual: $actualOption. Left value: $lValue, Right value: $rValue")
                }
              }
            }
          }
        }
      }
    }
  }

  def compareStringifiedFloats(float: Boolean)(expected: String, actual: String): Boolean = {

    // handle exact matches first
    if (expected == actual) {
      return true
    }
    float match {
      case true => compare(expected.toFloat, actual.toFloat)
      case false => compare(expected.toDouble, actual.toDouble)
    }
  }

  private def getAs(column: RapidsHostColumnVector, index: Int, dataType: DataType): Option[Any] = {
    if (column.isNullAt(index)) {
      None
    } else {
      Some(dataType match {
        case DataTypes.BooleanType => column.getBoolean(index)
        case DataTypes.ByteType => column.getByte(index)
        case DataTypes.ShortType => column.getShort(index)
        case DataTypes.IntegerType => column.getInt(index)
        case DataTypes.LongType => column.getLong(index)
        case DataTypes.FloatType => column.getFloat(index)
        case DataTypes.DoubleType => column.getDouble(index)
        case DataTypes.StringType => column.getUTF8String(index).toString
        case dt: DecimalType => column.getDecimal(index, dt.precision, dt.scale)
      })
    }

  }
}
