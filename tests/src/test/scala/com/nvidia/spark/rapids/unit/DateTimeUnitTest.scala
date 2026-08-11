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

package com.nvidia.spark.rapids.unit

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, GpuLiteral, GpuUnitTests}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.rapids.{GpuDateAdd, GpuDateSub}
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DateTimeUnitTest extends GpuUnitTests {
  val TIMES_DAY = Array(-1528, //1965-10-26
    17716, //2018-07-04
    19382, //2023-01-25
    -13528, 1716) //2018-07-04

  test("test date_add") {
    withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1498, 17746, 19412,
      -13498, 1746), DateType)) { expected0 =>
      withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1526, 17748, 19388,
        -13519, 1722), DateType)) { expected1 =>
        withResource(GpuColumnVector.from(ColumnVector.daysFromInts(17676, 17706, 17680,
          17683, 17680), DateType)) { expected2 =>

          withResource(GpuColumnVector.from(ColumnVector.daysFromInts(TIMES_DAY: _*), DateType)) {
            datesVector =>
              withResource(GpuColumnVector.from(ColumnVector.fromInts(2, 32, 6, 9, 6),
                IntegerType)) {
                daysVector =>
                  val datesExpressionVector = GpuBoundReference(0, DataTypes.DateType,
                    false)(NamedExpression.newExprId, "date")
                  val daysExpressionVector = GpuBoundReference(1, DataTypes.IntegerType,
                    false)(NamedExpression.newExprId, "days")
                  val batch = new ColumnarBatch(List(datesVector, daysVector).toArray,
                     TIMES_DAY.length)

                  val daysScalar = GpuLiteral(30, DataTypes.IntegerType)
                  // lhs = vector, rhs = scalar
                  checkEvaluation(GpuDateAdd(datesExpressionVector, daysScalar), expected0, batch)

                  // lhs = vector, rhs = vector
                  checkEvaluation(GpuDateAdd(datesExpressionVector, daysExpressionVector),
                     expected1, batch)

                  // lhs = scalar, rhs = vector
                  val datesS = GpuLiteral(17674, DataTypes.DateType)
                  checkEvaluation(GpuDateAdd(datesS, daysExpressionVector), expected2, batch)
              }
          }
        }
      }
    }
  }

  test("test date_sub") {
    withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1558, 17686, 19352, -13558,
      1686), DateType)) { expected0 =>
      withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1530, 17684, 19376,
        -13537, 1710), DateType)) { expected1 =>
        withResource(GpuColumnVector.from(ColumnVector.daysFromInts(17672, 17642, 17668,
          17665, 17668), DateType)) { expected2 =>

          withResource(GpuColumnVector.from(ColumnVector.daysFromInts(TIMES_DAY: _*), DateType)) {
            datesVector =>
              withResource(GpuColumnVector.from(ColumnVector.fromInts(2, 32, 6, 9, 6),
                IntegerType)) {
                daysVector =>
                  val datesExpressionVector = GpuBoundReference(0, DataTypes.DateType,
                    false)(NamedExpression.newExprId, "date")
                  val daysExpressionVector = GpuBoundReference(1, DataTypes.IntegerType,
                    false)(NamedExpression.newExprId, "days")
                  val batch = new ColumnarBatch(List(datesVector, daysVector).toArray,
                    TIMES_DAY.length)

                  val daysScalar = GpuLiteral(30, DataTypes.IntegerType)
                  // lhs = vector, rhs = scalar
                  checkEvaluation(GpuDateSub(datesExpressionVector, daysScalar), expected0, batch)

                  // lhs = vector, rhs = vector
                  checkEvaluation(GpuDateSub(datesExpressionVector, daysExpressionVector),
                    expected1, batch)

                  // lhs = scalar, rhs = vector
                  val datesS = GpuLiteral(17674, DataTypes.DateType)
                  checkEvaluation(GpuDateSub(datesS, daysExpressionVector), expected2, batch)
              }
          }
        }
      }
    }
  }

}
