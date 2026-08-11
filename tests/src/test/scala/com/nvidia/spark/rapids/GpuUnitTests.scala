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

import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.DType._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{Expression, Nondeterministic}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuUnitTests extends SparkQueryCompareTestSuite {

  def EmptyBatch = new ColumnarBatch(null)

  /**
   * Runs through the testFunc for all numeric data types except DecimalType.
   *
   * @param testFunc a test function that accepts a tuple of conversion functions one to convert a
   *                 column of integer into another data type, the other to convert an integer to
   *                 another data type
   */
  protected def testNumericDataTypes(testFunc: ((ColumnVector => GpuColumnVector,
    Any => Any)) => Unit): Unit = {
    /**
     * Method to convert Int vector to another numeric type (DecimalType not supported)
     */
    def convert(to: DataType)(v: ColumnVector): GpuColumnVector = {
      val cv = v.asInstanceOf[ColumnVector]
      // close the vector that was passed in and return a new vector
      withResource(cv) { cv =>
        GpuColumnVector.from(cv.castTo(GpuColumnVector.getNonNestedRapidsType(to)), to)
      }
    }

    /**
     * Method to convert any numeric value to 'to' DataType (DecimalType not supported)
     */
    def convertS(to: DataType)(v: Any): Any = {
      val i = v.asInstanceOf[Int]
      to match {
        case ByteType =>
          i.toByte
        case ShortType =>
          i.toShort
        case IntegerType =>
          i.toInt
        case LongType =>
          i.toLong
        case FloatType =>
          i.toFloat
        case DoubleType =>
          i.toDouble
      }
    }

    /**
     * Convenience method to avoid repeating the datatype
     */
    def converters(d: DataType): (ColumnVector => GpuColumnVector, Any => Any) = {
      (convert(d), convertS(d))
    }

    testFunc(converters(DataTypes.ByteType))
    testFunc(converters(DataTypes.IntegerType))
    testFunc(converters(DataTypes.ShortType))
    testFunc(converters(DataTypes.LongType))
    testFunc(converters(DataTypes.FloatType))
    testFunc(converters(DataTypes.DoubleType))
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte], Spread[Double], MapData and Row. Also check whether nullable in expression is
   * true if result is null
   */
  protected def checkResult(result: GpuColumnVector, expected: GpuColumnVector,
     expression: Expression): Unit = {
    // The result is null for a non-nullable expression
    assert(result != null || expression.nullable, "expression.nullable should be true if " +
        "result is null")
    assert(result.getBase().getType() == expected.getBase().getType(),
       "types should be the same")
    withResource(expected.copyToHost()) { hostExpected =>
      withResource(result.copyToHost()) { hostResult =>
        check(hostExpected, hostResult)
      }
    }
  }

  private def check(hostExpected: RapidsHostColumnVector,
     hostResult: RapidsHostColumnVector) = {
    for (row <- 0 until hostResult.getRowCount().toInt) {
      assert(hostExpected.isNullAt(row) == hostResult.isNullAt(row),
        "expected and actual differ at " + row + " one of them isn't null")
      if (!hostExpected.isNullAt(row)) {
        hostResult.getBase.getType() match {
          case INT8 | BOOL8 =>
            assert(hostExpected.getByte(row) == hostResult.getByte(row), "row " + row)
          case INT16 =>
            assert(hostExpected.getShort(row) == hostResult.getShort(row), "row " + row)

          case INT32 | TIMESTAMP_DAYS =>
            assert(hostExpected.getInt(row) == hostResult.getInt(row), "row " + row)

          case INT64 | TIMESTAMP_MICROSECONDS | TIMESTAMP_MILLISECONDS | TIMESTAMP_NANOSECONDS |
               TIMESTAMP_SECONDS =>
            assert(hostExpected.getLong(row) == hostResult.getLong(row), "row " + row)

          case FLOAT32 =>
            assert(compare(hostExpected.getFloat(row), hostResult.getFloat(row),
              0.0001), "row " + row)

          case FLOAT64 =>
            assert(compare(hostExpected.getDouble(row), hostResult.getDouble(row),
              0.0001), "row " + row)

          case STRING =>
            assert(hostExpected.getUTF8String(row) == hostResult.getUTF8String(row),
              "row " + row)

          case _ =>
            throw new IllegalArgumentException(hostResult.getBase.getType() +
                " is not supported yet")
        }
      }
    }
  }

  protected def evaluateWithoutCodegen(gpuExpression: GpuExpression,
     inputBatch: ColumnarBatch = EmptyBatch): GpuColumnVector = {
    gpuExpression.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    gpuExpression.columnarEval(inputBatch).asInstanceOf[GpuColumnVector]
  }

  private def checkEvaluationWithoutCodegen(gpuExpression: GpuExpression,
      expected: GpuColumnVector,
      inputBatch: ColumnarBatch): Unit = {
    try {
      withResource(evaluateWithoutCodegen(gpuExpression, inputBatch)) { actual =>
          checkResult(actual, expected, gpuExpression)
      }
    } catch {
      case e: Exception => e.printStackTrace()
        fail(s"Exception evaluating $gpuExpression", e)
    }
  }

  protected def checkEvaluation(gpuExpression: => GpuExpression,
     expected: GpuColumnVector,
     inputBatch: ColumnarBatch = EmptyBatch): Unit = {
    checkEvaluationWithoutCodegen(gpuExpression, expected, inputBatch)
  }
}