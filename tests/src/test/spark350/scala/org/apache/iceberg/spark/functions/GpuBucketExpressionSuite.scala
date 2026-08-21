/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
// scalastyle:off println
// Existing console output in this file is intentional. New code should prefer logging.

package org.apache.iceberg.spark.functions

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.rapids.{FuzzerUtils, GpuBoundReference, GpuColumnVector, GpuLiteral, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.FuzzerUtils.createSchema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.types.{DataType, DataTypes, Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

class GpuBucketExpressionSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var seed = 2L
  private val numBuckets = Int.MaxValue

  override def beforeAll(): Unit = {
    seed = System.currentTimeMillis()
    println(s"Random seed set to $seed")
  }

  test("Int bucket function") {
    // Also covers DateType (stored as days since epoch, uses same BucketInt hash)
    testBucketFunction[Int](DataTypes.IntegerType,
      (col, row) => col.getInt(row), BucketFunction.BucketInt.invoke)
  }

  test("Long bucket function") {
    // Also covers TimestampType (stored as microseconds since epoch, uses same BucketLong hash)
    testBucketFunction[Long](DataTypes.LongType,
      (col, row) => col.getLong(row), BucketFunction.BucketLong.invoke)
  }

  test("String bucket function") {
    testBucketFunction[UTF8String](DataTypes.StringType,
      (col, row) => UTF8String.fromString(col.getJavaString(row)),
      BucketFunction.BucketString.invoke)
  }

  test("Binary bucket function") {
    testBucketFunctionBinary()
  }

  test("Decimal bucket function") {
    testBucketFunctionDecimal(DecimalType(18, 2))
  }

  private def testBucketFunction[T](dataType: DataType,
    colEval: (HostColumnVector, Int) => T,
    icebergBucket: (Int, T) => Int): Unit = {
    val schema = createSchema(Seq(dataType), nullable = true)
    withResource(FuzzerUtils.createColumnarBatch(schema, 1000, seed = seed)) { batch =>
      val inputRefExpr = GpuBoundReference(0, dataType, nullable = true)(ExprId(0), "input0")
      val numBucketsExpr = GpuLiteral.create(numBuckets)
      val expr = GpuBucketExpression(numBucketsExpr, inputRefExpr)
      withResource(expr.columnarEvalAny(batch).asInstanceOf[GpuColumnVector]) { gpuResult =>
        withResource(gpuResult.getBase.copyToHost()) { hostResult =>
          withResource(icebergEval(batch.column(0).asInstanceOf[GpuColumnVector],
            colEval, icebergBucket)) { expected =>
            TestUtils.compareColumns(expected, hostResult)
          }
        }
      }
    }
  }

  private def testBucketFunctionBinary(): Unit = {
    val dataType = DataTypes.BinaryType
    val schema = createSchema(Seq(dataType), nullable = true)
    withResource(FuzzerUtils.createColumnarBatch(schema, 1000, seed = seed)) { batch =>
      val inputRefExpr = GpuBoundReference(0, dataType, nullable = true)(ExprId(0), "input0")
      val numBucketsExpr = GpuLiteral.create(numBuckets)
      val expr = GpuBucketExpression(numBucketsExpr, inputRefExpr)
      withResource(expr.columnarEvalAny(batch).asInstanceOf[GpuColumnVector]) { gpuResult =>
        withResource(gpuResult.getBase.copyToHost()) { hostResult =>
          withResource(icebergEvalBinary(batch.column(0).asInstanceOf[GpuColumnVector])) {
            expected => TestUtils.compareColumns(expected, hostResult)
          }
        }
      }
    }
  }

  private def testBucketFunctionDecimal(dataType: DecimalType): Unit = {
    val schema = createSchema(Seq(dataType), nullable = true)
    withResource(FuzzerUtils.createColumnarBatch(schema, 1000, seed = seed)) { batch =>
      val inputRefExpr = GpuBoundReference(0, dataType, nullable = true)(ExprId(0), "input0")
      val numBucketsExpr = GpuLiteral.create(numBuckets)
      val expr = GpuBucketExpression(numBucketsExpr, inputRefExpr)
      withResource(expr.columnarEvalAny(batch).asInstanceOf[GpuColumnVector]) { gpuResult =>
        withResource(gpuResult.getBase.copyToHost()) { hostResult =>
          withResource(icebergEvalDecimal(batch.column(0).asInstanceOf[GpuColumnVector])) {
            expected => TestUtils.compareColumns(expected, hostResult)
          }
        }
      }
    }
  }

  private def icebergEval[T](col: GpuColumnVector,
    colEval: (HostColumnVector, Int) => T,
    icebergBucket: (Int, T) => Int): HostColumnVector = {
    withResource(col.getBase.copyToHost()) { hostCol =>
      val builder = HostColumnVector.builder(DType.INT32, hostCol.getRowCount.toInt)

      for (row <- 0 until hostCol.getRowCount.toInt) {
        if (hostCol.isNull(row)) {
          builder.appendNull()
        } else {
          val v = colEval(hostCol, row)
          builder.append(icebergBucket(numBuckets, v))
        }
      }
      builder.build()
    }
  }

  private def icebergEvalBinary(col: GpuColumnVector): HostColumnVector = {
    withResource(col.getBase.copyToHost()) { hostCol =>
      val builder = HostColumnVector.builder(DType.INT32, hostCol.getRowCount.toInt)

      for (row <- 0 until hostCol.getRowCount.toInt) {
        if (hostCol.isNull(row)) {
          builder.appendNull()
        } else {
          val bytes = hostCol.getList(row).asInstanceOf[java.util.List[java.lang.Byte]]
          val byteArray = new Array[Byte](bytes.size())
          for (i <- 0 until bytes.size()) {
            byteArray(i) = bytes.get(i)
          }
          builder.append(BucketFunction.BucketBinary.invoke(numBuckets, byteArray))
        }
      }
      builder.build()
    }
  }

  private def icebergEvalDecimal(col: GpuColumnVector): HostColumnVector = {
    withResource(col.getBase.copyToHost()) { hostCol =>
      val builder = HostColumnVector.builder(DType.INT32, hostCol.getRowCount.toInt)

      for (row <- 0 until hostCol.getRowCount.toInt) {
        if (hostCol.isNull(row)) {
          builder.appendNull()
        } else {
          val bd = hostCol.getBigDecimal(row)
          val sparkDecimal = Decimal(bd)
          builder.append(BucketFunction.BucketDecimal.invoke(numBuckets, sparkDecimal))
        }
      }
      builder.build()
    }
  }
}
// scalastyle:on println
