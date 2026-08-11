/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.lang.{Byte => JByte}

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.catalyst.expressions.{BoundReference, ExprId, HiveHash}
import org.apache.spark.sql.rapids.GpuHiveHash
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class HiveHashTestSuite extends SparkQueryCompareTestSuite {
  // All columns should have the same length(now is 15) for multiple columns tests.
  def genBoolColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromBoxedBooleans(null, true, null, false, true, true, true,
      false, false, null, false, true, null, false, true),
    BooleanType)

  def genByteColumn: GpuColumnVector = {
    val bytes: Seq[JByte] = Seq[JByte](null, Byte.MaxValue, Byte.MinValue, null, null) ++
      Seq(0, -0, 1, -1, 10, -10, 126, -126, 111, -111).map(b => JByte.valueOf(b.toByte))
    GpuColumnVector.from(CudfColumnVector.fromBoxedBytes(bytes: _*), ByteType)
  }

  def genIntColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromBoxedInts(null, Int.MaxValue, Int.MinValue, 0, -0, 1, -1,
      null, 100, -100, null, null, 99, -99, null),
    IntegerType)

  def genLongColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromBoxedLongs(null, Long.MaxValue, Long.MinValue, 0, -0, 1, -1,
      null, 100, -100, null, null, 99, -99, null),
    LongType)

  def genStringColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromStrings(null, "", "1", "432", "a\nb", "a", "`@@$*&", " ", "\t",
      "dE\"\u0100\t\u0101 \ud720\ud721", "\ud720\ud721\ud720\ud721", "''", null, "   ",
      "This is a long string (greater than 128 bytes/char string) case to test this " +
        "hash function. Just want an abnormal case here to see if any error may " +
        "happen when doing the hive hashing"),
    StringType)

  def genFloatColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromBoxedFloats(null, 0.0f, -0.0f, 99.0f, -99.0f, Float.NaN,
      Float.MaxValue, Float.MinValue, Float.MinPositiveValue, Float.NegativeInfinity,
      Float.PositiveInfinity,
      FLOAT_POSITIVE_NAN_LOWER_RANGE, FLOAT_POSITIVE_NAN_UPPER_RANGE,
      FLOAT_NEGATIVE_NAN_LOWER_RANGE, FLOAT_NEGATIVE_NAN_UPPER_RANGE),
    FloatType)

  def genDoubleColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.fromBoxedDoubles(null, 0.0, -0.0, 199.0, -199.0, Double.NaN,
      Double.MaxValue, Double.MinValue, Double.MinPositiveValue, Double.NegativeInfinity,
      Double.PositiveInfinity,
      DOUBLE_POSITIVE_NAN_LOWER_RANGE, DOUBLE_POSITIVE_NAN_UPPER_RANGE,
      DOUBLE_NEGATIVE_NAN_LOWER_RANGE, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
    DoubleType)

  def genDateColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.timestampDaysFromBoxedInts(null, 0, null, 100, -100, 0x12345678,
      null, -0x12345678, 171922, 19899, 17766, -0, 16897, null, 18888),
    DateType)

  def genTimestampColumn: GpuColumnVector = GpuColumnVector.from(
    CudfColumnVector.timestampMicroSecondsFromBoxedLongs(null, 0, -0, 100, -100,
      0x123456789abcdefL, -0x123456789abcdefL, 1719561256196L, -1719561256196L, null,
      1234567890, -1234567890, 999, -999, null),
    TimestampType)

  private def testHiveHashOnGpuAndCpuThenClose(cols: GpuColumnVector*): Unit = {
    val (numRows, cpuRefs, gpuRefs) = closeOnExcept(cols) { _ =>
      val rowsNum = cols.head.getRowCount
      require(cols.tail.forall(_.getRowCount == rowsNum),
        s"All the input columns should have the same length: $rowsNum.")

      val cpuRefs = cols.zipWithIndex.map { case (col, id) =>
        BoundReference(id, col.dataType(), nullable = true)
      }
      val gpuRefs = cols.zipWithIndex.map { case (col, id) =>
        GpuBoundReference(id, col.dataType(), nullable = true)(ExprId(id),
          s"col${id}_${col.dataType().simpleString}")
      }
      (rowsNum.toInt, cpuRefs, gpuRefs)
    }

    // GPU run
    val gpuRet = closeOnExcept(cols) { _ =>
      val inputCB = new ColumnarBatch(cols.toArray, numRows)
      val hostRet = withResource(GpuHiveHash(gpuRefs).columnarEval(inputCB)) { retCol =>
        retCol.copyToHost()
      }
      withResource(hostRet) { _ =>
        (0 until numRows).map(hostRet.getInt).toArray
      }
    }
    // CPU run
    val cpuRet = withResource(cols) { _ =>
      withResource(new ColumnarBatch(cols.map(_.copyToHost()).toArray, numRows)) { cb =>
        val hiveHash = HiveHash(cpuRefs)
        val it = cb.rowIterator()
        (0 until numRows).map(_ => hiveHash.eval(it.next())).toArray
      }
    }
    assertResult(cpuRet)(gpuRet)
  }

  test("Test hive hash booleans") {
    testHiveHashOnGpuAndCpuThenClose(genBoolColumn)
  }

  test("Test hive hash bytes") {
    testHiveHashOnGpuAndCpuThenClose(genByteColumn)
  }

  test("Test hive hash ints") {
    testHiveHashOnGpuAndCpuThenClose(genIntColumn)
  }

  test("Test hive hash longs") {
    testHiveHashOnGpuAndCpuThenClose(genLongColumn)
  }

  test("Test hive hash floats") {
    testHiveHashOnGpuAndCpuThenClose(genFloatColumn)
  }

  test("Test hive hash doubles") {
    testHiveHashOnGpuAndCpuThenClose(genDoubleColumn)
  }

  test("Test hive hash dates") {
    testHiveHashOnGpuAndCpuThenClose(genDateColumn)
  }

  test("Test hive hash timestamps") {
    testHiveHashOnGpuAndCpuThenClose(genTimestampColumn)
  }

  test("Test hive hash strings") {
    testHiveHashOnGpuAndCpuThenClose(genStringColumn)
  }

  test("Test hive hash mixed {bytes, ints, longs, dates}") {
    val cols = closeOnExcept(new Array[GpuColumnVector](4)) { buf =>
      buf(0) = genByteColumn
      buf(1) = genIntColumn
      buf(2) = genLongColumn
      buf(3) = genDateColumn
      buf
    }
    testHiveHashOnGpuAndCpuThenClose(cols: _*)
  }

  test("Test hive hash mixed {booleans, floats, doubles, strings}") {
    val cols = closeOnExcept(new Array[GpuColumnVector](4)) { buf =>
      buf(0) = genBoolColumn
      buf(1) = genFloatColumn
      buf(2) = genDoubleColumn
      buf(3) = genStringColumn
      buf
    }
    testHiveHashOnGpuAndCpuThenClose(cols: _*)
  }
}
