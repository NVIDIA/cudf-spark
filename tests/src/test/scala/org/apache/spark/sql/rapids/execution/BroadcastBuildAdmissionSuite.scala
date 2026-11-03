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
package org.apache.spark.sql.rapids.execution

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

class BroadcastBuildAdmissionSuite extends AnyFunSuite {
  private val bytes = StructType(Seq(StructField("value", ByteType)))

  test("the driver includes a full padded validity mask before admission") {
    val bound = BroadcastBuildAdmission.upperBound(Array(4L, 6L), 10, 2, bytes)
    assert(bound.contains(BigInt(74)))
    assert(BroadcastBuildAdmission.accepts(bound, 74))
    assert(!BroadcastBuildAdmission.accepts(bound, 73))
    assert(!BroadcastBuildAdmission.accepts(bound, 10))
  }

  test("concatenation growth is rejected by the driver rather than executor assembly") {
    val bound = BroadcastBuildAdmission.upperBound(Array(32833L), 32833, 32769, bytes)
    assert(bound.contains(BigInt(36993)))
    assert(!BroadcastBuildAdmission.accepts(bound, 32833))
    assert(BroadcastBuildAdmission.accepts(bound, 36993))
  }

  test("string bounds include 64-bit offsets and masks even for non-nullable fields") {
    val schema = StructType(Seq(StructField("text", StringType, nullable = false)))
    val bound = BroadcastBuildAdmission.upperBound(Array(100L), 100, 10, schema)
    assert(bound.contains(BigInt(252)))
    assert(!BroadcastBuildAdmission.accepts(bound, 251))
    assert(BroadcastBuildAdmission.accepts(bound, 252))
  }

  test("the uncompressed writer metric prevents compact map-status sizes hiding large builds") {
    val bound = BroadcastBuildAdmission.upperBound(Array(10L), 100, 2, bytes)
    assert(bound.contains(BigInt(164)))
    assert(!BroadcastBuildAdmission.accepts(bound, 100))
    val overflow = BroadcastBuildAdmission.upperBound(
      Array(Long.MaxValue, 1L), Long.MaxValue, 2, bytes)
    assert(overflow.exists(_ > Long.MaxValue))
    assert(!BroadcastBuildAdmission.accepts(overflow, Long.MaxValue))
  }

  test("unknown child element counts and binary layouts retain standard broadcast") {
    Seq[DataType](ArrayType(ByteType), MapType(IntegerType, LongType),
      StructType(Seq(StructField("child", ByteType))), BinaryType).foreach { dataType =>
      val schema = StructType(Seq(StructField("value", dataType)))
      assert(!BroadcastBuildAdmission.supportsSchema(schema))
      assert(BroadcastBuildAdmission.upperBound(Array(10L), 10, 2, schema).isEmpty)
    }
    assert(!BroadcastBuildAdmission.accepts(None, Long.MaxValue))
  }

  test("unavailable or inconsistent completed metrics decline the native path") {
    assert(BroadcastBuildAdmission.upperBound(Array(-1L), 10, 2, bytes).isEmpty)
    assert(BroadcastBuildAdmission.upperBound(Array(10L), -1, 2, bytes).isEmpty)
    assert(BroadcastBuildAdmission.upperBound(Array(10L), 10, -1, bytes).isEmpty)
    assert(BroadcastBuildAdmission.upperBound(Array(10L), 10, Int.MaxValue.toLong + 1,
      bytes).isEmpty)
    assert(BroadcastBuildAdmission.upperBound(Array(10L), 10, 0, bytes).isEmpty)
    assert(BroadcastBuildAdmission.upperBound(Array(10L), 0, 2, bytes).isEmpty)
    assert(!BroadcastBuildAdmission.accepts(Some(BigInt(0)), 0))
  }

  test("empty and rows-only builds remain eligible") {
    val empty = BroadcastBuildAdmission.upperBound(Array.empty[Long], 0, 0, bytes)
    assert(empty.contains(BigInt(0)))
    assert(BroadcastBuildAdmission.accepts(empty, 1))
    val rowsOnly = BroadcastBuildAdmission.upperBound(Array(100L), 0, 200, StructType(Nil))
    assert(rowsOnly.contains(BigInt(100)))
    assert(BroadcastBuildAdmission.accepts(rowsOnly, 100))
  }

  test("flat scalar schemas are eligible without relying on declared nullability") {
    val schema = StructType(Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DateType, TimestampType, TimestampNTZType, NullType,
      DecimalType(38, 5), StringType, YearMonthIntervalType(), DayTimeIntervalType())
      .zipWithIndex.map { case (dataType, index) => StructField(s"c$index", dataType) })
    assert(BroadcastBuildAdmission.supportsSchema(schema))
  }
}
