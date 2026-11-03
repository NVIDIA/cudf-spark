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

import scala.concurrent.duration.Duration

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, LessThan, Literal, Rand}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, ProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class DriverlessBroadcastBuildAdmissionSuite extends AnyFunSuite {
  test("disabled broadcast timeout and huge positive timeouts remain infinite") {
    val conf = new SQLConf
    conf.setConfString("spark.sql.broadcastTimeout", "-1")
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(conf.broadcastTimeout) == Duration.Inf)
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(Long.MaxValue) == Duration.Inf)
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(300) == Duration(300, "seconds"))
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(0) == Duration.Zero)
  }

  test("non-determinism in a build projection or filter declines the driverless path") {
    val key = AttributeReference("key", IntegerType)()
    val scan = LocalTableScanExec(Seq(key), Seq(InternalRow(1)))
    assert(DriverlessBroadcastBuildAdmission.supportsPlan(scan))
    val randomProject = ProjectExec(Seq(Alias(Rand(Literal(42L)), "value")()), scan)
    assert(!DriverlessBroadcastBuildAdmission.supportsPlan(randomProject))
    assert(DriverlessBroadcastBuildAdmission.rejectionReason(randomProject)
      .exists(_.contains("non-deterministic")))
    val randomFilter = FilterExec(LessThan(Rand(Literal(42L)), Literal(0.5d)), scan)
    val deterministicProject = ProjectExec(Seq(key), randomFilter)
    assert(!DriverlessBroadcastBuildAdmission.supportsPlan(deterministicProject))
  }

  private val bytes = StructType(Seq(StructField("value", ByteType)))

  test("the driver includes a full padded validity mask before admission") {
    val bound = DriverlessBroadcastBuildAdmission.upperBound(Array(4L, 6L), 10, 2, bytes)
    assert(bound.contains(BigInt(74)))
    assert(DriverlessBroadcastBuildAdmission.accepts(bound, 74))
    assert(!DriverlessBroadcastBuildAdmission.accepts(bound, 73))
    assert(!DriverlessBroadcastBuildAdmission.accepts(bound, 10))
  }

  test("concatenation growth is rejected by the driver rather than executor assembly") {
    val bound = DriverlessBroadcastBuildAdmission.upperBound(Array(32833L), 32833, 32769, bytes)
    assert(bound.contains(BigInt(36993)))
    assert(!DriverlessBroadcastBuildAdmission.accepts(bound, 32833))
    assert(DriverlessBroadcastBuildAdmission.accepts(bound, 36993))
  }

  test("string bounds include 64-bit offsets and masks even for non-nullable fields") {
    val schema = StructType(Seq(StructField("text", StringType, nullable = false)))
    val bound = DriverlessBroadcastBuildAdmission.upperBound(Array(100L), 100, 10, schema)
    assert(bound.contains(BigInt(252)))
    assert(!DriverlessBroadcastBuildAdmission.accepts(bound, 251))
    assert(DriverlessBroadcastBuildAdmission.accepts(bound, 252))
  }

  test("the uncompressed writer metric prevents compact map-status sizes hiding large builds") {
    val bound = DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 100, 2, bytes)
    assert(bound.contains(BigInt(164)))
    assert(!DriverlessBroadcastBuildAdmission.accepts(bound, 100))
    val overflow = DriverlessBroadcastBuildAdmission.upperBound(
      Array(Long.MaxValue, 1L), Long.MaxValue, 2, bytes)
    assert(overflow.exists(_ > Long.MaxValue))
    assert(!DriverlessBroadcastBuildAdmission.accepts(overflow, Long.MaxValue))
  }

  test("unknown child element counts and binary layouts retain standard broadcast") {
    Seq[DataType](ArrayType(ByteType), MapType(IntegerType, LongType),
      StructType(Seq(StructField("child", ByteType))), BinaryType).foreach { dataType =>
      val schema = StructType(Seq(StructField("value", dataType)))
      assert(!DriverlessBroadcastBuildAdmission.supportsSchema(schema))
      assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 10, 2, schema).isEmpty)
    }
    assert(!DriverlessBroadcastBuildAdmission.accepts(None, Long.MaxValue))
  }

  test("unavailable or inconsistent completed metrics decline the native path") {
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(-1L), 10, 2, bytes).isEmpty)
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), -1, 2, bytes).isEmpty)
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 10, -1, bytes).isEmpty)
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 10, Int.MaxValue.toLong + 1,
      bytes).isEmpty)
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 10, 0, bytes).isEmpty)
    assert(DriverlessBroadcastBuildAdmission.upperBound(Array(10L), 0, 2, bytes).isEmpty)
    assert(!DriverlessBroadcastBuildAdmission.accepts(Some(BigInt(0)), 0))
  }

  test("empty and rows-only builds remain eligible") {
    val empty = DriverlessBroadcastBuildAdmission.upperBound(Array.empty[Long], 0, 0, bytes)
    assert(empty.contains(BigInt(0)))
    assert(DriverlessBroadcastBuildAdmission.accepts(empty, 1))
    val rowsOnly = DriverlessBroadcastBuildAdmission.upperBound(
      Array(100L), 0, 200, StructType(Nil))
    assert(rowsOnly.contains(BigInt(100)))
    assert(DriverlessBroadcastBuildAdmission.accepts(rowsOnly, 100))
  }

  test("flat scalar schemas are eligible without relying on declared nullability") {
    val schema = StructType(Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DateType, TimestampType, TimestampNTZType, NullType,
      DecimalType(38, 5), StringType, YearMonthIntervalType(), DayTimeIntervalType())
      .zipWithIndex.map { case (dataType, index) => StructField(s"c$index", dataType) })
    assert(DriverlessBroadcastBuildAdmission.supportsSchema(schema))
  }
}
