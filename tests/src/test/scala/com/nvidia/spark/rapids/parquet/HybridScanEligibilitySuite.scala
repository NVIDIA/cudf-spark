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

package com.nvidia.spark.rapids.parquet

import com.nvidia.spark.rapids.{DateTimeRebaseCorrected, DateTimeRebaseException, DateTimeRebaseLegacy, DateTimeRebaseMode}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class HybridScanEligibilitySuite extends AnyFunSuite {

  private val flatSchema = StructType(Seq(
    StructField("int_col", IntegerType),
    StructField("string_col", StringType)
  ))

  private def ineligibleReason(
      enabled: Boolean = true,
      schema: StructType = flatSchema,
      filters: Array[Filter] = Array(GreaterThan("int_col", 5)),
      isCaseSensitive: Boolean = true,
      useChunkedReader: Boolean = false,
      decompressAnyCpu: Boolean = false,
      useFieldId: Boolean = false,
      ignoreCorruptFiles: Boolean = false,
      dateRebaseMode: DateTimeRebaseMode = DateTimeRebaseException,
      int96RebaseMode: DateTimeRebaseMode = DateTimeRebaseException): Option[String] = {
    HybridScanParquetPartitionReader.ineligibleReason(enabled, schema,
      new HybridScanFilters(schema, isCaseSensitive, filters),
      useChunkedReader, decompressAnyCpu, useFieldId, ignoreCorruptFiles, dateRebaseMode,
      int96RebaseMode)
  }

  private def isEligible(
      enabled: Boolean = true,
      schema: StructType = flatSchema,
      filters: Array[Filter] = Array(GreaterThan("int_col", 5)),
      isCaseSensitive: Boolean = true,
      useChunkedReader: Boolean = false,
      decompressAnyCpu: Boolean = false,
      useFieldId: Boolean = false,
      ignoreCorruptFiles: Boolean = false,
      dateRebaseMode: DateTimeRebaseMode = DateTimeRebaseException,
      int96RebaseMode: DateTimeRebaseMode = DateTimeRebaseException): Boolean = {
    ineligibleReason(enabled, schema, filters, isCaseSensitive, useChunkedReader, decompressAnyCpu,
      useFieldId, ignoreCorruptFiles, dateRebaseMode, int96RebaseMode).isEmpty
  }

  test("a filtered read of a flat schema is eligible") {
    assert(isEligible())
  }

  test("the reader is only used when it is enabled") {
    assert(!isEligible(enabled = false))
  }

  test("reads the hybrid path does not handle are not eligible") {
    assert(!isEligible(useChunkedReader = true))
    assert(!isEligible(decompressAnyCpu = true))
    assert(!isEligible(useFieldId = true))
    assert(!isEligible(ignoreCorruptFiles = true))
  }

  test("a read with no columns is not eligible") {
    assert(!isEligible(schema = StructType(Seq.empty)))
  }

  test("a nested column is not eligible") {
    val nested = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("struct_col", StructType(Seq(StructField("child", IntegerType))))
    ))
    assert(!isEligible(schema = nested))
    val array = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("array_col", ArrayType(IntegerType))
    ))
    assert(!isEligible(schema = array))
  }

  test("primitive types the reader passes straight through are eligible") {
    val types = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
      DoubleType, StringType, DecimalType(10, 2))
    types.foreach { dataType =>
      val schema = StructType(Seq(StructField("int_col", IntegerType),
        StructField("other_col", dataType)))
      assert(isEligible(schema = schema), s"expected $dataType to be eligible")
    }
  }

  test("a binary column is not eligible") {
    // libcudf decodes a Parquet byte array as a string, so a binary read would come back as the
    // wrong type; it stays on legacy until the reader distinguishes the two.
    val schema = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("binary_col", BinaryType)
    ))
    assert(!isEligible(schema = schema))
  }

  test("datetime columns are only eligible when a needed rebase would be reported") {
    val schema = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("date_col", DateType)
    ))
    assert(isEligible(schema = schema))
    assert(!isEligible(schema = schema, dateRebaseMode = DateTimeRebaseCorrected))
    assert(!isEligible(schema = schema, dateRebaseMode = DateTimeRebaseLegacy))
    assert(!isEligible(schema = schema, int96RebaseMode = DateTimeRebaseCorrected))
  }

  test("a timestamp column follows the same rebase rule as a date column") {
    val schema = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("ts_col", TimestampType)
    ))
    assert(isEligible(schema = schema))
    assert(!isEligible(schema = schema, int96RebaseMode = DateTimeRebaseLegacy))
  }

  test("a conjunct on a timestamp column is withheld from the reader") {
    val schema = StructType(Seq(
      StructField("int_col", IntegerType),
      StructField("ts_col", TimestampType)
    ))
    assert(new HybridScanFilters(schema, isCaseSensitive = true,
      filters = Array(GreaterThan("ts_col", 0L))).pushed.isEmpty)
    assert(isEligible(schema = schema,
      filters = Array(GreaterThan("ts_col", 0L), GreaterThan("int_col", 5))))
    val pushable = new HybridScanFilters(schema, isCaseSensitive = false,
      filters = Array(And(GreaterThan("TS_COL", 0L), GreaterThan("int_col", 5)))).pushable
    assert(pushable.toSeq == Seq(GreaterThan("int_col", 5)))
    // Spark quotes a name with a space, so the check has to match the quoted form.
    val spaced = StructType(Seq(StructField("ts col", TimestampType)))
    assert(new HybridScanFilters(spaced, isCaseSensitive = true,
      filters = Array(GreaterThan("`ts col`", 0L))).pushable.isEmpty)
  }

  test("a conjunct with a date before 1582-10-15 is withheld from the reader") {
    val schema = StructType(Seq(StructField("date_col", DateType)))
    def pushable(filter: Filter): Array[Filter] =
      new HybridScanFilters(schema, isCaseSensitive = true, Array(filter)).pushable
    val old = java.time.LocalDate.of(1000, 1, 1)
    val cutover = java.time.LocalDate.of(1582, 10, 15)
    assert(pushable(EqualTo("date_col", old)).isEmpty)
    assert(pushable(EqualTo("date_col", java.sql.Date.valueOf("1000-01-01"))).isEmpty)
    assert(pushable(In("date_col", Array[Any](cutover, old))).isEmpty)
    assert(pushable(Or(GreaterThan("date_col", cutover), LessThan("date_col", old))).isEmpty)
    assert(pushable(GreaterThanOrEqual("date_col", cutover)).nonEmpty)
    assert(pushable(LessThan("date_col", java.sql.Date.valueOf("1998-09-02"))).nonEmpty)
  }

  test("a rebase mode only matters when the read has a datetime column") {
    assert(isEligible(dateRebaseMode = DateTimeRebaseLegacy,
      int96RebaseMode = DateTimeRebaseCorrected))
  }

  test("a read with nothing to push is eligible") {
    assert(isEligible(filters = Array.empty))
    assert(isEligible(filters = Array(StringStartsWith("string_col", "a"))))
  }

  test("a read that falls back throws when fallback is set to fail") {
    def check(enabled: Boolean, useChunkedReader: Boolean): Boolean =
      HybridScanParquetPartitionReader.isEligible(enabled, failOnFallback = true, flatSchema,
        new HybridScanFilters(flatSchema, true, Array(GreaterThan("int_col", 5))),
        useChunkedReader, decompressAnyCpu = false, useFieldId = false,
        ignoreCorruptFiles = false, DateTimeRebaseException, DateTimeRebaseException)
    val e = intercept[IllegalStateException] {
      check(enabled = true, useChunkedReader = true)
    }
    assert(e.getMessage.contains("chunked"))
    assert(check(enabled = true, useChunkedReader = false))
    // Only a read that asked for the hybrid path can fail for not getting it.
    assert(!check(enabled = false, useChunkedReader = false))
  }

  test("an Or is pushed only when both of its sides are") {
    def pushed(filter: Filter): Array[Filter] =
      new HybridScanFilters(flatSchema, isCaseSensitive = true, Array(filter)).pushed
    assert(pushed(Or(EqualTo("int_col", 1), EqualTo("int_col", 2))).nonEmpty)
    assert(pushed(Or(EqualTo("int_col", 1), StringStartsWith("string_col", "a"))).isEmpty)
  }

  test("a residual conjunct on a column the scan does not output is not eligible") {
    val filters: Array[Filter] = Array(
      GreaterThan("int_col", 5),
      StringStartsWith("not_projected", "a"))
    assert(!isEligible(filters = filters))
  }

  test("the reason a read is not eligible names what stopped it") {
    assert(ineligibleReason(useChunkedReader = true).exists(_.contains("chunked")))
    assert(ineligibleReason(useFieldId = true).exists(_.contains("field ID")))
    assert(ineligibleReason(ignoreCorruptFiles = true).exists(_.contains("corrupt")))
    assert(ineligibleReason(decompressAnyCpu = true).exists(_.contains("CPU")))
    assert(ineligibleReason(enabled = false).exists(_.contains("disabled")))
    val residual: Array[Filter] = Array(GreaterThan("int_col", 5),
      StringStartsWith("not_projected", "a"))
    assert(ineligibleReason(filters = residual).exists(_.contains("does not output")))
    val nested = StructType(Seq(StructField("struct_col",
      StructType(Seq(StructField("child", IntegerType))))))
    assert(ineligibleReason(schema = nested).exists(_.contains("struct_col")))
  }
}
