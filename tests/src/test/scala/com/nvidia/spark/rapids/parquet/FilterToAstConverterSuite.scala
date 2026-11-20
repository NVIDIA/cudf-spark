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

import java.sql.Date
import java.time.{Instant, LocalDate}

import ai.rapids.cudf.{ColumnVector, DType, Table}
import ai.rapids.cudf.ast.{AstExpression, BinaryOperation, BinaryOperator, ColumnNameReference, ColumnReference, CompiledExpression, Literal, UnaryOperation, UnaryOperator}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class FilterToAstConverterSuite extends AnyFunSuite {

  private val testSchema = StructType(Seq(
    StructField("int_col", IntegerType),
    StructField("long_col", LongType),
    StructField("float_col", FloatType),
    StructField("double_col", DoubleType),
    StructField("string_col", StringType),
    StructField("bool_col", BooleanType),
    StructField("date_col", DateType),
    StructField("ts_col", TimestampType),
    StructField("dec_col", DecimalType(10, 2)),
    StructField("byte_col", ByteType),
    StructField("short_col", ShortType),
    StructField("arr_col", ArrayType(IntegerType)),
    StructField("map_col", MapType(StringType, IntegerType))
  ))

  private def converter(caseSensitive: Boolean = true, useColumnNames: Boolean = false) =
    FilterToAstConverter(testSchema, caseSensitive, useColumnNames)

  private def withPlan(filters: Filter*)(body: FilterPushdownPlan => Unit): Unit = {
    body(converter().planPushdown(filters.toArray))
  }

  test("canPruneWithDictionary accepts only equality and In") {
    val tooMany = (0 to FilterToAstConverter.MaxInValues).toArray[Any]
    assert(!FilterToAstConverter.canPruneWithDictionary(In("int_col", tooMany)))
    assert(FilterToAstConverter.canPruneWithDictionary(EqualTo("int_col", 1)))
    assert(FilterToAstConverter.canPruneWithDictionary(EqualNullSafe("int_col", 1)))
    assert(FilterToAstConverter.canPruneWithDictionary(In("int_col", Array(1, 2))))
    assert(FilterToAstConverter.canPruneWithDictionary(Not(EqualTo("int_col", 1))))
    assert(!FilterToAstConverter.canPruneWithDictionary(GreaterThan("int_col", 1)))
    assert(!FilterToAstConverter.canPruneWithDictionary(IsNull("int_col")))
    assert(!FilterToAstConverter.canPruneWithDictionary(IsNotNull("int_col")))
    assert(FilterToAstConverter.canPruneWithDictionary(
      And(GreaterThan("int_col", 1), EqualTo("string_col", "a"))))
    assert(!FilterToAstConverter.canPruneWithDictionary(
      Or(EqualTo("int_col", 1), GreaterThan("int_col", 2))))
  }

  test("convertFilter leaves float and double greater-than and equality comparisons residual") {
    // cuDF ASTs use IEEE-754, where NaN loses every ordered comparison; Spark keeps NaN rows for
    // `>`/`>=`. `=` agrees on rows, but parquet-mr writes max = NaN and cuDF prunes `=` on
    // vmax >= lit. `<`, `<=` and `<=>` still push.
    val c = converter()
    for ((col, one) <- Seq[(String, Any)]("float_col" -> 1.0f, "double_col" -> 1.0)) {
      assert(c.convertFilter(GreaterThan(col, one)).isEmpty, s"$col > should be residual")
      assert(c.convertFilter(GreaterThanOrEqual(col, one)).isEmpty, s"$col >= should be residual")
      assert(c.convertFilter(EqualTo(col, one)).isEmpty, s"$col = should be residual")
      assert(c.convertFilter(Not(EqualTo(col, one))).isEmpty, s"$col != should be residual")
      assert(c.convertFilter(In(col, Array[Any](one))).isEmpty, s"$col IN should be residual")
      assert(c.convertFilter(LessThan(col, one)).isDefined, s"$col < should push")
      assert(c.convertFilter(LessThanOrEqual(col, one)).isDefined, s"$col <= should push")
      assert(c.convertFilter(EqualNullSafe(col, one)).isDefined, s"$col <=> should push")
      assert(c.convertFilter(IsNull(col)).isDefined, s"$col IsNull should push")
      assert(c.convertFilter(IsNotNull(col)).isDefined, s"$col IsNotNull should push")
    }
    // Integer `>` is unaffected -- the gate is only for floating point.
    assert(c.convertFilter(GreaterThan("int_col", 1)).isDefined)
  }

  test("convertFilter leaves any NaN-literal float or double comparison residual") {
    // Every cuDF comparison against NaN is false, so Spark's `< NaN`, `<=> NaN`, etc. all diverge.
    val c = converter()
    assert(c.convertFilter(LessThan("float_col", Float.NaN)).isEmpty)
    assert(c.convertFilter(LessThanOrEqual("double_col", Double.NaN)).isEmpty)
    assert(c.convertFilter(EqualNullSafe("double_col", Double.NaN)).isEmpty)
  }

  // Expected-AST builders. Each mirrors a shape FilterToAstConverter emits, so a test states the
  // tree it expects in one line. Literals stay explicit, so a test names the cuDF type it expects.

  /** A `testSchema` column, referenced by index or by name as the converter would. */
  private def col(name: String, byName: Boolean = false): AstExpression =
    if (byName) new ColumnNameReference(name) else new ColumnReference(testSchema.fieldIndex(name))

  private def micros(value: Long): Literal =
    Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS, value)

  private def days(value: Int): Literal = Literal.ofTimestampDaysFromInt(value)

  private def binary(op: BinaryOperator, l: AstExpression, r: AstExpression): AstExpression =
    new BinaryOperation(op, l, r)

  private def not(e: AstExpression): AstExpression = new UnaryOperation(UnaryOperator.NOT, e)

  /** SQL `=` as the converter builds it: NOT(NOT_EQUAL), which keeps a null operand null. */
  private def eqTo(c: AstExpression, l: AstExpression): AstExpression =
    not(binary(BinaryOperator.NOT_EQUAL, c, l))

  private def nullSafeEq(c: AstExpression, l: AstExpression): AstExpression =
    binary(BinaryOperator.NULL_EQUAL, c, l)

  private def gt(c: AstExpression, l: AstExpression): AstExpression =
    binary(BinaryOperator.GREATER, c, l)

  private def ge(c: AstExpression, l: AstExpression): AstExpression =
    binary(BinaryOperator.GREATER_EQUAL, c, l)

  private def lt(c: AstExpression, l: AstExpression): AstExpression =
    binary(BinaryOperator.LESS, c, l)

  private def le(c: AstExpression, l: AstExpression): AstExpression =
    binary(BinaryOperator.LESS_EQUAL, c, l)

  private def isNull(c: AstExpression): AstExpression =
    new UnaryOperation(UnaryOperator.IS_NULL, c)

  private def notNull(c: AstExpression): AstExpression = not(isNull(c))

  private def and(l: AstExpression, r: AstExpression): AstExpression =
    binary(BinaryOperator.NULL_LOGICAL_AND, l, r)

  private def or(l: AstExpression, r: AstExpression): AstExpression =
    binary(BinaryOperator.NULL_LOGICAL_OR, l, r)

  /** `IN` as the converter builds it: a left-deep Kleene OR of SQL equalities. */
  private def in(c: AstExpression, literals: Literal*): AstExpression =
    literals.map(eqTo(c, _)).reduce((l, r) => or(l, r))

  /** Asserts that `filter` converts to exactly `expected`, compared structurally. */
  private def assertPushes(filter: Filter, expected: AstExpression,
      c: FilterToAstConverter = converter()): Unit = {
    val actual = c.convertFilter(filter)
    assert(actual.isDefined, s"$filter should push")
    assertResult(expected.toString, s"for $filter")(actual.get.toString)
  }

  private def assertResidual(filter: Filter, c: FilterToAstConverter = converter()): Unit =
    assert(c.convertFilter(filter).isEmpty, s"$filter should stay residual")

  private def isFloatingPoint(name: String): Boolean = testSchema(name).dataType match {
    case FloatType | DoubleType => true
    case _ => false
  }

  /** A Spark comparison and the cuDF operation it must become. */
  private case class Comparison(
      symbol: String,
      filter: (String, Any) => Filter,
      expected: (AstExpression, AstExpression) => AstExpression)

  private val comparisons = Seq(
    Comparison("=", EqualTo(_, _), eqTo),
    Comparison("<=>", EqualNullSafe(_, _), nullSafeEq),
    Comparison(">", GreaterThan(_, _), gt),
    Comparison(">=", GreaterThanOrEqual(_, _), ge),
    Comparison("<", LessThan(_, _), lt),
    Comparison("<=", LessThanOrEqual(_, _), le))

  /** A value Spark may push for each column type, and the cuDF literal it must become. */
  private val literalCases: Seq[(String, Any, Literal)] = Seq(
    ("int_col", 42, Literal.ofInt(42)),
    ("long_col", 100L, Literal.ofLong(100L)),
    ("float_col", 3.14f, Literal.ofFloat(3.14f)),
    ("double_col", 2.718, Literal.ofDouble(2.718)),
    ("string_col", "x", Literal.ofString("x")),
    ("bool_col", true, Literal.ofBoolean(true)),
    ("byte_col", 5.toByte, Literal.ofByte(5.toByte)),
    ("short_col", 5.toShort, Literal.ofShort(5.toShort)),
    ("date_col", Date.valueOf("2026-08-04"), days(20669)),
    ("date_col", LocalDate.of(2026, 8, 4), days(20669)),
    // Spark pushes DateTimeUtils.toJavaDate(days): before the epoch, and before 1582.
    ("date_col", DateTimeUtils.toJavaDate(-1), days(-1)),
    ("date_col", DateTimeUtils.toJavaDate(-200000), days(-200000)),
    ("ts_col", Instant.ofEpochSecond(1786000000L, 123000L), micros(1786000000000123L)),
    ("ts_col", DateTimeUtils.toJavaTimestamp(1786000000000123L), micros(1786000000000123L)),
    // Before the epoch with sub-millisecond digits: Timestamp.getTime floors to the millisecond.
    ("ts_col", DateTimeUtils.toJavaTimestamp(-1L), micros(-1L)))

  test("convertFilter maps every comparison over every column type to its cuDF operation") {
    // Float and double `=`, `>` and `>=` stay residual; the tests above say why.
    val floatResidual = Set("=", ">", ">=")
    for (Comparison(symbol, filter, expected) <- comparisons; (name, value, lit) <- literalCases) {
      if (isFloatingPoint(name) && floatResidual.contains(symbol)) {
        assertResidual(filter(name, value))
      } else {
        assertPushes(filter(name, value), expected(col(name), lit))
      }
    }
  }

  test("convertFilter builds null checks and Kleene Not, And, Or and In") {
    val (intCol, longCol) = (col("int_col"), col("long_col"))
    assertPushes(IsNull("int_col"), isNull(intCol))
    assertPushes(IsNotNull("int_col"), notNull(intCol))
    assertPushes(EqualNullSafe("int_col", null), nullSafeEq(intCol, Literal.ofNull(DType.INT32)))
    assertPushes(Not(EqualTo("int_col", 1)), not(eqTo(intCol, Literal.ofInt(1))))
    assertPushes(And(EqualTo("int_col", 1), EqualTo("long_col", 2L)),
      and(eqTo(intCol, Literal.ofInt(1)), eqTo(longCol, Literal.ofLong(2L))))
    assertPushes(Or(EqualTo("int_col", 1), EqualTo("long_col", 2L)),
      or(eqTo(intCol, Literal.ofInt(1)), eqTo(longCol, Literal.ofLong(2L))))
    assertPushes(In("int_col", Array[Any](1, 2, 3)),
      in(intCol, Literal.ofInt(1), Literal.ofInt(2), Literal.ofInt(3)))
    // Not(int_col IN (1, null)) must reject 2: Spark computes the In as null, so Not is null.
    assertPushes(Not(In("int_col", Array[Any](1, null))),
      not(in(intCol, Literal.ofInt(1), Literal.ofNull(DType.INT32))))
  }

  test("convertFilter builds the micros Spark means for a pushed java.sql.Timestamp") {
    // Spark pushes DateTimeUtils.toJavaTimestamp(micros), which rebases to the hybrid calendar,
    // so the converter has to undo that rebase. 1500-01-01T00:00:00.123456Z is the first value.
    Seq(-14831769599876544L, -1L, 1786000000000123L).foreach { m =>
      assertPushes(EqualTo("ts_col", DateTimeUtils.toJavaTimestamp(m)),
        eqTo(col("ts_col"), micros(m)))
    }
  }

  test("convertLiteral accepts the boxed types ParquetFilters does, and only in range") {
    val c = converter()
    def pushes(f: Filter): Boolean = c.convertFilter(f).isDefined
    // Byte, short and int columns take any boxed integer that fits.
    for (col <- Seq("byte_col", "short_col", "int_col")) {
      for (v <- Seq[Any](5.toByte, 5.toShort, 5, 5L)) {
        assert(pushes(LessThan(col, v)), s"$col < ${v.getClass.getSimpleName} should push")
      }
    }
    assertPushes(EqualTo("byte_col", 5L), eqTo(col("byte_col"), Literal.ofByte(5.toByte)))
    // Narrowing would compare against a different value, so an out-of-range literal stays residual.
    assert(!pushes(LessThan("byte_col", 300)))
    assert(!pushes(LessThan("short_col", 40000)))
    assert(!pushes(LessThan("int_col", 4294967296L)))
    assert(!pushes(In("int_col", Array[Any](1, 4294967301L))))
    // Every other type takes only its own box.
    assert(!pushes(EqualTo("long_col", 1)))
    assert(!pushes(LessThan("float_col", 1.0)))
    assert(!pushes(LessThan("double_col", 1.0f)))
    assert(!pushes(EqualTo("bool_col", 1)))
    assert(!pushes(EqualTo("string_col", 1)))
    // A date or timestamp has to arrive as a date or time object, as ParquetFilters requires.
    assert(!pushes(EqualTo("date_col", 20669)))
    assert(!pushes(EqualTo("ts_col", 1786000000000000L)))
  }

  test("convertFilter references a column by index or by name per useColumnNames") {
    // Index mode resolves the attribute to its schema position.
    assertPushes(EqualTo("long_col", 1L), eqTo(new ColumnReference(1), Literal.ofLong(1L)),
      converter(useColumnNames = false))
    // Name mode carries the attribute name instead of an index.
    assertPushes(EqualTo("int_col", 1), eqTo(col("int_col", byName = true), Literal.ofInt(1)),
      converter(useColumnNames = true))
    // A case-insensitive match still resolves, to the schema's name.
    assertPushes(EqualTo("INT_COL", 1), eqTo(col("int_col", byName = true), Literal.ofInt(1)),
      converter(caseSensitive = false, useColumnNames = true))
  }

  test("convertFilter matches a column by the quoted name Spark pushes") {
    // Spark writes struct field b of a as a.b, and quotes a column whose name has a dot or space.
    // A dotted name stays residual: libcudf could resolve it to the struct field.
    val schema = StructType(Seq(
      StructField("a", StructType(Seq(StructField("b", IntegerType)))),
      StructField("a.b", IntegerType),
      StructField("my col", IntegerType)))
    val byIndex = FilterToAstConverter(schema, isCaseSensitive = true, useColumnNames = false)
    assert(byIndex.convertFilter(EqualTo("a.b", 1)).isEmpty)
    assert(byIndex.convertFilter(EqualTo("`a.b`", 1)).isEmpty)
    assertPushes(EqualTo("`my col`", 1), eqTo(new ColumnReference(2), Literal.ofInt(1)), byIndex)
    val byName = FilterToAstConverter(schema, isCaseSensitive = true, useColumnNames = true)
    assert(byName.convertFilter(EqualTo("`a.b`", 1)).isEmpty)
    assertPushes(EqualTo("`my col`", 1), eqTo(new ColumnNameReference("my col"), Literal.ofInt(1)),
      byName)
    assert(byName.resolvesAllColumns(Array(StringContains("`my col`", "x"))))
    assert(!byName.resolvesAllColumns(Array(StringContains("a.b", "x"))))
    assert(byName.columnType("`a.b`").contains(IntegerType))
    assert(byName.columnType("a.b").isEmpty)
  }

  test("planPushdown splits a float greater-than out of an And and pushes the rest") {
    withPlan(And(GreaterThan("float_col", 1.0f), EqualTo("int_col", 42))) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(GreaterThan("float_col", 1.0f)))
    }
  }

  test("convertFilter leaves a whole Or residual when a float greater-than side cannot push") {
    assert(converter().convertFilter(
      Or(GreaterThan("float_col", 1.0f), EqualTo("int_col", 42))).isEmpty)
  }

  test("convertFilter rejects a column outside the read schema") {
    assert(converter().convertFilter(EqualTo("unknown_col", 1)).isEmpty)
  }

  test("convertFilter rejects decimal literals") {
    // A decimal literal needs the column's precision and scale.
    assert(converter().convertFilter(EqualTo("dec_col", BigDecimal(1.5).bigDecimal)).isEmpty)
  }

  test("convertFilter rejects a literal whose type does not match the column") {
    assert(converter().convertFilter(EqualTo("int_col", "not a number")).isEmpty)
  }

  test("convertFilter honors case sensitivity") {
    assert(converter(caseSensitive = true).convertFilter(EqualTo("INT_COL", 1)).isEmpty)
    assert(converter(caseSensitive = false).convertFilter(EqualTo("INT_COL", 1)).isDefined)
  }

  test("convertFilter negates every filter it can convert") {
    val c = converter()
    Seq(
      EqualTo("int_col", 1),
      EqualNullSafe("int_col", 1),
      GreaterThan("int_col", 1),
      GreaterThanOrEqual("int_col", 1),
      LessThan("int_col", 1),
      LessThanOrEqual("int_col", 1),
      IsNull("int_col"),
      IsNotNull("int_col"),
      In("int_col", Array[Any](1, 2, 3)),
      And(EqualTo("int_col", 1), LessThan("long_col", 5L)),
      Or(EqualTo("int_col", 1), LessThan("long_col", 5L)),
      Not(EqualTo("int_col", 1))
    ).foreach { filter =>
      assert(c.convertFilter(Not(filter)).isDefined, s"Not($filter) should convert")
    }
  }

  test("convertFilter leaves a negation of an unconvertible filter residual") {
    val c = converter()
    assert(c.convertFilter(Not(In("int_col", Array.empty[Any]))).isEmpty)
    // One unconvertible side makes the whole negated filter residual.
    val withStringSide = And(EqualTo("int_col", 1), StringContains("string_col", "x"))
    assert(c.convertFilter(Not(withStringSide)).isEmpty)
    val withDecimalSide = Or(EqualTo("int_col", 1),
      EqualTo("dec_col", BigDecimal(1.5).bigDecimal))
    assert(c.convertFilter(Not(withDecimalSide)).isEmpty)
  }

  test("convertFilter rejects a whole Or when either side is unconvertible") {
    // Pushing only one side of an Or would drop rows the other side accepts.
    val c = converter()
    assert(c.convertFilter(
      Or(EqualTo("int_col", 1), StringStartsWith("string_col", "foo"))).isEmpty)
    assert(c.convertFilter(
      Or(StringStartsWith("string_col", "foo"), EqualTo("int_col", 1))).isEmpty)
    assert(c.convertFilter(Not(StringContains("string_col", "foo"))).isEmpty)
  }

  test("convertFilter rejects a whole In when any value is unconvertible") {
    // Dropping a value would reject the rows matching it.
    assert(converter().convertFilter(In("int_col", Array[Any](1, 2, "not a number"))).isEmpty)
    assert(converter().convertFilter(In("int_col", Array[Any](1, 2, 3))).isDefined)
  }

  test("convertFilter rejects an empty In") {
    assert(converter().convertFilter(In("int_col", Array.empty)).isEmpty)
  }

  test("planPushdown leaves an In longer than MaxInValues residual") {
    // Each value adds a level to the OR chain, and cuDF walks it recursively.
    val maxValues = (1 to FilterToAstConverter.MaxInValues).toArray[Any]
    assert(converter().convertFilter(In("int_col", maxValues)).isDefined)
    val tooMany = In("int_col", (0 to FilterToAstConverter.MaxInValues).toArray[Any])
    withPlan(tooMany, EqualTo("long_col", 1L)) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("long_col", 1L)))
      assert(plan.residual.toSeq == Seq(tooMany))
    }
  }

  test("planPushdown returns no AST and no residual for no filters") {
    withPlan() { plan =>
      assert(plan.ast.isEmpty)
      assert(plan.pushed.isEmpty)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown pushes every convertible conjunct") {
    withPlan(EqualTo("int_col", 42), GreaterThan("long_col", 100L)) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown reports an unconvertible conjunct as residual instead of dropping it") {
    val unsupported = StringStartsWith("string_col", "foo")
    withPlan(EqualTo("int_col", 42), unsupported) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown splits a nested And so its convertible half still pushes") {
    // The convertible half of a nested And is still pushed.
    val unsupported = StringContains("string_col", "foo")
    withPlan(And(EqualTo("int_col", 42), unsupported)) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown reports a column outside the read schema as residual") {
    val outside = EqualTo("unknown_col", 1)
    withPlan(EqualTo("int_col", 42), outside) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(outside))
    }
  }

  test("planPushdown returns no AST when nothing is convertible") {
    val filters = Seq[Filter](
      StringStartsWith("string_col", "foo"),
      StringEndsWith("string_col", "bar"))
    withPlan(filters: _*) { plan =>
      assert(plan.ast.isEmpty)
      assert(plan.pushed.isEmpty)
      assert(plan.residual.toSeq == filters)
    }
  }

  test("planPushdown leaves out a null check another pushed conjunct already implies") {
    // Spark infers this pair; `int_col = 42` already rules out a null.
    val redundant = IsNotNull("int_col")
    withPlan(redundant, EqualTo("int_col", 42)) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(redundant))
    }
    // `In` holds an Array, so compare against the same instances rather than equal copies.
    val notNull = Not(IsNull("int_col"))
    val in = In("int_col", Array[Any](1, 2))
    withPlan(notNull, in) { plan =>
      assert(plan.pushed.toSeq == Seq(in))
      assert(plan.residual.toSeq == Seq(notNull))
    }
  }

  test("planPushdown keeps a null check nothing else implies") {
    // A conjunct on a different column doesn't imply this null check.
    withPlan(IsNotNull("int_col"), EqualTo("long_col", 42L)) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
    // A null check is not implied by a conjunct that a null can satisfy.
    withPlan(IsNotNull("int_col"), EqualNullSafe("int_col", null)) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
    // Only one side of the Or rules out a null, so the Or as a whole does not.
    withPlan(IsNotNull("int_col"), Or(EqualTo("int_col", 1), IsNull("int_col"))) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown keeps a null check whose implying conjunct is not itself pushed") {
    // `startswith` stays residual, so it can't stand in for the null check.
    val unsupported = StringStartsWith("string_col", "foo")
    withPlan(IsNotNull("string_col"), unsupported) { plan =>
      assert(plan.pushed.toSeq == Seq(IsNotNull("string_col")))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown matches the null check column case-insensitively when asked to") {
    val plan = converter(caseSensitive = false)
        .planPushdown(Array[Filter](IsNotNull("INT_COL"), EqualTo("int_col", 42)))
    assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
    assert(plan.residual.toSeq == Seq(IsNotNull("INT_COL")))
  }

  test("convertFilter leaves a null check on a nested column residual") {
    // Spark infers these beside array_contains(arr_col, 1) and map_col['a'] = 1.
    val c = converter()
    assert(c.convertFilter(IsNotNull("arr_col")).isEmpty)
    assert(c.convertFilter(IsNull("map_col")).isEmpty)
    withPlan(IsNotNull("arr_col"), EqualTo("int_col", 1)) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 1)))
      assert(plan.residual.toSeq == Seq(IsNotNull("arr_col")))
    }
  }

  test("splitConjuncts flattens nested Ands but leaves Or and Not intact") {
    val or = Or(EqualTo("int_col", 1), EqualTo("int_col", 2))
    val not = Not(EqualTo("long_col", 3L))
    val split = FilterToAstConverter.splitConjuncts(
      Array(And(And(EqualTo("int_col", 1), IsNotNull("long_col")), or), not))
    assert(split.toSeq == Seq(EqualTo("int_col", 1), IsNotNull("long_col"), or, not))
  }

  test("resolvesAllColumns requires every referenced column to be read") {
    assert(converter().resolvesAllColumns(Array(StringContains("string_col", "x"))))
    assert(!converter().resolvesAllColumns(Array(StringContains("absent_col", "x"))))
    assert(converter().resolvesAllColumns(Array.empty))
  }

  test("resolvesAllColumns honors case sensitivity") {
    val residual = Array[Filter](StringContains("STRING_COL", "x"))
    assert(!converter(caseSensitive = true).resolvesAllColumns(residual))
    assert(converter(caseSensitive = false).resolvesAllColumns(residual))
  }

  test("getReferencedColumns collects columns through compound filters") {
    assert(FilterToAstConverter.getReferencedColumns(EqualTo("int_col", 1)) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      And(EqualTo("int_col", 1), GreaterThan("long_col", 100L))) == Set("int_col", "long_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      Or(EqualTo("int_col", 1), EqualTo("long_col", 100L))) == Set("int_col", "long_col"))
    assert(FilterToAstConverter.getReferencedColumns(Not(EqualTo("int_col", 1))) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      In("int_col", Array(1, 2, 3))) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      StringContains("string_col", "x")) == Set("string_col"))
  }

  // ---- Evaluation against Spark. These compile the AST and run it on the GPU, so they need the
  // cuDF native library and a CUDA device. Neither RMM nor a Spark session has to be set up.

  private val evalSchema = StructType(Seq(
    StructField("i", IntegerType), StructField("f", FloatType), StructField("d", DoubleType)))

  // Each null-handling choice changes a kept row here: a null beside a true and beside a false,
  // NaN in both float columns, and -0.0 and infinity.
  private val evalInts = Seq[java.lang.Integer](1, 2, null, 3, 1, null, null)
  private val evalFloats = Seq[java.lang.Float](
    1.0f, Float.NaN, null, -0.0f, 2.0f, Float.PositiveInfinity, 2.0f)
  private val evalDoubles = Seq[java.lang.Double](
    1.0, Double.NaN, null, 0.0, -1.0, Double.NaN, 1.0)

  private def evalConverter = FilterToAstConverter(evalSchema, isCaseSensitive = true,
    useColumnNames = false)

  private def withEvalTable[T](body: Table => T): T = {
    withResource(ColumnVector.fromBoxedInts(evalInts: _*)) { ints =>
      withResource(ColumnVector.fromBoxedFloats(evalFloats: _*)) { floats =>
        withResource(ColumnVector.fromBoxedDoubles(evalDoubles: _*)) { doubles =>
          withResource(new Table(ints, floats, doubles))(body)
        }
      }
    }
  }

  /** Rows where `compiled` is true. A null result drops the row, as a filter does. */
  private def gpuKeptRows(compiled: CompiledExpression): Seq[Int] = withEvalTable { table =>
    withResource(compiled.computeColumn(table)) { result =>
      withResource(result.copyToHost()) { host =>
        (0 until host.getRowCount.toInt).filter(r => !host.isNull(r) && host.getBoolean(r))
      }
    }
  }

  /** `filter` as a Catalyst predicate over `evalSchema`, so that Spark itself is the oracle. */
  private def toCatalyst(filter: Filter): expressions.Expression = {
    def ref(name: String): expressions.Expression = {
      val idx = evalSchema.fieldIndex(name)
      expressions.BoundReference(idx, evalSchema(idx).dataType, nullable = true)
    }
    def lit(name: String, value: Any): expressions.Expression =
      expressions.Literal.create(value, evalSchema(name).dataType)
    filter match {
      case EqualTo(a, v) => expressions.EqualTo(ref(a), lit(a, v))
      case EqualNullSafe(a, v) => expressions.EqualNullSafe(ref(a), lit(a, v))
      case GreaterThan(a, v) => expressions.GreaterThan(ref(a), lit(a, v))
      case GreaterThanOrEqual(a, v) => expressions.GreaterThanOrEqual(ref(a), lit(a, v))
      case LessThan(a, v) => expressions.LessThan(ref(a), lit(a, v))
      case LessThanOrEqual(a, v) => expressions.LessThanOrEqual(ref(a), lit(a, v))
      case IsNull(a) => expressions.IsNull(ref(a))
      case IsNotNull(a) => expressions.IsNotNull(ref(a))
      case In(a, values) => expressions.In(ref(a), values.toSeq.map(lit(a, _)))
      case And(l, r) => expressions.And(toCatalyst(l), toCatalyst(r))
      case Or(l, r) => expressions.Or(toCatalyst(l), toCatalyst(r))
      case Not(child) => expressions.Not(toCatalyst(child))
      case other => throw new IllegalArgumentException(s"No Catalyst form for $other")
    }
  }

  /** Rows where Spark's own evaluation of `filter` is true. */
  private def sparkKeptRows(filter: Filter): Seq[Int] = {
    val predicate = toCatalyst(filter)
    evalInts.indices.filter { r =>
      predicate.eval(InternalRow(evalInts(r), evalFloats(r), evalDoubles(r))) match {
        case true => true
        case _ => false
      }
    }
  }

  test("every pushed filter keeps the rows Spark keeps, over nulls, NaN and -0.0") {
    val c = evalConverter
    Seq[Filter](
      EqualTo("i", 1), Not(EqualTo("i", 1)), EqualNullSafe("i", null),
      Not(EqualNullSafe("i", 1)), LessThanOrEqual("i", 2), Not(GreaterThan("i", 1)),
      IsNull("i"), Not(IsNull("i")), In("i", Array[Any](1, 3)),
      In("i", Array[Any](1, null)), Not(In("i", Array[Any](1, null))),
      // Rows 5 and 6 pair a null with a false and with a true, so only Kleene AND and OR keep
      // what Spark keeps.
      Not(And(EqualTo("i", 2), LessThan("f", 1.0f))),
      Or(EqualTo("i", 1), LessThanOrEqual("f", 2.0f)),
      Not(Or(EqualTo("i", 1), LessThanOrEqual("f", 2.0f))),
      // Spark orders -0.0 equal to 0.0, as IEEE-754 does.
      LessThanOrEqual("f", 0.0f), LessThanOrEqual("d", -0.0),
      LessThanOrEqual("f", Float.PositiveInfinity), Not(LessThan("f", 1.5f)),
      Not(LessThan("d", 1.0))
    ).foreach { filter =>
      val ast = c.convertFilter(filter)
      assert(ast.isDefined, s"$filter should push")
      val gpuRows = withResource(ast.get.compile())(gpuKeptRows)
      assertResult(sparkKeptRows(filter), s"rows kept by $filter")(gpuRows)
    }
  }

  test("planPushdown's AST plus its residual keeps the rows Spark keeps") {
    val filters = Array[Filter](IsNotNull("i"), LessThanOrEqual("i", 3),
      Not(EqualTo("d", 1.0)), LessThanOrEqual("f", 2.0f), GreaterThan("f", -1.0f))
    val plan = evalConverter.planPushdown(filters)
    withResource(plan.ast.map(_.compile())) { ast =>
      assert(ast.isDefined)
      val kept = gpuKeptRows(ast.get)
        .filter(r => plan.residual.forall(f => sparkKeptRows(f).contains(r)))
      assertResult(sparkKeptRows(filters.reduce[Filter](And(_, _))))(kept)
    }
  }
}
