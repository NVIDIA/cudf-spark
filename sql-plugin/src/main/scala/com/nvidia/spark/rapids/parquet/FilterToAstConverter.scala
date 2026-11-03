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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.DType
import ai.rapids.cudf.ast.{AstExpression, BinaryOperation, BinaryOperator, ColumnNameReference, ColumnReference, Literal, UnaryOperation, UnaryOperator}
import com.nvidia.spark.rapids.{DataTypeUtils, GpuColumnVector}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, DateTimeUtils}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * How a set of Spark filters was split for a hybrid scan read.
 *
 * @param ast      uncompiled AST for `pushed`, or None if nothing was pushed. The reader only
 *                 references the compiled form, so compile it where it can stay open until every
 *                 reader using it is closed.
 * @param pushed   conjuncts the reader applies exactly
 * @param residual conjuncts the reader cannot apply; Spark's filter above the scan applies them
 */
case class FilterPushdownPlan(
    ast: Option[AstExpression],
    pushed: Array[Filter],
    residual: Array[Filter])

/**
 * Converts Spark filters into cuDF AST expressions for the Parquet hybrid scan reader, which uses
 * them to prune row groups and filter rows.
 *
 * The reader treats the AST as exact, so a conjunct that cannot be converted exactly is left
 * residual rather than approximated. Literals take their type from `schema`, not the file, so the
 * caller must not push a conjunct on a column the file lacks, decodes as another type, stores as
 * INT96, or wrote with LEGACY datetime rebasing.
 *
 * @param schema          the reader's include columns, in include order, since a
 *                        `ColumnReference(i)` is the i-th of them
 * @param isCaseSensitive whether column name matching is case-sensitive
 * @param useColumnNames  reference columns by name rather than by include index
 */
case class FilterToAstConverter(
    schema: StructType,
    isCaseSensitive: Boolean,
    useColumnNames: Boolean) extends Logging {

  private def normalize(name: String): String = {
    if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
  }

  private val columnsByName: Map[String, (Int, StructField)] =
    schema.fields.zipWithIndex.map { case (field, idx) =>
      normalize(FilterToAstConverter.filterColumnName(field.name)) -> ((idx, field))
    }.toMap

  /**
   * Split `filters`, an implicit conjunction, into the conjuncts the reader can evaluate and
   * those it cannot. Only top-level conjuncts can be left residual; an `Or` or `Not` converts
   * only if all of its children do.
   */
  def planPushdown(filters: Array[Filter]): FilterPushdownPlan = {
    val pushed = ArrayBuffer.empty[Filter]
    val residual = ArrayBuffer.empty[Filter]
    val exprs = ArrayBuffer.empty[AstExpression]
    val converted = FilterToAstConverter.splitConjuncts(filters).map { conjunct =>
      conjunct -> convertFilter(conjunct)
    }
    // Don't push an `IsNotNull` that a pushed predicate on the same column already implies (Spark
    // infers one beside most predicates): it changes no result but costs a per-row evaluation and
    // a per-row-group null statistic.
    val provenNotNull = converted.collect {
      // A not-null check proves nothing here, or IsNotNull(c) would make itself redundant.
      case (conjunct, Some(_)) if FilterToAstConverter.notNullCheckColumn(conjunct).isEmpty =>
        FilterToAstConverter.columnsProvenNotNull(conjunct).map(normalize)
    }.flatten.toSet
    converted.foreach { case (conjunct, expr) =>
      val redundant = FilterToAstConverter.notNullCheckColumn(conjunct)
          .exists(column => provenNotNull.contains(normalize(column)))
      expr match {
        case Some(e) if !redundant =>
          pushed += conjunct
          exprs += e
        case _ =>
          residual += conjunct
      }
    }
    if (residual.nonEmpty) {
      logInfo(s"Hybrid scan pushing [${pushed.mkString(", ")}]; " +
        s"[${residual.mkString(", ")}] will be applied after the scan")
    }
    FilterPushdownPlan(exprs.reduceOption(nullAwareAnd), pushed.toArray, residual.toArray)
  }

  /**
   * The AST for conjuncts that `planPushdown` pushed for the same schema, so the plan can be made
   * where the AST, which does not serialize, will not be used.
   */
  def pushedAst(pushed: Array[Filter]): Option[AstExpression] =
    pushed.map { conjunct =>
      convertFilter(conjunct).getOrElse(
        throw new IllegalArgumentException(s"Pushed conjunct $conjunct does not convert"))
    }.reduceOption(nullAwareAnd)

  /**
   * Convert a single Spark Filter to a cuDF AST expression, or None if it cannot be
   * represented exactly.
   */
  def convertFilter(filter: Filter): Option[AstExpression] = {
    filter match {
      case EqualTo(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.EQUAL)

      case EqualNullSafe(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.NULL_EQUAL)

      case GreaterThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER)

      case GreaterThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER_EQUAL)

      case LessThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS)

      case LessThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS_EQUAL)

      case IsNull(attribute) if !isNested(attribute) =>
        lookupColumn(attribute).map { case (col, _) =>
          new UnaryOperation(UnaryOperator.IS_NULL, col)
        }

      case IsNotNull(attribute) if !isNested(attribute) =>
        lookupColumn(attribute).map { case (col, _) =>
          new UnaryOperation(UnaryOperator.NOT,
            new UnaryOperation(UnaryOperator.IS_NULL, col))
        }

      case In(attribute, values) =>
        convertIn(attribute, values)

      case And(left, right) =>
        convertFilter(left).flatMap(l => convertFilter(right).map(nullAwareAnd(l, _)))

      case Or(left, right) =>
        convertFilter(left).flatMap(l => convertFilter(right).map(nullAwareOr(l, _)))

      case Not(child) =>
        convertFilter(child).map(new UnaryOperation(UnaryOperator.NOT, _))

      case other =>
        // String predicates (StringStartsWith/EndsWith/Contains) land here; libcudf ASTs have
        // no string-matching operators, so they stay residual.
        logInfo(s"Filter not convertible to a cuDF AST: $other")
        None
    }
  }

  /**
   * Kleene `AND`, as Spark means it: under a `NOT`, a propagating `LOGICAL_AND` would turn
   * `NOT(null AND false)` into null rather than true.
   */
  private def nullAwareAnd(left: AstExpression, right: AstExpression): AstExpression = {
    new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND, left, right)
  }

  /** Kleene `OR`. See `nullAwareAnd`. */
  private def nullAwareOr(left: AstExpression, right: AstExpression): AstExpression = {
    new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR, left, right)
  }

  private def convertComparison(
      attribute: String,
      value: Any,
      operator: BinaryOperator): Option[AstExpression] = {
    lookupColumn(attribute)
      .filter { case (_, dataType) => pushableFloatComparison(dataType, value, operator) }
      .flatMap { case (col, dataType) =>
        convertLiteral(value, dataType).map { literal =>
          operator match {
            case BinaryOperator.EQUAL => nullAwareEqual(col, literal)
            case _ => new BinaryOperation(operator, col, literal)
          }
        }
      }
  }

  /**
   * SQL `=` as `NOT(NOT_EQUAL(...))`, the shape `GpuEqualTo.convertToAst` uses. cuDF's `EQUAL` also
   * returns null for a null operand, so the two agree; this one keeps the plugin's AST producers
   * matching, and the Parquet reader folds it back to `EQUAL` before pruning.
   */
  private def nullAwareEqual(col: AstExpression, literal: AstExpression): AstExpression = {
    new UnaryOperation(UnaryOperator.NOT,
      new BinaryOperation(BinaryOperator.NOT_EQUAL, col, literal))
  }

  private def isFloatingPoint(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => true
    case _ => false
  }

  /** Whether a literal compared against a column is a NaN. */
  private def isNaNLiteral(value: Any): Boolean = value match {
    case f: Float => f.isNaN
    case d: Double => d.isNaN
    case _ => false
  }

  /**
   * cuDF ASTs compare floats with IEEE-754 semantics: a NaN equals nothing, not even itself, and
   * loses every ordered comparison. Spark instead orders NaN above every value and equal to
   * itself, so `>`, `>=` and any NaN literal would drop NaN rows Spark keeps. `=` agrees on rows
   * but not in statistics pruning: parquet-mr writes `max = NaN` for a row group holding a NaN,
   * and cuDF prunes `=` on `vmax >= lit`. That leaves `<`, `<=` and `<=>` against a non-NaN
   * literal; the rest stay residual for Spark's post-scan filter.
   */
  private def pushableFloatComparison(
      dataType: DataType, value: Any, operator: BinaryOperator): Boolean = {
    !isFloatingPoint(dataType) ||
      (!isNaNLiteral(value) &&
        operator != BinaryOperator.GREATER &&
        operator != BinaryOperator.GREATER_EQUAL &&
        operator != BinaryOperator.EQUAL)
  }

  /**
   * Convert `IN` to OR-ed equalities. Every value must convert. The chain is one level deeper per
   * value and cuDF walks it recursively, so a long one overflows the stack; lists longer than
   * `MaxInValues` stay residual.
   */
  private def convertIn(attribute: String, values: Array[Any]): Option[AstExpression] = {
    // The AST has no "always false" literal for an empty list.
    if (values.isEmpty || values.length > FilterToAstConverter.MaxInValues) {
      None
    } else {
      lookupColumn(attribute)
        .filter { case (_, dataType) => !isFloatingPoint(dataType) }
        .flatMap { case (col, dataType) =>
          allOrNothing(values.map(convertLiteral(_, dataType))).map { literals =>
            literals.map(nullAwareEqual(col, _)).reduce(nullAwareOr)
          }
        }
    }
  }

  /** Some(values) only if every element is defined. */
  private def allOrNothing[T: ClassTag](converted: Array[Option[T]]): Option[Array[T]] = {
    if (converted.forall(_.isDefined)) Some(converted.map(_.get)) else None
  }

  /** The type of the column a filter calls `attribute`, or None if it is not being read. */
  def columnType(attribute: String): Option[DataType] =
    columnsByName.get(normalize(attribute)).map(_._2.dataType)

  /** Whether every column `filters` reference is in the schema, so its rows can be filtered. */
  def resolvesAllColumns(filters: Array[Filter]): Boolean =
    filters.forall(FilterToAstConverter.getReferencedColumns(_).forall(columnType(_).isDefined))

  /**
   * A reference to `attribute` and its type, or None if it is not a column being read. None too
   * for a name with a dot, which libcudf reads as a struct path and can resolve to a struct field.
   */
  private def lookupColumn(attribute: String): Option[(AstExpression, DataType)] = {
    columnsByName.get(normalize(attribute)).collect { case (colIdx, field)
        if !field.name.contains('.') =>
      val col = if (useColumnNames) {
        new ColumnNameReference(field.name)
      } else {
        new ColumnReference(colIdx)
      }
      (col, field.dataType)
    }
  }

  /**
   * Whether `attribute` is a list, map, struct or variant column, which cuDF cannot null check.
   * The plugin reads a variant as a struct.
   */
  private def isNested(attribute: String): Boolean =
    lookupColumn(attribute).exists { case (_, dataType) =>
      DataTypeUtils.isNestedType(dataType) || GpuColumnVector.isVariantType(dataType)
    }

  /**
   * The literal for `value` in the column's type, or None if `value` is not a type
   * `ParquetFilters.valueCanMakeFilterOn` accepts for it. A value outside the column's range is
   * rejected rather than narrowed, since a narrowed value would prune rows it should keep.
   * Datetimes convert as `ParquetFilters` converts them outside `LEGACY` rebase mode; the
   * reader's gate admits only `EXCEPTION`.
   */
  private def convertLiteral(value: Any, dataType: DataType): Option[Literal] = {
    (dataType, value) match {
      case (_, null) => sparkTypeToCudfType(dataType).map(cudfType => Literal.ofNull(cudfType))
      case (BooleanType, v: JBoolean) => Some(Literal.ofBoolean(v))
      case (ByteType, FilterToAstConverter.IntegralValue(v)) if v.isValidByte =>
        Some(Literal.ofByte(v.toByte))
      case (ShortType, FilterToAstConverter.IntegralValue(v)) if v.isValidShort =>
        Some(Literal.ofShort(v.toShort))
      case (IntegerType, FilterToAstConverter.IntegralValue(v)) if v.isValidInt =>
        Some(Literal.ofInt(v.toInt))
      case (LongType, v: JLong) => Some(Literal.ofLong(v))
      case (FloatType, v: JFloat) => Some(Literal.ofFloat(v))
      case (DoubleType, v: JDouble) => Some(Literal.ofDouble(v))
      case (StringType, v: String) => Some(Literal.ofString(v))
      case (DateType, d: Date) =>
        Some(Literal.ofTimestampDaysFromInt(DateTimeUtils.fromJavaDate(d)))
      case (DateType, d: LocalDate) =>
        Some(Literal.ofTimestampDaysFromInt(DateTimeUtils.localDateToDays(d)))
      case (TimestampType, t: Timestamp) =>
        Some(Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS,
          DateTimeUtils.fromJavaTimestamp(t)))
      case (TimestampType, i: Instant) =>
        Some(Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS,
          DateTimeUtils.instantToMicros(i)))
      case _ =>
        // Decimal needs the column's precision and scale; nested types have no AST form.
        logInfo(s"No cuDF AST literal for $value as $dataType")
        None
    }
  }

  private def sparkTypeToCudfType(dataType: DataType): Option[DType] = dataType match {
    case BooleanType => Some(DType.BOOL8)
    case ByteType => Some(DType.INT8)
    case ShortType => Some(DType.INT16)
    case IntegerType => Some(DType.INT32)
    case LongType => Some(DType.INT64)
    case FloatType => Some(DType.FLOAT32)
    case DoubleType => Some(DType.FLOAT64)
    case StringType => Some(DType.STRING)
    case DateType => Some(DType.TIMESTAMP_DAYS)
    case TimestampType => Some(DType.TIMESTAMP_MICROSECONDS)
    case _ => None
  }
}

object FilterToAstConverter {

  /** The longest `IN` list pushed, the default of Spark's `parquet.pushdown.inFilterThreshold`. */
  private[parquet] val MaxInValues = 10

  /**
   * How a pushed filter names top-level column `name`. With nested predicate pushdown, on by
   * default for Parquet, Spark writes struct field `b` of `a` as `a.b` and a column named `a.b` as
   * `` `a.b` ``, quoting any name that is not plain letters, digits and underscores.
   */
  private def filterColumnName(name: String): String = quoteIfNeeded(name)

  /** A boxed integer that `ParquetFilters` accepts for a byte, short or int column. */
  private object IntegralValue {
    def unapply(value: Any): Option[Long] = value match {
      case v: JByte => Some(v.longValue())
      case v: JShort => Some(v.longValue())
      case v: Integer => Some(v.longValue())
      case v: JLong => Some(v.longValue())
      case _ => None
    }
  }

  /** Flatten nested `And`s into top-level conjuncts, so each can be pushed or left residual. */
  def splitConjuncts(filters: Array[Filter]): Array[Filter] = {
    def split(filter: Filter): Seq[Filter] = filter match {
      case And(left, right) => split(left) ++ split(right)
      case other => Seq(other)
    }
    filters.flatMap(split)
  }

  /**
   * Whether a filter can rule out a row group using its dictionary pages. libcudf's dictionary
   * filter only acts on `EQUAL` and `NOT_EQUAL`.
   */
  def canPruneWithDictionary(filter: Filter): Boolean = filter match {
    case _: EqualTo | _: EqualNullSafe => true
    case In(_, values) => values.nonEmpty && values.length <= MaxInValues
    case And(left, right) => canPruneWithDictionary(left) || canPruneWithDictionary(right)
    case Or(left, right) => canPruneWithDictionary(left) && canPruneWithDictionary(right)
    case Not(child) => canPruneWithDictionary(child)
    case _ => false
  }

  /** The column a filter checks for being non-null, if checking that is all the filter does. */
  def notNullCheckColumn(filter: Filter): Option[String] = filter match {
    case IsNotNull(attribute) => Some(attribute)
    case Not(IsNull(attribute)) => Some(attribute)
    case _ => None
  }

  /**
   * The columns a filter proves are not null wherever it holds. An `Or` proves only what both
   * sides prove; a negation proves nothing.
   */
  def columnsProvenNotNull(filter: Filter): Set[String] = filter match {
    case IsNotNull(attribute) => Set(attribute)
    case Not(IsNull(attribute)) => Set(attribute)
    case EqualTo(attribute, _) => Set(attribute)
    // `c <=> null` is how a null is matched, so it proves the opposite.
    case EqualNullSafe(attribute, value) if value != null => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case GreaterThanOrEqual(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case LessThanOrEqual(attribute, _) => Set(attribute)
    case In(attribute, values) if values.nonEmpty => Set(attribute)
    case StringStartsWith(attribute, _) => Set(attribute)
    case StringEndsWith(attribute, _) => Set(attribute)
    case StringContains(attribute, _) => Set(attribute)
    case And(left, right) => columnsProvenNotNull(left) ++ columnsProvenNotNull(right)
    case Or(left, right) => columnsProvenNotNull(left).intersect(columnsProvenNotNull(right))
    case _ => Set.empty
  }

  /** The column names a filter references. */
  def getReferencedColumns(filter: Filter): Set[String] = filter match {
    case EqualTo(attribute, _) => Set(attribute)
    case EqualNullSafe(attribute, _) => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case GreaterThanOrEqual(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case LessThanOrEqual(attribute, _) => Set(attribute)
    case IsNull(attribute) => Set(attribute)
    case IsNotNull(attribute) => Set(attribute)
    case In(attribute, _) => Set(attribute)
    case StringStartsWith(attribute, _) => Set(attribute)
    case StringEndsWith(attribute, _) => Set(attribute)
    case StringContains(attribute, _) => Set(attribute)
    case And(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
    case Or(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
    case Not(child) => getReferencedColumns(child)
    case _ => Set.empty
  }
}
