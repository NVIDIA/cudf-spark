/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.io.{File, FileOutputStream}

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.shims.{CastCheckShims, GpuTypeShims}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnaryExpression, WindowSpecDefinition}
import org.apache.spark.sql.types._

/**
 * The level of support that the plugin has for a given type.  Used for documentation generation.
 */
sealed abstract class SupportLevel {
  def htmlTag: String
  def text: String
}

/**
 * N/A neither spark nor the plugin supports this.
 */
object NotApplicable extends SupportLevel {
  override def htmlTag: String = "<td> </td>"
  override def text: String = "NA"
}

/**
 * Spark supports this but the plugin does not.
 */
object NotSupported extends SupportLevel {
  override def htmlTag: String = s"<td><b>$text</b></td>"
  override def text: String = "NS"
}

/**
 * Both Spark and the plugin support this.
 */
class Supported() extends SupportLevel {
  override def htmlTag: String = s"<td>$text</td>"
  override def text: String = "S"
}

/**
 * The plugin partially supports this type.
 * @param missingChildTypes child types that are not supported
 * @param needsLitWarning true if we need to warn that we only support a literal value when Spark
 *                        does not.
 * @param note any other notes we want to include about not complete support.
 */
class PartiallySupported(
    val missingChildTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    val needsLitWarning: Boolean = false,
    val note: Option[String] = None) extends SupportLevel {
  override def htmlTag: String = {
    val typeStr = if (missingChildTypes.isEmpty) {
      None
    } else {
      Some("unsupported child types " + missingChildTypes.mkString(", "))
    }
    val litOnly = if (needsLitWarning) {
      Some("Literal value only")
    } else {
      None
    }
    val extraInfo = (note.toSeq ++ litOnly.toSeq ++ typeStr.toSeq).mkString(";<br/>")
    val allText = s"$text<br/>$extraInfo"
    s"<td><em>$allText</em></td>"
  }

  // don't include the extra info in the supported text field for now
  // as the qualification tool doesn't use it
  override def text: String = "PS"
}

/**
 * The Supported Types. The TypeSig API should be preferred for this, except in a few cases when
 * TypeSig asks for a TypeEnum.
 */
object TypeEnum extends Enumeration {
  type TypeEnum = Value

  val BOOLEAN: Value = Value
  val BYTE: Value = Value
  val SHORT: Value = Value
  val INT: Value = Value
  val LONG: Value = Value
  val FLOAT: Value = Value
  val DOUBLE: Value = Value
  val DATE: Value = Value
  val TIMESTAMP: Value = Value
  val STRING: Value = Value
  val DECIMAL: Value = Value
  val NULL: Value = Value
  val BINARY: Value = Value
  val CALENDAR: Value = Value
  val ARRAY: Value = Value
  val MAP: Value = Value
  val STRUCT: Value = Value
  val UDT: Value = Value
  val DAYTIME: Value = Value
  val YEARMONTH: Value = Value
}

/**
 * A type signature. This is a bit limited in what it supports right now, but can express
 * a set of base types and a separate set of types that can be nested under the base types
 * (child types). It can also express if a particular base type has to be a literal or not.
 */
final class TypeSig private(
    private val initialTypes: TypeEnum.ValueSet,
    private val maxAllowedDecimalPrecision: Int = DType.DECIMAL64_MAX_PRECISION,
    private val childTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    private val litOnlyTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    private val notes: Map[TypeEnum.Value, String] = Map.empty) {

  /**
   * Add a literal restriction to the signature
   * @param dataType the type that has to be literal.  Will be added if it does not already exist.
   * @return the new signature.
   */
  def withLit(dataType: TypeEnum.Value): TypeSig = {
    val it = initialTypes + dataType
    val lt = litOnlyTypes + dataType
    new TypeSig(it, maxAllowedDecimalPrecision, childTypes, lt, notes)
  }

  /**
   * Add a literal restriction to the signature
   * @param dataTypes the types that have to be literal. Will be added if they do not already exist.
   * @return the new signature.
   */
  def withLit(dataTypes: TypeEnum.ValueSet): TypeSig = {
    val it = initialTypes ++ dataTypes
    val lt = litOnlyTypes ++ dataTypes
    new TypeSig(it, maxAllowedDecimalPrecision, childTypes, lt, notes)
  }

  /**
   * All currently supported types can only be literal values.
   * @return the new signature.
   */
  def withAllLit(): TypeSig = {
    // don't need to combine initialTypes with litOnlyTypes because litOnly should be a subset
    new TypeSig(initialTypes, maxAllowedDecimalPrecision, childTypes, initialTypes, notes)
  }

  /**
   * Combine two type signatures together. Base types and child types will be the union of
   * both as will limitations on literal values.
   * @param other what to combine with.
   * @return the new signature
   */
  def + (other: TypeSig): TypeSig = {
    val it = initialTypes ++ other.initialTypes
    val nt = childTypes ++ other.childTypes
    val lt = litOnlyTypes ++ other.litOnlyTypes
    val dp = Math.max(maxAllowedDecimalPrecision, other.maxAllowedDecimalPrecision)
    // TODO nested types is not always going to do what you want, so we might want to warn
    val nts = notes ++ other.notes
    new TypeSig(it, dp, nt, lt, nts)
  }

  /**
   * Remove a type signature. The reverse of +
   * @param other what to remove
   * @return the new signature
   */
  def - (other: TypeSig): TypeSig = {
    val it = initialTypes -- other.initialTypes
    val nt = childTypes -- other.childTypes
    val lt = litOnlyTypes -- other.litOnlyTypes
    val nts = notes -- other.notes.keySet
    new TypeSig(it, maxAllowedDecimalPrecision, nt, lt, nts)
  }

  def intersect(other: TypeSig): TypeSig = {
    val it = initialTypes & other.initialTypes
    val nt = childTypes & other.childTypes
    val lt = litOnlyTypes & other.initialTypes
    val nts = notes.filterKeys(other.initialTypes)
    new TypeSig(it, maxAllowedDecimalPrecision, nt, lt, nts.toMap)
  }

  /**
   * Add child types to this type signature. Note that these do not stack so if childTypes has
   * child types too they are ignored.
   * @param childTypes the basic types to add.
   * @return the new type signature
   */
  def nested(childTypes: TypeSig): TypeSig = {
    val mp = Math.max(maxAllowedDecimalPrecision, childTypes.maxAllowedDecimalPrecision)
    new TypeSig(initialTypes, mp, this.childTypes ++ childTypes.initialTypes, litOnlyTypes, notes)
  }

  /**
   * Update this type signature to be nested with the initial types too.
   * @return the update type signature
   */
  def nested(): TypeSig =
    new TypeSig(initialTypes, maxAllowedDecimalPrecision, initialTypes ++ childTypes,
      litOnlyTypes, notes)

  /**
   * Add a note about a given type that marks it as partially supported.
   * @param dataType the type this note is for.
   * @param note the note itself
   * @return the updated TypeSignature.
   */
  def withPsNote(dataType: TypeEnum.Value, note: String): TypeSig =
    new TypeSig(initialTypes + dataType, maxAllowedDecimalPrecision, childTypes, litOnlyTypes,
      notes.+((dataType, note)))

  /**
   * Add a note about given types that marks them as partially supported.
   * @param dataTypes the types this note is for.
   * @param note the note itself
   * @return the updated TypeSignature.
   */
  def withPsNote(dataTypes: Seq[TypeEnum.Value], note: String): TypeSig =
    new TypeSig(
      dataTypes.foldLeft(initialTypes)(_+_), maxAllowedDecimalPrecision, childTypes,
      litOnlyTypes, dataTypes.foldLeft(notes)((notes, dataType) => notes.+((dataType, note))))

  def withInitialTypesPsNote(note: String): TypeSig = {
    withPsNote(initialTypes.toSeq, note)
  }

  private def isSupportedType(dataType: TypeEnum.Value): Boolean =
      initialTypes.contains(dataType)

  /**
   * Given an expression tag the associated meta for it to be supported or not.
   *
   * @param meta     the meta that gets marked for support or not.
   * @param exprMeta the meta of expression to check against.
   * @param name     the name of the expression (typically a parameter name)
   */
  def tagExprParam(
      meta: RapidsMeta[_, _, _],
      exprMeta: BaseExprMeta[_],
      name: String,
      willNotWork: String => Unit): Unit = {
    val typeMeta = exprMeta.typeMeta
    // This is for a parameter so skip it if there is no data type for the expression
    typeMeta.dataType.foreach { dt =>
      val expr = exprMeta.wrapped.asInstanceOf[Expression]

      if (!isSupportedByPlugin(dt)) {
        willNotWork(s"$name expression ${expr.getClass.getSimpleName} $expr " +
            reasonNotSupported(dt).mkString("(", ", ", ")"))
      } else if (isLitOnly(dt) && !GpuOverrides.isLit(expr)) {
        willNotWork(s"$name only supports $dt if it is a literal value")
      }
      if (typeMeta.typeConverted) {
        meta.addConvertedDataType(expr, typeMeta)
      }
    }
  }

  /**
   * Check if this type is supported by the plugin or not.
   * @param dataType the data type to be checked
   * @return true if it is allowed else false.
   */
  def isSupportedByPlugin(dataType: DataType): Boolean =
    isSupported(initialTypes, dataType)

  private [this] def isLitOnly(dataType: DataType): Boolean = dataType match {
    case BooleanType => litOnlyTypes.contains(TypeEnum.BOOLEAN)
    case ByteType => litOnlyTypes.contains(TypeEnum.BYTE)
    case ShortType => litOnlyTypes.contains(TypeEnum.SHORT)
    case IntegerType => litOnlyTypes.contains(TypeEnum.INT)
    case LongType => litOnlyTypes.contains(TypeEnum.LONG)
    case FloatType => litOnlyTypes.contains(TypeEnum.FLOAT)
    case DoubleType => litOnlyTypes.contains(TypeEnum.DOUBLE)
    case DateType => litOnlyTypes.contains(TypeEnum.DATE)
    case TimestampType => litOnlyTypes.contains(TypeEnum.TIMESTAMP)
    case StringType => litOnlyTypes.contains(TypeEnum.STRING)
    case _: DecimalType => litOnlyTypes.contains(TypeEnum.DECIMAL)
    case NullType => litOnlyTypes.contains(TypeEnum.NULL)
    case BinaryType => litOnlyTypes.contains(TypeEnum.BINARY)
    case CalendarIntervalType => litOnlyTypes.contains(TypeEnum.CALENDAR)
    case _: ArrayType => litOnlyTypes.contains(TypeEnum.ARRAY)
    case _: MapType => litOnlyTypes.contains(TypeEnum.MAP)
    case _: StructType => litOnlyTypes.contains(TypeEnum.STRUCT)
    case _: DayTimeIntervalType => litOnlyTypes.contains(TypeEnum.DAYTIME)
    case _: YearMonthIntervalType => litOnlyTypes.contains(TypeEnum.YEARMONTH)
    case _ => false
  }

  def isSupportedBySpark(dataType: DataType): Boolean =
    isSupported(initialTypes, dataType)

  private[this] def isSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType): Boolean =
    dataType match {
      case BooleanType => check.contains(TypeEnum.BOOLEAN)
      case ByteType => check.contains(TypeEnum.BYTE)
      case ShortType => check.contains(TypeEnum.SHORT)
      case IntegerType => check.contains(TypeEnum.INT)
      case LongType => check.contains(TypeEnum.LONG)
      case FloatType => check.contains(TypeEnum.FLOAT)
      case DoubleType => check.contains(TypeEnum.DOUBLE)
      case DateType => check.contains(TypeEnum.DATE)
      case TimestampType => check.contains(TypeEnum.TIMESTAMP)
      case StringType => check.contains(TypeEnum.STRING)
      case dt: DecimalType =>
          check.contains(TypeEnum.DECIMAL) &&
          dt.precision <= maxAllowedDecimalPrecision
      case NullType => check.contains(TypeEnum.NULL)
      case BinaryType => check.contains(TypeEnum.BINARY)
      case CalendarIntervalType => check.contains(TypeEnum.CALENDAR)
      case ArrayType(elementType, _) if check.contains(TypeEnum.ARRAY) =>
        isSupported(childTypes, elementType)
      case MapType(keyType, valueType, _) if check.contains(TypeEnum.MAP) =>
        isSupported(childTypes, keyType) &&
            isSupported(childTypes, valueType)
      case StructType(fields) if check.contains(TypeEnum.STRUCT) =>
        fields.map(_.dataType).forall { t =>
          isSupported(childTypes, t)
        }
      case _: DayTimeIntervalType => check.contains(TypeEnum.DAYTIME)
      case _: YearMonthIntervalType => check.contains(TypeEnum.YEARMONTH)
      case _ => false
    }

  def reasonNotSupported(dataType: DataType): Seq[String] =
    reasonNotSupported(initialTypes, dataType, isChild = false)

  private[this] def withChild(isChild: Boolean, msg: String): String = if (isChild) {
    "child " + msg
  } else {
    msg
  }

  private[this] def basicNotSupportedMessage(dataType: DataType,
      te: TypeEnum.Value, check: TypeEnum.ValueSet, isChild: Boolean): Seq[String] = {
    if (check.contains(te)) {
      Seq.empty
    } else {
      Seq(withChild(isChild, s"$dataType is not supported"))
    }
  }

  private[this] def reasonNotSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      isChild: Boolean): Seq[String] =
    dataType match {
      case BooleanType =>
        basicNotSupportedMessage(dataType, TypeEnum.BOOLEAN, check, isChild)
      case ByteType =>
        basicNotSupportedMessage(dataType, TypeEnum.BYTE, check, isChild)
      case ShortType =>
        basicNotSupportedMessage(dataType, TypeEnum.SHORT, check, isChild)
      case IntegerType =>
        basicNotSupportedMessage(dataType, TypeEnum.INT, check, isChild)
      case LongType =>
        basicNotSupportedMessage(dataType, TypeEnum.LONG, check, isChild)
      case FloatType =>
        basicNotSupportedMessage(dataType, TypeEnum.FLOAT, check, isChild)
      case DoubleType =>
        basicNotSupportedMessage(dataType, TypeEnum.DOUBLE, check, isChild)
      case DateType =>
        basicNotSupportedMessage(dataType, TypeEnum.DATE, check, isChild)
      case TimestampType =>
        basicNotSupportedMessage(dataType, TypeEnum.TIMESTAMP, check, isChild)
      case StringType =>
        basicNotSupportedMessage(dataType, TypeEnum.STRING, check, isChild)
      case dt: DecimalType =>
        if (check.contains(TypeEnum.DECIMAL)) {
          var reasons = Seq[String]()
          if (dt.precision > maxAllowedDecimalPrecision) {
            reasons ++= Seq(withChild(isChild, s"$dataType precision is larger " +
                s"than we support $maxAllowedDecimalPrecision"))
          }
          reasons
        } else {
          basicNotSupportedMessage(dataType, TypeEnum.DECIMAL, check, isChild)
        }
      case NullType =>
        basicNotSupportedMessage(dataType, TypeEnum.NULL, check, isChild)
      case BinaryType =>
        basicNotSupportedMessage(dataType, TypeEnum.BINARY, check, isChild)
      case CalendarIntervalType =>
        basicNotSupportedMessage(dataType, TypeEnum.CALENDAR, check, isChild)
      case ArrayType(elementType, _) =>
        if (check.contains(TypeEnum.ARRAY)) {
          reasonNotSupported(childTypes, elementType, isChild = true)
        } else {
          basicNotSupportedMessage(dataType, TypeEnum.ARRAY, check, isChild)
        }
      case MapType(keyType, valueType, _) =>
        if (check.contains(TypeEnum.MAP)) {
          reasonNotSupported(childTypes, keyType, isChild = true) ++
              reasonNotSupported(childTypes, valueType, isChild = true)
        } else {
          basicNotSupportedMessage(dataType, TypeEnum.MAP, check, isChild)
        }
      case StructType(fields) =>
        if (check.contains(TypeEnum.STRUCT)) {
          fields.flatMap { sf =>
            reasonNotSupported(childTypes, sf.dataType, isChild = true)
          }
        } else {
          basicNotSupportedMessage(dataType, TypeEnum.STRUCT, check, isChild)
        }
      case _: DayTimeIntervalType =>
        basicNotSupportedMessage(dataType, TypeEnum.DAYTIME, check, isChild)
      case _: YearMonthIntervalType =>
        basicNotSupportedMessage(dataType, TypeEnum.YEARMONTH, check, isChild)
      case _ => Seq(withChild(isChild, s"$dataType is not supported"))
    }

  def areAllSupportedByPlugin(types: Seq[DataType]): Boolean =
    types.forall(isSupportedByPlugin)

  /**
   * Get the level of support for a given type compared to what Spark supports.
   * Used for documentation.
   */
  def getSupportLevel(dataType: TypeEnum.Value, allowed: TypeSig): SupportLevel = {
    if (!allowed.isSupportedType(dataType)) {
      NotApplicable
    } else if (!isSupportedType(dataType)) {
      NotSupported
    } else {
      var note = notes.get(dataType)
      val needsLitWarning = litOnlyTypes.contains(dataType) &&
          !allowed.litOnlyTypes.contains(dataType)
      val lowerPrecision =
        dataType == TypeEnum.DECIMAL && maxAllowedDecimalPrecision < DecimalType.MAX_PRECISION
      if (lowerPrecision) {
        val msg = s"max DECIMAL precision of $maxAllowedDecimalPrecision"
        note = if (note.isEmpty) {
          Some(msg)
        } else {
          Some(note.get + ";<br/>" + msg)
        }
      }

      if (dataType == TypeEnum.TIMESTAMP) {
        val msg = s"UTC is only supported TZ for TIMESTAMP"
        note = if (note.isEmpty) {
          Some(msg)
        } else {
          Some(note.get + ";<br/>" + msg)
        }
      }

      dataType match {
        case TypeEnum.ARRAY | TypeEnum.MAP | TypeEnum.STRUCT =>
          val subTypeLowerPrecision = childTypes.contains(TypeEnum.DECIMAL) &&
              maxAllowedDecimalPrecision < DecimalType.MAX_PRECISION
          if (subTypeLowerPrecision) {
            val msg = s"max child DECIMAL precision of $maxAllowedDecimalPrecision"
            note = if (note.isEmpty) {
              Some(msg)
            } else {
              Some(note.get + ";<br/>" + msg)
            }
          }

          if (childTypes.contains(TypeEnum.TIMESTAMP)) {
            val msg = s"UTC is only supported TZ for child TIMESTAMP"
            note = if (note.isEmpty) {
              Some(msg)
            } else {
              Some(note.get + ";<br/>" + msg)
            }
          }

          val subTypesMissing = allowed.childTypes -- childTypes
          if (subTypesMissing.isEmpty && note.isEmpty && !needsLitWarning) {
            new Supported()
          } else {
            new PartiallySupported(missingChildTypes = subTypesMissing,
              needsLitWarning = needsLitWarning,
              note = note)
          }
        case _ if note.isDefined || needsLitWarning =>
          new PartiallySupported(needsLitWarning = needsLitWarning, note = note)
        case _ =>
          new Supported()
      }
    }
  }
}

object TypeSig {
  /**
   * Create a TypeSig that only supports a literal of the given type.
   */
  def lit(dataType: TypeEnum.Value): TypeSig =
    TypeSig.none.withLit(dataType)

  /**
   * Create a TypeSig that only supports literals of certain given types.
   */
  def lit(dataTypes: TypeEnum.ValueSet): TypeSig =
    TypeSig.none.withLit(dataTypes)

  /**
   * Create a TypeSig that supports only literals of common primitive CUDF types.
   */
  def commonCudfTypesLit(): TypeSig = {
    lit(TypeEnum.ValueSet(
      TypeEnum.BOOLEAN,
      TypeEnum.BYTE,
      TypeEnum.SHORT,
      TypeEnum.INT,
      TypeEnum.LONG,
      TypeEnum.FLOAT,
      TypeEnum.DOUBLE,
      TypeEnum.DATE,
      TypeEnum.TIMESTAMP,
      TypeEnum.STRING
    ))
  }

  /**
   * Create a TypeSig that has partial support for the given type.
   */
  def psNote(dataType: TypeEnum.Value, note: String): TypeSig =
    TypeSig.none.withPsNote(dataType, note)

  def decimal(maxPrecision: Int): TypeSig =
    new TypeSig(TypeEnum.ValueSet(TypeEnum.DECIMAL), maxPrecision)

  /**
   * All types nested and not nested
   */
  val all: TypeSig = {
    val allSupportedTypes: TypeEnum.ValueSet = TypeEnum.values
    new TypeSig(allSupportedTypes, DecimalType.MAX_PRECISION, allSupportedTypes)
  }

  /**
   * No types supported at all
   */
  val none: TypeSig = new TypeSig(TypeEnum.ValueSet())

  val BOOLEAN: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BOOLEAN))
  val BYTE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BYTE))
  val SHORT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.SHORT))
  val INT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.INT))
  val LONG: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.LONG))
  val FLOAT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.FLOAT))
  val DOUBLE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DOUBLE))
  val DATE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DATE))
  val TIMESTAMP: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.TIMESTAMP))
  val STRING: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.STRING))
  val DECIMAL_64: TypeSig = decimal(DType.DECIMAL64_MAX_PRECISION)

  /**
   * Full support for 128 bit DECIMAL. In the future we expect to have other types with
   * slightly less than full DECIMAL support. This are things like math operations where
   * we cannot replicate the overflow behavior of Spark. These will be added when needed.
   */
  val DECIMAL_128: TypeSig = decimal(DType.DECIMAL128_MAX_PRECISION)

  val NULL: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.NULL))
  val BINARY: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BINARY))
  val CALENDAR: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.CALENDAR))
  /**
   * ARRAY type support, but not very useful on its own because no child types under
   * it are supported
   */
  val ARRAY: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.ARRAY))
  /**
   * MAP type support, but not very useful on its own because no child types under
   * it are supported
   */
  val MAP: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.MAP))
  /**
   * STRUCT type support, but only matches empty structs unless you add child types to it.
   */
  val STRUCT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.STRUCT))
  /**
   * User Defined Type (We don't support these in the plugin yet)
   */
  val UDT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.UDT))

  /**
   * DayTimeIntervalType of Spark 3.2.0+ support
   */
  val DAYTIME: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DAYTIME))

  /**
   * YearMonthIntervalType of Spark 3.2.0+ support
   */
  val YEARMONTH: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.YEARMONTH))

  /**
   * A signature for types that are generally supported by the plugin/CUDF. Please make sure to
   * check what Spark actually supports instead of blindly using this in a signature.
   */
  val commonCudfTypes: TypeSig = BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING

  /**
   * All floating point types
   */
  val fp: TypeSig = FLOAT + DOUBLE

  /**
   * All integer types
   */
  val integral: TypeSig = BYTE + SHORT + INT + LONG

  /**
   * All numeric types fp + integral + DECIMAL_128
   */
  val gpuNumeric: TypeSig = integral + fp + DECIMAL_128

  /**
   * All numeric types fp + integral + DECIMAL_128
   */
  val cpuNumeric: TypeSig = integral + fp + DECIMAL_128

  /**
   * All values that correspond to Spark's AtomicType but supported by GPU
   */
  val gpuAtomics: TypeSig = gpuNumeric + BINARY + BOOLEAN + DATE + STRING + TIMESTAMP

  /**
   * All values that correspond to Spark's AtomicType
   */
  val cpuAtomics: TypeSig = cpuNumeric + BINARY + BOOLEAN + DATE + STRING + TIMESTAMP

  /**
   * numeric + CALENDAR but only for GPU
   */
  val gpuNumericAndInterval: TypeSig = gpuNumeric + CALENDAR

  /**
   * numeric + CALENDAR
   */
  val numericAndInterval: TypeSig = TypeSig.cpuNumeric + TypeSig.CALENDAR + TypeSig.DAYTIME +
    TypeSig.YEARMONTH

  /**
   * ANSI year-month and day-time interval
   */
  val ansiIntervals: TypeSig = TypeSig.DAYTIME + TypeSig.YEARMONTH

  /**
   * All types that CUDF supports sorting/ordering on.
   */
  val gpuOrderable: TypeSig = (BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING + DECIMAL_64 + NULL + STRUCT).nested()

  /**
   * All types that Spark supports sorting/ordering on (really everything but MAP)
   */
  val orderable: TypeSig = (BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING + DECIMAL_128 + NULL + BINARY + CALENDAR + ARRAY + STRUCT +
      UDT).nested()

  /**
   * All types that Spark supports for comparison operators (really everything but MAP according
   * to https://spark.apache.org/docs/latest/api/sql/index.html#_12), e.g. "<=>", "=", "==".
   */
  val comparable: TypeSig = (BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING + DECIMAL_128 + NULL + BINARY + CALENDAR + ARRAY + STRUCT +
      UDT).nested()

  /**
   * Different types of Pandas UDF support different sets of output type. Please refer to
   *   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
   * for more details.
   *
   * It is impossible to specify the exact type signature for each Pandas UDF type in a single
   * expression 'PythonUDF'.
   *
   * So here comes the union of all the sets of supported type, to cover all the cases.
   */
  val unionOfPandasUdfOut: TypeSig =
    (commonCudfTypes + BINARY + DECIMAL_64 + NULL + ARRAY + MAP).nested() + STRUCT

  /** All types that can appear in AST expressions */
  val astTypes: TypeSig = BOOLEAN + integral + fp + TIMESTAMP + DATE + STRING

  /** All AST types that work for comparisons */
  val comparisonAstTypes: TypeSig = astTypes - fp

  /** All types that can appear in an implicit cast AST expression */
  val implicitCastsAstTypes: TypeSig = astTypes - BYTE - SHORT

  def getDataType(expr: Expression): Option[DataType] = {
    try {
      Some(expr.dataType)
    } catch {
      case _: java.lang.UnsupportedOperationException =>None
    }
  }
}

abstract class TypeChecks[RET] {
  def tag(meta: RapidsMeta[_, _, _]): Unit

  def support(dataType: TypeEnum.Value): RET

  val shown: Boolean = true

  private def stringifyTypeAttributeMap(groupedByType: Map[DataType, Set[String]]): String = {
    groupedByType.map { case (dataType, nameSet) =>
      dataType + " " + nameSet.mkString("[", ", ", "]")
    }.mkString(", ")
  }

  protected def tagUnsupportedTypes(
    meta: RapidsMeta[_, _, _],
    sig: TypeSig,
    fields: Seq[StructField],
    msgFormat: String
    ): Unit = {
    val unsupportedTypes: Map[DataType, Set[String]] = fields
      .filterNot(attr => sig.isSupportedByPlugin(attr.dataType))
      .groupBy(_.dataType)
      .mapValues(_.map(_.name).toSet).toMap

    if (unsupportedTypes.nonEmpty) {
      meta.willNotWorkOnGpu(msgFormat.format(stringifyTypeAttributeMap(unsupportedTypes)))
    }
  }
}

/**
 * Checks a set of named inputs to an SparkPlan node against a TypeSig
 */
case class InputCheck(cudf: TypeSig, spark: TypeSig, notes: List[String] = List.empty)

/**
 * Checks a single parameter by position against a TypeSig
 */
case class ParamCheck(name: String, cudf: TypeSig, spark: TypeSig)

/**
 * Checks the type signature for a parameter that repeats (Can only be used at the end of a list
 * of position parameters)
 */
case class RepeatingParamCheck(name: String, cudf: TypeSig, spark: TypeSig)

/**
 * Checks an expression that have input parameters and a single output.  This is intended to be
 * given for a specific ExpressionContext. If your expression does not meet this pattern you may
 * need to create a custom ExprChecks instance.
 */
case class ContextChecks(
    outputCheck: TypeSig,
    sparkOutputSig: TypeSig,
    paramCheck: Seq[ParamCheck] = Seq.empty,
    repeatingParamCheck: Option[RepeatingParamCheck] = None)
    extends TypeChecks[Map[String, SupportLevel]] {

  def tagAst(exprMeta: BaseExprMeta[_]): Unit = {
    tagBase(exprMeta, exprMeta.willNotWorkInAst)
  }

  override def tag(rapidsMeta: RapidsMeta[_, _, _]): Unit = {
    tagBase(rapidsMeta, rapidsMeta.willNotWorkOnGpu)
  }

  private[this] def tagBase(rapidsMeta: RapidsMeta[_, _, _], willNotWork: String => Unit): Unit = {
    val meta = rapidsMeta.asInstanceOf[BaseExprMeta[_]]
    val expr = meta.wrapped.asInstanceOf[Expression]
    meta.typeMeta.dataType match {
      case Some(dt: DataType) =>
        if (!outputCheck.isSupportedByPlugin(dt)) {
          willNotWork(s"expression ${expr.getClass.getSimpleName} $expr " +
              s"produces an unsupported type $dt")
        }
        if (meta.typeMeta.typeConverted) {
          meta.addConvertedDataType(expr, meta.typeMeta)
        }
      case None =>
        if (!meta.ignoreUnsetDataTypes) {
          willNotWork(s"expression ${expr.getClass.getSimpleName} $expr " +
              s" does not have a corresponding dataType.")
        }
    }

    val children = meta.childExprs
    val fixedChecks = paramCheck.toArray
    assert (fixedChecks.length <= children.length,
      s"${expr.getClass.getSimpleName} expected at least ${fixedChecks.length} but " +
          s"found ${children.length}")
    fixedChecks.zipWithIndex.foreach { case (check, i) =>
      check.cudf.tagExprParam(meta, children(i), check.name, willNotWork)
    }
    if (repeatingParamCheck.isEmpty) {
      assert(fixedChecks.length == children.length,
        s"${expr.getClass.getSimpleName} expected ${fixedChecks.length} but " +
            s"found ${children.length}")
    } else {
      val check = repeatingParamCheck.get
      (fixedChecks.length until children.length).foreach { i =>
        check.cudf.tagExprParam(meta, children(i), check.name, willNotWork)
      }
    }
  }

  override def support(dataType: TypeEnum.Value): Map[String, SupportLevel] = {
    val fixed = paramCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val variable = repeatingParamCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val output = ("result", outputCheck.getSupportLevel(dataType, sparkOutputSig))

    (fixed ++ variable ++ Seq(output)).toMap
  }
}

/**
 * Checks for either a read or a write of a given file format.
 */
class FileFormatChecks private (
    sig: TypeSig,
    sparkSig: TypeSig)
    extends TypeChecks[SupportLevel] {

  def tag(meta: RapidsMeta[_, _, _],
      schema: StructType,
      fileType: FileFormatType,
      op: FileFormatOp): Unit = {
    tagUnsupportedTypes(meta, sig, schema.fields,
      s"unsupported data types %s in $op for $fileType")
  }

  override def support(dataType: TypeEnum.Value): SupportLevel =
    sig.getSupportLevel(dataType, sparkSig)

  override def tag(meta: RapidsMeta[_, _, _]): Unit =
    throw new IllegalStateException("Internal Error not supported")

  def getFileFormat: TypeSig = sig
}

object FileFormatChecks {
  /**
   * File format checks with separate read and write signatures for cudf.
   */
  def apply(
      cudfRead: TypeSig,
      cudfWrite: TypeSig,
      sparkSig: TypeSig): Map[FileFormatOp, FileFormatChecks] = Map(
    (ReadFileOp, new FileFormatChecks(cudfRead, sparkSig)),
    (WriteFileOp, new FileFormatChecks(cudfWrite, sparkSig))
  )

  /**
   * File format checks where read and write have the same signature for cudf.
   */
  def apply(
      cudfReadWrite: TypeSig,
      sparkSig: TypeSig): Map[FileFormatOp, FileFormatChecks] =
    apply(cudfReadWrite, cudfReadWrite, sparkSig)

  def tag(meta: RapidsMeta[_, _, _],
      schema: StructType,
      fileType: FileFormatType,
      op: FileFormatOp): Unit = {
    GpuOverrides.fileFormats(fileType)(op).tag(meta, schema, fileType, op)
  }
}

/**
 * Checks the input and output types supported by a SparkPlan node. We don't currently separate
 * input checks from output checks.  We can add this in if something needs it.
 *
 * The namedChecks map can be used to provide checks for specific groups of expressions.
 */
class ExecChecks private(
    val check: TypeSig,
    sparkSig: TypeSig,
    val namedChecks: Map[String, InputCheck],
    override val shown: Boolean = true)
    extends TypeChecks[Map[String, SupportLevel]] {

  override def tag(rapidsMeta: RapidsMeta[_, _, _]): Unit = {
    val meta = rapidsMeta.asInstanceOf[SparkPlanMeta[_]]

    // expression.toString to capture ids in not-on-GPU tags
    def toStructField(a: Attribute) = StructField(name = a.toString(), dataType = a.dataType)

    tagUnsupportedTypes(meta, check, meta.outputAttributes.map(toStructField),
      "unsupported data types in output: %s")
    tagUnsupportedTypes(meta, check, meta.childPlans.flatMap(_.outputAttributes.map(toStructField)),
      "unsupported data types in input: %s")

    val namedChildExprs = meta.namedChildExprs

    val missing = namedChildExprs.keys.filterNot(namedChecks.contains)
    if (missing.nonEmpty) {
      throw new IllegalStateException(s"${meta.getClass.getSimpleName} " +
        s"is missing ExecChecks for ${missing.mkString(",")}")
    }

    namedChecks.foreach {
      case (fieldName, pc) =>
        val fieldMeta = namedChildExprs(fieldName)
          .flatMap(_.typeMeta.dataType)
          .zipWithIndex
          .map(t => StructField(s"c${t._2}", t._1))
        tagUnsupportedTypes(meta, pc.cudf, fieldMeta,
          s"unsupported data types in '$fieldName': %s")
    }
  }

  override def support(dataType: TypeEnum.Value): Map[String, SupportLevel] = {
    val groups = namedChecks.map { case (name, pc) =>
      (name, pc.cudf.getSupportLevel(dataType, pc.spark))
    }
    groups ++ Map("Input/Output" -> check.getSupportLevel(dataType, sparkSig))
  }

  def supportNotes: Map[String, List[String]] = {
    namedChecks.map { case (name, pc) =>
      (name, pc.notes)
    }.filter {
      case (_, notes) => notes.nonEmpty
    }
  }
}

/**
 * gives users an API to create ExecChecks.
 */
object ExecChecks {
  def apply(check: TypeSig, sparkSig: TypeSig) : ExecChecks = {
    new ExecChecks(check, sparkSig, Map.empty)
  }

  def apply(check: TypeSig,
      sparkSig: TypeSig,
      namedChecks: Map[String, InputCheck]): ExecChecks = {
    new ExecChecks(check, sparkSig, namedChecks)
  }

  def hiddenHack(): ExecChecks = {
    new ExecChecks(TypeSig.all, TypeSig.all, Map.empty, shown = false)
  }
}

/**
 * Base class all Partition checks must follow
 */
abstract class PartChecks extends TypeChecks[Map[String, SupportLevel]]

case class PartChecksImpl(
    paramCheck: Seq[ParamCheck] = Seq.empty,
    repeatingParamCheck: Option[RepeatingParamCheck] = None)
    extends PartChecks {

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val part = meta.wrapped
    val children = meta.childExprs

    val fixedChecks = paramCheck.toArray
    assert (fixedChecks.length <= children.length,
      s"${part.getClass.getSimpleName} expected at least ${fixedChecks.length} but " +
          s"found ${children.length}")
    fixedChecks.zipWithIndex.foreach { case (check, i) =>
      check.cudf.tagExprParam(meta, children(i), check.name, meta.willNotWorkOnGpu)
    }
    if (repeatingParamCheck.isEmpty) {
      assert(fixedChecks.length == children.length,
        s"${part.getClass.getSimpleName} expected ${fixedChecks.length} but " +
            s"found ${children.length}")
    } else {
      val check = repeatingParamCheck.get
      (fixedChecks.length until children.length).foreach { i =>
        check.cudf.tagExprParam(meta, children(i), check.name, meta.willNotWorkOnGpu)
      }
    }
  }

  override def support(dataType: TypeEnum.Value): Map[String, SupportLevel] = {
    val fixed = paramCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val variable = repeatingParamCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))

    (fixed ++ variable).toMap
  }
}

object PartChecks {
  def apply(repeatingParamCheck: RepeatingParamCheck): PartChecks =
    PartChecksImpl(Seq.empty, Some(repeatingParamCheck))

  def apply(): PartChecks = PartChecksImpl()
}

/**
 * Base class all Expression checks must follow.
 */
abstract class ExprChecks extends TypeChecks[Map[ExpressionContext, Map[String, SupportLevel]]] {
  /**
   * Tag this for AST or not.
   */
  def tagAst(meta: BaseExprMeta[_]): Unit
}

case class ExprChecksImpl(contexts: Map[ExpressionContext, ContextChecks])
    extends ExprChecks {
  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    val checks = contexts.get(context)
    if (checks.isEmpty) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      checks.get.tag(meta)
    }
  }

  override def support(
      dataType: TypeEnum.Value): Map[ExpressionContext, Map[String, SupportLevel]] = {
    contexts.map {
      case (expContext: ExpressionContext, check: ContextChecks) =>
        (expContext, check.support(dataType))
    }
  }

  override def tagAst(exprMeta: BaseExprMeta[_]): Unit = {
    val checks = contexts.get(AstExprContext)
    if (checks.isEmpty) {
      exprMeta.willNotWorkInAst(AstExprContext.notSupportedMsg)
    } else {
      checks.get.tagAst(exprMeta)
    }
  }
}

/**
 * This is specific to CaseWhen, because it does not follow the typical parameter convention.
 */
object CaseWhenCheck extends ExprChecks {
  val check: TypeSig = (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
    TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested()

  val sparkSig: TypeSig = TypeSig.all

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
    // when this supports AST tagBase(exprMeta, meta.willNotWorkInAst)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      tagBase(exprMeta, meta.willNotWorkOnGpu)
    }
  }

  private[this] def tagBase(exprMeta: BaseExprMeta[_], willNotWork: String => Unit): Unit = {
    // children of CaseWhen: branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue (Optional)
    //
    // The length of children will be odd if elseValue is not None, which means we can detect
    // both branch pair and possible elseValue via a size 2 grouped iterator.
    exprMeta.childExprs.grouped(2).foreach {
      case Seq(pred, value) =>
        TypeSig.BOOLEAN.tagExprParam(exprMeta, pred, "predicate", willNotWork)
        check.tagExprParam(exprMeta, value, "value", willNotWork)
      case Seq(elseValue) =>
        check.tagExprParam(exprMeta, elseValue, "else", willNotWork)
    }
  }

  override def support(dataType: TypeEnum.Value):
    Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = check.getSupportLevel(dataType, sparkSig)
    val projectPredSupport = TypeSig.BOOLEAN.getSupportLevel(dataType, TypeSig.BOOLEAN)
    Map((ProjectExprContext,
        Map(
          ("predicate", projectPredSupport),
          ("value", projectSupport),
          ("result", projectSupport))))
  }
}

/**
 * This is specific to Invoke, because it does not follow the typical parameter convention.
 * Invoke is a dynamic expression, it can wrap arbitrary expressions.
 * This does very few checks, the main checks are in the `InvokeExprMeta` class.
 */
object InvokeCheck extends ExprChecks {

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    if (exprMeta.context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the ${exprMeta.context} context")
    }
  }

  /**
   * Partially supports all the output types since `Invoke` is a dynamic expression.
   */
  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = new PartiallySupported(
      note = Some("Invoke is a dynamic expression, all types are partially supported."))
    Map((ProjectExprContext, Map(("result", projectSupport))))
  }
}

/**
 * This is specific to StaticInvoke, because it does not follow the typical parameter convention.
 * StaticInvoke is a dynamic expression, it can wrap arbitrary expressions.
 * This does very few checks, the main checks are in the `StaticInvokeExprMeta` class.
 */
object StaticInvokeCheck extends ExprChecks {

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    if (exprMeta.context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the ${exprMeta.context} context")
    }
  }

  /**
   * Partially supports all the output types since `StaticInvoke` is a dynamic expression.
   */
  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = new PartiallySupported(
      note = Some("StaticInvoke is a dynamic expression, all types are partially supported."))
    Map((ProjectExprContext, Map(("result", projectSupport))))
  }
}

/**
 * This is specific to WindowSpec, because it does not follow the typical parameter convention.
 */
object WindowSpecCheck extends ExprChecks {
  val check: TypeSig =
    TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
      TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
  val sparkSig: TypeSig = TypeSig.all

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
    // when this supports AST tagBase(exprMeta, meta.willNotWorkInAst)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      tagBase(exprMeta, meta.willNotWorkOnGpu)
    }
  }

  private [this] def tagBase(exprMeta: BaseExprMeta[_], willNotWork: String => Unit): Unit = {
    val win = exprMeta.wrapped.asInstanceOf[WindowSpecDefinition]
    // children of WindowSpecDefinition: partitionSpec ++ orderSpec :+ frameSpecification
    win.partitionSpec.indices.foreach(i =>
      check.tagExprParam(exprMeta, exprMeta.childExprs(i), "partition", willNotWork))
    val partSize = win.partitionSpec.length
    win.orderSpec.indices.foreach(i =>
      check.tagExprParam(exprMeta, exprMeta.childExprs(i + partSize), "order",
        willNotWork))
  }

  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = check.getSupportLevel(dataType, sparkSig)
    Map((ProjectExprContext,
        Map(
          ("partition", projectSupport),
          ("value", projectSupport),
          ("result", projectSupport))))
  }
}

object CreateMapCheck extends ExprChecks {

  // Spark supports all types except for Map for key (Map is not supported
  // even in child types)
  private val keySig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
    TypeSig.ARRAY + TypeSig.STRUCT).nested()

  private val valueSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.NULL +
    TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst("CreateMap is not supported by AST")
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    }
  }

  override def support(
      dataType: TypeEnum.Value): Map[ExpressionContext, Map[String, SupportLevel]] = {
    Map((ProjectExprContext,
      Map(
        ("key", keySig.getSupportLevel(dataType, keySig)),
        ("value", valueSig.getSupportLevel(dataType, valueSig)))))
  }
}


/**
 * A check for CreateNamedStruct.  The parameter values alternate between one type and another.
 * If this pattern shows up again we can make this more generic at that point.
 */
object CreateNamedStructCheck extends ExprChecks {
  val nameSig: TypeSig = TypeSig.lit(TypeEnum.STRING)
  val sparkNameSig: TypeSig = TypeSig.lit(TypeEnum.STRING)
  val valueSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
      TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT + TypeSig.BINARY).nested()
  val sparkValueSig: TypeSig = TypeSig.all
  val resultSig: TypeSig = TypeSig.STRUCT.nested(valueSig)
  val sparkResultSig: TypeSig = TypeSig.STRUCT.nested(sparkValueSig)

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
    // when this supports AST tagBase(exprMeta, meta.willNotWorkInAst)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      tagBase(exprMeta, meta.willNotWorkOnGpu)
    }
  }

  private[this] def tagBase(exprMeta: BaseExprMeta[_], willNotWork: String => Unit): Unit = {
    exprMeta.childExprs.grouped(2).foreach {
      case Seq(nameMeta, valueMeta) =>
        nameSig.tagExprParam(exprMeta, nameMeta, "name", willNotWork)
        valueSig.tagExprParam(exprMeta, valueMeta, "value", willNotWork)
    }
    exprMeta.typeMeta.dataType.foreach { dt =>
      if (!resultSig.isSupportedByPlugin(dt)) {
        willNotWork(s"unsupported data type in output: $dt")
      }
    }
  }

  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val nameProjectSupport = nameSig.getSupportLevel(dataType, sparkNameSig)
    val valueProjectSupport = valueSig.getSupportLevel(dataType, sparkValueSig)
    val resultProjectSupport = resultSig.getSupportLevel(dataType, sparkResultSig)
    Map((ProjectExprContext,
        Map(
          ("name", nameProjectSupport),
          ("value", valueProjectSupport),
          ("result", resultProjectSupport))))
  }
}

class CastChecks extends ExprChecks {
  // Don't show this with other operators show it in a different location
  override val shown: Boolean = false

  // When updating these please check child classes too
  import TypeSig._
  val nullChecks: TypeSig = integral + fp + BOOLEAN + TIMESTAMP + DATE + STRING +
    NULL + DECIMAL_128
  val sparkNullSig: TypeSig = all

  def booleanChecks: TypeSig = integral + fp + BOOLEAN + STRING + DECIMAL_128 +
    CastCheckShims.additionalTypesBooleanCanCastTo
  def sparkBooleanSig: TypeSig = cpuNumeric + BOOLEAN + STRING +
    CastCheckShims.additionalTypesBooleanCanCastTo

  val integralChecks: TypeSig = gpuNumeric + BOOLEAN + TIMESTAMP + STRING +
      BINARY + CastCheckShims.additionalTypesIntegralCanCastTo
  val sparkIntegralSig: TypeSig = cpuNumeric + BOOLEAN + TIMESTAMP + STRING +
      BINARY + CastCheckShims.additionalTypesIntegralCanCastTo

  val fpToStringPsNote: String = s"Conversion may produce different results and requires " +
      s"${RapidsConf.ENABLE_CAST_FLOAT_TO_STRING} to be true."
  val fpChecks: TypeSig = (gpuNumeric + BOOLEAN + TIMESTAMP + STRING)
      .withPsNote(TypeEnum.STRING, fpToStringPsNote)
  val sparkFpSig: TypeSig = cpuNumeric + BOOLEAN + TIMESTAMP + STRING

  def dateChecks: TypeSig = TIMESTAMP + DATE + STRING +
    CastCheckShims.additionalTypesDateCanCastTo
  def sparkDateSig: TypeSig = DECIMAL_128 + TIMESTAMP + DATE + STRING +
    CastCheckShims.additionalTypesDateCanCastTo

  def timestampChecks: TypeSig = integral + fp + TIMESTAMP + DATE + STRING +
    CastCheckShims.additionalTypesTimestampCanCastTo
  def sparkTimestampSig: TypeSig = cpuNumeric + TIMESTAMP + DATE + STRING +
    CastCheckShims.additionalTypesTimestampCanCastTo

  val stringChecks: TypeSig = gpuNumeric + BOOLEAN + TIMESTAMP + DATE + STRING +
      BINARY + CastCheckShims.additionalTypesStringCanCastTo
  val sparkStringSig: TypeSig = cpuNumeric + BOOLEAN + TIMESTAMP + DATE + CALENDAR + STRING +
      BINARY + CastCheckShims.additionalTypesStringCanCastTo

  val binaryChecks: TypeSig = STRING + BINARY
  val sparkBinarySig: TypeSig = STRING + BINARY

  val decimalChecks: TypeSig = gpuNumeric + STRING
  val sparkDecimalSig: TypeSig = cpuNumeric + BOOLEAN + TIMESTAMP + STRING

  val calendarChecks: TypeSig = none
  val sparkCalendarSig: TypeSig = CALENDAR + STRING

  val arrayChecks: TypeSig = psNote(TypeEnum.STRING, "the array's child type must also support " +
    "being cast to string") + ARRAY.nested(commonCudfTypes + DECIMAL_128 + NULL +
    ARRAY + BINARY + STRUCT + MAP + GpuTypeShims.additionalCommonOperatorSupportedTypes) +
    psNote(TypeEnum.ARRAY, "The array's child type must also support being cast to the " +
      "desired child type(s)")

  val sparkArraySig: TypeSig = STRING + ARRAY.nested(all)

  val mapChecks: TypeSig = MAP.nested(commonCudfTypes + DECIMAL_128 + NULL + ARRAY + BINARY +
      STRUCT + MAP + GpuTypeShims.additionalCommonOperatorSupportedTypes) +
      psNote(TypeEnum.MAP, "the map's key and value must also support being cast to the " +
      "desired child types") +
      psNote(TypeEnum.STRING, "the map's key and value must also support being cast to string")
  val sparkMapSig: TypeSig = STRING + MAP.nested(all)

  val structChecks: TypeSig = psNote(TypeEnum.STRING, "the struct's children must also support " +
      "being cast to string") +
      STRUCT.nested(commonCudfTypes + DECIMAL_128 + NULL + ARRAY + BINARY + STRUCT + MAP +
          GpuTypeShims.additionalCommonOperatorSupportedTypes) +
      psNote(TypeEnum.STRUCT, "the struct's children must also support being cast to the " +
          "desired child type(s)")
  val sparkStructSig: TypeSig = STRING + STRUCT.nested(all)

  val udtChecks: TypeSig = none
  val sparkUdtSig: TypeSig = STRING + UDT

  val daytimeChecks: TypeSig = CastCheckShims.typesDayTimeCanCastTo
  val sparkDaytimeChecks: TypeSig = CastCheckShims.typesDayTimeCanCastToOnSpark

  val yearmonthChecks: TypeSig = CastCheckShims.typesYearMonthCanCastTo
  val sparkYearmonthChecks: TypeSig = CastCheckShims.typesYearMonthCanCastToOnSpark

  private[this] def getChecksAndSigs(from: DataType): (TypeSig, TypeSig) = from match {
    case NullType => (nullChecks, sparkNullSig)
    case BooleanType => (booleanChecks, sparkBooleanSig)
    case ByteType | ShortType | IntegerType | LongType => (integralChecks, sparkIntegralSig)
    case FloatType | DoubleType => (fpChecks, sparkFpSig)
    case DateType => (dateChecks, sparkDateSig)
    case TimestampType => (timestampChecks, sparkTimestampSig)
    case StringType => (stringChecks, sparkStringSig)
    case BinaryType => (binaryChecks, sparkBinarySig)
    case _: DecimalType => (decimalChecks, sparkDecimalSig)
    case CalendarIntervalType => (calendarChecks, sparkCalendarSig)
    case _: ArrayType => (arrayChecks, sparkArraySig)
    case _: MapType => (mapChecks, sparkMapSig)
    case _: StructType => (structChecks, sparkStructSig)
    case _: DayTimeIntervalType => getChecksAndSigs(TypeEnum.DAYTIME)
    case _: YearMonthIntervalType => getChecksAndSigs(TypeEnum.YEARMONTH)
    case _ => getChecksAndSigs(TypeEnum.UDT) // default to UDT
  }

  private[this] def getChecksAndSigs(from: TypeEnum.Value): (TypeSig, TypeSig) = from match {
    case TypeEnum.NULL => (nullChecks, sparkNullSig)
    case TypeEnum.BOOLEAN => (booleanChecks, sparkBooleanSig)
    case TypeEnum.BYTE | TypeEnum.SHORT | TypeEnum.INT | TypeEnum.LONG =>
      (integralChecks, sparkIntegralSig)
    case TypeEnum.FLOAT | TypeEnum.DOUBLE => (fpChecks, sparkFpSig)
    case TypeEnum.DATE => (dateChecks, sparkDateSig)
    case TypeEnum.TIMESTAMP => (timestampChecks, sparkTimestampSig)
    case TypeEnum.STRING => (stringChecks, sparkStringSig)
    case TypeEnum.BINARY => (binaryChecks, sparkBinarySig)
    case TypeEnum.DECIMAL => (decimalChecks, sparkDecimalSig)
    case TypeEnum.CALENDAR => (calendarChecks, sparkCalendarSig)
    case TypeEnum.ARRAY => (arrayChecks, sparkArraySig)
    case TypeEnum.MAP => (mapChecks, sparkMapSig)
    case TypeEnum.STRUCT => (structChecks, sparkStructSig)
    case TypeEnum.UDT => (udtChecks, sparkUdtSig)
    case TypeEnum.DAYTIME => (daytimeChecks, sparkDaytimeChecks)
    case TypeEnum.YEARMONTH => (yearmonthChecks, sparkYearmonthChecks)
  }

  override def tagAst(meta: BaseExprMeta[_]): Unit = {
    meta.willNotWorkInAst(AstExprContext.notSupportedMsg)
    // when this supports AST tagBase(meta, meta.willNotWorkInAst)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      tagBase(meta, meta.willNotWorkOnGpu)
    }
  }

  protected def tagBase(meta: RapidsMeta[_, _, _], willNotWork: String => Unit): Unit = {
    val cast = meta.wrapped.asInstanceOf[UnaryExpression]
    val from = cast.child.dataType
    val to = cast.dataType
    if (!gpuCanCast(from, to)) {
      willNotWork(s"${meta.wrapped.getClass.getSimpleName} from $from to $to is not supported")
    }
  }

  override def support(
      dataType: TypeEnum.Value): Map[ExpressionContext, Map[String, SupportLevel]] = {
    throw new IllegalStateException("support is different for cast")
  }

  def support(from: TypeEnum.Value, to: TypeEnum.Value): SupportLevel = {
    val (checks, sparkSig) = getChecksAndSigs(from)
    checks.getSupportLevel(to, sparkSig)
  }

  def sparkCanCast(from: DataType, to: DataType): Boolean = {
    val (_, sparkSig) = getChecksAndSigs(from)
    sparkSig.isSupportedBySpark(to)
  }

  def gpuCanCast(from: DataType, to: DataType): Boolean = {
    val (checks, _) = getChecksAndSigs(from)
    checks.isSupportedByPlugin(to)
  }
}

object ToPrettyStringChecks {
  private val BINARY_STYLE_KEY = "spark.sql.binaryOutputStyle"
}

/** 
 * This class is just restricting the 'to' dataType to a StringType in the CastChecks class
 */ 
class ToPrettyStringChecks extends CastChecks {
  import ToPrettyStringChecks.BINARY_STYLE_KEY

  override protected def tagBase(meta: RapidsMeta[_, _, _], willNotWork: String => Unit): Unit = {
    val cast = meta.wrapped.asInstanceOf[UnaryExpression]
    val from = cast.child.dataType
    val to = StringType
    if (!gpuCanCast(from, to)) {
      willNotWork(s"${meta.wrapped.getClass.getSimpleName} from $from to $to is not supported")
    }

    if (from == BinaryType) {
      // Technically this is only needed for 4.0.0+, but we are doing it for all of them
      // because it should not hurt, and it makes the code simpler.
      // https://github.com/NVIDIA/spark-rapids/issues/10884 and
      // https://github.com/apache/spark/pull/46133/files
      meta.conf.getStr(BINARY_STYLE_KEY) match {
        case Some(style) =>
          willNotWork(s"$style is not supported for $BINARY_STYLE_KEY")
        case None =>
          // Noop the only one we support (the default)
      }
    }
  }
}

object ExprChecks {
  /**
   * A check for an expression that only supports project.
   */
  def projectOnly(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (ProjectExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * A check for an expression that supports project and as much of AST as it can.
   */
  def projectAndAst(
      allowedAstTypes: TypeSig,
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val astOutputCheck = outputCheck.intersect(allowedAstTypes)
    val astParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, pc.cudf.intersect(allowedAstTypes), pc.spark)
    }
    val astRepeatingParamCheck = repeatingParamCheck.map { rpc =>
      RepeatingParamCheck(rpc.name, rpc.cudf.intersect(allowedAstTypes), rpc.spark)
    }
    ExprChecksImpl(Map(
      ProjectExprContext ->
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck),
      AstExprContext ->
          ContextChecks(astOutputCheck, sparkOutputSig, astParamCheck, astRepeatingParamCheck)
    ))
  }

  /**
   * A check for a unary expression that only support project.
   */
  def unaryProject(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      inputCheck: TypeSig,
      sparkInputSig: TypeSig): ExprChecks =
    projectOnly(outputCheck, sparkOutputSig,
      Seq(ParamCheck("input", inputCheck, sparkInputSig)))

  /**
   * A check for a unary expression that supports project and as much AST as it can.
   */
  def unaryProjectAndAst(
      allowedAstTypes: TypeSig,
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      inputCheck: TypeSig,
      sparkInputSig: TypeSig): ExprChecks =
    projectAndAst(allowedAstTypes, outputCheck, sparkOutputSig,
      Seq(ParamCheck("input", inputCheck, sparkInputSig)))

  /**
   * Unary expression checks for project where the input matches the output.
   */
  def unaryProjectInputMatchesOutput(check: TypeSig, sparkSig: TypeSig): ExprChecks =
    unaryProject(check, sparkSig, check, sparkSig)

  /**
   * Unary expression checks for project where the input matches the output and it also
   * supports as much of AST as it can.
   */
  def unaryProjectAndAstInputMatchesOutput(
      allowedAstTypes: TypeSig,
      check: TypeSig,
      sparkSig: TypeSig): ExprChecks =
    unaryProjectAndAst(allowedAstTypes, check, sparkSig, check, sparkSig)

  /**
   * Math unary checks where input and output are both DoubleType.
   */
  val mathUnary: ExprChecks = unaryProjectInputMatchesOutput(TypeSig.DOUBLE, TypeSig.DOUBLE)

  /**
   * Math unary checks where input and output are both DoubleType and AST is supported.
   */
  val mathUnaryWithAst: ExprChecks =
    unaryProjectAndAstInputMatchesOutput(
      TypeSig.implicitCastsAstTypes, TypeSig.DOUBLE, TypeSig.DOUBLE)

  /**
   * Helper function for a binary expression where the plugin only supports project.
   */
  def binaryProject(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      param1: (String, TypeSig, TypeSig),
      param2: (String, TypeSig, TypeSig)): ExprChecks =
    projectOnly(outputCheck, sparkOutputSig,
      Seq(ParamCheck(param1._1, param1._2, param1._3),
        ParamCheck(param2._1, param2._2, param2._3)))

  /**
   * Helper function for a binary expression where the plugin supports project and AST.
   */
  def binaryProjectAndAst(
      allowedAstTypes: TypeSig,
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      param1: (String, TypeSig, TypeSig),
      param2: (String, TypeSig, TypeSig)): ExprChecks =
    projectAndAst(allowedAstTypes, outputCheck, sparkOutputSig,
      Seq(ParamCheck(param1._1, param1._2, param1._3),
        ParamCheck(param2._1, param2._2, param2._3)))

  /**
   * Aggregate operation where only group by agg and reduction is supported in the plugin and in
   * Spark.
   */
  def reductionAndGroupByAgg(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * Aggregate operation where window, reduction, and group by agg are all supported the same.
   */
  def fullAgg(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * For a generic expression that can work as both an aggregation and in the project context.
   * This is really just for PythonUDF.
   */
  def fullAggAndProject(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ProjectExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * An aggregation check where group by and reduction are supported by the plugin, but Spark
   * also supports window operations on these.
   */
  def aggNotWindow(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val windowParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    val windowRepeat = repeatingParamCheck.map { pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(TypeSig.none, sparkOutputSig, windowParamCheck, windowRepeat))))
  }

  /**
   * Window only operations. Spark does not support these operations as anything but a window
   * operation.
   */
  def windowOnly(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))


  /**
   * An aggregation check where group by is supported by the plugin, but Spark also supports
   * reduction and window operations on these.
   */
  def groupByOnly(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val noneParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    val noneRepeatCheck = repeatingParamCheck.map { pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    ExprChecksImpl(Map(
      (ReductionAggExprContext,
        ContextChecks(TypeSig.none, sparkOutputSig, noneParamCheck, noneRepeatCheck)),
      (GroupByAggExprContext,
        ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
        ContextChecks(TypeSig.none, sparkOutputSig, noneParamCheck, noneRepeatCheck))))
  }

  /**
   * An aggregation check where group by and window operations are supported by the plugin, but
   * Spark also supports reduction on these.
   */
  def aggNotReduction(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val noneParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    val noneRepeatCheck = repeatingParamCheck.map { pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    ExprChecksImpl(Map(
      (ReductionAggExprContext,
          ContextChecks(TypeSig.none, sparkOutputSig, noneParamCheck, noneRepeatCheck)),
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))
  }
}

/**
 * Used for generating the support docs.
 */
object SupportedOpsDocs {
  private lazy val allSupportedTypes: TypeEnum.ValueSet = TypeEnum.values

  private def execChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Executor</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    println("<th>Param(s)</th>")
    allSupportedTypes.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def exprChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Expression</th>")
    println("<th>SQL Functions(s)</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    println("<th>Context</th>")
    println("<th>Param/Output</th>")
    allSupportedTypes.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def partChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Partition</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    println("<th>Param</th>")
    allSupportedTypes.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def ioChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Format</th>")
    println("<th>Direction</th>")
    allSupportedTypes.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  def help(): Unit = {
    val headerEveryNLines = 15
    // scalastyle:off line.size.limit
    println("---")
    println("layout: page")
    println("title: Supported Operators")
    println("nav_order: 6")
    println("---")
    MarkdownUtils.printApacheSparkVersion("SupportedOpsDocs.help")
    println("Apache Spark supports processing various types of data. Not all expressions")
    println("support all data types. The RAPIDS Accelerator for Apache Spark has further")
    println("restrictions on what types are supported for processing. This tries")
    println("to document what operations are supported and what data types each operation supports.")
    println()
    println("# General limitations")
    println("## `Decimal`")
    println("The `Decimal` type in Spark supports a precision up to 38 digits (128-bits). ")
    println("The RAPIDS Accelerator supports 128-bit starting from version 21.12 and decimals are ")
    println("enabled by default.")
    println("Please check [Decimal Support](compatibility.md#decimal-support) for more details.")
    println()
    println("`Decimal` precision and scale follow the same rule as CPU mode in Apache Spark:")
    println()
    println("```")
    println(" * In particular, if we have expressions e1 and e2 with precision/scale p1/s1 and p2/s2")
    println(" * respectively, then the following operations have the following precision / scale:")
    println(" *")
    println(" *   Operation    Result Precision                        Result Scale")
    println(" *   ------------------------------------------------------------------------")
    println(" *   e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)")
    println(" *   e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)")
    println(" *   e1 * e2      p1 + p2 + 1                             s1 + s2")
    println(" *   e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)")
    println(" *   e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)")
    println(" *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)")
    println("```")
    println()
    println("However, Spark inserts `PromotePrecision` to CAST both sides to the same type.")
    println("GPU mode may fall back to CPU even if the result Decimal precision is within 18 digits.")
    println("For example, `Decimal(8,2)` x `Decimal(6,3)` resulting in `Decimal (15,5)` runs on CPU,")
    println("because due to `PromotePrecision`, GPU mode assumes the result is `Decimal(19,6)`.")
    println("There are even extreme cases where Spark can temporarily return a Decimal value")
    println("larger than what can be stored in 128-bits and then uses the `CheckOverflow`")
    println("operator to round it to a desired precision and scale. This means that even when")
    println("the accelerator supports 128-bit decimal, we might not be able to support all")
    println("operations that Spark can support.")
    println()
    println("## `Timestamp`")
    println("Timestamps in Spark will all be converted to the local time zone before processing")
    println("and are often converted to UTC before being stored, like in Parquet or ORC.")
    println("The RAPIDS Accelerator only supports UTC as the time zone for timestamps.")
    println()
    println("## `CalendarInterval`")
    println("In Spark `CalendarInterval`s store three values, months, days, and microseconds.")
    println("Support for this type is still very limited in the accelerator. In some cases")
    println("only a a subset of the type is supported, like window ranges only support days currently.")
    println()
    println("## Configuration")
    println("There are lots of different configuration values that can impact if an operation")
    println("is supported or not. Some of these are a part of the RAPIDS Accelerator and cover")
    println("the level of compatibility with Apache Spark.  Those are covered [here](configs.md).")
    println("Others are a part of Apache Spark itself and those are a bit harder to document.")
    println("The work of updating this to cover that support is still ongoing.")
    println()
    println("In general though if you ever have any question about why an operation is not running")
    println("on the GPU you may set `spark.rapids.sql.explain` to ALL and it will try to give all of")
    println("the reasons why this particular operator or expression is on the CPU or GPU.")
    println()
    println("# Key")
    println("## Types")
    println()
    println("|Type Name|Type Description|")
    println("|---------|----------------|")
    println("|BOOLEAN|Holds true or false values.|")
    println("|BYTE|Signed 8-bit integer value.|")
    println("|SHORT|Signed 16-bit integer value.|")
    println("|INT|Signed 32-bit integer value.|")
    println("|LONG|Signed 64-bit integer value.|")
    println("|FLOAT|32-bit floating point value.|")
    println("|DOUBLE|64-bit floating point value.|")
    println("|DATE|A date with no time component. Stored as 32-bit integer with days since Jan 1, 1970.|")
    println("|TIMESTAMP|A date and time. Stored as 64-bit integer with microseconds since Jan 1, 1970 in the current time zone.|")
    println("|STRING|A text string. Stored as UTF-8 encoded bytes.|")
    println("|DECIMAL|A fixed point decimal value with configurable precision and scale.|")
    println("|NULL|Only stores null values and is typically only used when no other type can be determined from the SQL.|")
    println("|BINARY|An array of non-nullable bytes.|")
    println("|CALENDAR|Represents a period of time.  Stored as months, days and microseconds.|")
    println("|ARRAY|A sequence of elements.|")
    println("|MAP|A set of key value pairs, the keys cannot be null.|")
    println("|STRUCT|A series of named fields.|")
    println("|UDT|User defined types and java Objects. These are not standard SQL types.|")
    println()
    println("## Support")
    println()
    println("|Value|Description|")
    println("|---------|----------------|")
    println("|S| (Supported) Both Apache Spark and the RAPIDS Accelerator support this type fully.|")
    println("| | (Not Applicable) Neither Spark not the RAPIDS Accelerator support this type in this situation.|")
    println("|_PS_| (Partial Support) Apache Spark supports this type, but the RAPIDS Accelerator only partially supports it. An explanation for what is missing will be included with this.|")
    println("|**NS**| (Not Supported) Apache Spark supports this type but the RAPIDS Accelerator does not.")
    println()
    println("# SparkPlan or Executor Nodes")
    println("Apache Spark uses a Directed Acyclic Graph(DAG) of processing to build a query.")
    println("The nodes in this graph are instances of `SparkPlan` and represent various high")
    println("level operations like doing a filter or project. The operations that the RAPIDS")
    println("Accelerator supports are described below.")
    println("<table>")
    execChecksHeaderLine()
    var totalCount = 0
    var nextOutputAt = headerEveryNLines
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          execChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        println("<tr>")
        val execChecks = checks.get.asInstanceOf[ExecChecks]
        val allData = allSupportedTypes.toSeq.map { t =>
          (t, execChecks.support(t))
        }.toMap

        val notes = execChecks.supportNotes
        // Now we should get the same keys for each type, so we are only going to look at the first
        // type for now
        val totalSpan = allData.values.head.size
        val inputs = allData.values.head.keys

        println(s"""<td rowspan="$totalSpan">${rule.tag.runtimeClass.getSimpleName}</td>""")
        println(s"""<td rowspan="$totalSpan">${rule.description}</td>""")
        println(s"""<td rowspan="$totalSpan">${rule.notes().getOrElse("None")}</td>""")
        var count = 0
        inputs.foreach { input =>
          val named = notes.get(input)
              .map(l => input + "<br/>(" + l.mkString(";<br/>") + ")")
              .getOrElse(input)
          println(s"<td>$named</td>")
          allSupportedTypes.foreach { t =>
            println(allData(t)(input).htmlTag)
          }
          println("</tr>")
          count += 1
          if (count < totalSpan) {
            println("<tr>")
          }
        }

        totalCount += totalSpan
      }
    }
    println("</table>")
    println()
    println("# Expression and SQL Functions")
    println("Inside each node in the DAG there can be one or more trees of expressions")
    println("that describe various types of processing that happens in that part of the plan.")
    println("These can be things like adding two numbers together or checking for null.")
    println("These expressions can have multiple input parameters and one output value.")
    println("These expressions also can happen in different contexts. Because of how the")
    println("accelerator works different contexts have different levels of support.")
    println()
    println("The most common expression context is `project`. In this context values from a single")
    println("input row go through the expression and the result will also be use to produce")
    println("something in the same row. Be aware that even in the case of aggregation and window")
    println("operations most of the processing is still done in the project context either before")
    println("or after the other processing happens.")
    println()
    println("Aggregation operations like count or sum can take place in either the `aggregation`,")
    println("`reduction`, or `window` context. `aggregation` is when the operation was done while")
    println("grouping the data by one or more keys. `reduction` is when there is no group by and")
    println("there is a single result for an entire column. `window` is for window operations.")
    println()
    println("The final expression context is `AST` or Abstract Syntax Tree.")
    println("Before explaining AST we first need to explain in detail how project context operations")
    println("work. Generally for a project context operation the plan Spark developed is read")
    println("on the CPU and an appropriate set of GPU kernels are selected to do those")
    println("operations. For example `a >= b + 1`. Would result in calling a GPU kernel to add")
    println("`1` to `b`, followed by another kernel that is called to compare `a` to that result.")
    println("The interpretation is happening on the CPU, and the GPU is used to do the processing.")
    println("For AST the interpretation for some reason cannot happen on the CPU and instead must")
    println("be done in the GPU kernel itself. An example of this is conditional joins. If you")
    println("want to join on `A.a >= B.b + 1` where `A` and `B` are separate tables or data")
    println("frames, the `+` and `>=` operations cannot run as separate independent kernels")
    println("because it is done on a combination of rows in both `A` and `B`. Instead part of the")
    println("plan that Spark developed is turned into an abstract syntax tree and sent to the GPU")
    println("where it can be interpreted. The number and types of operations supported in this")
    println("are limited.")
    println("<table>")
    exprChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.isDefined && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          exprChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val sqlFunctions =
          ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))
        val exprChecks = checks.get.asInstanceOf[ExprChecks]
        // Params can change between contexts, but should not
        val allData = allSupportedTypes.toSeq.map { t =>
          (t, exprChecks.support(t))
        }.toMap
        // Now we should get the same keys for each type, so we are only going to look at the first
        // type for now
        val totalSpan = allData.values.head.map {
          case (_, m: Map[String, SupportLevel]) => m.size
        }.sum
        val representative = allData.values.head
        println("<tr>")
        println("<td rowSpan=\"" + totalSpan + "\">" +
            s"${rule.tag.runtimeClass.getSimpleName}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${sqlFunctions.getOrElse(" ")}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.description}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.notes().getOrElse("None")}</td>")
        var count = 0
        representative.foreach {
          case (context, data) =>
            val contextSpan = data.size
            println("<td rowSpan=\"" + contextSpan + "\">" + s"$context</td>")
            data.keys.foreach { param =>
              println(s"<td>$param</td>")
              allSupportedTypes.foreach { t =>
                println(allData(t)(context)(param).htmlTag)
              }
              println("</tr>")
              count += 1
              if (count < totalSpan) {
                println("<tr>")
              }
            }
        }
        totalCount += totalSpan
      }
    }
    println("</table>")
    println()
    println("## Casting")
    println("The above table does not show what is and is not supported for cast.")
    println("This table shows the matrix of supported casts.")
    println("Nested types like MAP, Struct, and Array can only be cast if the child types")
    println("can be cast.")
    println()
    println("Some of the casts to/from string on the GPU are not 100% the same and are disabled")
    println("by default. Please see the configs for more details on these specific cases.")
    println()
    println("Please note that even though casting from one type to another is supported")
    println("by Spark it does not mean they all produce usable results.  For example casting")
    println("from a date to a boolean always produces a null. This is for Hive compatibility")
    println("and the accelerator produces the same result.")
    println()
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      rule.getChecks match {
        case Some(cc: CastChecks) =>
          println(s"### `${rule.tag.runtimeClass.getSimpleName}`")
          println()
          println("<table>")
          val numTypes = allSupportedTypes.size
          println("<tr><th rowSpan=\"2\" colSpan=\"2\"></th><th colSpan=\"" + numTypes + "\">TO</th></tr>")
          println("<tr>")
          allSupportedTypes.foreach { t =>
            println(s"<th>$t</th>")
          }
          println("</tr>")

          println("<tr><th rowSpan=\"" + numTypes + "\">FROM</th>")
          var count = 0
          allSupportedTypes.foreach { from =>
            println(s"<th>$from</th>")
            allSupportedTypes.foreach { to =>
              println(cc.support(from, to).htmlTag)
            }
            println("</tr>")
            count += 1
            if (count < numTypes) {
              println("<tr>")
            }
          }
          println("</table>")
          println()
        case _ => // Nothing
      }
    }
    println()
    println("# Partitioning")
    println("When transferring data between different tasks the data is partitioned in")
    println("specific ways depending on requirements in the plan. Be aware that the types")
    println("included below are only for rows that impact where the data is partitioned.")
    println("So for example if we are doing a join on the column `a` the data would be")
    println("hash partitioned on `a`, but all of the other columns in the same data frame")
    println("as `a` don't show up in the table. They are controlled by the rules for")
    println("`ShuffleExchangeExec` which uses the `Partitioning`.")
    println("<table>")
    partChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.isDefined && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          partChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val partChecks = checks.get.asInstanceOf[PartChecks]
        val allData = allSupportedTypes.toSeq.map { t =>
          (t, partChecks.support(t))
        }.toMap
        // Now we should get the same keys for each type, so we are only going to look at the first
        // type for now
        val totalSpan = allData.values.head.size
        if (totalSpan > 0) {
          val representative = allData.values.head
          println("<tr>")
          println("<td rowSpan=\"" + totalSpan + "\">" +
              s"${rule.tag.runtimeClass.getSimpleName}</td>")
          println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.description}</td>")
          println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.notes().getOrElse("None")}</td>")
          var count = 0
          representative.keys.foreach { param =>
            println(s"<td>$param</td>")
            allSupportedTypes.foreach { t =>
              println(allData(t)(param).htmlTag)
            }
            println("</tr>")
            count += 1
            if (count < totalSpan) {
              println("<tr>")
            }
          }
          totalCount += totalSpan
        } else {
          // No arguments...
          println("<tr>")
          println(s"<td>${rule.tag.runtimeClass.getSimpleName}</td>")
          println(s"<td>${rule.description}</td>")
          println(s"<td>${rule.notes().getOrElse("None")}</td>")
          println(NotApplicable.htmlTag) // param
          allSupportedTypes.foreach { _ =>
            println(NotApplicable.htmlTag)
          }
          println("</tr>")
          totalCount += 1
        }
      }
    }
    println("</table>")
    println()
    println("## Input/Output")
    println("For Input and Output it is not cleanly exposed what types are supported and which are not.")
    println("This table tries to clarify that. Be aware that some types may be disabled in some")
    println("cases for either reads or writes because of processing limitations, like rebasing")
    println("dates or timestamps, or for a lack of type coercion support.")
    println("<table>")
    ioChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.fileFormats.toSeq.sortBy(_._1.toString).foreach {
      case (format, ioMap) =>
        if (totalCount >= nextOutputAt) {
          ioChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val read = ioMap(ReadFileOp)
        val write = ioMap(WriteFileOp)
        println("<tr>")
        println("<th rowSpan=\"2\">" + s"$format</th>")
        println("<th>Read</th>")
        allSupportedTypes.foreach { t =>
          println(read.support(t).htmlTag)
        }
        println("</tr>")
        println("<tr>")
        println("<th>Write</th>")
        allSupportedTypes.foreach { t =>
          println(write.support(t).htmlTag)
        }
        println("</tr>")
        totalCount += 2
    }
    println("</table>")
    println()
    println("### Apache Iceberg Support")
    println("Support for Apache Iceberg has additional limitations. See the")
    println("[Apache Iceberg Support](https://docs.nvidia.com/spark-rapids/user-guide/latest/additional-functionality/iceberg-support.html) document.")
    // scalastyle:on line.size.limit
  }

  def main(args: Array[String]): Unit = {
    val out = new FileOutputStream(new File(args(0)))
    Console.withOut(out) {
      Console.withErr(out) {
        SupportedOpsDocs.help()
      }
    }
  }
}

object SupportedOpsForTools {

  private lazy val allSupportedTypes: TypeEnum.ValueSet = TypeEnum.values

  // if a string contains what we are going to use for a delimiter, replace
  // it with something else
  private def replaceDelimiter(str: String, delimiter: String): String = {
    if (str != null && str.contains(delimiter)) {
      val replaceWith = if (delimiter.equals(",")) {
        ";"
      } else if (delimiter.equals(";")) {
        ":"
      } else {
        ";"
      }
      str.replace(delimiter, replaceWith)
    } else {
      str
    }
  }

  private def outputSupportIO(): Unit = {
    // Look at what we have for defaults for some configs because if the configs are off
    // it likely means something isn't completely compatible.
    val conf = new RapidsConf(Map.empty[String, String])
    val types = allSupportedTypes.toSeq
    val header = Seq("Format", "Direction") ++ types
    println(header.mkString(","))
    GpuOverrides.fileFormats.toSeq.sortBy(_._1.toString).foreach {
      case (format, ioMap) =>
        val formatLowerCase = format.toString.toLowerCase
        // Get a tuple of (formatEnabled, readEnabled, Optional(writeEnabled)).
        // The writeEnabled configuration is optional because it is not always defined.
        val (formatEnabled, readEnabled, writeEnabled) = formatLowerCase match {
          case "csv" =>
            (conf.isCsvEnabled, conf.isCsvReadEnabled, None)
          case "delta" =>
            // Delta Lake reads of data files appear as a normal Parquet read
            (conf.isParquetEnabled, conf.isParquetReadEnabled, Some(conf.isParquetWriteEnabled))
          case "parquet" =>
            (conf.isParquetEnabled, conf.isParquetReadEnabled, Some(conf.isParquetWriteEnabled))
          case "orc" =>
            (conf.isOrcEnabled, conf.isOrcReadEnabled, Some(conf.isOrcWriteEnabled))
          case "json" =>
            (conf.isJsonEnabled, conf.isJsonReadEnabled, None)
          case "avro" =>
            (conf.isAvroEnabled, conf.isAvroReadEnabled, None)
          case "iceberg" =>
            (conf.isIcebergEnabled, conf.isIcebergReadEnabled, Some(conf.isIcebergWriteEnabled))
          case "hivetext" =>
            (conf.isHiveDelimitedTextEnabled,
              conf.isHiveDelimitedTextReadEnabled,
              Some(conf.isHiveDelimitedTextWriteEnabled))
          case _ =>
            throw new IllegalArgumentException("Format is unknown we need to add it here!")
        }
        if (!formatEnabled) {
          // Configured off by default.
          // Write CO for all types in the read section.
          println(s"$format,read,${types.map(_ => "CO").mkString(",")}")
        } else {
          // Step-1: fill in the read values.
          val readObj = ioMap(ReadFileOp)
          // we have lots of configs for various operations, just try to get the main ones
          val finalReadOps = types.map { t =>
            if (!readEnabled) {
              // indicate configured off by default
              "CO"
            } else {
              readObj.support(t).text
            }
          }
          // print read formats and types
          println(s"${(Seq(format, "read") ++ finalReadOps).mkString(",")}")

          // Step-2: fill in the write values if any.
          val writeOpObj = ioMap(WriteFileOp)
          val writeFileFormat = writeOpObj.getFileFormat
          if (writeFileFormat != TypeSig.none) { // types are defined
            val finalWriteOps = types.map { t =>
              if (writeEnabled.getOrElse(false)) {
                // indicate configured off by default
                writeOpObj.support(t).text
              } else {
                "CO"
              }
            }
            println(s"${(Seq(format, "write") ++ finalWriteOps).mkString(",")}")
          }
        }
    }
  }

  private def operatorMappingWithScore(): Unit = {
    val header = Seq("CPUOperator", "Score")
    println(header.mkString(","))
    val operatorCustomSpeedUp =  Map(
      ("BroadcastHashJoinExec", "5.1"),
      ("ShuffleExchangeExec", "4.2"),
      ("FilterExec", "2.8"),
      ("HashAggregateExec", "4.5"),
      ("SortExec", "8.0"),
      ("SortMergeJoinExec", "22.7"),
      ("ArrowEvalPythonExec", "1.2"),
      ("AggregateInPandasExec", "1.2"),
      ("FlatMapGroupsInPandasExec", "1.2"),
      ("MapInPandasExec", "1.2"),
      ("WindowInPandasExec", "1.2")
    )
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.forall(_.shown)) {
        val cpuName = rule.tag.runtimeClass.getSimpleName
        // We have estimated speed up of some of the operators by running various queries. Assign
        // custom speed up for the operators which are evaluated. For other operators we are
        // assigning speed up of 2.0
        val allCols = if (operatorCustomSpeedUp.contains(cpuName)) {
          Seq(cpuName, operatorCustomSpeedUp(cpuName))
        } else {
          Seq(cpuName, "3.0")
        }
        println(s"${allCols.mkString(",")}")
      }
    }

    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.runtimeClass.getSimpleName).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.forall(_.shown)) {
        val cpuName = rule.tag.runtimeClass.getSimpleName
        // We are assigning speed up of 3 to all the Exprs supported by the plugin. This can be
        // adjusted later.
        val allCols = Seq(cpuName, "4")
        println(s"${allCols.mkString(",")}")
      }
    }
  }

  private def outputSupportedExecs(): Unit = {
    // TODO Look at what we have for defaults for some configs because if the configs are off
    // it likely means something isn't completely compatible.
    val types = allSupportedTypes.toSeq
    val header = Seq("Exec", "Supported", "Notes", "Params") ++ types
    println(header.mkString(","))
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      val isConfigDisabled = rule.disabledMsg.isDefined
      if (rule.isVisible && checks.forall(_.shown)) {
        val execChecks = checks.get.asInstanceOf[ExecChecks]
        val allData = allSupportedTypes.toSeq.map { t =>
          (t, execChecks.support(t))
        }.toMap

        val notes = execChecks.supportNotes
        val inputs = allData.values.head.keys

        val firstCol = Seq(rule.tag.runtimeClass.getSimpleName)
        val thirdCol = Seq(rule.notes().getOrElse("None"))
        inputs.foreach { input =>
          val named = notes.get(input)
              .map(l => input + "(" + l.mkString(";") + ")")
              .getOrElse(input)
          val supportLevelOps = allSupportedTypes.toSeq.map { t =>
            allData(t)(input).text
          }
          val isSupportedExec = Seq(
            if (supportLevelOps.forall(_.equals("NS")) || isConfigDisabled) "NS" else "S")
          val allCols = (firstCol ++ isSupportedExec ++ thirdCol ++ Seq(named) ++ supportLevelOps)
          println(s"${allCols.map(replaceDelimiter(_, ",")).mkString(",")}")
        }
      }
    }
  }

  private def outputSupportedExpressions(): Unit = {
    // TODO Look at what we have for defaults for some configs because if the configs are off
    // it likely means something isn't completely compatible.
    val types = allSupportedTypes.toSeq
    val header = Seq("Expression", "Supported", "SQL Func", "Notes", "Context", "Params") ++ types
    println(header.mkString(","))
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      val isConfigDisabled = rule.disabledMsg.isDefined
      if (rule.isVisible && checks.isDefined && checks.forall(_.shown)) {
        val sqlFunctions =
          ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))
        val exprChecks = checks.get.asInstanceOf[ExprChecks]
        // Params can change between contexts, but should not
        val allData = allSupportedTypes.toSeq.map { t =>
          (t, exprChecks.support(t))
        }.toMap
        val representative = allData.values.head
        val firstCol = Seq(rule.tag.runtimeClass.getSimpleName)
        val staticCols = Seq(sqlFunctions.getOrElse(" "), rule.notes().getOrElse("None"))

        representative.foreach {
          case (context, data) =>
            data.keys.foreach { param =>
              val supportLevelOps = allSupportedTypes.toSeq.map { t =>
                allData(t)(context)(param).text
              }
              val isSupportedExpr = Seq(
                if (supportLevelOps.forall(_.equals("NS")) || isConfigDisabled) "NS" else "S")
              val allCols = (firstCol ++ isSupportedExpr ++ staticCols ++ Seq(context.toString)
                  ++ Seq(param) ++ supportLevelOps)
              println(s"${allCols.map(replaceDelimiter(_, ",")).mkString(",")}")
            }
        }
      }
    }
  }

  def help(printType: String): Unit = {
    printType match {
      case a if a.equals("execs") => outputSupportedExecs()
      case expr if expr.equals("exprs") => outputSupportedExpressions()
      case score if score.equals("operatorScore") => operatorMappingWithScore()
      case io if io.equals("ioOnly") => outputSupportIO()
      case _ => throw new IllegalArgumentException("SupportedOpsForTools: Invalid option. Valid" +
          "options are `execs`, `exprs`, `operatorScore` and `ioOnly`")
    }
  }

  def main(args: Array[String]): Unit = {
    val out = new FileOutputStream(new File(args(0)))
    val printType = if (args.size > 1) {
      args(1)
    } else {
      "ioOnly"
    }
    Console.withOut(out) {
      Console.withErr(out) {
        SupportedOpsForTools.help(printType)
      }
    }
  }
}
