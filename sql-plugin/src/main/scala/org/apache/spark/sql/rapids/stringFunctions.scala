/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.nio.charset.Charset
import java.text.DecimalFormatSymbols
import java.util.{EnumSet, Locale, Optional}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ast, BinaryOp, BinaryOperable, CaptureGroups, ColumnVector, ColumnView, DType, PadSide, RegexFlag, RegexProgram, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{Arithmetic, RoundMode}
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.jni.CharsetDecode
import com.nvidia.spark.rapids.jni.GpuSubstringIndexUtils
import com.nvidia.spark.rapids.jni.NumberConverter
import com.nvidia.spark.rapids.jni.RegexRewriteUtils
import com.nvidia.spark.rapids.shims.{NullIntolerantShim, ShimExpression, SparkShimImpl}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.errors.ConvUtils
import org.apache.spark.sql.rapids.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

abstract class GpuUnaryString2StringExpression extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StringType
}

case class GpuUpper(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"upper($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.upper()
}

case class GpuLower(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"lower($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.lower()
}

case class GpuLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.getCharLengths()
}

case class GpuBitLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"bit_length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.getByteCount) { byteCnt =>
      // bit count = byte count * 8
      withResource(GpuScalar.from(3, IntegerType)) { factor =>
        byteCnt.binaryOp(BinaryOp.SHIFT_LEFT, factor, DType.INT32)
      }
    }
  }
}

case class GpuOctetLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"octet_length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.getByteCount
}

case class GpuStringLocate(substr: Expression, col: Expression, start: Expression)
  extends GpuTernaryExpressionArgsScalarAnyScalar
      with ImplicitCastInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = substr
  override def second: Expression = col
  override def third: Expression = start

  def this(substr: Expression, col: Expression) = {
    this(substr, col, GpuLiteral(1, IntegerType))
  }

  override def doColumnar(val0: GpuScalar, val1: GpuColumnVector,
      val2: GpuScalar): ColumnVector = {
    if (!val2.isValid) {
      withResource(Scalar.fromInt(0)) { zeroScalar =>
        ColumnVector.fromScalar(zeroScalar, val1.getRowCount().toInt)
      }
    } else if (!val0.isValid) {
      //if null substring // or null column? <-- needs to be looked for/tested
      GpuColumnVector.columnVectorFromNull(val1.getRowCount().toInt, IntegerType)
    } else {
      val val2Int = val2.getValue.asInstanceOf[Int]
      val val0Str = val0.getValue.asInstanceOf[UTF8String]
      if (val2Int < 1 || val0Str.numChars() == 0) {
        withResource(val1.getBase.isNotNull()) { isNotNullColumn =>
          withResource(Scalar.fromNull(DType.INT32)) { nullScalar =>
            if (val2Int >= 1) {
              withResource(Scalar.fromInt(1)) { sv1 =>
                isNotNullColumn.ifElse(sv1, nullScalar)
              }
            } else {
              withResource(Scalar.fromInt(0)) { sv0 =>
                isNotNullColumn.ifElse(sv0, nullScalar)
              }
            }
          }
        }
      } else {
        withResource(val1.getBase.stringLocate(val0.getBase, val2Int - 1, -1)) {
          skewedResult =>
            withResource(Scalar.fromInt(1)) { sv1 =>
              skewedResult.add(sv1)
            }
        }
      }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val1, numRows)) { val1Col =>
      doColumnar(val0, val1Col, val2)
    }
  }

}

case class GpuStartsWith(left: Expression, right: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with Predicate
      with ImplicitCastInputTypes
      with NullIntolerantShim {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL STARTSWITH ($listSQL))"
  }

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.startsWith(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuEndsWith(left: Expression, right: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with Predicate
      with ImplicitCastInputTypes
      with NullIntolerantShim {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL ENDSWITH ($listSQL))"
  }

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.endsWith(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuStringTrim(column: Expression, trimParameters: Option[Expression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "BOTH"

  val trimMethod = "gpuTrim"

  override def strippedColumnVector(column: GpuColumnVector, t: Scalar): GpuColumnVector =
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.strip(t), dataType)
    }
}

case class GpuStringTrimLeft(column: Expression, trimParameters: Option[Expression] = None)
  extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "LEADING"

  val trimMethod = "gpuTrimLeft"

  override def strippedColumnVector(column: GpuColumnVector, t: Scalar): GpuColumnVector =
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.lstrip(t), dataType)
    }
}

case class GpuStringTrimRight(column: Expression, trimParameters: Option[Expression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "TRAILING"

  val trimMethod = "gpuTrimRight"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector =
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.rstrip(t), dataType)
    }
}

case class GpuConcatWs(children: Seq[Expression])
    extends GpuExpression with ShimExpression with ImplicitCastInputTypes {
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  /** The 1st child (separator) is str, and rest are either str or array of str. */
  override def inputTypes: Seq[AbstractDataType] = {
    val arrayOrStr = TypeCollection(ArrayType(StringType), StringType)
    StringType +: Seq.fill(children.size - 1)(arrayOrStr)
  }

  private def concatArrayCol(colOrScalarSep: Any,
      cv: ColumnView): GpuColumnVector = {
    colOrScalarSep match {
      case sepScalar: GpuScalar =>
        withResource(GpuScalar.from("", StringType)) { emptyStrScalar =>
          GpuColumnVector.from(cv.stringConcatenateListElements(sepScalar.getBase, emptyStrScalar,
            false, false), dataType)
        }
      case sepVec: GpuColumnVector =>
        GpuColumnVector.from(cv.stringConcatenateListElements(sepVec.getBase), dataType)
    }
  }

  private def processSingleColScalarSep(cv: ColumnVector): GpuColumnVector = {
    // single column with scalar separator just replace any nulls with empty string
    withResource(Scalar.fromString("")) { emptyStrScalar =>
      GpuColumnVector.from(cv.replaceNulls(emptyStrScalar), dataType)
    }
  }

  private def stringConcatSeparatorScalar(columns: ArrayBuffer[ColumnVector],
      sep: GpuScalar): GpuColumnVector = {
    withResource(Scalar.fromString("")) { emptyStrScalar =>
      GpuColumnVector.from(ColumnVector.stringConcatenate(sep.getBase, emptyStrScalar,
        columns.toArray[ColumnView], false), dataType)
    }
  }

  private def stringConcatSeparatorVector(columns: ArrayBuffer[ColumnVector],
      sep: GpuColumnVector): GpuColumnVector = {
    // GpuOverrides doesn't allow only specifying a separator, you have to specify at
    // least one value column
    GpuColumnVector.from(ColumnVector.stringConcatenate(columns.toArray[ColumnView],
      sep.getBase), dataType)
  }

  private def resolveColumnVectorAndConcatArrayCols(
      expr: Expression,
      numRows: Int,
      colOrScalarSep: Any,
      batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(expr.columnarEvalAny(batch)) {
      case vector: GpuColumnVector =>
        vector.dataType() match {
          case ArrayType(StringType, _) => concatArrayCol(colOrScalarSep, vector.getBase)
          case _ => vector.incRefCount()
        }
      case s: GpuScalar =>
        s.dataType match {
          case ArrayType(StringType, _) =>
            // we have to first concatenate any array types
            withResource(GpuColumnVector.from(s, numRows).getBase) { cv =>
              concatArrayCol(colOrScalarSep, cv)
            }
          case _ => GpuColumnVector.from(s, numRows)
        }
      case other =>
        throw new IllegalArgumentException(s"Cannot resolve a ColumnVector from the value:" +
          s" $other. Please convert it to a GpuScalar or a GpuColumnVector before returning.")
      }
  }

  private def checkScalarSeparatorNull(colOrScalarSep: Any,
      numRows: Int): Option[GpuColumnVector] = {
    colOrScalarSep match {
      case sepScalar: GpuScalar if (!sepScalar.getBase.isValid()) =>
        // if null scalar separator just return a column of all nulls
        Some(GpuColumnVector.from(sepScalar, numRows))
      case _ =>
        None
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    withResourceIfAllowed(children.head.columnarEvalAny(batch)) { colOrScalarSep =>
      // check for null scalar separator
      checkScalarSeparatorNull(colOrScalarSep, numRows).getOrElse {
        withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
          columns ++= children.tail.map {
            resolveColumnVectorAndConcatArrayCols(_, numRows, colOrScalarSep, batch).getBase
          }
          colOrScalarSep match {
            case sepScalar: GpuScalar =>
              if (columns.size == 1) {
                processSingleColScalarSep(columns.head)
              } else {
                stringConcatSeparatorScalar(columns, sepScalar)
              }
            case sepVec: GpuColumnVector  => stringConcatSeparatorVector(columns, sepVec)
          }
        }
      }
    }
  }
}

case class GpuContains(left: Expression, right: Expression)
    extends GpuBinaryExpression
        with Predicate
        with ImplicitCastInputTypes
        with NullIntolerantShim
        with GpuCombinable {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL CONTAINS ($listSQL))"
  }

  override def toString: String = s"gpucontains($left, $right)"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.stringContains(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // stringContainsPerRow returns false for null targets, but Spark expects null
    // when either argument is null (nullIntolerant). Merge validity from both inputs.
    withResource(lhs.getBase.stringContainsPerRow(rhs.getBase)) { result =>
      result.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs.getBase, rhs.getBase)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  /**
   * Get a combiner that can be used to find candidates to combine
   */
  override def getCombiner(): GpuExpressionCombiner = new ContainsCombiner(this)
}

case class GpuMultiContains(left: Expression, targets: Seq[UTF8String], output: StructType)
    extends GpuExpression with ShimExpression {

  override def otherCopyArgs: Seq[AnyRef] = Nil

  override def dataType: DataType = output

  override def nullable: Boolean = false

  override def prettyName: String = "multi_contains"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val targetsBytes = targets.map(t => t.getBytes).toArray
    val boolCvs = withResource(ColumnVector.fromUTF8Strings(targetsBytes: _*)) { targetsCv =>
      withResource(left.columnarEval(batch)) { lhs =>
        lhs.getBase.stringContains(targetsCv)
      }
    }
    withResource(boolCvs) { _ =>
      val retView = ColumnView.makeStructView(batch.numRows(), boolCvs: _*)
      GpuColumnVector.from(retView.copyToColumnVector(), dataType)
    }
  }

  override def children: Seq[Expression] = Seq(left)
}

class ContainsCombiner(private val exp: GpuContains) extends GpuExpressionCombiner {
  private var outputLocation = 0
  /**
   * A mapping between an expression and where in the output struct of
   * the MultiGetJsonObject will the output be.
   */
  private val toCombine = mutable.HashMap.empty[GpuExpressionEquals, Int]
  addExpression(exp)

  override def toString: String = s"ContainsCombiner $toCombine"

  override def hashCode: Int = {
    // We already know that we are Contains, and what we can combine is based
    // on the string column being the same.
    "Contains".hashCode + (exp.left.semanticHash() * 17)
  }

  /**
   * only combine when targets are literals
   */
  override def equals(o: Any): Boolean = o match {
    case other: ContainsCombiner => exp.left.semanticEquals(other.exp.left) &&
        exp.right.isInstanceOf[GpuLiteral] && other.exp.right.isInstanceOf[GpuLiteral]
    case _ => false
  }

  override def addExpression(e: Expression): Unit = {
    val localOutputLocation = outputLocation
    outputLocation += 1
    val key = GpuExpressionEquals(e)
    if (!toCombine.contains(key)) {
      toCombine.put(key, localOutputLocation)
    }
  }

  override def useCount: Int = toCombine.size

  private def fieldName(id: Int): String =
    s"_mc_$id"

  @tailrec
  private def extractLiteral(exp: Expression): GpuLiteral = exp match {
    case l: GpuLiteral => l
    case a: Alias => extractLiteral(a.child)
    case other => throw new RuntimeException("Unsupported expression in contains combiner, " +
        "should be a literal type, actual type is " + other.getClass.getName)
  }

  private lazy val multiContains: GpuMultiContains = {
    val input = toCombine.head._1.e.asInstanceOf[GpuContains].left
    val fieldsNPaths = toCombine.toSeq.map {
      case (k, id) =>
        (id, k.e)
    }.sortBy(_._1).map {
      case (id, e: GpuContains) =>
        val target = extractLiteral(e.right).value.asInstanceOf[UTF8String]
        (StructField(fieldName(id), e.dataType, e.nullable), target)
    }
    val dt = StructType(fieldsNPaths.map(_._1))
    GpuMultiContains(input, fieldsNPaths.map(_._2), dt)
  }

  override def getReplacementExpression(e: Expression): Option[Expression] = {
    toCombine.get(GpuExpressionEquals(e)).map { localId =>
      GpuGetStructField(multiContains, localId, Some(fieldName(localId)))
    }
  }
}

case class GpuSubstring(str: Expression, pos: Expression, len: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def first: Expression = str
  override def second: Expression = pos
  override def third: Expression = len

  def this(str: Expression, pos: Expression) = {
    this(str, pos, GpuLiteral(Integer.MAX_VALUE, IntegerType))
  }

  private[this] def computeStarts(strs: ColumnView, poses: ColumnView): ColumnVector = {
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    // cudf `substring(column, column, column)` treats negative start always as 0, so
    // need to do the similar calculation as CPU here.

    // 1) pos + str_size
    val negConvertedPoses = withResource(strs.getCharLengths) { strSizes =>
      poses.add(strSizes, DType.INT32)
    }
    withResource(negConvertedPoses) { _ =>
      withResource(Scalar.fromInt(0)) { zero =>
        // 2) (pos > 0) ? pos - 1 : 0
        val subOnePoses = withResource(Scalar.fromInt(1)) { one =>
          poses.sub(one, DType.INT32)
        }
        val zeroBasedPoses = withResource(subOnePoses) { _ =>
          withResource(poses.greaterThan(zero)) { posPosFlags =>
            // Use "poses" here instead of "zero" as the false path to keep the nulls,
            // since "ifElse" will erase the null mask of "poses".
            posPosFlags.ifElse(subOnePoses, poses)
          }
        }

        withResource(zeroBasedPoses) { _ =>
          withResource(poses.lessThan(zero)) { negPosFlags =>
            negPosFlags.ifElse(negConvertedPoses, zeroBasedPoses)
          }
        }
      }
    }
  }

  private[this] def computeEnds(starts: BinaryOperable, lens: BinaryOperable): ColumnVector = {
    // CPU:
    //     end = start + length
    //   , along with integer overflow check
    val endLongCol = withResource(starts.add(lens, DType.INT64)) { endColAsLong =>
      // If (end < 0), end = 0, let cudf return empty string.
      // If (end > Int.MaxValue), end = Int.MaxValue, let cudf return string
      //   from start until the string end.
      // To align with the CPU's behavior.
      withResource(Scalar.fromLong(0L)) { zero =>
        withResource(Scalar.fromLong(Int.MaxValue.toLong)) { maxInt =>
          endColAsLong.clamp(zero, maxInt)
        }
      }
    }
    withResource(endLongCol) { _ =>
      endLongCol.castTo(DType.INT32)
    }
  }

  private[this] def substringColumn(strs: ColumnView, starts: ColumnView,
      ends: ColumnView): ColumnVector = {
    // cudf does not allow nulls in starts and ends.
    val noNullStarts = new ColumnView(starts.getType, starts.getRowCount, Optional.of(0L),
      starts.getData, null)
    withResource(noNullStarts) { _ =>
      val noNullEnds = new ColumnView(ends.getType, ends.getRowCount, Optional.of(0L),
        ends.getData, null)
      withResource(noNullEnds) { _ =>
        // Spark returns null if any of (str, pos, len) is null, and `ends`'s null mask
        // should already cover pos and len.
        withResource(strs.mergeAndSetValidity(BinaryOp.BITWISE_AND, strs, ends)) { rets =>
          rets.substring(noNullStarts, noNullEnds)
        }
      }
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posCol: GpuColumnVector,
      lenCol: GpuColumnVector): ColumnVector = {
    val strs = strCol.getBase
    val poses = posCol.getBase
    val lens = lenCol.getBase
    withResource(computeStarts(strs, poses)) { starts =>
      withResource(computeEnds(starts, lens)) { ends =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar(strS: GpuScalar, posCol: GpuColumnVector,
      lenCol: GpuColumnVector): ColumnVector = {
    val numRows = posCol.getRowCount.toInt
    withResource(GpuColumnVector.from(strS, numRows)) { strCol =>
      doColumnar(strCol, posCol, lenCol)
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posS: GpuScalar,
      lenCol: GpuColumnVector): ColumnVector = {
    val strs = strCol.getBase
    val lens = lenCol.getBase
    val pos = posS.getValue.asInstanceOf[Int]
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    val starts = if (pos < 0) {
      withResource(strs.getCharLengths) { strSizes =>
        withResource(Scalar.fromInt(pos)) { posS =>
          posS.add(strSizes, DType.INT32)
        }
      }
    } else { // pos >= 0
      val start = if (pos > 0) pos - 1 else 0
      withResource(Scalar.fromInt(start)) { startS =>
        ColumnVector.fromScalar(startS, strs.getRowCount.toInt)
      }
    }

    withResource(starts) { _ =>
      withResource(computeEnds(starts, lens)) { ends =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar (strS: GpuScalar, posS: GpuScalar,
      lenCol: GpuColumnVector): ColumnVector = {
    val strValue = strS.getValue.asInstanceOf[UTF8String]
    val pos = posS.getValue.asInstanceOf[Int]
    val lens = lenCol.getBase
    val numRows = lenCol.getRowCount.toInt
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    val start = if (pos < 0) {
      pos + strValue.numChars()
    } else if (pos > 0) {
      pos - 1
    } else 0

    val starts = withResource(Scalar.fromInt(start)) { startS =>
      ColumnVector.fromScalar(startS, numRows)
    }

    withResource(starts) { _ =>
      withResource(computeEnds(starts, lens)) { ends =>
        withResource(ColumnVector.fromScalar(strS.getBase, numRows)) { strs =>
          substringColumn(strs, starts, ends)
        }
      }
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posCol: GpuColumnVector,
      lenS: GpuScalar): ColumnVector = {
    val strs = strCol.getBase
    val poses = posCol.getBase
    withResource(computeStarts(strs, poses)) { starts =>
      withResource(computeEnds(starts, lenS.getBase)) { ends =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar(strS: GpuScalar, posCol: GpuColumnVector,
      lenS: GpuScalar): ColumnVector = {
    val numRows = posCol.getRowCount.toInt
    withResource(GpuColumnVector.from(strS, numRows)) { strCol =>
      doColumnar(strCol, posCol, lenS)
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posS: GpuScalar,
      lenS: GpuScalar): ColumnVector = {
    val strs = strCol.getBase
    val pos = posS.getValue.asInstanceOf[Int]
    val len = lenS.getValue.asInstanceOf[Int]
    val (start, endOpt) = if (len <= 0) {
      // Spark returns empty string if length is negative or zero
      (0, Some(0))
    } else if (pos > 0) {
      // 1-based index, convert to 0-based index
      val head = pos - 1
      val tail = if (head.toLong + len > Int.MaxValue) Int.MaxValue else head + len
      (head, Some(tail))
    } else if (pos == 0) {
      // 0-based index, calculate substring from 0 to length
      (0, Some(len))
    } else if (pos + len < 0) {
      // Drop the last "abs(substringPos + substringLen)" chars.
      // e.g.
      //    >> substring("abc", -3, 1)
      //    >> "a"  // dropping the last 2 [= abs(-3+1)] chars.
      // `pos + len` does not overflow as `pos < 0 && len > 0` here.
      (pos, Some(pos + len))
    } else { // pos + len >= 0
      // Read from start until the end.
      // e.g. `substring("abc", -3, 4)` outputs "abc".
      (pos, None)
    }
    endOpt.map(strs.substring(start, _)).getOrElse(strs.substring(start))
  }

  override def doColumnar(numRows: Int, strS: GpuScalar, posS: GpuScalar,
      lenS: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(strS, numRows)) { strCol =>
      doColumnar(strCol, posS, lenS)
    }
  }
}

case class GpuInitCap(child: Expression) extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    withResource(Scalar.fromString(" ")) { space =>
      // Spark only sees the space character as a word deliminator.
      input.getBase.capitalize(space)
    }
}

case class GpuStringRepeat(input: Expression, repeatTimes: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {
  override def left: Expression = input
  override def right: Expression = repeatTimes
  override def dataType: DataType = input.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)

  def doColumnar(input: GpuScalar, repeatTimes: GpuColumnVector): ColumnVector = {
    assert(input.dataType == StringType)

    withResource(GpuColumnVector.from(input, repeatTimes.getRowCount.asInstanceOf[Int])) {
      replicatedInput => doColumnar(replicatedInput, repeatTimes)
    }
  }

  def doColumnar(input: GpuColumnVector, repeatTimes: GpuColumnVector): ColumnVector = {
    input.getBase.repeatStrings(repeatTimes.getBase)
  }

  def doColumnar(input: GpuColumnVector, repeatTimes: GpuScalar): ColumnVector = {
    if (!repeatTimes.isValid) {
      // If the input scala repeatTimes is invalid, the results should be all nulls.
      withResource(Scalar.fromNull(DType.STRING)) {
        nullString => ColumnVector.fromScalar(nullString, input.getRowCount.asInstanceOf[Int])
      }
    } else {
      assert(repeatTimes.dataType == IntegerType)
      val repeatTimesVal = repeatTimes.getBase.getInt

      // Get the input size to check for overflow for the output.
      // Note that this is not an accurate check since the total buffer size of the input
      // strings column may be larger than the total length of strings that will be repeated in
      // this function.
      val inputBufferSize: Long = Option(input.getBase.getData).map(_.getLength).getOrElse(0)
      if (repeatTimesVal > 0 && inputBufferSize > Int.MaxValue / repeatTimesVal) {
        throw new RuntimeException("Output strings have total size exceed maximum allowed size")
      }

      // Finally repeat the strings.
      input.getBase.repeatStrings(repeatTimesVal)
    }
  }

  def doColumnar(numRows: Int, input: GpuScalar, repeatTimes: GpuScalar): ColumnVector = {
    assert(input.dataType == StringType)

    if (!repeatTimes.isValid) {
      // If the input scala repeatTimes is invalid, the results should be all nulls.
      withResource(Scalar.fromNull(DType.STRING)) {
        nullString => ColumnVector.fromScalar(nullString, numRows)
      }
    } else {
      assert(repeatTimes.dataType == IntegerType)
      val repeatTimesVal = repeatTimes.getBase.getInt

      withResource(input.getBase.repeatString(repeatTimesVal)) {
        repeatedString => ColumnVector.fromScalar(repeatedString, numRows)
      }
    }
  }

}

trait HasGpuStringReplace {
  def doStringReplace(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    // When search or replace string is null, return all nulls like the CPU does.
    if (!searchExpr.isValid || !replaceExpr.isValid) {
      GpuColumnVector.columnVectorFromNull(strExpr.getRowCount.toInt, StringType)
    } else if (searchExpr.getValue.asInstanceOf[UTF8String].numChars() == 0) {
      // Return original string if search string is empty
      strExpr.getBase.asStrings()
    } else {
      strExpr.getBase.stringReplace(searchExpr.getBase, replaceExpr.getBase)
    }
  }

  def doStringReplaceMulti(
    strExpr: GpuColumnVector,
    search: Seq[String],
    replacement: String): ColumnVector = {
      withResource(ColumnVector.fromStrings(search: _*)) { targets =>
        withResource(ColumnVector.fromStrings(replacement)) {  repls =>
          strExpr.getBase.stringReplace(targets, repls)
        }
      }
  }
}

case class GpuStringReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
  extends GpuTernaryExpression
      with ImplicitCastInputTypes
      with HasGpuStringReplace {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

  def this(srcExpr: Expression, searchExpr: Expression) = {
    this(srcExpr, searchExpr, GpuLiteral("", StringType))
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    doStringReplace(strExpr, searchExpr, replaceExpr)
  }

  override def doColumnar(
      numRows: Int,
      strExpr: GpuScalar,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(strExpr, numRows)) { strExprCol =>
      doColumnar(strExprCol, searchExpr, replaceExpr)
    }
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuColumnVector): ColumnVector = {
    strExpr.getBase.stringReplacePerRow(searchExpr.getBase, replaceExpr.getBase)
  }

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(strExpr, searchExpr.getRowCount.toInt)) { expandedStr =>
      doColumnar(expandedStr, searchExpr, replaceExpr)
    }
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(searchExpr, strExpr.getRowCount.toInt)) { expandedSearch =>
      doColumnar(strExpr, expandedSearch, replaceExpr)
    }
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(replaceExpr, strExpr.getRowCount.toInt)) { expandedRepl =>
      doColumnar(strExpr, searchExpr, expandedRepl)
    }
  }

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuScalar,
      replaceExpr: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(strExpr, replaceExpr.getRowCount.toInt)) { expandedStr =>
      withResource(GpuColumnVector.from(searchExpr, replaceExpr.getRowCount.toInt)) {
        expandedSearch =>
          doColumnar(expandedStr, expandedSearch, replaceExpr)
      }
    }
  }

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(strExpr, searchExpr.getRowCount.toInt)) { expandedStr =>
      withResource(GpuColumnVector.from(replaceExpr, searchExpr.getRowCount.toInt)) {
        expandedRepl =>
          doColumnar(expandedStr, searchExpr, expandedRepl)
      }
    }
  }
}

case class GpuStringTranslate(
    srcExpr: Expression,
    fromExpr: Expression,
    toExpr: Expression)
  extends GpuTernaryExpressionArgsAnyScalarScalar
      with ImplicitCastInputTypes {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = fromExpr
  override def third: Expression = toExpr

  private def buildLists(fromExpr: GpuScalar, toExpr: GpuScalar): (List[String], List[String]) = {
    val fromString = fromExpr.getValue.asInstanceOf[UTF8String].toString
    val toString = toExpr.getValue.asInstanceOf[UTF8String].toString
    var fromCharsArray = Array[String]()
    var toCharsArray = Array[String]()
    var i = 0
    var j = 0
    while (i < fromString.length) {
      val replaceStr = if (j < toString.length) {
        val repCharCount = Character.charCount(toString.codePointAt(j))
        val repStr = toString.substring(j, j + repCharCount)
        j += repCharCount
        repStr
      } else {
        ""
      }
      val matchCharCount = Character.charCount(fromString.codePointAt(i))
      val matchStr = fromString.substring(i, i + matchCharCount)
      i += matchCharCount
      fromCharsArray :+= matchStr
      toCharsArray :+= replaceStr
    }
    (fromCharsArray.toList, toCharsArray.toList)
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      fromExpr: GpuScalar,
      toExpr: GpuScalar): ColumnVector = {
    // When from or to string is null, return all nulls like the CPU does.
    if (!fromExpr.isValid || !toExpr.isValid) {
      GpuColumnVector.columnVectorFromNull(strExpr.getRowCount.toInt, StringType)
    } else if (fromExpr.getValue.asInstanceOf[UTF8String].numChars() == 0) {
      // Return original string if search string is empty
      strExpr.getBase.incRefCount()
    } else {
      val (fromStringList, toStringList) = buildLists(fromExpr, toExpr)
      withResource(ColumnVector.fromStrings(fromStringList: _*)) { fromStringCol =>
        withResource(ColumnVector.fromStrings(toStringList: _*)) { toStringCol =>
          strExpr.getBase.stringReplace(fromStringCol, toStringCol)
        }
      }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }
}

object CudfRegexp {
  val escapeForCudfCharSet = Seq('^', '-', ']')

  def notCharSet(c: Char): String = c match {
    case '\n' => "(?:.|\r)"
    case '\r' => "(?:.|\n)"
    case chr if escapeForCudfCharSet.contains(chr) => "(?:[^\\" + chr + "]|\r|\n)"
    case chr => "(?:[^" + chr + "]|\r|\n)"
  }

  val escapeForCudf = Seq('[', '^', '$', '.', '|', '?', '*','+', '(', ')', '\\', '{', '}')

  def cudfQuote(c: Character): String = c match {
    case chr if escapeForCudf.contains(chr) => "\\" + chr
    case chr => Character.toString(chr)
  }
}

case class GpuLike(left: Expression, right: Expression, escapeChar: Char)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerantShim {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def toString: String = escapeChar match {
    case '\\' => s"$left gpulike $right"
    case c => s"$left gpulike $right ESCAPE '$c'"
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(Scalar.fromString(Character.toString(escapeChar))) { escapeScalar =>
      lhs.getBase.like(rhs.getBase, escapeScalar)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
}

object GpuRegExpUtils {
  private def parseAST(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

  /**
   * Convert numbered `$group_index` back-reference symbols into cuDF's `${group_index}` form.
   * Java `Matcher#appendReplacement` treats `\digit` as the literal digit character, not as a
   * back-reference. Escaped pairs are kept verbatim here and are later normalized by
   * `unescapeReplaceString`.
   *
   * Java's `Matcher.appendReplacement` reads the digits after `$` one at a time and
   * stops as soon as adding the next digit would make the running group index exceed the
   * actual capture-group count; any further digits are treated as literal characters
   * (greedy-with-backoff). When `numCaptureGroups` is negative the caller is opting out of the
   * Java-spec check (used by tests and legacy callers), in which case we keep the original
   * eagerly-greedy behavior so cuDF still raises on out-of-range indices.
   *
   * @param rep replacement string
   * @param numCaptureGroups number of capturing groups in the corresponding pattern; pass a
   *                         negative value to disable greedy-with-backoff
   * @return A pair consists of a boolean indicating whether containing any backref and the
   *         converted replacement.
   */
  def backrefConversion(rep: String, numCaptureGroups: Int): (Boolean, String) = {
    val b = new StringBuilder
    var i = 0
    var hasBracedBackref = false
    while (i < rep.length) {
      // Pass through already-braced `${N}` tokens unchanged. The replacement AST uses this
      // form for backrefs that have already been resolved by the transpiler, so re-parsing
      // them against the caller-provided group count could change their meaning.
      if (rep.charAt(i) == '$' && i + 2 < rep.length && rep.charAt(i + 1) == '{') {
        val close = rep.indexOf('}', i + 2)
        val allDigits = close > i + 2 &&
          (i + 2 until close).forall(k => rep.charAt(k).isDigit)
        if (allDigits) {
          b.append(rep.substring(i, close + 1))
          hasBracedBackref = true
          i = close + 1
        } else {
          b.append(rep.charAt(i))
          i += 1
        }
      } else if (rep.charAt(i) == '$' && i + 1 < rep.length && rep.charAt(i + 1).isDigit) {

        // Consume digits one at a time. If the running group index would exceed the actual
        // capture-group count, stop and leave the remaining digits as literals. When no digit
        // can be consumed without exceeding the count (e.g. `$5` against a 4-group pattern),
        // fall through to the legacy eagerly-greedy path so cuDF surfaces the out-of-range
        // error.
        var j = i + 1
        var n = 0
        var stopped = false
        while (j < rep.length && rep.charAt(j).isDigit && !stopped) {
          val nextN = n * 10 + (rep.charAt(j) - '0')
          if (numCaptureGroups >= 0 && nextN > numCaptureGroups) {
            stopped = true
          } else {
            n = nextN
            j += 1
          }
        }
        if (j > i + 1) {
          b.append("${").append(n).append("}")
          i = j
        } else {
          // Legacy path: greedily swallow every digit so cuDF errors on the out-of-range
          // index (preserves existing behavior covered by
          // `test_re_replace_backrefs_idx_out_of_bounds`).
          b.append("${")
          var k = i + 1
          do {
            b.append(rep.charAt(k))
            k += 1
          } while (k < rep.length && rep.charAt(k).isDigit)
          b.append("}")
          i = k
        }
      } else if (rep.charAt(i) == '\\' && i + 1 < rep.length) {
        // Intentionally keep the whole escape sequence (`\` + the next char) together so
        // `unescapeReplaceString` can strip just the leading backslash. The cases that matter
        // here are `\$` (escaped dollar -> literal `$`) and `\\` (escaped backslash); any other
        // `\X` could fall through to the else branch, but keeping the pair together is harmless.
        b.append('\\').append(rep.charAt(i + 1))
        i += 2
      } else {
        b.append(rep.charAt(i))
        i += 1
      }
    }

    val converted = b.toString
    // A pass-through `${N}` token does not modify the string, so equality alone would miss
    // it; treat it as a backref so the caller routes through `stringReplaceWithBackrefs`.
    (hasBracedBackref || !rep.equals(converted)) -> converted
  }

  /**
   * We need to remove escape characters in the regexp_replace
   * replacement string before passing to cuDF.
   */
  def unescapeReplaceString(s: String): String = {
    val b = new StringBuilder
    var i = 0
    while (i < s.length) {
      if (s.charAt(i) == '\\' && i+1 < s.length) {
        i += 1
      }
      b.append(s.charAt(i))
      i += 1
    }
    b.toString
  }

  def tagForRegExpEnabled(meta: ExprMeta[_]): Unit = {
    if (!meta.conf.isRegExpEnabled) {
      meta.willNotWorkOnGpu(s"regular expression support is disabled. " +
        s"Set ${RapidsConf.ENABLE_REGEXP}=true to enable it")
    }

    Charset.defaultCharset().name() match {
      case "UTF-8" =>
        // supported
      case _ =>
        meta.willNotWorkOnGpu(s"regular expression support is disabled because the GPU only " +
        "supports the UTF-8 charset when using regular expressions")
    }
  }

  /**
   * Recursively check if pattern contains only zero-match repetitions
   * ?, *, {0,}, or {0,n} or any combination of them.
   */
  def isEmptyRepetition(pattern: String): Boolean = {
    def isASTEmptyRepetition(regex: RegexAST): Boolean = {
      regex match {
        case RegexRepetition(_, term) => term match {
          case SimpleQuantifier('*') | SimpleQuantifier('?') => true
          case QuantifierFixedLength(0) => true
          case QuantifierVariableLength(0, _) => true
          case _ => false
        }
        case RegexGroup(_, term, _) =>
          isASTEmptyRepetition(term)
        case RegexSequence(parts) =>
          parts.forall(isASTEmptyRepetition)
        // cuDF does not support repetitions adjacent to a choice (eg. "a*|a"), but if
        // we did, we would need to add a `case RegexChoice()` here
        case _ => false
      }
    }
    parseAST(pattern) match {
      case RegexSequence(parts) if parts.lastOption.contains(RegexChar('$')) =>
        // handle pattern ".*$"
        isASTEmptyRepetition(RegexSequence(parts.dropRight(1)))
      case other => isASTEmptyRepetition(other)
    }
  }

  /**
   * Returns the number of capturing groups in regexp.
   */
  def countGroups(pattern: String): Int = {
    def countGroups(regexp: RegexAST): Int = {
      regexp match {
        case RegexGroup(capture, term, _) =>
          (if (capture) 1 else 0) + countGroups(term)
        case other => other.children().map(countGroups).sum
      }
   }
   countGroups(parseAST(pattern))
  }

  def getChoicesFromRegex(regex: RegexAST): Option[Seq[String]] = {
    regex match {
      case RegexGroup(_, t, None) =>
        getChoicesFromRegex(t)
      case RegexChoice(a, b) =>
        getChoicesFromRegex(a) match {
          case Some(la) =>
            getChoicesFromRegex(b) match {
              case Some(lb) => Some(la ++ lb)
              case _ => None
            }
          case _ => None
        }
      case RegexSequence(parts) =>
        if (parts.isEmpty) {
          None
        } else if (GpuOverrides.isSupportedStringReplacePattern(regex.toRegexString)) {
          Some(Seq(regex.toRegexString))
        } else if (parts.size == 1) {
          getChoicesFromRegex(parts.head)
        } else {
          // A sequence represents concatenation, not a union. Fixed single-literal
          // children can be joined; alternatives require the regex engine.
          parts.foldLeft(Option(new StringBuilder)) {
            case (Some(builder), part) =>
              getChoicesFromRegex(part) match {
                case Some(literals) if literals.size == 1 =>
                  Some(builder.append(literals.head))
                case _ => None
              }
            case (None, _) => None
          }.map(builder => Seq(builder.result()))
        }
      case _ =>
        if (GpuOverrides.isSupportedStringReplacePattern(regex.toRegexString)) {
          Some(Seq(regex.toRegexString))
        } else {
          None
        }
    }
  }


}

class GpuRLikeMeta(
      expr: RLike,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      rule: DataFromReplacementRule) extends BinaryExprMeta[RLike](expr, conf, parent, rule) {
    import RegexOptimizationType._
    private var pattern: Option[String] = None
    private var rewriteOptimizationType: RegexOptimizationType = NoOptimization

    override def tagExprForGpu(): Unit = {
      GpuRegExpUtils.tagForRegExpEnabled(this)
      expr.right match {
        case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
          try {
            // verify that we support this regex and can transpile it to cuDF format
            val originalPattern = str.toString
            val regexAst = new RegexParser(originalPattern).parse()
            if (conf.isRlikeRegexRewriteEnabled) {
              rewriteOptimizationType = RegexRewrite.matchSimplePattern(regexAst)
            }
            val (transpiledAST, _) = new CudfRegexTranspiler(RegexFindMode)
                .getTranspiledAST(regexAst, None, None)
            pattern = Some(transpiledAST.toRegexString)
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        case _ =>
          willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
      }
    }

    override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
      rewriteOptimizationType match {
        case NoOptimization => {
          val patternStr = pattern.getOrElse(throw new IllegalStateException(
            "Expression has not been tagged with cuDF regex pattern"))
          GpuRLike(lhs, rhs, patternStr)
        }
        case StartsWith(s) => GpuStartsWith(lhs, GpuLiteral(s, StringType))
        case Contains(s) => GpuContains(lhs, GpuLiteral(UTF8String.fromString(s), StringType))
        case MultipleContains(ls) => GpuContainsAny(lhs, ls)
        case PrefixRange(s, length, start, end) =>
          GpuLiteralRangePattern(lhs, GpuLiteral(s, StringType), length, start, end)
        case _ => throw new IllegalStateException("Unexpected optimization type")
      }
    }
}

case class GpuRLike(left: Expression, right: Expression, pattern: String)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerantShim {

  override def toString: String = s"$left gpurlike $right"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    lhs.getBase.containsRe(new RegexProgram(pattern,
      EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE))
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
}

case class GpuContainsAny(input: Expression, targets: Seq[UTF8String])
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def dataType: DataType = BooleanType

  override def child: Expression = input

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  def multiOrsAst: ast.AstExpression = {
    (1 until targets.length)
    .foldLeft(new ast.ColumnReference(0).asInstanceOf[ast.AstExpression]) { (acc, id) =>
      new ast.BinaryOperation(ast.BinaryOperator.NULL_LOGICAL_OR, acc, new ast.ColumnReference(id))
    }
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val targetsBytes = targets.map(t => t.getBytes).toArray
    val boolCvs = withResource(ColumnVector.fromUTF8Strings(targetsBytes: _*)) { targetsCv =>
      input.getBase.stringContains(targetsCv)
    }
    val boolTable = withResource(boolCvs) { _ =>
      new Table(boolCvs: _*)
    }
    withResource(boolTable) { _ =>
      withResource(multiOrsAst.compile()) { compiledAst =>
        compiledAst.computeColumn(boolTable)
      }
    }
  }
}

case class GpuLiteralRangePattern(left: Expression, right: Expression, 
    length: Int, start: Int, end: Int)
  extends GpuBinaryExpressionArgsAnyScalar with ImplicitCastInputTypes with NullIntolerantShim {

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    RegexRewriteUtils.literalRangePattern(lhs.getBase, rhs.getBase, length, start, end)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

abstract class GpuRegExpTernaryBase
    extends GpuTernaryExpressionArgsAnyScalarScalar {

  override def dataType: DataType = StringType

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }
}

case class GpuRegExpReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
    (javaRegexpPattern: String,
    cudfRegexPattern: String,
    cudfReplacementString: String,
    searchList: Option[Seq[String]],
    replaceOpt: Option[GpuRegExpReplaceOpt])
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with HasGpuStringReplace {

  override def otherCopyArgs: Seq[AnyRef] = Seq(javaRegexpPattern,
    cudfRegexPattern, cudfReplacementString, searchList, replaceOpt)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

  def this(srcExpr: Expression, searchExpr: Expression)(javaRegexpPattern: String,
    cudfRegexPattern: String, cudfReplacementString: String) = {

    this(srcExpr, searchExpr, GpuLiteral("", StringType))(javaRegexpPattern,
      cudfRegexPattern, cudfReplacementString, None, None)
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    // For empty strings and a regex containing only a zero-match repetition,
    // the behavior in some versions of Spark is different.
    // see https://github.com/NVIDIA/spark-rapids/issues/5456
    replaceOpt match {
      case Some(GpuRegExpStringReplace) =>
        doStringReplace(strExpr, searchExpr, replaceExpr)
      case Some(GpuRegExpStringReplaceMulti) =>
        searchList match {
          case Some(searches) =>
            doStringReplaceMulti(strExpr, searches, cudfReplacementString)
          case _ =>
            throw new IllegalStateException("Need a replace")
        }
      case _ =>
        val prog = new RegexProgram(cudfRegexPattern,
          EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE)
        if (SparkShimImpl.reproduceEmptyStringBug &&
            GpuRegExpUtils.isEmptyRepetition(javaRegexpPattern)) {
          val isEmpty = withResource(strExpr.getBase.getCharLengths) { len =>
            withResource(Scalar.fromInt(0)) { zero =>
              len.equalTo(zero)
            }
          }
          withResource(isEmpty) { _ =>
            withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
              withResource(GpuScalar.from(cudfReplacementString, DataTypes.StringType)) { rep =>
                withResource(strExpr.getBase.replaceRegex(prog, rep)) { replacement =>
                  isEmpty.ifElse(emptyString, replacement)
                }
              }
            }
          }
        } else {
          withResource(Scalar.fromString(cudfReplacementString)) { rep =>
            strExpr.getBase.replaceRegex(prog, rep)
          }
        }
    }

  }

}

case class GpuRegExpReplaceWithBackref(
    override val child: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
    (javaRegexpPattern: String,
     cudfRegexPattern: String,
    cudfReplacementString: String)
  extends GpuUnaryExpression with ImplicitCastInputTypes {

  override def otherCopyArgs: Seq[AnyRef] = Seq(javaRegexpPattern, cudfRegexPattern,
    cudfReplacementString)
  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def dataType: DataType = StringType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val prog = new RegexProgram(cudfRegexPattern, EnumSet.of(RegexFlag.EXT_NEWLINE))
    if (SparkShimImpl.reproduceEmptyStringBug &&
        GpuRegExpUtils.isEmptyRepetition(javaRegexpPattern)) {
      val isEmpty = withResource(input.getBase.getCharLengths) { len =>
        withResource(Scalar.fromInt(0)) { zero =>
          len.equalTo(zero)
        }
      }
      withResource(isEmpty) { _ =>
        withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
          withResource(input.getBase.stringReplaceWithBackrefs(prog,
              cudfReplacementString)) { replacement =>
            isEmpty.ifElse(emptyString, replacement)
          }
        }
      }
    } else {
      input.getBase.stringReplaceWithBackrefs(prog, cudfReplacementString)
    }
  }

}

class GpuRegExpExtractMeta(
    expr: RegExpExtract,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtract](expr, conf, parent, rule) {

  private var pattern: Option[String] = None

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    ShimLoader.getShimVersion match {
      case _: DatabricksShimVersion if expr.subject.isInstanceOf[InputFileName] =>
        willNotWorkOnGpu("avoiding Databricks Delta problem with regexp extract")
      case _ =>
    }

    var numGroups = 0
    val groupIdx = expr.idx match {
      case Literal(value, DataTypes.IntegerType) =>
        Some(value.asInstanceOf[Int])
      case _ =>
        willNotWorkOnGpu("GPU only supports literal index")
        None
    }

    expr.regexp match {
      case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
        try {
          // verify that we support this regex and can transpile it to cuDF format
          val javaRegexpPattern = str.toString
          val (transpiledAST, _) =
            new CudfRegexTranspiler(RegexFindMode).getTranspiledAST(
              javaRegexpPattern, groupIdx, None)
          pattern = Some(transpiledAST.toRegexString)
          numGroups = GpuRegExpUtils.countGroups(javaRegexpPattern)
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    groupIdx.foreach { idx =>
      if (idx < 0) {
        willNotWorkOnGpu("the specified group index cannot be less than zero")
      }
      if (idx > numGroups) {
        willNotWorkOnGpu(
          s"regex group count is $numGroups, but the specified group index is $idx")
      }
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtract(str, regexp, idx)(cudfPattern)
  }
}

case class GpuRegExpExtract(
    subject: Expression,
    regexp: Expression,
    idx: Expression)(cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerantShim {

  override def otherCopyArgs: Seq[AnyRef] = cudfRegexPattern :: Nil
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = subject
  override def second: Expression = regexp
  override def third: Expression = idx

  override def prettyName: String = "regexp_extract"

  override def doColumnar(
      str: GpuColumnVector,
      regexp: GpuScalar,
      idx: GpuScalar): ColumnVector = {

    // When group index is 0, it means extract the entire pattern including those parts which
    // don't belong to any group. For instance,
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 0) => 123abcE
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 1) => 123
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 2) => E
    //
    // To support the full match (group index 0), we wrap a pair of parentheses on the original
    // cudfRegexPattern.
    val (extractPattern, groupIndex) = idx.getValue match {
      case i: Int if i == 0 =>
        ("(" + cudfRegexPattern + ")", 0)
      case _ =>
        // Since we have transpiled all but one of the capture groups to non-capturing, the index
        // here moves to 0 to single out the one capture group left
        (cudfRegexPattern, 0)
    }

    // There are some differences in behavior between cuDF and Java so we have
    // to handle those cases here.
    //
    // Given the pattern `^([a-z]*)([0-9]*)([a-z]*)$` the following table
    // shows the value that would be extracted for group index 2 given a range
    // of inputs. The behavior is mostly consistent except for the case where
    // the input is non-null and does not match the pattern.
    //
    // | Input  | Java  | cuDF  |
    // |--------|-------|-------|
    // | ''     | ''    | ''    |
    // | NULL   | NULL  | NULL  |
    // | 'a1a'  | '1'   | '1'   |
    // | '1a1'  | ''    | NULL  |

    withResource(str.getBase.extractRe(new RegexProgram(extractPattern,
      EnumSet.of(RegexFlag.EXT_NEWLINE)))) { extract =>
      withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
        val outputNullAndInputNotNull =
          withResource(extract.getColumn(groupIndex).isNull) { outputNull =>
            withResource(str.getBase.isNotNull) { inputNotNull =>
              outputNull.and(inputNotNull)
            }
          }
        withResource(outputNullAndInputNotNull) {
          _.ifElse(emptyString, extract.getColumn(groupIndex))
        }
      }
    }
  }
}

class GpuRegExpExtractAllMeta(
    expr: RegExpExtractAll,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtractAll](expr, conf, parent, rule) {

  private var pattern: Option[String] = None

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    var numGroups = 0
    val groupIdx = expr.idx match {
      case Literal(value, DataTypes.IntegerType) =>
        Some(value.asInstanceOf[Int])
      case _ =>
        willNotWorkOnGpu("GPU only supports literal index")
        None
    }

    expr.regexp match {
      case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
        try {
          val javaRegexpPattern = str.toString
          // verify that we support this regex and can transpile it to cuDF format
          val (transpiledAST, _) =
            new CudfRegexTranspiler(RegexFindMode).getTranspiledAST(
              javaRegexpPattern, groupIdx, None)
          pattern = Some(transpiledAST.toRegexString)
          numGroups = GpuRegExpUtils.countGroups(javaRegexpPattern)
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    groupIdx.foreach { idx =>
      if (idx < 0) {
        willNotWorkOnGpu("the specified group index cannot be less than zero")
      }
      if (idx > numGroups) {
        willNotWorkOnGpu(
          s"regex group count is $numGroups, but the specified group index is $idx")
      }
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtractAll(str, regexp, idx)(cudfPattern)
  }
}

case class GpuRegExpExtractAll(
    str: Expression,
    regexp: Expression,
    idx: Expression)(cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerantShim {

  override def otherCopyArgs: Seq[AnyRef] = cudfRegexPattern :: Nil
  override def dataType: DataType = ArrayType(StringType, containsNull = true)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = str
  override def second: Expression = regexp
  override def third: Expression = idx

  override def prettyName: String = "regexp_extract_all"

  override def doColumnar(
      str: GpuColumnVector,
      regexp: GpuScalar,
      idx: GpuScalar): ColumnVector = {
    idx.getValue.asInstanceOf[Int] match {
      case 0 =>
        val prog = new RegexProgram(cudfRegexPattern,
          EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE)
        str.getBase.extractAllRecord(prog, 0)
      case _ =>
        val rowCount = str.getRowCount
        val prog = new RegexProgram(cudfRegexPattern, EnumSet.of(RegexFlag.EXT_NEWLINE))

        // The transpiler leaves only the requested group as a capture group, so cuDF already
        // returns one list element per regex match. Align the remaining result semantics with
        // Spark: an unmatched capture is an empty string, no matches is an empty list, and a
        // null input is a null list.
        withResource(str.getBase.extractAllRecord(prog, 1)) { extracted =>
          val noMatchesAsEmptyLists = withResource(GpuScalar.from(
            new GenericArrayData(Array.empty[Any]), dataType)) { emptyStringList =>
            // cuDF returns a zero-row list column when the entire input has no matches.
            // Restore the input row count before applying Spark's null-input semantics.
            if (extracted.getRowCount == 0) {
              ColumnVector.fromScalar(emptyStringList, rowCount.toInt)
            } else {
              val capturesWithEmptyStrings = withResource(extracted.getChildColumnView(0)) {
                captures =>
                  withResource(Scalar.fromString("")) { emptyString =>
                    withResource(captures.replaceNulls(emptyString)) { normalizedCaptures =>
                      withResource(extracted.replaceListChild(normalizedCaptures)) {
                        _.copyToColumnVector()
                      }
                    }
                  }
              }
              withResource(capturesWithEmptyStrings) { normalized =>
                withResource(normalized.isNull) { noMatchesOrNullInput =>
                  noMatchesOrNullInput.ifElse(emptyStringList, normalized)
                }
              }
            }
          }
          withResource(noMatchesAsEmptyLists) { normalized =>
            withResource(GpuScalar.from(null, dataType)) { nullStringList =>
              withResource(str.getBase.isNull) { isInputNull =>
                isInputNull.ifElse(nullStringList, normalized)
              }
            }
          }
        }
    }
  }
}

class SubstringIndexMeta(
    expr: SubstringIndex,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends TernaryExprMeta[SubstringIndex](expr, conf, parent, rule) {


  override def convertToGpu(
      column: Expression,
      delim: Expression,
      count: Expression): GpuExpression = GpuSubstringIndex(column, delim, count)
}


case class GpuSubstringIndex(strExpr: Expression,
    ignoredDelimExpr: Expression,
    ignoredCountExpr: Expression)
  extends GpuTernaryExpressionArgsAnyScalarScalar with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def first: Expression = strExpr
  override def second: Expression = ignoredDelimExpr
  override def third: Expression = ignoredCountExpr

  override def prettyName: String = "substring_index"

  override def doColumnar(str: GpuColumnVector, delim: GpuScalar,
      count: GpuScalar): ColumnVector = {
    if (delim.isValid && count.isValid) {
      GpuSubstringIndexUtils.substringIndex(str.getBase, delim.getBase,
          count.getValue.asInstanceOf[Int])
    } else {
      GpuColumnVector.columnVectorFromNull(str.getRowCount.toInt, StringType)
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }
}

trait BasePad
    extends GpuTernaryExpressionArgsAnyScalarScalar
        with ImplicitCastInputTypes
        with NullIntolerantShim {
  val str: Expression
  val len: Expression
  val pad: Expression
  val direction: PadSide

  override def first: Expression = str
  override def second: Expression = len
  override def third: Expression = pad

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def doColumnar(str: GpuColumnVector, len: GpuScalar, pad: GpuScalar): ColumnVector = {
    if (len.isValid && pad.isValid) {
      val l = math.max(0, len.getValue.asInstanceOf[Int])
      val padStr = if (pad.isValid) {
        pad.getValue.asInstanceOf[UTF8String].toString
      } else {
        null
      }
      withResource(str.getBase.pad(l, direction, padStr)) { padded =>
        padded.substring(0, l)
      }
    } else {
      GpuColumnVector.columnVectorFromNull(str.getRowCount.toInt, StringType)
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }
}

case class GpuStringLPad(str: Expression, len: Expression, pad: Expression)
  extends BasePad {
  val direction = PadSide.LEFT
  override def prettyName: String = "lpad"

  def this(str: Expression, len: Expression) = {
    this(str, len, GpuLiteral(" ", StringType))
  }
}

case class GpuStringRPad(str: Expression, len: Expression, pad: Expression)
  extends BasePad {
  val direction = PadSide.RIGHT
  override def prettyName: String = "rpad"

  def this(str: Expression, len: Expression) = {
    this(str, len, GpuLiteral(" ", StringType))
  }
}

abstract class StringSplitRegExpMeta[INPUT <: TernaryExpression](expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[INPUT](expr, conf, parent, rule) {
  import GpuOverrides._

  /**
   * Return the cudf transpiled regex pattern, and a boolean flag indicating whether the input
   * delimiter is really a regex patter or just a literal string.
   */
  def checkRegExp(delimExpr: Expression): Option[(String, Boolean)] = {
    var pattern: String = ""
    var isRegExp: Boolean = false

    val delim = extractLit(delimExpr)
    if (delim.isEmpty) {
      willNotWorkOnGpu("Only literal delimiter patterns are supported")
    } else {
      val utf8Str = delim.get.value.asInstanceOf[UTF8String]
      if (utf8Str == null) {
        willNotWorkOnGpu("Delimiter pattern is null")
      } else {
        if (utf8Str.numChars() == 0) {
          willNotWorkOnGpu("An empty delimiter pattern is not supported")
        }
        val transpiler = new CudfRegexTranspiler(RegexSplitMode)
        transpiler.transpileToSplittableString(utf8Str.toString) match {
          case Some(simplified) =>
            pattern = simplified
          case None =>
            try {
              val (transpiledAST, _) = transpiler.getTranspiledAST(utf8Str.toString, None, None)
              pattern = transpiledAST.toRegexString
              isRegExp = true
            } catch {
              case e: RegexUnsupportedException =>
                willNotWorkOnGpu(e.getMessage)
            }
        }
      }
    }

    Some((pattern, isRegExp))
  }

  def throwUncheckedDelimiterException() =
    throw new IllegalStateException("Delimiter expression has not been checked for regex pattern")
}

class GpuStringSplitMeta(
    expr: StringSplit,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends StringSplitRegExpMeta[StringSplit](expr, conf, parent, rule) {
  import GpuOverrides._

  private var pattern = ""
  private var isRegExp = false

  override def tagExprForGpu(): Unit = {
    checkRegExp(expr.regex) match {
      case Some((p, isRe)) =>
        pattern = p
        isRegExp = isRe
      case _ => throwUncheckedDelimiterException()
    }

    // if this is a valid regular expression, then we should check the configuration
    // whether to run this on the GPU
    if (isRegExp) {
      GpuRegExpUtils.tagForRegExpEnabled(this)
    }

    extractLit(expr.limit) match {
      case Some(Literal(_: Int, _)) =>
      case _ =>
        willNotWorkOnGpu("only literal limit is supported")
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      limit: Expression): GpuExpression = {
    GpuStringSplit(str, regexp, limit, pattern, isRegExp)
  }
}

case class GpuStringSplit(str: Expression, regex: Expression, limit: Expression,
    pattern: String, isRegExp: Boolean)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = ArrayType(StringType, containsNull = false)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = str
  override def second: Expression = regex
  override def third: Expression = limit

  override def prettyName: String = "split"

  override def doColumnar(str: GpuColumnVector, regex: GpuScalar,
      limit: GpuScalar): ColumnVector = {
    limit.getValue.asInstanceOf[Int] match {
      case 0 =>
        // Same as splitting as many times as possible
        if (isRegExp) {
          str.getBase.stringSplitRecord(new RegexProgram(pattern,
            EnumSet.of(RegexFlag.EXT_NEWLINE) ,CaptureGroups.NON_CAPTURE), -1)
        } else {
          str.getBase.stringSplitRecord(pattern, -1)
        }
      case 1 =>
        // Short circuit GPU and just return a list containing the original input string
        withResource(str.getBase.isNull) { isNull =>
          withResource(GpuScalar.from(null, dataType)) { nullStringList =>
            withResource(ColumnVector.makeList(str.getBase)) { list =>
              isNull.ifElse(nullStringList, list)
            }
          }
        }
      case n =>
        if (isRegExp) {
          str.getBase.stringSplitRecord(new RegexProgram(pattern,
            EnumSet.of(RegexFlag.EXT_NEWLINE) ,CaptureGroups.NON_CAPTURE), n)
        } else {
          str.getBase.stringSplitRecord(pattern, n)
        }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuColumnVector,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuScalar,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuColumnVector,
      limit: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuScalar,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")
}

class GpuStringToMapMeta(expr: StringToMap,
                         conf: RapidsConf,
                         parent: Option[RapidsMeta[_, _, _]],
                         rule: DataFromReplacementRule)
  extends StringSplitRegExpMeta[StringToMap](expr, conf, parent, rule) {

  private def checkFoldable(children: Seq[Expression]): Unit = {
    if (children.forall(_.foldable)) {
      willNotWorkOnGpu("result can be compile-time evaluated")
    }
  }

  private var pairDelimInfo: Option[(String, Boolean)] = None
  private var keyValueDelimInfo: Option[(String, Boolean)] = None

  override def tagExprForGpu(): Unit = {
    checkFoldable(expr.children)
    pairDelimInfo = checkRegExp(expr.pairDelim)
    keyValueDelimInfo = checkRegExp(expr.keyValueDelim)
  }

  override def convertToGpu(strExpr: Expression,
                            pairDelimExpr: Expression,
                            keyValueDelimExpr: Expression): GpuExpression = {
    val pairDelim: (String, Boolean) = pairDelimInfo.getOrElse(throwUncheckedDelimiterException())
    val keyValueDelim: (String, Boolean) = keyValueDelimInfo.getOrElse(
      throwUncheckedDelimiterException())

    GpuStringToMap(strExpr, pairDelimExpr, keyValueDelimExpr, pairDelim._1, pairDelim._2,
      keyValueDelim._1, keyValueDelim._2)
  }
}

case class GpuStringToMap(strExpr: Expression,
                          pairDelimExpr: Expression,
                          keyValueDelimExpr: Expression,
                          pairDelim: String, isPairDelimRegExp: Boolean,
                          keyValueDelim: String, isKeyValueDelimRegExp: Boolean)
  extends GpuExpression with ShimExpression with ExpectsInputTypes {
  override def dataType: MapType = MapType(StringType, StringType)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def prettyName: String = "str_to_map"
  override def children: Seq[Expression] = Seq(strExpr, pairDelimExpr, keyValueDelimExpr)
  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(strExpr.columnarEvalAny(batch)) {
      case strsCol: GpuColumnVector => toMap(strsCol)
      case str: GpuScalar =>
        withResource(GpuColumnVector.from(str, batch.numRows)) {
          strsCol => toMap(strsCol)
        }
      case v =>
        throw new UnsupportedOperationException(s"Unsupported data '($v: ${v.getClass} " +
          "for GpuStringToMap.")
    }
  }

  private def toMap(str: GpuColumnVector): GpuColumnVector = {
    // Firstly, split the input strings into lists of strings.
    val listsOfStrings = if (isPairDelimRegExp) {
      str.getBase.stringSplitRecord(new RegexProgram(pairDelim,
        EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE))
    } else {
      str.getBase.stringSplitRecord(pairDelim)
    }
    withResource(listsOfStrings) { listsOfStrings =>
      // Extract strings column from the output lists column.
      withResource(listsOfStrings.getChildColumnView(0)) { stringsCol =>
        // Split the key-value strings into pairs of strings of key-value (using limit = 2).
        val keysValuesTable = if (isKeyValueDelimRegExp) {
          stringsCol.stringSplit(new RegexProgram(keyValueDelim,
            EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE), 2)
        } else {
          stringsCol.stringSplit(keyValueDelim, 2)
        }
        withResource(keysValuesTable) { keysValuesTable =>

          def toMapFromValues(values: ColumnVector): GpuColumnVector = {
            // This code is safe, because the `keysValuesTable` always has at least one column
            // (guarantee by `cudf::strings::split` implementation).
            val keys = keysValuesTable.getColumn(0)

            // Zip the key-value pairs into structs.
            withResource(ColumnView.makeStructView(keys, values)) { structsCol =>
              // Make a lists column from the new structs column, which will have the same shape
              // as the previous lists of strings column.
              withResource(GpuListUtils.replaceListDataColumnAsView(listsOfStrings, structsCol)) {
                listsOfStructs =>
                  GpuCreateMap.createMapFromKeysValuesAsStructs(dataType, listsOfStructs)
              }
            }
          }

          // If the output from stringSplit has only one column (the map keys), we set all the
          // output values to nulls.
          if (keysValuesTable.getNumberOfColumns < 2) {
            withResource(GpuColumnVector.columnVectorFromNull(
              keysValuesTable.getRowCount.asInstanceOf[Int], StringType)) {
              allNulls => toMapFromValues(allNulls)
            }
          } else {
            toMapFromValues(keysValuesTable.getColumn(1))
          }
        }
      }
    }
  }
}

object GpuStringInstr {
  def optimizeContains(cmp: GpuExpression): GpuExpression = {
    cmp match {
      case GpuGreaterThan(GpuStringInstr(str, substr: GpuLiteral), GpuLiteral(0, _)) =>
        // instr(A, B) > 0 becomes contains(A, B)
        GpuContains(str, substr)
      case GpuGreaterThanOrEqual(GpuStringInstr(str, substr: GpuLiteral), GpuLiteral(1, _)) =>
        // instr(A, B) >= 1 becomes contains(A, B)
        GpuContains(str, substr)
      case GpuLessThan(GpuLiteral(0, _), GpuStringInstr(str, substr: GpuLiteral)) =>
        // 0 < instr(A, B) becomes contains(A, B)
        GpuContains(str, substr)
      case GpuLessThanOrEqual(GpuLiteral(1, _), GpuStringInstr(str, substr: GpuLiteral)) =>
        // 1 <= instr(A, B) becomes contains(A, B)
        GpuContains(str, substr)
      case _ =>
        cmp
    }
  }
}

case class GpuStringInstr(str: Expression, substr: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerantShim {
  // Locate the position of the first occurrence of substr column in the given string.
  // returns null if one of the arguments is null
  // returns zero if not found
  // return values are 1 based.
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def left: Expression = str
  override def right: Expression = substr

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(lhs.getBase.stringLocate(rhs.getBase)) { strLocateRes =>
      withResource(Scalar.fromInt(1)) { sv1 =>
        strLocateRes.add(sv1)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuConv(num: Expression, fromBase: Expression, toBase: Expression, ansiEnabled: Boolean)
  extends GpuTernaryExpression {

  override def nullable: Boolean = true

  override def doColumnar(
    strCv: GpuColumnVector,
    fromCv: GpuColumnVector,
    toCv: GpuColumnVector): ColumnVector = {
    if (ansiEnabled &&
      NumberConverter.isConvertOverflowCvCvCv(strCv.getBase, fromCv.getBase, toCv.getBase)) {
      ConvUtils.overflowInConvError()
    }
    NumberConverter.convertCvCvCv(strCv.getBase, fromCv.getBase, toCv.getBase)
  }

  override def doColumnar(
      strS: GpuScalar, fromCv: GpuColumnVector, toCv: GpuColumnVector): ColumnVector = {
    if (!strS.isValid) {
      GpuColumnVector.columnVectorFromNull(fromCv.getRowCount.toInt, dataType)
    } else {
      if (ansiEnabled &&
        NumberConverter.isConvertOverflowSCvCv(strS.getBase, fromCv.getBase, toCv.getBase)) {
        ConvUtils.overflowInConvError()
      }
      NumberConverter.convertSCvCv(strS.getBase, fromCv.getBase, toCv.getBase)
    }
  }

  override def doColumnar(
      strS: GpuScalar, fromS: GpuScalar, toCv: GpuColumnVector): ColumnVector = {

    if (!strS.isValid || !fromS.isValid) {
      GpuColumnVector.columnVectorFromNull(toCv.getRowCount.toInt, dataType)
    } else {
      val fromRadix = fromS.getValue.asInstanceOf[Int]
      if (ansiEnabled &&
      NumberConverter.isConvertOverflowSSCv (strS.getBase, fromRadix, toCv.getBase) ) {
        ConvUtils.overflowInConvError()
      }
      NumberConverter.convertSSCv(strS.getBase, fromRadix, toCv.getBase)
    }
  }

  override def doColumnar(
      strS: GpuScalar, fromCv: GpuColumnVector, toS: GpuScalar): ColumnVector = {
    if (!strS.isValid || !toS.isValid) {
      GpuColumnVector.columnVectorFromNull(fromCv.getRowCount.toInt, dataType)
    } else {
        val toRadix = toS.getValue.asInstanceOf[Int]
        if (ansiEnabled &&
          NumberConverter.isConvertOverflowSCvS(strS.getBase, fromCv.getBase, toRadix)) {
          ConvUtils.overflowInConvError()
        }
        NumberConverter.convertSCvS(strS.getBase, fromCv.getBase, toRadix)
    }
  }

  override def doColumnar(
      strCv: GpuColumnVector, fromS: GpuScalar, toCv: GpuColumnVector): ColumnVector = {
    if (!fromS.isValid) {
      GpuColumnVector.columnVectorFromNull(toCv.getRowCount.toInt, dataType)
    } else {
      val fromRadix = fromS.getValue.asInstanceOf[Int]
      if (ansiEnabled &&
        NumberConverter.isConvertOverflowCvSCv(strCv.getBase, fromRadix, toCv.getBase)) {
        ConvUtils.overflowInConvError()
      }
      NumberConverter.convertCvSCv(strCv.getBase, fromRadix, toCv.getBase)
    }
  }

  override def doColumnar(
      strCv: GpuColumnVector, fromCv: GpuColumnVector, toS: GpuScalar): ColumnVector = {
    if (!toS.isValid) {
      GpuColumnVector.columnVectorFromNull(fromCv.getRowCount.toInt, dataType)
    } else {
      val toRadix = toS.getValue.asInstanceOf[Int]
      if (ansiEnabled &&
        NumberConverter.isConvertOverflowCvCvS(strCv.getBase, fromCv.getBase, toRadix)) {
        ConvUtils.overflowInConvError()
      }
      NumberConverter.convertCvCvS(strCv.getBase, fromCv.getBase, toRadix)
    }
  }

  override def doColumnar(
    numRows: Int,
    strSr: GpuScalar,
    fromS: GpuScalar,
    toS: GpuScalar
  ): ColumnVector = {
    withResource(GpuColumnVector.from(toS, numRows)) { expandedToS =>
      doColumnar(strSr, fromS, expandedToS)
    }
  }

  override def doColumnar(
    strCv: GpuColumnVector,
    fromS: GpuScalar,
    toS: GpuScalar
  ): ColumnVector = {
    if (!fromS.isValid || !toS.isValid) {
      GpuColumnVector.columnVectorFromNull(strCv.getRowCount.toInt, dataType)
    } else {
      val fromRadix = fromS.getValue.asInstanceOf[Int]
      val toRadix = toS.getValue.asInstanceOf[Int]
      if (ansiEnabled &&
        NumberConverter.isConvertOverflowCvSS(strCv.getBase, fromRadix, toRadix)) {
        ConvUtils.overflowInConvError()
      }
      NumberConverter.convertCvSS(strCv.getBase, fromRadix, toRadix)
    }
  }

  override def first: Expression = num

  override def second: Expression = fromBase

  override def third: Expression = toBase

  override def dataType: DataType = StringType
}

case class GpuFormatNumber(x: Expression, d: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes with NullIntolerantShim {
  
  override def left: Expression = x
  override def right: Expression = d
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  private def removeNegSign(cv: ColumnVector): ColumnVector = {
    withResource(Scalar.fromString("-")) { negativeSign =>
      cv.lstrip(negativeSign)
    }
  }

  private def getPartsFromDecimal(cv: ColumnVector, d: Int, scale: Int): 
      (ColumnVector, ColumnVector) = {
    // prevent d too large to fit in decimalType
    val roundingScale = scale.min(d)
    // append zeros to the end of decPart, zerosNum = d - scale
    // if d <= scale, no need to append zeros, if scale < 0, append d zeros
    val appendZeroNum = (d - scale).max(0).min(d)
    val (intPart, decTemp) = if (roundingScale <= 0) {
      withResource(ArrayBuffer.empty[ColumnVector]) { resourceArray =>
        val intPart = withResource(Arithmetic.round(cv, roundingScale, 
          RoundMode.HALF_EVEN)) { rounded =>
          rounded.castTo(DType.STRING)
        }
        resourceArray += intPart
        // if intString starts with 0, it must be "00000...", replace it with "0"
        val (isZero, zeroCv) = withResource(Scalar.fromString("0")) { zero =>
          withResource(intPart.startsWith(zero)) { isZero =>
              (isZero.incRefCount(), ColumnVector.fromScalar(zero, cv.getRowCount.toInt))
          }
        }
        val intPartZeroHandled = withResource(isZero) { isZero =>
          withResource(zeroCv) { zeroCv =>
            isZero.ifElse(zeroCv, intPart)
          }
        }
        resourceArray += intPartZeroHandled
        // a temp decPart is empty before appending zeros
        val decPart = withResource(Scalar.fromString("")) { emptyString =>
          ColumnVector.fromScalar(emptyString, cv.getRowCount.toInt)
        }
        resourceArray += decPart
        (intPartZeroHandled.incRefCount(), decPart.incRefCount())
      }
    } else {
      withResource(Arithmetic.round(cv, roundingScale, RoundMode.HALF_EVEN)) { rounded =>
        withResource(rounded.castTo(DType.STRING)) { roundedStr =>
          withResource(roundedStr.stringSplit(".", 2)) { intAndDec =>
            (intAndDec.getColumn(0).incRefCount(), intAndDec.getColumn(1).incRefCount())
          }
        }
      }
    }
    closeOnExcept(ArrayBuffer.empty[ColumnVector]) { resourceArray =>
      // remove negative sign from intPart, sign will be handled later
      val intPartPos = closeOnExcept(decTemp) { _ =>
        withResource(intPart) { _ =>
          removeNegSign(intPart)
        }
      }
      resourceArray += intPartPos
      // append zeros
      val appendZeros = "0" * appendZeroNum
      val appendZerosCv = closeOnExcept(decTemp) { _ =>
        withResource(Scalar.fromString(appendZeros)) { zeroString =>
          ColumnVector.fromScalar(zeroString, cv.getRowCount.toInt)
        }
      }
      val decPart = withResource(decTemp) { _ =>
        withResource(appendZerosCv) { _ =>
          ColumnVector.stringConcatenate(Array(decTemp, appendZerosCv))
        }
      }
      (intPartPos, decPart)
    }
  }

  private def getParts(cv: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // get int part and dec part from a column vector, int part will be set to positive
    x.dataType match {
      case DecimalType.Fixed(_, scale) => {
        getPartsFromDecimal(cv, d, scale)
      }
      case IntegerType | LongType | ShortType | ByteType => {
        val intPartPos = withResource(cv.castTo(DType.STRING)) { intPart =>
          removeNegSign(intPart)
        }
        // dec part is all zeros
        val dzeros = "0" * d
        val decPart = closeOnExcept(intPartPos) { _ =>
          withResource(Scalar.fromString(dzeros)) { zeroString =>
            ColumnVector.fromScalar(zeroString, cv.getRowCount.toInt)
          }
        }
        (intPartPos, decPart)
      }
      case _ => {
        throw new UnsupportedOperationException(s"format_number doesn't support type ${x.dataType}")
      }
    }
  }

  private def removeExtraCommas(str: ColumnVector): ColumnVector = {
    withResource(Scalar.fromString(",")) { comma =>
      str.rstrip(comma)
    }
  }

  private def addCommas(str: ColumnVector): ColumnVector = {
    val maxstrlen = withResource(str.getCharLengths()) { strlen =>
      withResource(strlen.max()) { maxlen =>
        maxlen.isValid match {
          case true => maxlen.getInt
          case false => 0
        }
      }
    }
    if (maxstrlen <= 3) {
      // no commas are needed for strings of 3 or fewer chars
      str.incRefCount()
    } else {
      val substrs = (0 until maxstrlen by 3).safeMap { i =>
        str.substring(i, i + 3).asInstanceOf[ColumnView]
      }.toArray
      withResource(substrs) { _ =>
        // join the 3-char chunks with commas using a scalar separator
        withResource(Scalar.fromString(",")) { sep =>
          withResource(Scalar.fromString("")) { narep =>
            withResource(ColumnVector.stringConcatenate(sep, narep, substrs)) { res =>
              removeExtraCommas(res)
            }
          }
        }
      }
    }
  }

  private def formatNumberNonKernel(cv: ColumnVector, d: Int): ColumnVector = {
    val (integerPart, decimalPart) = getParts(cv, d)
    // reverse integer part for adding commas
    val integerWithCommas = closeOnExcept(decimalPart) { _ =>
      val reversed = withResource(integerPart) { _ =>
        integerPart.reverseStringsOrLists()
      }
      val reversedWithCommas = withResource(reversed) { _ =>
        addCommas(reversed)
      }
      withResource(reversedWithCommas) { _ =>
        reversedWithCommas.reverseStringsOrLists()
      }
    }
    // build a small per-row sign prefix column ("-" or "") based on the sign
    // of the value, that we will prepend at the end.
    // this way, we avoid creating bigger signed/unsigned versions of the formatted column
    // followed by an ifElse.
    val signCol = closeOnExcept(decimalPart) { _ =>
      closeOnExcept(integerWithCommas) { _ =>
        // since we only need the sign bit, cast to float and compare < 0.
        // this is cheaper than casting to string and checking for "-".
        val isNeg = withResource(cv.castTo(DType.FLOAT32)) { cvFloat =>
          withResource(Scalar.fromFloat(0.0f)) { zero =>
            cvFloat.lessThan(zero)
          }
        }
        withResource(isNeg) { _ =>
          withResource(Scalar.fromString("-")) { neg =>
            withResource(Scalar.fromString("")) { empty =>
              isNeg.ifElse(neg, empty)
            }
          }
        }
      }
    }
    // single concatenation pass for sign + integer [+ "." + decimal]
    val formatted = d match {
      case 0 =>
        decimalPart.close()
        // no decimal - just prepend the precomputed sign prefix
        withResource(signCol) { _ =>
          withResource(integerWithCommas) { _ =>
            ColumnVector.stringConcatenate(
              Array[ColumnView](signCol, integerWithCommas))
          }
        }
      case _ =>
        // join integer and decimal with scalar separator "."
        val intDotDec = closeOnExcept(signCol) { _ =>
          withResource(integerWithCommas) { _ =>
            withResource(decimalPart) { _ =>
              withResource(Scalar.fromString(".")) { dot =>
                withResource(Scalar.fromString("")) { narep =>
                  ColumnVector.stringConcatenate(dot, narep,
                    Array[ColumnView](integerWithCommas, decimalPart))
                }
              }
            }
          }
        }
        // prepend the precomputed sign prefix to the formatted number
        withResource(signCol) { _ =>
          withResource(intDotDec) { _ =>
            ColumnVector.stringConcatenate(
              Array[ColumnView](signCol, intDotDec))
          }
        }
    }
    // handle null case
    val anyNull = closeOnExcept(formatted) { _ =>
      cv.getNullCount > 0
    }
    anyNull match {
      case true =>
        withResource(formatted) { _ =>
          withResource(cv.isNull) { isNull =>
            withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
              isNull.ifElse(nullScalar, formatted)
            }
          }
        }
      case false => formatted
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // get int d from rhs
    if (!rhs.isValid || rhs.getValue.asInstanceOf[Int] < 0) {
      return GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, StringType)
    }
    val d = rhs.getValue.asInstanceOf[Int]
    x.dataType match {
      case FloatType | DoubleType => {
        val nanSymbol = DecimalFormatSymbols.getInstance(Locale.US).getNaN
        // JDK 8's nan symbol is "�" ('\uFFFD'), which is also the jni kernel's nan symbol
        // In higher JDK version, nan symbol is "NaN", so we need to handle it here.
        if (nanSymbol == String.valueOf('\uFFFD')) {
          CastStrings.fromFloatWithFormat(lhs.getBase, d)
        } else {
          withResource(Scalar.fromString(nanSymbol)) { nan =>
            withResource(lhs.getBase.isNan) { isNan =>
              withResource(CastStrings.fromFloatWithFormat(lhs.getBase, d)) { res =>
                isNan.ifElse(nan, res)
              }
            }
          }
        }
      }
      case _ => {
        formatNumberNonKernel(lhs.getBase, d) 
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { col =>
      doColumnar(col, rhs)
    }
  }
}

case class GpuStringDecode(
    bin: Expression,
    charsetName: String,
    reportMalformed: Boolean = false)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def child: Expression = bin

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val charsetId = charsetName match {
      case "GBK" => CharsetDecode.GBK
      case other =>
        throw new UnsupportedOperationException(s"Unsupported charset on GPU: $other")
    }
    val errorAction =
      if (reportMalformed) CharsetDecode.REPORT else CharsetDecode.REPLACE
    try {
      CharsetDecode.decode(input.getBase, charsetId, errorAction)
    } catch {
      case _: CharsetDecode.MalformedInputException =>
        // MALFORMED_CHARACTER_CODING was introduced in Spark 4.0 alongside the
        // legacyCodingErrorAction flag, which is the only code path that can set
        // reportMalformed=true. Invoke QueryExecutionErrors.malformedCharacterCoding via
        // reflection so this file still compiles against Spark 3.x shims.
        throw GpuStringDecode.raiseMalformedCharacterCoding(charsetName)
    }
  }
}

object GpuStringDecode {
  private def raiseMalformedCharacterCoding(charset: String): RuntimeException = {
    val cls    = Class.forName("org.apache.spark.sql.errors.QueryExecutionErrors")
    val method = cls.getMethod("malformedCharacterCoding", classOf[String], classOf[String])
    method.invoke(null, "decode", charset).asInstanceOf[RuntimeException]
  }
}
