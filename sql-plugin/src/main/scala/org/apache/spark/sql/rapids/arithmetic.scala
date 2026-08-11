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

import java.math.BigInteger

import ai.rapids.cudf._
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{Arithmetic, ExceptionWithRowIndex}
import com.nvidia.spark.rapids.shims.{DecimalMultiply128, GpuTypeShims, NullIntolerantShim, ShimExpression, SparkShimImpl}

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object AddOverflowChecks {
  def basicOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector,
      origin: Origin = CurrentOrigin.get,
      mask: Option[ColumnVector] = None): Unit = {
    // Check overflow. It is true if the arguments have different signs and
    // the sign of the result is different from the sign of x.
    // Which is equal to "((x ^ r) & (y ^ r)) < 0" in the form of arithmetic.
    val signCV = withResource(ret.bitXor(lhs)) { lXor =>
      withResource(ret.bitXor(rhs)) { rXor =>
        lXor.bitAnd(rXor)
      }
    }
    val signDiffCV = withResource(signCV) { sign =>
      withResource(Scalar.fromInt(0)) { zero =>
        sign.lessThan(zero)
      }
    }
    withResource(signDiffCV) { tmpSignDiff =>
      val signDiff = if (mask.isDefined) {
        // If a mask is passed in we only want to look for overflow within the mask
        mask.get.and(tmpSignDiff)
      } else {
        tmpSignDiff.incRefCount()
      }
      withResource(signDiff) { signDiff =>
        withResource(signDiff.any()) { any =>
          if (any.isValid && any.getBoolean) {
            throw RapidsErrorUtils.arithmeticOverflowError(
              "One or more rows overflow for Add operation.",
              origin)
          }
        }
      }
    }
  }

  def didDecimalOverflow(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector): ColumnVector = {
    // We need a special overflow check for decimal because CUDF does not support INT128 so we
    // cannot reuse the same code for the other types.
    // Overflow happens if the arguments have the same signs and it is different from the sign of
    // the result
    val numRows = ret.getRowCount.toInt
    val zero = BigDecimal(0).bigDecimal
    withResource(DecimalUtils.lessThan(rhs, zero, numRows)) { rhsLz =>
      val argsSignSame = withResource(DecimalUtils.lessThan(lhs, zero, numRows)) { lhsLz =>
        lhsLz.equalTo(rhsLz)
      }
      withResource(argsSignSame) { argsSignSame =>
        val resultAndRhsDifferentSign =
          withResource(DecimalUtils.lessThan(ret, zero)) { resultLz =>
            rhsLz.notEqualTo(resultLz)
          }
        withResource(resultAndRhsDifferentSign) { resultAndRhsDifferentSign =>
          resultAndRhsDifferentSign.and(argsSignSame)
        }
      }
    }
  }

  def decimalOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector,
      failOnError: Boolean,
      origin: Origin = CurrentOrigin.get): ColumnVector = {
    withResource(didDecimalOverflow(lhs, rhs, ret)) { overflow =>
      if (failOnError) {
        withResource(overflow.any()) { any =>
          if (any.isValid && any.getBoolean) {
            throw RapidsErrorUtils.arithmeticOverflowError(
              "One or more rows overflow for Add operation.", origin)
          }
        }
        ret.incRefCount()
      } else {
        withResource(Scalar.fromNull(ret.getType)) { nullVal =>
          overflow.ifElse(nullVal, ret)
        }
      }
    }
  }
}

object SubtractOverflowChecks {
  def basicOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector,
      origin: Origin = CurrentOrigin.get): Unit = {
    // Check overflow. It is true if the arguments have different signs and
    // the sign of the result is different from the sign of x.
    // Which is equal to "((x ^ y) & (x ^ r)) < 0" in the form of arithmetic.
    val signCV = withResource(lhs.bitXor(rhs)) { xyXor =>
      withResource(lhs.bitXor(ret)) { xrXor =>
        xyXor.bitAnd(xrXor)
      }
    }
    val signDiffCV = withResource(signCV) { sign =>
      withResource(Scalar.fromInt(0)) { zero =>
        sign.lessThan(zero)
      }
    }
    withResource(signDiffCV) { signDiff =>
      withResource(signDiff.any()) { any =>
        if (any.isValid && any.getBoolean) {
          throw RapidsErrorUtils.arithmeticOverflowError(
            "One or more rows overflow for Subtract operation.", origin)
        }
      }
    }
  }
}

object GpuAnsi {
  def needBasicOpOverflowCheck(dt: DataType): Boolean =
    dt.isInstanceOf[IntegralType]

  def minValueScalar(dt: DataType): Scalar = dt match {
    case ByteType => Scalar.fromByte(Byte.MinValue)
    case ShortType => Scalar.fromShort(Short.MinValue)
    case IntegerType => Scalar.fromInt(Int.MinValue)
    case LongType => Scalar.fromLong(Long.MinValue)
    case other =>
      throw new IllegalArgumentException(s"$other does not need an ANSI check for this operator")
  }

  def assertMinValueOverflow(cv: GpuColumnVector, op: String, origin: Origin): Unit = {
    withResource(minValueScalar(cv.dataType())) { minVal =>
      assertMinValueOverflow(minVal, cv, op, origin)
    }
  }

  def assertMinValueOverflow(cv: GpuColumnVector, op: String): Unit = {
    assertMinValueOverflow(cv, op, CurrentOrigin.get)
  }

  def assertMinValueOverflow(minVal: Scalar, cv: GpuColumnVector, op: String): Unit = {
    assertMinValueOverflow(minVal, cv, op, CurrentOrigin.get)
  }

  def assertMinValueOverflow(minVal: Scalar,
      cv: GpuColumnVector, op: String, origin: Origin): Unit = {
    withResource(cv.getBase.equalToNullAware(minVal)) { isMinVal =>
      if (BoolUtils.isAnyValidTrue(isMinVal)) {
        throw RapidsErrorUtils.arithmeticOverflowError(
          s"One or more rows overflow for $op operation", origin)
      }
    }
  }
}

case class GpuUnaryMinus(child: Expression, failOnError: Boolean) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerantShim {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  override def sql: String = s"(- ${child.sql})"

  override def hasSideEffects: Boolean = super.hasSideEffects ||
    (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType))

  override def doColumnar(input: GpuColumnVector) : ColumnVector = {
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Because of 2s compliment we need to only worry about the min value for integer types.
      GpuAnsi.assertMinValueOverflow(input, "minus", origin)
    }

    def commonMinus(input: GpuColumnVector): ColumnVector = {
      withResource(Scalar.fromByte(0.toByte)) { scalar =>
        scalar.sub(input.getBase)
      }
    }

    dataType match {
      case dt: DecimalType =>
        val zeroLit = Decimal(0L, dt.precision, dt.scale)
        withResource(GpuScalar.from(zeroLit, dt)) { scalar =>
          scalar.sub(input.getBase)
        }
      case t if GpuTypeShims.isSupportedDayTimeType(t) =>
        // For day-time interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        withResource(Scalar.fromLong(Long.MinValue)) { minVal =>
          GpuAnsi.assertMinValueOverflow(minVal, input, "minus", origin)
        }
        commonMinus(input)
      case t if GpuTypeShims.isSupportedYearMonthType(t) =>
        // For year-month interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        withResource(Scalar.fromInt(Int.MinValue)) { minVal =>
          GpuAnsi.assertMinValueOverflow(minVal, input, "minus", origin)
        }
        commonMinus(input)
      case _ =>
        commonMinus(input)
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    val literalZero = dataType match {
      case LongType => ast.Literal.ofLong(0)
      case FloatType => ast.Literal.ofFloat(0)
      case DoubleType => ast.Literal.ofDouble(0)
      case IntegerType => ast.Literal.ofInt(0)
    }
    new ast.BinaryOperation(ast.BinaryOperator.SUB, literalZero,
      child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns))
  }
}

case class GpuUnaryPositive(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerantShim {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : ColumnVector = input.getBase.incRefCount()

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)
  }
}

case class GpuAbs(child: Expression, failOnError: Boolean) extends CudfUnaryExpression
    with ExpectsInputTypes with NullIntolerantShim {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def unaryOp: UnaryOp = UnaryOp.ABS

  override def hasSideEffects: Boolean = super.hasSideEffects ||
    (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType))

  override def doColumnar(input: GpuColumnVector) : ColumnVector = {
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Because of 2s compliment we need to only worry about the min value for integer types.
      GpuAnsi.assertMinValueOverflow(input, "abs", origin)
    }

    if (GpuTypeShims.isSupportedDayTimeType(dataType)) {
      // For day-time interval, Spark throws an exception when overflow,
      // regardless of whether `SQLConf.get.ansiEnabled` is true or false
      withResource(Scalar.fromLong(Long.MinValue)) { minVal =>
        GpuAnsi.assertMinValueOverflow(minVal, input, "abs", origin)
      }
    } else if (GpuTypeShims.isSupportedYearMonthType(dataType)) {
      // For year-month interval, Spark throws an exception when overflow,
      // regardless of whether `SQLConf.get.ansiEnabled` is true or false
      withResource(Scalar.fromInt(Int.MinValue)) { minVal =>
        GpuAnsi.assertMinValueOverflow(minVal, input, "abs", origin)
      }
    }

    super.doColumnar(input)
  }
}

abstract class GpuAddBase extends CudfBinaryArithmetic with Serializable {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.ADD)

  override def hasSideEffects: Boolean =
    (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) || super.hasSideEffects

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    withResource(ret) { ret =>
      // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
      if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType) ||
          GpuTypeShims.isSupportedDayTimeType(dataType) ||
          GpuTypeShims.isSupportedYearMonthType(dataType)) {
        // For day time interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        AddOverflowChecks.basicOpOverflowCheck(lhs, rhs, ret, origin)
      }

      if (dataType.isInstanceOf[DecimalType]) {
        AddOverflowChecks.decimalOpOverflowCheck(lhs, rhs, ret, failOnError, origin)
      } else {
        ret.incRefCount()
      }
    }
  }
}

abstract class GpuSubtractBase extends CudfBinaryArithmetic with Serializable {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.SUB)

  private[this] def decimalOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector): ColumnVector = {
    // We need a special overflow check for decimal because CUDF does not support INT128 so we
    // cannot reuse the same code for the other types.
    // Overflow happens if the arguments have different signs and the sign of the result is
    // different from the sign of subtractend (RHS).
    val numRows = ret.getRowCount.toInt
    val zero = BigDecimal(0).bigDecimal
    val overflow = withResource(DecimalUtils.lessThan(rhs, zero, numRows)) { rhsLz =>
      val argsSignDifferent = withResource(DecimalUtils.lessThan(lhs, zero, numRows)) { lhsLz =>
        lhsLz.notEqualTo(rhsLz)
      }
      withResource(argsSignDifferent) { argsSignDifferent =>
        val resultAndSubtrahendSameSign =
          withResource(DecimalUtils.lessThan(ret, zero)) { resultLz =>
            rhsLz.equalTo(resultLz)
          }
        withResource(resultAndSubtrahendSameSign) { ss =>
          ss.and(argsSignDifferent)
        }
      }
    }
    withResource(overflow) { overflow =>
      if (failOnError) {
        withResource(overflow.any()) { any =>
          if (any.isValid && any.getBoolean) {
            throw RapidsErrorUtils.arithmeticOverflowError(
              "One or more rows overflow for Subtract operation.", origin)
          }
        }
        ret.incRefCount()
      } else {
        withResource(GpuScalar.from(null, dataType)) { nullVal =>
          overflow.ifElse(nullVal, ret)
        }
      }
    }
  }

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    withResource(ret) { ret =>
      // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
      if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType) ||
          GpuTypeShims.isSupportedDayTimeType(dataType) ||
          GpuTypeShims.isSupportedYearMonthType(dataType)) {
        // For day time interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        SubtractOverflowChecks.basicOpOverflowCheck(lhs, rhs, ret, origin)
      }

      if (dataType.isInstanceOf[DecimalType]) {
        decimalOpOverflowCheck(lhs, rhs, ret)
      } else {
        ret.incRefCount()
      }
    }
  }
}

trait GpuDecimalMultiplyBase extends GpuExpression {

  def dataType: DecimalType
  def failOnError: Boolean
  def left: Expression
  def right: Expression
  def useLongMultiply: Boolean

  override def toString: String = s"($left * $right)"

  override def sql: String = s"(${left.sql} * ${right.sql})"

  private[this] lazy val lhsType: DecimalType = DecimalUtil.asDecimalType(left.dataType)
  private[this] lazy val rhsType: DecimalType = DecimalUtil.asDecimalType(right.dataType)
  private[this] lazy val (intermediateLhsType, intermediateRhsType) =
    DecimalMultiplyChecks.intermediateLhsRhsTypes(lhsType, rhsType, dataType)
  private[this] lazy val intermediateResultType =
    DecimalMultiplyChecks.intermediateResultType(lhsType, rhsType, dataType)

  def regularMultiply(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      GpuCast.doCast(
        lhs.getBase,
        lhs.dataType(),
        intermediateLhsType,
        CastOptions.getArithmeticCastOptions(failOnError))
    }
    val ret = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        GpuCast.doCast(rhs.getBase, rhs.dataType(), intermediateRhsType,
          CastOptions.getArithmeticCastOptions(failOnError))
      }
      withResource(castRhs) { castRhs =>
        withResource(castLhs.mul(castRhs,
          GpuColumnVector.getNonNestedRapidsType(intermediateResultType))) { mult =>
          if (useLongMultiply) {
            withResource(DecimalMultiplyChecks
                .checkForOverflow(castLhs, castRhs)) { wouldOverflow =>
              if (failOnError) {
                withResource(wouldOverflow.any()) { anyOverflow =>
                  if (anyOverflow.isValid && anyOverflow.getBoolean) {
                    throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
                  }
                }
                mult.incRefCount()
              } else {
                withResource(GpuScalar.from(null, intermediateResultType)) { nullVal =>
                  wouldOverflow.ifElse(nullVal, mult)
                }
              }
            }
          } else {
            mult.incRefCount()
          }
        }
      }
    }
    withResource(ret) { ret =>
      GpuColumnVector.from(GpuCast.doCast(ret, intermediateResultType, dataType,
        CastOptions.getArithmeticCastOptions(failOnError)),
        dataType)
    }
  }

  def longMultiply(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      lhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, lhs.getBase.getType.getScale))
    }
    val retTab = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        rhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, rhs.getBase.getType.getScale))
      }
      withResource(castRhs) { castRhs =>
        DecimalMultiply128(castLhs, castRhs, -dataType.scale)
      }
    }
    val retCol = withResource(retTab) { retTab =>
      if (failOnError) {
        withResource(retTab.getColumn(0).any()) { anyOverflow =>
          if (anyOverflow.isValid && anyOverflow.getBoolean) {
            throw new ArithmeticException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
        retTab.getColumn(1).incRefCount()
      } else {
        withResource(GpuScalar.from(null, dataType)) { nullVal =>
          retTab.getColumn(0).ifElse(nullVal, retTab.getColumn(1))
        }
      }
    }
    GpuColumnVector.from(retCol, dataType)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (useLongMultiply) {
      longMultiply(batch)
    } else {
      regularMultiply(batch)
    }
  }
}

object DecimalMultiplyChecks {
  // For Spark the final desired output is
  // new_scale = lhs.scale + rhs.scale
  // new_precision = lhs.precision + rhs.precision + 1
  // But Spark will round the final result, so we need at least one more
  // decimal place on the scale to be able to do the rounding too.

  // In CUDF the output scale is the same lhs.scale + rhs.scale, but because we need one more
  // we will need to increase the scale for either the lhs or the rhs so it works. We will pick
  // the one with the smallest precision to do it, because it minimises the chance of requiring a
  // larger data type to do the multiply.

  /**
   * Get the scales that are needed for the lhs and rhs to produce the desired result.
   */
  def lhsRhsNeededScales(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): (Int, Int) = {
    val cudfIntermediateScale = lhs.scale + rhs.scale
    val requiredIntermediateScale = outputType.scale + 1
    if (requiredIntermediateScale > cudfIntermediateScale) {
      // In practice this should only ever be 1, but just to be cautious...
      val neededScaleDiff = requiredIntermediateScale - cudfIntermediateScale
      // So we need to add some to the LHS and some to the RHS.
      var addToLhs = 0
      var addToRhs = 0
      // We start by trying
      // to bring them both to the same precision.
      val precisionDiff = lhs.precision - rhs.precision
      if (precisionDiff > 0) {
        addToRhs = math.min(precisionDiff, neededScaleDiff)
      } else {
        addToLhs = math.min(math.abs(precisionDiff), neededScaleDiff)
      }
      val stillNeeded = neededScaleDiff - (addToLhs + addToRhs)
      if (stillNeeded > 0) {
        // We need to split it between the two
        val l = stillNeeded/2
        val r = stillNeeded - l
        addToLhs += l
        addToRhs += r
      }
      (lhs.scale + addToLhs, rhs.scale + addToRhs)
    } else {
      (lhs.scale, rhs.scale)
    }
  }

  def nonRoundedIntermediatePrecision(
      l: DecimalType,
      r: DecimalType,
      outputType: DecimalType): Int = {
    // CUDF ignores the precision, except for the underlying device type, so in general we
    // need to find the largest precision needed between the LHS, RHS, and intermediate output
    // In practice this should probably always be outputType.precision + 1, but just to be
    // cautions we calculate it all out.
    val (lhsScale, rhsScale) = lhsRhsNeededScales(l, r, outputType)
    val lhsPrecision = l.precision - l.scale + lhsScale
    val rhsPrecision = r.precision - r.scale + rhsScale
    // we add 1 to the output precision so we can round the final result to match Spark
    math.max(math.max(lhsPrecision, rhsPrecision), outputType.precision + 1)
  }

  def intermediatePrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int =
    math.min(
      nonRoundedIntermediatePrecision(lhs, rhs, outputType),
      DType.DECIMAL128_MAX_PRECISION)

  def intermediateLhsRhsTypes(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): (DecimalType, DecimalType) = {
    val precision = intermediatePrecision(lhs, rhs, outputType)
    val (lhsScale, rhsScale) = lhsRhsNeededScales(lhs, rhs, outputType)
    (DecimalType(precision, lhsScale), DecimalType(precision, rhsScale))
  }

  def intermediateResultType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediatePrecision(lhs, rhs, outputType)
    DecimalType(precision,
      math.min(outputType.scale + 1, DType.DECIMAL128_MAX_PRECISION))
  }

  private[this] lazy val max128Int = new BigInteger(Array(2.toByte)).pow(127)
      .subtract(BigInteger.ONE)
  private[this] lazy val min128Int = new BigInteger(Array(2.toByte)).pow(127)
      .negate()

  def checkForOverflow(a: ColumnView, b: ColumnView): ColumnVector = {
    assert(a.getType.isDecimalType)
    assert(b.getType.isDecimalType)
    // a > MAX_INT / b || a < MIN_INT / b
    // So to do this we need the unscaled value, but we have to get it in terms of a
    // DECIMAL_128 with a scale of 0
    withResource(a.bitCastTo(DType.create(DType.DTypeEnum.DECIMAL128, 0))) { castA =>
      withResource(b.bitCastTo(DType.create(DType.DTypeEnum.DECIMAL128, 0))) { castB =>
        val isNotZero = withResource(Scalar.fromDecimal(0, BigInteger.ZERO)) { zero =>
          castB.notEqualTo(zero)
        }
        withResource(isNotZero) { isNotZero =>
          val gt = withResource(Scalar.fromDecimal(0, max128Int)) { maxDecimal =>
            withResource(maxDecimal.div(castB)) { divided =>
              castA.greaterThan(divided)
            }
          }
          withResource(gt) { gt =>
            val lt = withResource(Scalar.fromDecimal(0, min128Int)) { minDecimal =>
              withResource(minDecimal.div(castB)) { divided =>
                castA.lessThan(divided)
              }
            }
            withResource(lt) { lt =>
              withResource(lt.or(gt)) { ored =>
                ored.and(isNotZero)
              }
            }
          }
        }
      }
    }
  }
}

object GpuDivModLike {
  def replaceZeroWithNull(v: ColumnVector): ColumnVector = {
    var zeroScalar: Scalar = null
    var nullScalar: Scalar = null
    var zeroVec: ColumnVector = null
    var nullVec: ColumnVector = null
    try {
      val dtype = v.getType
      zeroScalar = makeZeroScalar(dtype)
      nullScalar = Scalar.fromNull(dtype)
      zeroVec = ColumnVector.fromScalar(zeroScalar, 1)
      nullVec = ColumnVector.fromScalar(nullScalar, 1)
      v.findAndReplaceAll(zeroVec, nullVec)
    } finally {
      if (zeroScalar != null) {
        zeroScalar.close()
      }
      if (nullScalar != null) {
        nullScalar.close()
      }
      if (zeroVec != null) {
        zeroVec.close()
      }
      if (nullVec != null) {
        nullVec.close()
      }
    }
  }

  def isScalarZero(s: Scalar): Boolean = {
    s.getType match {
      case DType.INT8 => s.getByte == 0
      case DType.INT16 => s.getShort == 0
      case DType.INT32 => s.getInt == 0
      case DType.INT64 => s.getLong == 0
      case DType.FLOAT32 => s.getFloat == 0f
      case DType.FLOAT64 => s.getDouble == 0
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL128 =>
        s.getBigDecimal.toBigInteger.equals(BigInteger.ZERO)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL64 => s.getLong == 0
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL32 => s.getInt == 0
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }

  def makeZeroScalar(dtype: DType): Scalar = {
    dtype match {
      case DType.INT8 => Scalar.fromByte(0.toByte)
      case DType.INT16 => Scalar.fromShort(0.toShort)
      case DType.INT32 => Scalar.fromInt(0)
      case DType.INT64 => Scalar.fromLong(0L)
      case DType.FLOAT32 => Scalar.fromFloat(0f)
      case DType.FLOAT64 => Scalar.fromDouble(0)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL128 =>
        Scalar.fromDecimal(d.getScale, BigInteger.ZERO)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL64 =>
        Scalar.fromDecimal(d.getScale, 0L)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL32 =>
        Scalar.fromDecimal(d.getScale, 0)
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }

  /**
   * This is for the case as below.
   *
   *   left : [1,  2,  Long.MinValue,  3, Long.MinValue]
   *   right: [2, -1,             -1, -1,             6]
   *
   * The 3rd row (Long.MinValue, -1) will cause an overflow of the integral division.
   */
  def isDivOverflow(left: GpuColumnVector, right: GpuColumnVector): Boolean = {
    left.dataType() match {
      case LongType =>
        val overFlowVector = withResource(Seq(Long.MinValue, -1).safeMap(Scalar.fromLong)) {
          case Seq(minLong, minusOne) =>
            withResource(left.getBase.equalTo(minLong)) { eqToMinLong =>
              withResource(right.getBase.equalTo(minusOne)) {
                eqToMinLong.and
              }
            }
        }
        val overFlowVectorAny = withResource(overFlowVector) {
          _.any()
        }
        withResource(overFlowVectorAny) { isOverFlow =>
          isOverFlow.isValid && isOverFlow.getBoolean
        }
      case _ => false
    }
  }

  def isDivOverflow(left: GpuColumnVector, right: GpuScalar): Boolean = {
    left.dataType() match {
      case LongType =>
        (right.isValid && right.getValue == -1) && {
          withResource(Scalar.fromLong(Long.MinValue)) { minLong =>
            left.getBase.contains(minLong)
          }
        }
      case _ => false
    }
  }

  def isDivOverflow(left: GpuScalar, right: GpuColumnVector): Boolean = {
    (left.isValid && left.getValue == Long.MinValue) && {
      withResource(Scalar.fromInt(-1)) { minusOne =>
        right.getBase.contains(minusOne)
      }
    }
  }
}

case class GpuMultiply(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)(
    override val origin: Origin = CurrentOrigin.get)
    extends CudfBinaryArithmetic {
  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  assert(!left.dataType.isInstanceOf[DecimalType],
    "DecimalType multiplies need to be handled by GpuDecimalMultiply")

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  override def binaryOp: BinaryOp = BinaryOp.MUL
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.MUL)

  private def multiplyOverflowError(msg: String): ArithmeticException = {
    RapidsErrorUtils.arithmeticOverflowError(msg, origin)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    try {
      Arithmetic.multiply(lhs.getBase, rhs.getBase, /* ansi */ failOnError, /* try_mode */ false)
    } catch {
      case rowException: ExceptionWithRowIndex =>
        val errorRowIndex = rowException.getRowIndex
        val leftValue = ColumnViewUtils.getElementStringFromColumnView(lhs.getBase, errorRowIndex)
        val rightValue = ColumnViewUtils.getElementStringFromColumnView(rhs.getBase, errorRowIndex)
        throw multiplyOverflowError(
          s"Multiplication failed in ANSI mode: $leftValue * $rightValue")
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    try {
      Arithmetic.multiply(lhs.getBase, rhs.getBase, /* ansi */ failOnError, /* try_mode */ false)
    } catch {
      case rowException: ExceptionWithRowIndex =>
        val errorRowIndex = rowException.getRowIndex
        val rightValue = ColumnViewUtils.getElementStringFromColumnView(rhs.getBase, errorRowIndex)
        throw multiplyOverflowError(
          s"Multiplication failed in ANSI mode: ${lhs.getBase} * $rightValue")
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    try {
      Arithmetic.multiply(lhs.getBase, rhs.getBase, /* ansi */ failOnError, /* try_mode */ false)
    } catch {
      case rowException: ExceptionWithRowIndex =>
        val errorRowIndex = rowException.getRowIndex
        val leftValue = ColumnViewUtils.getElementStringFromColumnView(lhs.getBase, errorRowIndex)
        throw multiplyOverflowError(
          s"Multiplication failed in ANSI mode: $leftValue * ${rhs.getBase}")
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    if (!lhs.isValid || !rhs.isValid) {
      GpuColumnVector.columnVectorFromNull(numRows, lhs.dataType)
    } else {
      withResource(GpuColumnVector.from(lhs, numRows)) { lhs_cv =>
        doColumnar(lhs_cv, rhs)
      }
    }
  }
}

trait GpuDivModLike extends CudfBinaryArithmetic {

  override def nullable: Boolean = true

  // Whether we should check overflow or not in ANSI mode.
  protected def checkDivideOverflow: Boolean = false

  import GpuDivModLike._

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    if (failOnError) {
      // Throw out a "divByZeroError" only when the row in "rhs" is zero and the
      // corresponding row in "lhs" is not null. So need to merge nulls first.
      withResource(NullUtilities.mergeNulls(rhs.getBase, lhs.getBase)) { nullMergedRhs =>
        withResource(makeZeroScalar(rhs.getBase.getType)) { zeroScalar =>
          if (nullMergedRhs.contains(zeroScalar)) {
            throw RapidsErrorUtils.divByZeroError(origin)
          }
        }
      }
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        throw RapidsErrorUtils.divOverflowError(origin)
      }
      super.doColumnar(lhs, rhs)
    } else {
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        throw RapidsErrorUtils.divOverflowError(origin)
      }
      withResource(replaceZeroWithNull(rhs.getBase)) { replaced =>
        super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if (failOnError) {
      if (lhs.isValid) {
        withResource(makeZeroScalar(rhs.getBase.getType)) { zeroScalar =>
          if (rhs.getBase.contains(zeroScalar)) {
            throw RapidsErrorUtils.divByZeroError(origin)
          }
        }
      }
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        throw RapidsErrorUtils.divOverflowError(origin)
      }
      super.doColumnar(lhs, rhs)
    } else {
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        throw RapidsErrorUtils.divOverflowError(origin)
      }
      withResource(replaceZeroWithNull(rhs.getBase)) { replaced =>
        super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (isScalarZero(rhs.getBase)) {
      if (failOnError && lhs.numNulls() != lhs.getRowCount) {
        throw RapidsErrorUtils.divByZeroError(origin)
      } else {
        withResource(Scalar.fromNull(outputType(lhs.getBase, rhs.getBase))) { nullScalar =>
          ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt)
        }
      }
    } else {
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        throw RapidsErrorUtils.divOverflowError(origin)
      }
      super.doColumnar(lhs, rhs)
    }
  }
}

/**
 * A version of Divide specifically for DecimalType that does not force the left and right to be
 * the same type. This lets us calculate the correct result on a wider range of values without
 * the need for unbounded precision in the processing.
 */
trait GpuDecimalDivideBase extends GpuExpression {
  def dataType: DataType
  def left: Expression
  def right: Expression
  def failOnError: Boolean
  def failOnDivideByZero: Boolean
  def integerDivide: Boolean

  // For all decimal128 output we will use the long division version.
  protected lazy val useLongDivision: Boolean = decimalType.precision > Decimal.MAX_LONG_DIGITS

  override def toString: String = s"($left / $right)"

  override def sql: String = s"(${left.sql} / ${right.sql})"

  def decimalType: DecimalType = dataType match {
    case DecimalType.Fixed(_, _) => dataType.asInstanceOf[DecimalType]
    case LongType => DecimalType.LongDecimal
  }

  private[this] lazy val lhsType: DecimalType = DecimalUtil.asDecimalType(left.dataType)
  private[this] lazy val rhsType: DecimalType = DecimalUtil.asDecimalType(right.dataType)
  // This is the type that the LHS will be cast to. The precision will match the precision of
  // the intermediate rhs (to make CUDF happy doing the divide), but the scale will be shifted
  // enough so CUDF produces the desired output scale
  private[this] lazy val intermediateLhsType =
    DecimalDivideChecks.intermediateLhsType(lhsType, rhsType, decimalType)
  // This is the type that the RHS will be cast to. The precision will match the precision of the
  // intermediate lhs (to make CUDF happy doing the divide), but the scale will be the same
  // as the input RHS scale.
  private[this] lazy val intermediateRhsType =
    DecimalDivideChecks.intermediateRhsType(lhsType, rhsType, decimalType)

  // This is the data type that CUDF will return as the output of the divide. It should be
  // very close to outputType, but with the scale increased by 1 so that we can round the result
  // and produce the same answer as Spark.
  private[this] lazy val intermediateResultType =
    DecimalDivideChecks.intermediateResultType(decimalType)

  private[this] def divByZeroFixes(lhs: ColumnView, rhs: ColumnVector): ColumnVector = {
    if (failOnDivideByZero) {
      withResource(NullUtilities.mergeNulls(rhs, lhs)) { nullMergedRhs =>
        withResource(GpuDivModLike.makeZeroScalar(rhs.getType)) { zeroScalar =>
          if (nullMergedRhs.contains(zeroScalar)) {
            throw RapidsErrorUtils.divByZeroError(origin)
          }
        }
      }
      rhs.incRefCount()
    } else {
      GpuDivModLike.replaceZeroWithNull(rhs)
    }
  }

  def regularDivide(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      GpuCast.doCast(
        lhs.getBase,
        lhs.dataType(),
        intermediateLhsType,
        CastOptions.getArithmeticCastOptions(failOnError))

    }
    val ret = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        withResource(divByZeroFixes(castLhs, rhs.getBase)) { fixed =>
          GpuCast.doCast(fixed, rhs.dataType(), intermediateRhsType,
            CastOptions.getArithmeticCastOptions(failOnError))
        }
      }
      withResource(castRhs) { castRhs =>
        castLhs.div(castRhs, GpuColumnVector.getNonNestedRapidsType(intermediateResultType))
      }
    }
    withResource(ret) { ret =>
      // Here we cast the output of CUDF to the final result. This will handle overflow checks
      // to see if the divide is too large to fit in the expected type. This should never happen
      // in the common case with us. It will also handle rounding the result to the final scale
      // to match what Spark does.
      GpuColumnVector.from(GpuCast.doCast(ret, intermediateResultType, dataType,
        CastOptions.getArithmeticCastOptions(failOnError)),
        dataType)
    }
  }

  def longDivide(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      lhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, lhs.getBase.getType.getScale))
    }
    val retTab = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        withResource(divByZeroFixes(castLhs, rhs.getBase)) { fixed =>
          fixed.castTo(DType.create(DType.DTypeEnum.DECIMAL128, fixed.getType.getScale))
        }
      }
      withResource(castRhs) { castRhs =>
          if (integerDivide) {
            com.nvidia.spark.rapids.jni.DecimalUtils.integerDivide128(castLhs, castRhs)
          } else {
            com.nvidia.spark.rapids.jni.DecimalUtils.divide128(castLhs, castRhs, -decimalType.scale)
          }
      }
    }
    val outputType = if (integerDivide) LongType else dataType
    val retCol = withResource(retTab) { retTab =>
      val overflowed = retTab.getColumn(0)
      val quotient = retTab.getColumn(1)
      if (failOnError) {
        withResource(overflowed.any()) { anyOverflow =>
          if (anyOverflow.isValid && anyOverflow.getBoolean) {
            throw new ArithmeticException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
        quotient.incRefCount()
      } else {
        withResource(GpuScalar.from(null, outputType)) { nullVal =>
          overflowed.ifElse(nullVal, quotient)
        }
      }
    }
    GpuColumnVector.from(retCol, outputType)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (useLongDivision) {
      longDivide(batch)
    } else {
      regularDivide(batch)
    }
  }

  override def nullable: Boolean = true

}

object DecimalDivideChecks {
  // This comes from DecimalType.MINIMUM_ADJUSTED_SCALE, but for some reason it is gone
  // in databricks so we have it here.
  private val MINIMUM_ADJUSTED_SCALE = 6

  def calcOrigSparkOutputType(lhs: DecimalType, rhs: DecimalType): DecimalType = {
    // This comes almost directly from Spark unchanged
    val allowPrecisionLoss = SQLConf.get.decimalOperationsAllowPrecisionLoss
    val p1 = lhs.precision
    val s1 = lhs.scale
    val p2 = rhs.precision
    val s2 = rhs.scale
    if (allowPrecisionLoss) {
      // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
      // Scale: max(6, s1 + p2 + 1)
      val intDig = p1 - s1 + s2
      val scale = math.max(MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1)
      val prec = intDig + scale
      DecimalType.adjustPrecisionScale(prec, scale)
    } else {
      var intDig = math.min(DecimalType.MAX_SCALE, p1 - s1 + s2)
      var decDig = math.min(DecimalType.MAX_SCALE, math.max(6, s1 + p2 + 1))
      val diff = (intDig + decDig) - DecimalType.MAX_SCALE
      if (diff > 0) {
        decDig -= diff / 2 + 1
        intDig = DecimalType.MAX_SCALE - decDig
      }
      DecimalType.bounded(intDig + decDig, decDig)
    }
  }

  // For Spark the final desired output is
  // new_scale = max(6, lhs.scale + rhs.precision + 1)
  // new_precision = lhs.precision - lhs.scale + rhs.scale + new_scale
  // But Spark will round the final result, so we need at least one more
  // decimal place on the scale to be able to do the rounding too.

  def lhsNeededScale(rhs: DecimalType, outputType: DecimalType): Int =
    outputType.scale + rhs.scale + 1

  def lhsNeededPrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int = {
    val neededLhsScale = lhsNeededScale(rhs, outputType)
    (lhs.precision - lhs.scale) + neededLhsScale
  }

  def nonRoundedIntermediateArgPrecision(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): Int = {
    val neededLhsPrecision = lhsNeededPrecision(lhs, rhs, outputType)
    math.max(neededLhsPrecision, rhs.precision)
  }

  def intermediateArgPrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int =
    math.min(
      nonRoundedIntermediateArgPrecision(lhs, rhs, outputType),
      DType.DECIMAL128_MAX_PRECISION)

  def intermediateLhsType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediateArgPrecision(lhs, rhs, outputType)
    val scale = math.min(lhsNeededScale(rhs, outputType), precision)
    DecimalType(precision, scale)
  }

  def intermediateRhsType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediateArgPrecision(lhs, rhs, outputType)
    DecimalType(precision, rhs.scale)
  }

  def intermediateResultType(outputType: DecimalType): DecimalType = {
    // If the user says that this will not overflow we will still
    // try to do rounding for a correct answer, unless we cannot
    // because it is already a scale of 38
    DecimalType(
      math.min(outputType.precision + 1, DType.DECIMAL128_MAX_PRECISION),
      math.min(outputType.scale + 1, DType.DECIMAL128_MAX_PRECISION))
  }
}

case class GpuDivide(left: Expression, right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)(
    override val origin: Origin = CurrentOrigin.get)
    extends GpuDivModLike {
  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  assert(!left.dataType.isInstanceOf[DecimalType],
    "DecimalType divides need to be handled by GpuDecimalDivide")

  override def inputType: AbstractDataType = DoubleType

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV

  override def outputTypeOverride: DType = GpuColumnVector.getNonNestedRapidsType(dataType)
}

abstract class GpuIntegralDivideParent(
    left: Expression,
    right: Expression)
    extends GpuDivModLike with Serializable {
  override def inputType: AbstractDataType = TypeCollection(IntegralType, DecimalType)

  lazy val failOnOverflow: Boolean =
    SparkShimImpl.shouldFailDivOverflow

  override def checkDivideOverflow: Boolean = left.dataType match {
    case LongType if failOnOverflow => true
    case _ => false
  }

  override lazy val dataType: DataType = LongType
  override def outputTypeOverride: DType = DType.INT64
  // CUDF does not support casting output implicitly for decimal binary ops, so we work around
  // it here where we want to force the output to be a Long.
  override def castOutputAtEnd: Boolean = left.dataType.isInstanceOf[DecimalType]

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.DIV

  override def sqlOperator: String = "div"
}

abstract class GpuRemainderBase(left: Expression, right: Expression)
    extends GpuDivModLike with Serializable {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"

  override def binaryOp: BinaryOp = BinaryOp.MOD
}

abstract class GpuPmodBase(left: Expression, right: Expression)
    extends GpuDivModLike with Serializable {
  override def inputType: AbstractDataType = NumericType

  override def binaryOp: BinaryOp = BinaryOp.PMOD

  override def symbol: String = "pmod"

  override lazy val dataType: DataType = left.dataType
}

trait GpuGreatestLeastBase extends ComplexTypeMergingExpression with GpuExpression
  with ShimExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  /**
   * The binary operation that should be performed when combining two values together.
   */
  def binaryOp: BinaryOp

  /**
   * In the case of floating point values should NaN win and become the output if NaN is
   * the value for either input, or lose and not be the output unless the other choice is
   * null.
   */
  def shouldNanWin: Boolean

  private[this] def isFp = dataType == FloatType || dataType == DoubleType
  // TODO need a better way to do this for nested types
  protected lazy val dtype: DType = GpuColumnVector.getNonNestedRapidsType(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
            s" got LEAST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  /**
   * Convert the input into either a ColumnVector or a Scalar
   * @param a what to convert
   * @param expandScalar if we get a scalar should we expand it out to a ColumnVector to avoid
   *                     scalar scalar math.
   * @param rows If we expand a scalar how many rows should we do?
   * @return the resulting ColumnVector or Scalar
   */
  private[this] def convertAndCloseIfNeeded(
      a: Any,
      expandScalar: Boolean,
      rows: Int): AutoCloseable =
    a match {
      case cv: ColumnVector => cv
      case gcv: GpuColumnVector => gcv.getBase
      case gs: GpuScalar => withResource(gs) { s =>
          if (expandScalar) {
            ColumnVector.fromScalar(s.getBase, rows)
          } else {
            gs.getBase.incRefCount()
          }
      }
      case null =>
        if (expandScalar) {
          GpuColumnVector.columnVectorFromNull(rows, dataType)
        } else {
          GpuScalar.from(null, dataType)
        }
      case o =>
        // It should not be here. since other things here should be converted to a GpuScalar
        throw new IllegalStateException(s"Unexpected inputs: $o")
    }

  /**
   * Take 2 inputs that are either a Scalar or a ColumnVector and combine them with the correct
   * operator. This will blow up if both of the values are scalars though.
   * @param r first value
   * @param c second value
   * @return the combined value
   */
  private[this] def combineButNoClose(r: Any, c: Any): Any = (r, c) match {
    case (r: ColumnVector, c: ColumnVector) =>
      r.binaryOp(binaryOp, c, dtype)
    case (r: ColumnVector, c: Scalar) =>
      r.binaryOp(binaryOp, c, dtype)
    case (r: Scalar, c: ColumnVector) =>
      r.binaryOp(binaryOp, c, dtype)
    case _ => throw new IllegalStateException(s"Unexpected inputs: $r, $c")
  }

  private[this] def makeNanWin(checkForNans: ColumnVector, result: ColumnVector): ColumnVector = {
    withResource(checkForNans.isNan) { shouldReplace =>
      shouldReplace.ifElse(checkForNans, result)
    }
  }

  private[this] def makeNanWin(checkForNans: Scalar, result: ColumnVector): ColumnVector = {
    if (GpuScalar.isNan(checkForNans)) {
      ColumnVector.fromScalar(checkForNans, result.getRowCount.toInt)
    } else {
      result.incRefCount()
    }
  }

  private[this] def makeNanLose(resultIfNotNull: ColumnVector,
      checkForNans: ColumnVector): ColumnVector = {
    withResource(checkForNans.isNan) { isNan =>
      withResource(resultIfNotNull.isNotNull) { isNotNull =>
        withResource(isNan.and(isNotNull)) { shouldReplace =>
          shouldReplace.ifElse(resultIfNotNull, checkForNans)
        }
      }
    }
  }

  private[this] def makeNanLose(resultIfNotNull: Scalar,
      checkForNans: ColumnVector): ColumnVector = {
    if (resultIfNotNull.isValid) {
      withResource(checkForNans.isNan) { shouldReplace =>
        shouldReplace.ifElse(resultIfNotNull, checkForNans)
      }
    } else {
      // Nothing to replace because the scalar is null
      checkForNans.incRefCount()
    }
  }

  /**
   * Cudf does not handle floating point like Spark wants when it comes to NaN values.
   * Spark wants NaN > anything except for null, and null is either the smallest value when used
   * with the greatest operator or the largest value when used with the least value.
   * This does more computation, but gets the right answer in those cases.
   * @param r first value
   * @param c second value
   * @return the combined value
   */
  private[this] def combineButNoCloseFp(r: Any, c: Any): Any = (r, c) match {
    case (r: ColumnVector, c: ColumnVector) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case (r: ColumnVector, c: Scalar) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case (r: Scalar, c: ColumnVector) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case _ => throw new IllegalStateException(s"Unexpected inputs: $r, $c")
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()

    val result = children.foldLeft[Any](null) { (r, c) =>
      withResource(
        convertAndCloseIfNeeded(c.columnarEval(batch), false, numRows)) { cVal =>
        withResource(convertAndCloseIfNeeded(r, cVal.isInstanceOf[Scalar], numRows)) { rVal =>
          if (isFp) {
            combineButNoCloseFp(rVal, cVal)
          } else {
            combineButNoClose(rVal, cVal)
          }
        }
      }
    }
    // The result should always be a ColumnVector at this point
    GpuColumnVector.from(result.asInstanceOf[ColumnVector], dataType)
  }
}

case class GpuLeast(children: Seq[Expression]) extends GpuGreatestLeastBase {
  override def binaryOp: BinaryOp = BinaryOp.NULL_MIN
  override def shouldNanWin: Boolean = false
}

case class GpuGreatest(children: Seq[Expression]) extends GpuGreatestLeastBase {
  override def binaryOp: BinaryOp = BinaryOp.NULL_MAX
  override def shouldNanWin: Boolean = true
}
