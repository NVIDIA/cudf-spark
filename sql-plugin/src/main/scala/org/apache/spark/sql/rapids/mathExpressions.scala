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

import java.io.Serializable

import ai.rapids.cudf._
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.{Arithmetic, CastStrings, ExceptionWithRowIndex, RoundMode}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.rapids.shims.{OriginContextShim, RapidsErrorUtils}
import org.apache.spark.sql.types._

abstract class CudfUnaryMathExpression(name: String) extends GpuUnaryMathExpression(name)
  with CudfUnaryExpression

abstract class GpuUnaryMathExpression(name: String) extends GpuUnaryExpression
  with Serializable with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"
  override def prettyName: String = name
}

case class GpuAcos(child: Expression) extends CudfUnaryMathExpression("ACOS") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuToDegrees(child: Expression) extends GpuUnaryMathExpression("DEGREES") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Spark's implementation of toDegrees is directly using toDegrees(angrad) in Java,
    // In jdk8, toDegrees implemention is angrad * 180.0 / PI, while since jdk9,
    // toDegrees is angrad * DEGREES_TO_RADIANS, where DEGREES_TO_RADIANS is 180.0/PI.
    // So when jdk8 or earlier is used, toDegrees will produce different result on GPU,
    // where it will not overflow when input is very large.
    withResource(Scalar.fromDouble(180d / Math.PI)) { multiplier =>
      input.getBase.mul(multiplier)
    }
  }
}

case class GpuToRadians(child: Expression) extends GpuUnaryMathExpression("RADIANS") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(Math.PI / 180d)) { multiplier =>
      input.getBase.mul(multiplier)
    }
  }
}

case class GpuBin(child: Expression) extends GpuUnaryExpression
  with ImplicitCastInputTypes with Serializable  {
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    CastStrings.fromLongToBinary(input.getBase)
  }
}

case class GpuHex(child: Expression) extends GpuUnaryExpression with Serializable {
  // Preserve the input StringType (including collation) for string inputs,
  // matching Spark's Hex.dataType behavior.
  override def dataType: DataType = child.dataType match {
    case st: StringType => st
    case _ => StringType
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    child.dataType match {
      case LongType =>
        CastStrings.fromIntegersWithBase(input.getBase, 16)
      case _: StringType | BinaryType =>
        CastStrings.bytesToHex(input.getBase)
    }
  }
}

case class GpuAcoshImproved(child: Expression) extends CudfUnaryMathExpression("ACOSH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOSH
}

case class GpuAcoshCompat(child: Expression) extends GpuUnaryMathExpression("ACOSH") {
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    // StrictMath.log(x + math.sqrt(x * x - 1.0))
    val base = input.getBase
    val squaredMinOne = withResource(base.mul(base)) { squared =>
      withResource(Scalar.fromDouble(1.0)) {
        squared.sub
      }
    }
    val sqrt = withResource(squaredMinOne) {
      _.sqrt()
    }
    val sum = withResource(sqrt) {
      base.add
    }
    withResource(sum) {
      _.log()
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    // StrictMath.log(x + math.sqrt(x * x - 1.0))
    val x = child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)
    new ast.UnaryOperation(ast.UnaryOperator.LOG,
      new ast.BinaryOperation(ast.BinaryOperator.ADD, x,
        new ast.UnaryOperation(ast.UnaryOperator.SQRT,
          new ast.BinaryOperation(ast.BinaryOperator.SUB,
            new ast.BinaryOperation(ast.BinaryOperator.MUL, x, x), ast.Literal.ofDouble(1)))))
  }
}

case class GpuAsin(child: Expression) extends CudfUnaryMathExpression("ASIN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAsinhImproved(child: Expression) extends CudfUnaryMathExpression("ASINH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSINH
}

case class GpuAsinhCompat(child: Expression) extends GpuUnaryMathExpression("ASINH") {
  override def outputTypeOverride: DType = DType.FLOAT64

  def computeBasic(input: ColumnVector): ColumnVector = {
    val squaredPlusOne = withResource(input.mul(input)) { squared =>
       withResource(Scalar.fromDouble(1.0)) {
        squared.add
       }
    }
    val sqrt = withResource(squaredPlusOne) {
      _.sqrt()
    }
    val sum = withResource(sqrt) {
      input.add
    }
    withResource(sum) {
      _.log()
    }
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Typically we would just use UnaryOp.ARCSINH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    //  x match {
    //    case Double.NegativeInfinity => Double.NegativeInfinity
    //    case _ => StrictMath.log(x + math.sqrt(x * x + 1.0)) }
    val base = input.getBase
    withResource(computeBasic(base)) { basic =>
      withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negInf =>
        withResource(base.equalTo(negInf)) { eqNegInf =>
          eqNegInf.ifElse(negInf, basic)
        }
      }
    }
  }
}

case class GpuAtan(child: Expression) extends CudfUnaryMathExpression("ATAN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAtanh(child: Expression) extends CudfUnaryMathExpression("ATANH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

object GpuFloorCeil {
  def unboundedOutputPrecision(dt: DecimalType): Int = {
    if (dt.scale == 0) {
      dt.precision
    } else {
      dt.precision - dt.scale + 1
    }
  }
}

case class GpuCeil(child: Expression, outputType: DataType)
    extends CudfUnaryMathExpression("CEIL") {
  override def dataType: DataType = outputType

  override def hasSideEffects: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.CEIL
  override def outputTypeOverride: DType =
    dataType match {
      case dt: DecimalType =>
        DecimalUtil.createCudfDecimal(dt)
      case _ =>
        DType.INT64
    }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.dataType() match {
      case DoubleType =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          super.doColumnar(GpuColumnVector.from(inputWithNansToZero, DoubleType))
        }
      case LongType =>
        // Long is a noop in spark, but for cudf it is not.
        input.getBase.incRefCount()
      case dt: DecimalType =>
        val outputType = dataType.asInstanceOf[DecimalType]
        // check for out of bound values when output precision is constrained by MAX_PRECISION
        if (outputType.precision == DecimalType.MAX_PRECISION && dt.scale < 0) {
          withResource(DecimalUtil.outOfBounds(input.getBase, outputType)) { outOfBounds =>
            withResource(outOfBounds.any()) { isAny =>
              if (isAny.isValid && isAny.getBoolean) {
                throw RoundingErrorUtil.cannotChangeDecimalPrecisionError(
                  input.getBase, outOfBounds, outputType
                )
              }
            }
          }
        }
        super.doColumnar(input)
    }
  }
}

case class GpuCos(child: Expression) extends CudfUnaryMathExpression("COS") {
  override def unaryOp: UnaryOp = UnaryOp.COS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExp(child: Expression) extends CudfUnaryMathExpression("EXP") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExpm1(child: Expression) extends CudfUnaryMathExpression("EXPM1") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.unaryOp(unaryOp)) { cv =>
      withResource(Scalar.fromInt(1)) { sc =>
        cv.binaryOp(BinaryOp.SUB, sc, outputTypeOverride)
      }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    new ast.BinaryOperation(ast.BinaryOperator.SUB,
      super.convertToAst(numFirstTableColumns),
      ast.Literal.ofDouble(1))
  }
}

case class GpuFloor(child: Expression, outputType: DataType)
    extends CudfUnaryMathExpression("FLOOR") {
  override def dataType: DataType = outputType

  override def hasSideEffects: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.FLOOR

  override def outputTypeOverride: DType =
    dataType match {
      case dt: DecimalType =>
        DecimalUtil.createCudfDecimal(dt)
      case _ =>
        DType.INT64
    }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.dataType() match {
      case DoubleType =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          super.doColumnar(GpuColumnVector.from(inputWithNansToZero, DoubleType))
        }
      case LongType =>
        // Long is a noop in spark, but for cudf it is not.
        input.getBase.incRefCount()
      case dt: DecimalType =>
        val outputType = dataType.asInstanceOf[DecimalType]
        // check for out of bound values when output precision is constrained by MAX_PRECISION
        if (outputType.precision == DecimalType.MAX_PRECISION && dt.scale < 0) {
          withResource(DecimalUtil.outOfBounds(input.getBase, outputType)) { outOfBounds =>
            withResource(outOfBounds.any()) { isAny =>
              if (isAny.isValid && isAny.getBoolean) {
                throw RoundingErrorUtil.cannotChangeDecimalPrecisionError(
                  input.getBase, outOfBounds, outputType
                )
              }
            }
          }
        }
        super.doColumnar(input)
    }
  }
}

case class GpuLog(child: Expression) extends CudfUnaryMathExpression("LOG") {
  override def unaryOp: UnaryOp = UnaryOp.LOG
  override def outputTypeOverride: DType = DType.FLOAT64
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.nullOutNegatives(input, child.dataType)) { normalized =>
      super.doColumnar(normalized)
    }
  }
}

object GpuLogarithm {

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned GpuColumnVector.
   */
  def nullOutNegatives(input: GpuColumnVector, dt: DataType): GpuColumnVector = {
    val ret = withResource(Scalar.fromDouble(0)) { zero =>
      withResource(input.getBase.binaryOp(BinaryOp.LESS_EQUAL, zero, DType.BOOL8)) { zeroOrLess =>
        withResource(Scalar.fromNull(DType.FLOAT64)) { nullScalar =>
          zeroOrLess.ifElse(nullScalar, input.getBase)
        }
      }
    }
    GpuColumnVector.from(ret, dt)
  }

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned Scalar.
   */
  def nullOutNegatives(input: GpuScalar): GpuScalar = {
    if (input.isValid && input.getValue.asInstanceOf[Double] <= 0) {
      GpuScalar(null, DoubleType)
    } else {
      input.incRefCount
    }
  }
}

case class GpuLogarithm(left: Expression, right: Expression)
  extends CudfBinaryMathExpression("LOG_BASE") {

  override def binaryOp: BinaryOp = BinaryOp.LOG_BASE
  override def outputTypeOverride: DType = DType.FLOAT64
  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.nullOutNegatives(lhs, left.dataType)) { fixedLhs =>
      withResource(GpuLogarithm.nullOutNegatives(rhs, right.dataType)) { fixedRhs =>
        super.doColumnar(fixedLhs, fixedRhs)
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.nullOutNegatives(lhs)) { fixedLhs =>
      withResource(GpuLogarithm.nullOutNegatives(rhs, right.dataType)) { fixedRhs =>
        super.doColumnar(fixedLhs, fixedRhs)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuLogarithm.nullOutNegatives(lhs, left.dataType)) { fixedLhs =>
      withResource(GpuLogarithm.nullOutNegatives(rhs)) { fixedRhs =>
        super.doColumnar(fixedLhs, fixedRhs)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuLogarithm.nullOutNegatives(lhs)) { fixedLhs =>
      withResource(GpuLogarithm.nullOutNegatives(rhs)) { fixedRhs =>
        super.doColumnar(numRows, fixedLhs, fixedRhs)
      }
    }
  }
}

case class GpuSin(child: Expression) extends CudfUnaryMathExpression("SIN") {
  override def unaryOp: UnaryOp = UnaryOp.SIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSignum(child: Expression) extends GpuUnaryMathExpression("SIGNUM") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(0)) { num =>
      withResource(Scalar.fromDouble(1)) { hiReplace =>
        withResource(Scalar.fromDouble(-1)) { loReplace =>
          input.getBase.clamp(num, loReplace, num, hiReplace)
        }
      }
    }
  }

  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTanh(child: Expression) extends CudfUnaryMathExpression("TANH") {
  override def unaryOp: UnaryOp = UnaryOp.TANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCosh(child: Expression) extends CudfUnaryMathExpression("COSH") {
  override def unaryOp: UnaryOp = UnaryOp.COSH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSinh(child: Expression) extends CudfUnaryMathExpression("SINH") {
  override def unaryOp: UnaryOp = UnaryOp.SINH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSqrt(child: Expression) extends CudfUnaryMathExpression("SQRT") {
  override def unaryOp: UnaryOp = UnaryOp.SQRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCbrt(child: Expression) extends CudfUnaryMathExpression("CBRT") {
  override def unaryOp: UnaryOp = UnaryOp.CBRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTan(child: Expression) extends CudfUnaryMathExpression("TAN") {
  override def unaryOp: UnaryOp = UnaryOp.TAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCot(child: Expression) extends GpuUnaryMathExpression("COT") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromInt(1)) { one =>
      withResource(input.getBase.unaryOp(UnaryOp.TAN)) { tan =>
        one.div(tan)
      }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    new ast.BinaryOperation(ast.BinaryOperator.DIV, ast.Literal.ofDouble(1),
      new ast.UnaryOperation(ast.UnaryOperator.TAN,
        child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)))
  }
}

object GpuHypot {
  def chooseXandY(lhs: ColumnVector, rhs: ColumnVector): Seq[ColumnVector] = {
    withResource(lhs.abs) { lhsAbs =>
      withResource(rhs.abs) { rhsAbs =>
        withResource(lhsAbs.greaterThan(rhsAbs)) { lhsGreater =>
          closeOnExcept(lhsGreater.ifElse(lhsAbs, rhsAbs)) { x =>
            Seq(x, lhsGreater.ifElse(rhsAbs, lhsAbs))
          }
        }
      }
    }
  }

  def either(value: Scalar, x: ColumnVector, y: ColumnVector): ColumnVector = {
    withResource(x.equalTo(value)) { xIsVal =>
      withResource(y.equalTo(value)) { yIsVal =>
        xIsVal.or(yIsVal)
      }
    }
  }

  def both(value: Scalar, x: ColumnVector, y: ColumnVector): ColumnVector = {
    withResource(x.equalTo(value)) { xIsVal =>
      withResource(y.equalTo(value)) { yIsVal =>
        xIsVal.and(yIsVal)
      }
    }
  }

  def eitherNan(x: ColumnVector,
                y: ColumnVector): ColumnVector = {
    withResource(x.isNan) { xIsNan =>
      withResource(y.isNan) { yIsNan =>
        xIsNan.or(yIsNan)
      }
    }
  }

  def computeHypot(x: ColumnVector, y: ColumnVector): ColumnVector = {
    val yOverXSquared = withResource(y.div(x)) { yOverX =>
      yOverX.mul(yOverX)
    }

    val onePlusTerm = withResource(yOverXSquared) { _ =>
      withResource(Scalar.fromDouble(1)) { one =>
        one.add(yOverXSquared)
      }
    }

    val onePlusTermSqrt = withResource(onePlusTerm) { _ =>
      onePlusTerm.sqrt
    }

    withResource(onePlusTermSqrt) { _ =>
      x.mul(onePlusTermSqrt)
    }
  }
}

case class GpuHypot(left: Expression, right: Expression) extends CudfBinaryMathExpression("HYPOT") {

  override def binaryOp: BinaryOp = BinaryOp.ADD

  // naive implementation of hypot
  // r = sqrt(lhs^2 + rhs^2)
  // This is prone to overflow with regards either square term
  // However, we can reduce it to
  // r = sqrt(x^2 + y^2) = x * sqrt(1 + (y/x)^2)
  // where x = max(abs(lhs), abs(rhs)), y = min(abs(lhs), abs(rhs))
  // which will only overflow if both terms are near the maximum representation space
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {

    // in spark SQL HYPOT (java.math.Math.hypot), there are a couple of edge cases that produce
    // specific results. Note that terms are absolute values (always positive)
    //  - if either term is inf, then the answer is inf
    //  - if either term is nan, and neither is inf, then the answer is nan
    //  - if both terms are 0, then the answer is 0

    withResource(GpuHypot.chooseXandY(lhs.getBase, rhs.getBase)) { case Seq(x, y) =>
      val zeroOrBase = withResource(Scalar.fromDouble(0)) { zero =>
        withResource(GpuHypot.both(zero, x, y)) { bothZero =>
          withResource(GpuHypot.computeHypot(x, y)) { hypotComputed =>
            bothZero.ifElse(zero, hypotComputed)
          }
        }
      }

      val nanOrRest = withResource(zeroOrBase) { _ =>
        withResource(Scalar.fromDouble(Double.NaN)) { nan =>
          withResource(GpuHypot.eitherNan(x, y)) { eitherNan =>
            eitherNan.ifElse(nan, zeroOrBase)
          }
        }
      }

      val infOrRest = withResource(nanOrRest) { _ =>
          withResource(Scalar.fromDouble(Double.PositiveInfinity)) { inf =>
          withResource(GpuHypot.either(inf, x, y)) { eitherInf =>
            eitherInf.ifElse(inf, nanOrRest)
          }
        }
      }

      withResource(infOrRest) { _ =>
        infOrRest.mergeAndSetValidity(BinaryOp.BITWISE_AND, x, y)
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { expandedRhs =>
      doColumnar(lhs, expandedRhs)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      withResource(GpuColumnVector.from(rhs, numRows)) { expandedRhs =>
        doColumnar(expandedLhs, expandedRhs)
      }
    }
  }
}

abstract class CudfBinaryMathExpression(name: String) extends CudfBinaryExpression
    with Serializable with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType)
  override def toString: String = s"$name($left, $right)"
  override def prettyName: String = name
  override def dataType: DataType = DoubleType
}

// Due to SPARK-39226, the dataType of round-like functions differs by Spark versions.
abstract class GpuRoundBase(
    child: Expression,
    scale: Expression,
    outputType: DataType,
    val ansiEnabled: Boolean)
  extends GpuBinaryExpressionArgsAnyScalar with Serializable with ImplicitCastInputTypes {

  override def left: Expression = child
  override def right: Expression = scale

  def roundMode: RoundMode

  override def dataType: DataType = outputType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  override def doColumnar(value: GpuColumnVector, scale: GpuScalar): ColumnVector = {

    val lhsValue = value.getBase
    val scaleVal = scale.getValue.asInstanceOf[Int]

    child.dataType match {
      case DecimalType.Fixed(_, s) =>
        // Only needs to perform round when required scale < input scale
        val rounded = if (scaleVal < s) {
          Arithmetic.round(lhsValue, scaleVal, roundMode)
        } else {
          lhsValue.incRefCount()
        }
        withResource(rounded) { _ =>
          // Fit the output datatype
          rounded.castTo(
            DecimalUtil.createCudfDecimal(dataType.asInstanceOf[DecimalType]))
        }
      case ByteType =>
        fixUpOverflowInts(() => Scalar.fromByte(0.toByte), scaleVal, lhsValue)
      case ShortType =>
        fixUpOverflowInts(() => Scalar.fromShort(0.toShort), scaleVal, lhsValue)
      case IntegerType =>
        fixUpOverflowInts(() => Scalar.fromInt(0), scaleVal, lhsValue)
      case LongType =>
        fixUpOverflowInts(() => Scalar.fromLong(0L), scaleVal, lhsValue)
      case FloatType =>
        fpZeroReplacement(
          () => Scalar.fromFloat(0.0f),
          () => Scalar.fromFloat(Float.PositiveInfinity),
          () => Scalar.fromFloat(Float.NegativeInfinity),
          scaleVal, lhsValue)
      case DoubleType =>
        fpZeroReplacement(
          () => Scalar.fromDouble(0.0),
          () => Scalar.fromDouble(Double.PositiveInfinity),
          () => Scalar.fromDouble(Double.NegativeInfinity),
          scaleVal, lhsValue)
      case _ =>
        throw new IllegalArgumentException(s"Round operator doesn't support $dataType")
    }
  }

  // Fixes up integral values rounded by a scale exceeding/reaching the max digits of data
  // type. Under this circumstance, cuDF may produce different results to Spark.
  //
  // In this method, we handle round overflow, aligning the inconsistent results to Spark.
  //
  // For scales exceeding max digits, we can simply return zero values.
  //
  // For scales equaling to max digits, we need to perform round. Fortunately, round up
  // will NOT occur on the max digits of numeric types except LongType. Therefore, we only
  // need to handle round down for most of types, through returning zero values.
  private def fixUpOverflowInts(zeroFn: () => Scalar,
      scale: Int,
      lhs: ColumnVector): ColumnVector = {
    // Rounding on the max digit of long values, which should be specialized handled since
    // it may be needed to round up, which will produce inconsistent results because of
    // overflow. Otherwise, we only need to handle round down situations.
    if (-scale == 19 && lhs.getType == DType.INT64) {
      fixUpInt64OnBounds(lhs)
    } else if (-scale >= lhs.getType.getPrecisionForInt) {
      withResource(zeroFn()) { s =>
        withResource(ColumnVector.fromScalar(s, lhs.getRowCount.toInt)) { zero =>
          // set null mask if necessary
          if (lhs.hasNulls) {
            zero.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs)
          } else {
            zero.incRefCount()
          }
        }
      }
    } else {
      try {
        Arithmetic.round(lhs, scale, roundMode, ansiEnabled)
      } catch {
        // ANSI mode throws ExceptionWithRowIndex if overflow would occur
        case rowException: ExceptionWithRowIndex =>
          val errorRowIndex = rowException.getRowIndex
          val inputValue = ColumnViewUtils.getElementStringFromColumnView(lhs, errorRowIndex)
          throw new ArithmeticException(
            s"Rounding $inputValue to scale $scale with mode $roundMode caused " +
              s"overflow in ANSI mode")
      }
    }
  }

  // Compared to other non-decimal numeric types, Int64(LongType) is a bit special in terms of
  // rounding by the max digit. Because the bound values of LongType can be rounded up, while
  // other numeric types can only be rounded down:
  //
  //  the max value of Byte: 127
  //  The first digit is up to 1, which can't be rounded up.
  //  the max value of Short: 32767
  //  The first digit is up to 3, which can't be rounded up.
  //  the max value of Int32: 2147483647
  //  The first digit is up to 2, which can't be rounded up.
  //  the max value of Float32: 3.4028235E38
  //  The first digit is up to 3, which can't be rounded up.
  //  the max value of Float64: 1.7976931348623157E308
  //  The first digit is up to 1, which can't be rounded up.
  //  the max value of Int64: 9223372036854775807
  //  The first digit is up to 9, which can be rounded up.
  //
  // When rounding up 19-digits long values on the first digit, the result can be 1e19 or -1e19.
  // Since LongType can not hold these two values, the 1e19 overflows as -8446744073709551616L,
  // and the -1e19 overflows as 8446744073709551616L. The overflow happens in the same way for
  // HALF_UP (round) and HALF_EVEN (bround).
  private def fixUpInt64OnBounds(lhs: ColumnVector): ColumnVector = {
    // Builds predicates on whether there is a round up on the max digit or not
    val litForCmp = Seq(Scalar.fromLong(1000000000000000000L),
                        Scalar.fromLong(4L),
                        Scalar.fromLong(-4L))
    val (needRep, needNegRep) = withResource(litForCmp) { case Seq(base, four, minusFour) =>
      withResource(lhs.div(base)) { headDigit =>
        closeOnExcept(headDigit.greaterThan(four)) { posRep =>
          closeOnExcept(headDigit.lessThan(minusFour)) { negRep =>
            posRep -> negRep
          }
        }
      }
    }
    // Replaces with corresponding literals
    val litForRep = Seq(Scalar.fromLong(0L),
                        Scalar.fromLong(8446744073709551616L),
                        Scalar.fromLong(-8446744073709551616L))
    val repVal = withResource(litForRep) { case Seq(zero, upLit, negUpLit) =>
      withResource(needRep) { _ =>
        withResource(needNegRep) { _ =>
          withResource(needNegRep.ifElse(upLit, zero)) { negBranch =>
            needRep.ifElse(negUpLit, negBranch)
          }
        }
      }
    }
    // Handles null values
    withResource(repVal) { _ =>
      if (lhs.hasNulls) {
        repVal.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs)
      } else {
        repVal.incRefCount()
      }
    }
  }

  // Fixes up float points rounded by a scale exceeding the max digits of data type. Under this
  // circumstance, cuDF produces different results to Spark.
  // Compared to integral values, fixing up round overflow of float points needs to take care
  // of some special values: nan, inf, -inf.
  def fpZeroReplacement(zeroFn: () => Scalar,
      infFn: () => Scalar,
      negInfFn: () => Scalar,
      scale: Int,
      lhs: ColumnVector): ColumnVector = {
    val maxDigits = if (dataType == FloatType) 39 else 309
    if (-scale >= maxDigits) {
      // replaces common values (!Null AND !Nan AND !Inf And !-Inf) with zero, while keeps
      // all the special values unchanged
      withResource(Seq(zeroFn(), infFn(), negInfFn())) { case Seq(zero, inf, negInf) =>
        // builds joined predicate: !Null AND !Nan AND !Inf And !-Inf
        val joinedPredicate = {
          val conditions = Seq(() => lhs.isNotNan,
                               () => lhs.notEqualTo(inf),
                               () => lhs.notEqualTo(negInf))
          conditions.foldLeft(lhs.isNotNull) { case (buffer, builder) =>
            withResource(buffer) { _ =>
              withResource(builder()) { predicate =>
                buffer.and(predicate)
              }
            }
          }
        }
        withResource(joinedPredicate) { cond =>
          cond.ifElse(zero, lhs)
        }
      }
    } else if (scale >= maxDigits) {
      // just returns the original values
      lhs.incRefCount()
    } else {
      Arithmetic.round(lhs, scale, roundMode)
    }
  }

  override def doColumnar(numRows: Int, value: GpuScalar, scale: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(value, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, scale)
    }
  }
}

case class GpuBRound(
    child: Expression,
    scale: Expression,
    outputType: DataType,
    override val ansiEnabled: Boolean) extends
  GpuRoundBase(child, scale, outputType, ansiEnabled) {
  override def roundMode: RoundMode = RoundMode.HALF_EVEN
}

case class GpuRound(
    child: Expression,
    scale: Expression,
    outputType: DataType,
    override val ansiEnabled: Boolean) extends
  GpuRoundBase(child, scale, outputType, ansiEnabled) {
  override def roundMode: RoundMode = RoundMode.HALF_UP
}

case class GpuPow(left: Expression, right: Expression)
    extends CudfBinaryMathExpression("POWER") {
  override def binaryOp: BinaryOp = BinaryOp.POW
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.POW)
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuRint(child: Expression) extends CudfUnaryMathExpression("ROUND") {
  override def unaryOp: UnaryOp = UnaryOp.RINT
  override def outputTypeOverride: DType = DType.FLOAT64
}

object RoundingErrorUtil {
  /**
   * Wrapper of the `cannotChangeDecimalPrecisionError` of RapidsErrorUtils.
   *
   * Uses `Decimal(BigDecimal)` so the value's natural precision is preserved; the
   * `(value, p, s)` constructor caps precision at 38 and would itself throw
   * "Decimal precision N exceeds max precision 38" exactly when the overflow value
   * already exceeds 38 digits, swallowing the intended overflow message.
   *
   * @param values A decimal column which contains values that try to cast.
   * @param outOfBounds A boolean column that indicates which value cannot be casted.
   * Users must make sure that there is at least one `true` in this column.
   * @param toType The type to cast.
   */
  def cannotChangeDecimalPrecisionError(
      values: ColumnView,
      outOfBounds: ColumnView,
      toType: DecimalType): ArithmeticException = {
    val rowId = withResource(outOfBounds.copyToHost()) { hcv =>
      (0L until outOfBounds.getRowCount)
        .find(i => !hcv.isNull(i) && hcv.getBoolean(i))
        .get
    }
    val value = withResource(values.getScalarElement(rowId.toInt)) { s =>
      s.getBigDecimal
    }
    // Pass the SQL query context (set on the executor via `CurrentOrigin.withOrigin`
    // around `doColumnar`) so the exception message preserves SPARK-39190 parity.
    RapidsErrorUtils.cannotChangeDecimalPrecisionError(
      Decimal(value), toType, OriginContextShim.queryContext(CurrentOrigin.get))
  }
}
