/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims

import java.math.BigInteger

import ai.rapids.cudf.{BinaryOperable, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{BoolUtils, GpuBinaryExpression, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{Arithmetic, RoundMode}
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.rapids.GpuDivModLike.makeZeroScalar
import org.apache.spark.sql.types._

object IntervalUtils {

  /**
   * Convert long cv to int cv, throws exception if any value in `longCv` exceeds the int limits.
   * Check (int)(long_value) == long_value
   */
  def castLongToIntWithOverflowCheck(longCv: ColumnView): ColumnVector = {
    withResource(longCv.castTo(DType.INT32)) { intResult =>
      withResource(longCv.notEqualTo(intResult)) { notEquals =>
        if (BoolUtils.isAnyValidTrue(notEquals)) {
          throw RapidsErrorUtils.arithmeticOverflowError("overflow occurs")
        } else {
          intResult.incRefCount()
        }
      }
    }
  }

  def checkDecimal128CvInRange(decimal128Cv: ColumnView, minValue: Long, maxValue: Long): Unit = {
    // check min
    withResource(Scalar.fromLong(minValue)) { minScalar =>
      withResource(decimal128Cv.lessThan(minScalar)) { lessThanMin =>
        if (BoolUtils.isAnyValidTrue(lessThanMin)) {
          throw RapidsErrorUtils.arithmeticOverflowError("overflow occurs")
        }
      }
    }

    // check max
    withResource(Scalar.fromLong(maxValue)) { maxScalar =>
      withResource(decimal128Cv.greaterThan(maxScalar)) { greaterThanMax =>
        if (BoolUtils.isAnyValidTrue(greaterThanMax)) {
          throw RapidsErrorUtils.arithmeticOverflowError("overflow occurs")
        }
      }
    }
  }

  /**
   * Multiple with overflow check, then cast to long
   * Equivalent to Math.multiplyExact
   *
   * @param left   cv(byte, short, int, long) or scalar
   * @param right  cv(byte, short, int, long) or scalar, will not be scalar if left is scalar
   * @return the long result of left * right
   */
  def multipleToLongWithOverflowCheck(left: BinaryOperable, right: BinaryOperable): ColumnVector = {
    val decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, 0)
    withResource(left.mul(right, decimal128Type)) { ret =>
      checkDecimal128CvInRange(ret, Long.MinValue, Long.MaxValue)
      ret.castTo(DType.INT64)
    }
  }

  /**
   * Multiple with overflow check, then cast to int
   * Equivalent to Math.multiplyExact
   *
   * @param left   cv(byte, short, int, long) or scalar
   * @param right  cv(byte, short, int, long) or scalar, will not be scalar if left is scalar
   * @return the int result of left * right
   */
  def multipleToIntWithOverflowCheck(left: BinaryOperable, right: BinaryOperable): ColumnVector = {
    val decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, 0)
    withResource(left.mul(right, decimal128Type)) { ret =>
      checkDecimal128CvInRange(ret, Int.MinValue, Int.MaxValue)
      ret.castTo(DType.INT32)
    }
  }

  def checkDoubleInfNan(doubleCv: ColumnVector): Unit = {
    // check infinity
    withResource(Scalar.fromDouble(Double.PositiveInfinity)) { positiveInfScalar =>
      withResource(doubleCv.equalTo(positiveInfScalar)) { equalsInfinity =>
        if (BoolUtils.isAnyValidTrue(equalsInfinity)) {
          throw new ArithmeticException("Has infinity")
        }
      }
    }

    // check -infinity
    withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negativeInfScalar =>
      withResource(doubleCv.equalTo(negativeInfScalar)) { equalsInfinity =>
        if (BoolUtils.isAnyValidTrue(equalsInfinity)) {
          throw new ArithmeticException("Has -infinity")
        }
      }
    }

    // check NaN
    withResource(doubleCv.isNan) { isNan =>
      if (BoolUtils.isAnyValidTrue(isNan)) {
        throw new ArithmeticException("Has NaN")
      }
    }
  }

  /**
   * Round double cv to int with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundDoubleToIntWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    withResource(Arithmetic.round(doubleCv, RoundMode.HALF_UP)) { roundedDouble =>
      // throws exception if the result exceeds int limits
      withResource(roundedDouble.castTo(DType.INT64)) { long =>
        castLongToIntWithOverflowCheck(long)
      }
    }
  }

  /**
   * round double cv to long with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundDoubleToLongWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    roundToLongWithCheck(doubleCv)
  }

  /**
   * Check if double cv exceeds long limits
   * Rewrite from
   * com.google.common.math.DoubleMath.roundToLong:
   * z = roundIntermediate(x, mode)
   * checkInRange(MIN_LONG_AS_DOUBLE - z < 1.0 & z < MAX_LONG_AS_DOUBLE_PLUS_ONE)
   * return z.toLong
   */
  def roundToLongWithCheck(doubleCv: ColumnVector): ColumnVector = {
    val MIN_LONG_AS_DOUBLE: Double = -9.223372036854776E18
    val MAX_LONG_AS_DOUBLE_PLUS_ONE: Double = 9.223372036854776E18

    withResource(Arithmetic.round(doubleCv, RoundMode.HALF_UP)) { z =>
      withResource(Scalar.fromDouble(MAX_LONG_AS_DOUBLE_PLUS_ONE)) { max =>
        withResource(z.greaterOrEqualTo(max)) { invalid =>
          if (BoolUtils.isAnyValidTrue(invalid)) {
            throw new ArithmeticException("Round double to long overflow")
          }
        }
      }

      withResource(Scalar.fromDouble(MIN_LONG_AS_DOUBLE)) { min =>
        withResource(min.sub(z)) { diff =>
          withResource(Scalar.fromDouble(1.0d)) { one =>
            withResource(diff.greaterOrEqualTo(one)) { invalid =>
              if (BoolUtils.isAnyValidTrue(invalid)) {
                throw new ArithmeticException("Round double to long overflow")
              }
            }
          }
        }
      }

      z.castTo(DType.INT64)
    }
  }

  def getDouble(s: Scalar): Double = {
    s.getType match {
      case DType.INT8 => s.getByte.toDouble
      case DType.INT16 => s.getShort.toDouble
      case DType.INT32 => s.getInt.toDouble
      case DType.INT64 => s.getLong.toDouble
      case DType.FLOAT32 => s.getFloat.toDouble
      case DType.FLOAT64 => s.getDouble
      case t => throw new IllegalArgumentException(s"Unexpected type $t")
    }
  }

  def getLong(s: Scalar): Long = {
    s.getType match {
      case DType.INT8 => s.getByte.toLong
      case DType.INT16 => s.getShort.toLong
      case DType.INT32 => s.getInt.toLong
      case DType.INT64 => s.getLong
      case t => throw new IllegalArgumentException(s"Unexpected type $t")
    }
  }

  def hasZero(cv: BinaryOperable): Boolean = {
    cv match {
      case s: Scalar => getDouble(s) == 0.0d
      case cv: ColumnVector =>
        withResource(Scalar.fromInt(0)) { zero =>
          withResource(cv.equalTo(zero)) { isZero =>
            BoolUtils.isAnyValidTrue(isZero)
          }
        }
    }
  }

  /**
   * Divide p by q rounding with the HALF_UP mode.
   * Logic is round(p.asDecimal / q, HALF_UP).
   *
   * It's equivalent to
   * com.google.common.math.LongMath.divide(p, q, RoundingMode.HALF_UP):
   * long div = p / q; // throws if q == 0
   * long rem = p - q * div; // equals p % q
   * // signum is 1 if p and q are both non-negative or both negative, and -1 otherwise.
   * int signum = 1 | (int) ((p xor q) >> (Long.SIZE - 1));
   * long absRem = abs(rem);
   * long cmpRemToHalfDivisor = absRem - (abs(q) - absRem);
   * increment = cmpRemToHalfDivisor >= 0
   * return increment ? div + signum : div;
   *
   * @param p integer(int/long) cv or scalar
   * @param q integer(byte/short/int/long) cv or Scalar, if p is scala, q will not be scala
   * @return decimal 128 cv of p / q
   */
  def divWithHalfUpModeWithOverflowCheck(p: BinaryOperable, q: BinaryOperable): ColumnVector = {
    // 1. overflow check q is 0
    if (IntervalUtils.hasZero(q)) {
      throw new ArithmeticException("overflow: interval / zero")
    }

    // 2. overflow check (p == min(int min or long min) && q == -1)
    val min = p.getType match {
      case DType.INT32 => Int.MinValue.toLong
      case DType.INT64 => Long.MinValue
      case t => throw new IllegalArgumentException(s"Unexpected type $t")
    }
    withResource(Scalar.fromLong(min)) { minScalar =>
      withResource(Scalar.fromLong(-1L)) { negOneScalar =>
        (p, q) match {
          case (lCv: ColumnVector, rCv: ColumnVector) =>
            withResource(lCv.equalTo(minScalar)) { isMin =>
              withResource(rCv.equalTo(negOneScalar)) { isNegOne =>
                withResource(isMin.and(isNegOne)) { invalid =>
                  if (BoolUtils.isAnyValidTrue(invalid)) {
                    throw RapidsErrorUtils.overflowInIntegralDivideError()
                  }
                }
              }
            }
          case (lCv: ColumnVector, rS: Scalar) =>
            withResource(lCv.equalTo(minScalar)) { isMin =>
              if (getLong(rS) == -1L && BoolUtils.isAnyValidTrue(isMin)) {
                throw RapidsErrorUtils.arithmeticOverflowError("overflow occurs")
              }
            }
          case (lS: Scalar, rCv: ColumnVector) =>
            withResource(rCv.equalTo(negOneScalar)) { isNegOne =>
              if (getLong(lS) == min && BoolUtils.isAnyValidTrue(isNegOne)) {
                throw RapidsErrorUtils.arithmeticOverflowError("overflow occurs")
              }
            }
          case (lS: Scalar, rS: Scalar) =>
            getLong(lS) == min && getLong(rS) == -1L
        }
      }
    }

    // 3. round(p.asDecimal / q)
    val dT = DType.create(DType.DTypeEnum.DECIMAL128, -1)
    val leftDecimal = p match {
      case pCv: ColumnVector => pCv.castTo(dT)
      case pS: Scalar => Scalar.fromDecimal(-1, new BigInteger((getLong(pS) * 10L).toString))
    }
    val t = withResource(leftDecimal) { leftDecimal =>
      leftDecimal.div(q, dT)
    }
    withResource(t) { t =>
      Arithmetic.round(t, RoundMode.HALF_UP)
    }
  }
}

/**
 * Multiply a year-month interval by a numeric:
 * year-month interval * number(byte, short, int, long, float, double)
 * Note not support year-month interval * decimal
 * Year-month interval's internal type is int, the value of int is 12 * year + month
 * left expression is interval, right expression is number
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L506
 *
 */
case class GpuMultiplyYMInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnarImp(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnarImp(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numType match {
      case ByteType | ShortType | IntegerType => // num is byte, short or int
        // compute interval * num
        // interval and num are both in the range: [Int.MinValue, Int.MaxValue],
        // so long fits the result and no need to check overflow
        val longResultCv: ColumnVector = interval.mul(numOperable, DType.INT64)

        withResource(longResultCv) { longResult =>
          // throws exception if exceeds int limits
          IntervalUtils.castLongToIntWithOverflowCheck(longResult)
        }

      case LongType => // num is long
        // The following is equivalent to Math.toIntExact(Math.multiplyExact(months, num))
        IntervalUtils.multipleToIntWithOverflowCheck(interval, numOperable)

      case FloatType | DoubleType => // num is float or double
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)

        withResource(doubleResultCv) { doubleResult =>
          // round to long with overflow check
          IntervalUtils.roundDoubleToIntWithOverflowCheck(doubleResult)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuMultiplyYMInterval")
    }
  }

  override def toString: String = s"$interval * $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)

  override def dataType: DataType = YearMonthIntervalType()
}

/**
 * Multiply a day-time interval by a numeric
 * day-time interval * number(byte, short, int, long, float, double)
 * Note not support day-time interval * decimal
 * Day-time interval's interval type is long, the value of long is the total microseconds
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L558
 */
case class GpuMultiplyDTInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnarImp(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnarImp(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numType match {
      case ByteType | ShortType | IntegerType | LongType =>
        // interval is long type; num is byte, short, int or long
        IntervalUtils.multipleToLongWithOverflowCheck(interval, numOperable)
      case _: FloatType | DoubleType =>
        // interval is long type; num is float or double
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)
        withResource(doubleResultCv) { doubleResult =>
          // round to long with overflow check
          IntervalUtils.roundDoubleToLongWithOverflowCheck(doubleResult)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in MultiplyDTInterval")
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)

  override def dataType: DataType = DayTimeIntervalType()

}

/**
 * Divide a day-time interval by a numeric with a HALF_UP mode:
 *   e.g.: 3 / 2 = 1.5 will be rounded to 2; -3 / 2 = -1.5 will be rounded to -2
 * year-month interval / number(byte, short, int, long, float, double)
 * Note not support year-month interval / decimal
 * Year-month interval's internal type is int, the value of int is 12 * year + month
 * left expression is interval, right expression is number
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L615
 *
 */
case class GpuDivideYMInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnar(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnar(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnar(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnar(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numOperable match {
      case s: Scalar =>
        withResource(makeZeroScalar(s.getType)) { zero =>
          if (s.equals(zero)) throw RapidsErrorUtils.intervalDivByZeroError(origin)
        }
      case cv: ColumnVector =>
        withResource(makeZeroScalar(cv.getType)) { zero =>
          if (cv.contains(zero)) throw RapidsErrorUtils.intervalDivByZeroError(origin)
        }
    }

    numType match {
      case ByteType | ShortType | IntegerType | LongType =>
        // interval is int; num is byte, short, int or long
        // For overflow check: num is 0; interval == Int.Min && num == -1
        withResource(IntervalUtils.divWithHalfUpModeWithOverflowCheck(interval, numOperable)) {
          // overflow already checked, directly cast without overflow check
          decimalRet => decimalRet.castTo(DType.INT32)
        }

      case FloatType | DoubleType => // num is float or double
        withResource(interval.div(numOperable, DType.FLOAT64)) { double =>
          // check overflow, then round to int
          IntervalUtils.roundDoubleToIntWithOverflowCheck(double)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuDivideYMInterval")
    }
  }

  override def toString: String = s"$interval / $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)

  override def dataType: DataType = YearMonthIntervalType()
}

/**
 * Divide a day-time interval by a numeric with a HALF_UP mode:
 *   e.g.: 3 / 2 = 1.5 will be rounded to 2; -3 / 2 = -1.5 will be rounded to -2
 * day-time interval / number(byte, short, int, long, float, double)
 * Note not support day-time interval / decimal
 * Day-time interval's interval type is long, the value of long is the total microseconds
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L693
 */
case class GpuDivideDTInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    withResource(makeZeroScalar(numScalar.getBase.getType)) { zeroScalar =>
      if (numScalar.getBase.equals(zeroScalar)) {
        throw RapidsErrorUtils.intervalDivByZeroError(origin)
      }
    }
    doColumnar(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    withResource(makeZeroScalar(num.getBase.getType)) { zeroScalar =>
      if (num.getBase.contains(zeroScalar)) {
        throw RapidsErrorUtils.intervalDivByZeroError(origin)
      }
    }
    doColumnar(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    withResource(makeZeroScalar(num.getBase.getType)) { zeroScalar =>
      if (num.getBase.contains(zeroScalar)) {
        throw RapidsErrorUtils.intervalDivByZeroError(origin)
      }
    }
    doColumnar(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnar(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numType match {
      case ByteType | ShortType | IntegerType | LongType =>
        // interval is long; num is byte, short, int or long
        // For overflow check: num is 0; interval == Long.Min && num == -1
        withResource(IntervalUtils.divWithHalfUpModeWithOverflowCheck(interval, numOperable)) {
          // overflow already checked, directly cast without overflow check
          decimalRet => decimalRet.castTo(DType.INT64)
        }

      case FloatType | DoubleType =>
        // interval is long; num is float or double
        withResource(interval.div(numOperable, DType.FLOAT64)) { double =>
          // check overflow, then round to long
          IntervalUtils.roundDoubleToLongWithOverflowCheck(double)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuDivideDTInterval")
    }
  }

  override def toString: String = s"$interval / $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)

  override def dataType: DataType = DayTimeIntervalType()

}
