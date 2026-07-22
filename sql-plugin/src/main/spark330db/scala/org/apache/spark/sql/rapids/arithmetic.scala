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

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import scala.math.{max, min}

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerantShim {

  protected val failOnError: Boolean

  protected def allowPrecisionLoss = SQLConf.get.decimalOperationsAllowPrecisionLoss

  // arithmetic operations can overflow and throw exceptions in ANSI mode
  override def hasSideEffects: Boolean = super.hasSideEffects || failOnError

  def dataTypeInternal(lhs: Expression, rhs: Expression) = (lhs.dataType, rhs.dataType) match {
    case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
      resultDecimalType(p1, s1, p2, s2)
    case _ => lhs.dataType
  }
  override lazy val dataType: DataType = dataTypeInternal(left, right)

  protected def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    throw new IllegalStateException(
      s"${getClass.getSimpleName} must override `resultDecimalType`.")
  }

  override def checkInputDataTypes(): TypeCheckResult = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) if inputType.acceptsType(l) && inputType.acceptsType(r) =>
      // We allow decimal type inputs with different precision and scale, and use special formulas
      // to calculate the result precision and scale.
      TypeCheckResult.TypeCheckSuccess
    case _ => super.checkInputDataTypes()
  }

  override def nullable: Boolean = super.nullable || {
    if (left.dataType.isInstanceOf[DecimalType]) {
      // For decimal arithmetic, we may return null even if both inputs are not null,
      // if overflow happens and this `failOnError` flag is false.
      !failOnError
    } else {
      // For non-decimal arithmetic, the calculation always return non-null result when inputs
      // are not null. If overflow happens, we return either the overflowed value or fail.
      false
    }
  }
}

trait GpuAddSub extends CudfBinaryArithmetic {
  override def outputTypeOverride: DType = GpuColumnVector.getNonNestedRapidsType(dataType)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val outputType = dataType
    val leftInputType = left.dataType
    val rightInputType = right.dataType

    if (outputType.isInstanceOf[DecimalType]) {
      (leftInputType, rightInputType) match {
        case (DecimalType.Fixed(_, _), DecimalType.Fixed(_, _)) =>
          val resultType = outputType.asInstanceOf[DecimalType]
          if (resultType.precision < 38) {
            // SPARK-39316 https://github.com/apache/spark/commit/301a139638
            // moves the Cast of the operands to the result type
            // from the arithmetic expression plan to the datatype-specific operation
            // implementation. libcudf requires identical DType for both operands.
            // We ensure this in the Plugin by handling these cases:
            // - if both Spark input types are identical, no casting is necessary,
            //   since they ultimately map to the same DType
            // - if precision >= 38 we execute a DECIMAL128 binop casting both operands
            //   to DECIMAL128
            // - if precision < 38 cast both operands to the Spark output type
            //
            if (leftInputType == rightInputType) {
              super.columnarEval(batch)
            } else {
              // eval operands using the output precision
              val castLhs = withResource(left.columnarEval(batch)) { lhs =>
                GpuCast.doCast(lhs.getBase(), leftInputType, resultType)
              }
              val castRhs = closeOnExcept(castLhs){ _ =>
                withResource(right.columnarEval(batch)) { rhs =>
                  GpuCast.doCast(rhs.getBase(), rightInputType, resultType)
                }
              }

              withResource(Seq(castLhs, castRhs)) { _ =>
                GpuColumnVector.from(super.doColumnar(castLhs, castRhs), resultType)
              }
            }
          } else {
            // call the kernel to add 128-bit numbers
            prepareInputAndExecute(batch, resultType)
          }
        case _ => throw new IllegalStateException("Both operands should be Decimal")
      }
    } else {
      super.columnarEval(batch)
    }
  }

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  protected def prepareInputAndExecute(
      batch: ColumnarBatch,
      resultType: DecimalType): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      lhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, lhs.getBase.getType.getScale))
    }
    val retTab = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        rhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, rhs.getBase.getType.getScale))
      }
      withResource(castRhs) { castRhs =>
        do128BitOperation(castLhs, castRhs, -resultType.scale)
      }
    }
    val retCol = withResource(retTab) { retTab =>
      val overflowed = retTab.getColumn(0)
      val sum = retTab.getColumn(1)
      if (failOnError) {
        withResource(overflowed.any()) { anyOverflow =>
          if (anyOverflow.isValid && anyOverflow.getBoolean) {
            throw new ArithmeticException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
        sum.incRefCount()
      } else {
        withResource(GpuScalar.from(null, resultType)) { nullVal =>
          overflowed.ifElse(nullVal, sum)
        }
      }
    }
    GpuColumnVector.from(retCol, resultType)
  }

  protected def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table
}

case class GpuAdd(
    left: Expression,
    right: Expression,
    failOnError: Boolean)(
    override val origin: Origin = CurrentOrigin.get)
    extends GpuAddBase with GpuAddSub {
  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table = {
    com.nvidia.spark.rapids.jni.DecimalUtils.add128(castLhs, castRhs, outputScale)
  }
}

case class GpuSubtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean)(
    override val origin: Origin = CurrentOrigin.get)
    extends GpuSubtractBase with GpuAddSub {
  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table = {
    com.nvidia.spark.rapids.jni.DecimalUtils.subtract128(castLhs, castRhs, outputScale)
  }
}

case class GpuRemainder(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends GpuRemainderBase(left, right) {
  assert(!left.dataType.isInstanceOf[DecimalType] ||
         !right.dataType.isInstanceOf[DecimalType],
    "DecimalType remainder need to be handled by GpuDecimalRemainder")
}

object DecimalRemainderChecks {
  def neededScale(lhs: DecimalType, rhs: DecimalType): Int =
    math.max(lhs.scale, rhs.scale)

  // For Remainder, the operands need to have the same precision (for CUDF to the do the 
  // computation) *and* the same scale (to account for the part of the remainder < 1 in the output).
  // This means that first start with the needed scale (in this case the max of the scales between 
  // the 2 operands), and then account for enough space (precision) to store the resulting value 
  // without overflow
  def neededPrecision(lhs: DecimalType, rhs: DecimalType): Int =
    math.max(lhs.precision - lhs.scale, rhs.precision - rhs.scale) + neededScale(lhs, rhs)

  def intermediateArgPrecision(lhs: DecimalType, rhs: DecimalType): Int =
    math.min(
      neededPrecision(lhs, rhs),
      DType.DECIMAL128_MAX_PRECISION)

  def intermediateLhsRhsType(
      lhs: DecimalType,
      rhs: DecimalType): DecimalType = {
    val precision = intermediateArgPrecision(lhs, rhs)
    val scale = neededScale(lhs, rhs)
    DecimalType(precision, scale)
  }
}

case class GpuDecimalRemainder(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends GpuRemainderBase(left, right) with Logging {

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: min(p1-s1, p2-s2) + max(s1, s2)
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  def decimalType: DecimalType = dataType match {
    case DecimalType.Fixed(_, _) => dataType.asInstanceOf[DecimalType]
    case LongType => DecimalType.LongDecimal
  }

  private[this] lazy val lhsType: DecimalType = DecimalUtil.asDecimalType(left.dataType)
  private[this] lazy val rhsType: DecimalType = DecimalUtil.asDecimalType(right.dataType)

  // We should only use the long remainder algorithm when 
  // the intermedite precision required will overflow one of the operands
  private[this] lazy val useLongDivision: Boolean = {
    DecimalRemainderChecks.neededPrecision(lhsType, rhsType) > DType.DECIMAL128_MAX_PRECISION
  }

  // This is the type that the LHS will be cast to. The precision will match the precision of
  // the intermediate rhs (to make CUDF happy doing the divide), but the scale will be shifted
  // enough so CUDF produces the desired output scale
  private[this] lazy val intermediateLhsType =
    DecimalRemainderChecks.intermediateLhsRhsType(lhsType, rhsType)

  // This is the type that the RHS will be cast to. The precision will match the precision of the
  // intermediate lhs (to make CUDF happy doing the divide), but the scale will be the same
  // as the input RHS scale.
  private[this] lazy val intermediateRhsType =
    DecimalRemainderChecks.intermediateLhsRhsType(lhsType, rhsType)

  private[this] def divByZeroFixes(rhs: ColumnVector): ColumnVector = {
    if (failOnError) {
      withResource(GpuDivModLike.makeZeroScalar(rhs.getType)) { zeroScalar =>
        if (rhs.contains(zeroScalar)) {
          throw RapidsErrorUtils.divByZeroError(origin)
        }
      }
      rhs.incRefCount()
    } else {
      GpuDivModLike.replaceZeroWithNull(rhs)
    }
  }


  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (useLongDivision) {
      longRemainder(batch)
    } else {
      regularRemainder(batch)
    }
  }

  private def longRemainder(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      lhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, lhs.getBase.getType.getScale))
    }
    val retTab = withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        withResource(divByZeroFixes(rhs.getBase)) { fixed =>
          fixed.castTo(DType.create(DType.DTypeEnum.DECIMAL128, fixed.getType.getScale))
        }
      }
      withResource(castRhs) { castRhs =>
        com.nvidia.spark.rapids.jni.DecimalUtils.remainder128(castLhs, castRhs, -decimalType.scale)
      }
    }
    val retCol = withResource(retTab) { retTab =>
      val overflowed = retTab.getColumn(0)
      val remainder = retTab.getColumn(1)
      if (failOnError) {
        withResource(overflowed.any()) { anyOverflow =>
          if (anyOverflow.isValid && anyOverflow.getBoolean) {
            throw new ArithmeticException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
        remainder.incRefCount()
      } else {
        // With remainder, the return type can actually be a lower precision type than 
        // DECIMAL128, the output of DecimalUtils.remainder128, so we need to cast it here.
        val castRemainder = remainder.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
        withResource(castRemainder) { castRemainder =>
          withResource(GpuScalar.from(null, dataType)) { nullVal =>
            overflowed.ifElse(nullVal, castRemainder)
          }
        }
      }
    }
    GpuColumnVector.from(retCol, dataType)
  }

  private def regularRemainder(batch: ColumnarBatch): GpuColumnVector = {
    val castLhs = withResource(left.columnarEval(batch)) { lhs =>
      GpuCast.doCast(lhs.getBase, lhs.dataType(), intermediateLhsType,
        CastOptions.getArithmeticCastOptions(failOnError))
    }
    withResource(castLhs) { castLhs =>
      val castRhs = withResource(right.columnarEval(batch)) { rhs =>
        withResource(divByZeroFixes(rhs.getBase)) { fixed =>
          GpuCast.doCast(fixed, rhs.dataType(), intermediateRhsType,
            CastOptions.getArithmeticCastOptions(failOnError))
        }
      }
      withResource(castRhs) { castRhs =>
        GpuColumnVector.from(
          castLhs.mod(castRhs, GpuColumnVector.getNonNestedRapidsType(dataType)),
          dataType)
      }
    }
  }
}

case class GpuPmod(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends GpuPmodBase(left, right) {
  // This follows Remainder rule
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

object GpuDecimalDivide {
  def apply(left: Expression, right: Expression, dataType: DecimalType): GpuDecimalDivide = {
    val ansi = SQLConf.get.ansiEnabled
    GpuDecimalDivide(left, right, dataType, ansi, ansi)
  }

  def apply(left: Expression, right: Expression, dataType: DecimalType,
            failOnError: Boolean): GpuDecimalDivide =
    GpuDecimalDivide(left, right, dataType, failOnError, failOnError)
}

case class GpuDecimalDivide(
    left: Expression,
    right: Expression,
    dt: DecimalType,
    override val failOnError: Boolean,
    override val failOnDivideByZero: Boolean)
    extends CudfBinaryArithmetic with GpuDecimalDivideBase {
  override def inputType: AbstractDataType = DecimalType

  override lazy val dataType: DecimalType = dt

  override def symbol: String = "/"

  // We aren't using this
  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV

  override def integerDivide = false

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
  // Result Scale:     max(6, s1 + p2 + 1)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    if (allowPrecisionLoss) {
      val intDig = p1 - s1 + s2
      val scale = max(DecimalType.MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1)
      val prec = intDig + scale
      DecimalType.adjustPrecisionScale(prec, scale)
    } else {
      var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
      var decDig = min(DecimalType.MAX_SCALE, max(6, s1 + p2 + 1))
      val diff = (intDig + decDig) - DecimalType.MAX_SCALE
      if (diff > 0) {
        decDig -= diff / 2 + 1
        intDig = DecimalType.MAX_SCALE - decDig
      }
      DecimalType.bounded(intDig + decDig, decDig)
    }
  }
}

case class GpuDecimalMultiply(
    left: Expression,
    right: Expression,
    dt: DecimalType,
    useLongMultiply: Boolean = false,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends CudfBinaryArithmetic
    with GpuDecimalMultiplyBase {
  override def inputType: AbstractDataType = DecimalType

  override lazy val dataType: DecimalType = dt

  override def symbol: String = "*"

  // We aren't using this
  override def binaryOp: BinaryOp = BinaryOp.MUL

  override def sql: String = s"(${left.sql} * ${right.sql})"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: p1 + p2 + 1
  // Result Scale:     s1 + s2
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = s1 + s2
    val resultPrecision = p1 + p2 + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  // Should follow the nullability of GpuMultiply (inherited from
  // CudfBinaryArithmetic for Spark340+)
  // Since this is only for decimal type, no need to check the type here.
  // For decimal multiply, we may return null even if both inputs are not null,
  // if overflow happens and this `failOnError` flag is false.
  override def nullable: Boolean = left.nullable || right.nullable || !failOnError
}

case class GpuIntegralDivide(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)(
    override val origin: Origin = CurrentOrigin.get)
    extends GpuIntegralDivideParent(left, right) {
  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  assert(!left.dataType.isInstanceOf[DecimalType] ||
         !right.dataType.isInstanceOf[DecimalType],
    "DecimalType integral divides need to be handled by GpuIntegralDecimalDivide")
}

case class GpuIntegralDecimalDivide(
    left: Expression,
    right: Expression,
    override val failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends CudfBinaryArithmetic with GpuDecimalDivideBase {
  override val failOnDivideByZero: Boolean = failOnError

  override def inputType: AbstractDataType = TypeCollection(IntegralType, DecimalType)

  def integerDivide: Boolean = true

  override lazy val dataType: DataType = LongType

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.DIV

  override def sqlOperator: String = "div"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    super.columnarEval(batch)
  }

  /**
   * We are not overriding resultDecimalType as the method `dataType` is overridden in this class
   * and so the superclass method that calls resultDecimalType will never be called.
  */
}
