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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class PreciseTimestampConversionRuleMeta(
    a: PreciseTimestampConversion,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[PreciseTimestampConversion](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuPreciseTimestampConversion(child, a.fromType, a.toType)
}

case class UnaryMinusRuleMeta(
    a: UnaryMinus,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[UnaryMinus](a, conf, p, r) {
  val ansiEnabled = SQLConf.get.ansiEnabled

  override def tagSelfForAst(): Unit = {
    if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
      willNotWorkInAst("AST unary minus does not support ANSI mode.")
    }
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuUnaryMinus(child, ansiEnabled)
}

case class UnaryPositiveRuleMeta(
    a: UnaryPositive,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[UnaryPositive](a, conf, p, r) {
  override val isFoldableNonLitAllowed: Boolean = true
  override def convertToGpuImpl(): GpuExpression =
    GpuUnaryPositive(childExprs.head.convertToGpu())
}

case class YearRuleMeta(
    a: Year,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Year](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuYear(child)
}

case class MonthRuleMeta(
    a: Month,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Month](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuMonth(child)
}

case class QuarterRuleMeta(
    a: Quarter,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Quarter](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuQuarter(child)
}

case class DayOfMonthRuleMeta(
    a: DayOfMonth,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[DayOfMonth](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuDayOfMonth(child)
}

case class DayOfYearRuleMeta(
    a: DayOfYear,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[DayOfYear](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuDayOfYear(child)
}

case class SecondsToTimestampRuleMeta(
    a: SecondsToTimestamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[SecondsToTimestamp](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuSecondsToTimestamp(child)
}

case class MillisToTimestampRuleMeta(
    a: MillisToTimestamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MillisToTimestamp](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMillisToTimestamp(child)
}

case class MicrosToTimestampRuleMeta(
    a: MicrosToTimestamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MicrosToTimestamp](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMicrosToTimestamp(child)
}

case class AcosRuleMeta(
    a: Acos,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Acos](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
}

case class AcoshRuleMeta(
    a: Acosh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Acosh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    if (this.conf.includeImprovedFloat) {
      GpuAcoshImproved(child)
    } else {
      GpuAcoshCompat(child)
    }
}

case class AsinRuleMeta(
    a: Asin,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Asin](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuAsin(child)
}

case class AsinhRuleMeta(
    a: Asinh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Asinh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    if (this.conf.includeImprovedFloat) {
      GpuAsinhImproved(child)
    } else {
      GpuAsinhCompat(child)
    }

  override def tagSelfForAst(): Unit = {
    if (!this.conf.includeImprovedFloat) {
      // AST is not expressive enough yet to implement the conditional expression needed
      // to emulate Spark's behavior
      willNotWorkInAst("asinh is not AST compatible unless " +
          s"${RapidsConf.IMPROVED_FLOAT_OPS.key} is enabled")
    }
  }
}

case class SqrtRuleMeta(
    a: Sqrt,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Sqrt](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuSqrt(child)
}

case class CbrtRuleMeta(
    a: Cbrt,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Cbrt](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuCbrt(child)
}

case class HypotRuleMeta(
    a: Hypot,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Hypot](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuHypot(lhs, rhs)
}

case class FloorRuleMeta(
    a: Floor,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Floor](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.dataType match {
      case dt: DecimalType =>
        val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
        if (precision > DType.DECIMAL128_MAX_PRECISION) {
          willNotWorkOnGpu(s"output precision $precision would require overflow " +
              s"checks, which are not supported yet")
        }
      case _ => // NOOP
    }
  }

  override def convertToGpu(child: Expression): GpuExpression = {
    // use Spark `Floor.dataType` to keep consistent between Spark versions.
    GpuFloor(child, a.dataType)
  }
}

case class CeilRuleMeta(
    a: Ceil,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Ceil](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.dataType match {
      case dt: DecimalType =>
        val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
        if (precision > DType.DECIMAL128_MAX_PRECISION) {
          willNotWorkOnGpu(s"output precision $precision would require overflow " +
              s"checks, which are not supported yet")
        }
      case _ => // NOOP
    }
  }

  override def convertToGpu(child: Expression): GpuExpression = {
    // use Spark `Ceil.dataType` to keep consistent between Spark versions.
    GpuCeil(child, a.dataType)
  }
}

case class NotRuleMeta(
    a: Not,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Not](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuNot(child)
}

case class IsNullRuleMeta(
    a: IsNull,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[IsNull](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuIsNull(child)
}

case class IsNotNullRuleMeta(
    a: IsNotNull,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[IsNotNull](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuIsNotNull(child)
}

case class IsNaNRuleMeta(
    a: IsNaN,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[IsNaN](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuIsNan(child)
}

case class RintRuleMeta(
    a: Rint,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Rint](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuRint(child)
}

case class AtLeastNNonNullsRuleMeta(
    a: AtLeastNNonNulls,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[AtLeastNNonNulls](a, conf, p, r) {
  def convertToGpuImpl(): GpuExpression =
    GpuAtLeastNNonNulls(a.n, childExprs.map(_.convertToGpu()))
}

case class DateAddRuleMeta(
    a: DateAdd,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[DateAdd](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuDateAdd(lhs, rhs)
}

case class DateSubRuleMeta(
    a: DateSub,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[DateSub](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuDateSub(lhs, rhs)
}

case class NaNvlRuleMeta(
    a: NaNvl,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[NaNvl](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuNaNvl(lhs, rhs)
}

case class ShiftLeftRuleMeta(
    a: ShiftLeft,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ShiftLeft](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuShiftLeft(lhs, rhs)
}

case class ShiftRightRuleMeta(
    a: ShiftRight,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ShiftRight](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuShiftRight(lhs, rhs)
}

case class ShiftRightUnsignedRuleMeta(
    a: ShiftRightUnsigned,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuShiftRightUnsigned(lhs, rhs)
}

case class BitwiseAndRuleMeta(
    a: BitwiseAnd,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[BitwiseAnd](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuBitwiseAnd(lhs, rhs)
}

case class BitwiseOrRuleMeta(
    a: BitwiseOr,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[BitwiseOr](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuBitwiseOr(lhs, rhs)
}

case class BitwiseXorRuleMeta(
    a: BitwiseXor,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[BitwiseXor](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuBitwiseXor(lhs, rhs)
}

case class BitwiseNotRuleMeta(
    a: BitwiseNot,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[BitwiseNot](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuBitwiseNot(child)
}

case class BitwiseCountRuleMeta(
    a: BitwiseCount,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[BitwiseCount](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuBitwiseCount(child)
}

case class BitAndAggRuleMeta(
    a: BitAndAgg,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[BitAndAgg](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuBitAndAgg(childExprs.head)

  override def needsAnsiCheck: Boolean = false
}

case class BitOrAggRuleMeta(
    a: BitOrAgg,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[BitOrAgg](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuBitOrAgg(childExprs.head)

  override def needsAnsiCheck: Boolean = false
}

case class BitXorAggRuleMeta(
    a: BitXorAgg,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[BitXorAgg](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuBitXorAgg(childExprs.head)

  override def needsAnsiCheck: Boolean = false
}

case class CoalesceRuleMeta(
    a: Coalesce,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[Coalesce](a, conf, p, r) {
  // Allow foldable non-literal Coalesce (e.g. coalesce(cast(null as bigint), -1001)):
  // AQE can regenerate these after ConstantFolding ran; GpuCoalesce evaluates them on GPU.
  override val isFoldableNonLitAllowed: Boolean = true
  override def convertToGpuImpl(): GpuExpression =
    GpuCoalesce(childExprs.map(_.convertToGpu()))
}

case class LeastRuleMeta(
    a: Least,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[Least](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuLeast(childExprs.map(_.convertToGpu()))
}

case class GreatestRuleMeta(
    a: Greatest,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[Greatest](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuGreatest(childExprs.map(_.convertToGpu()))
}

case class AtanRuleMeta(
    a: Atan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Atan](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuAtan(child)
}

case class AtanhRuleMeta(
    a: Atanh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Atanh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuAtanh(child)
}

case class CosRuleMeta(
    a: Cos,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Cos](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuCos(child)
}

case class ExpRuleMeta(
    a: Exp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Exp](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuExp(child)
}

case class Expm1RuleMeta(
    a: Expm1,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Expm1](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuExpm1(child)
}

case class InitCapRuleMeta(
    a: InitCap,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[InitCap](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuInitCap(child)
}

case class LogRuleMeta(
    a: Log,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Log](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuLog(child)
}

case class Log1pRuleMeta(
    a: Log1p,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Log1p](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = {
    // No need for overflow checking on the GpuAdd in Double as Double handles overflow
    // the same in all modes.
    GpuLog(GpuAdd(child, GpuLiteral(1d, DataTypes.DoubleType), false)())
  }
}

case class Log2RuleMeta(
    a: Log2,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Log2](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuLogarithm(child, GpuLiteral(2d, DataTypes.DoubleType))
}

case class Log10RuleMeta(
    a: Log10,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Log10](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuLogarithm(child, GpuLiteral(10d, DataTypes.DoubleType))
}

case class LogarithmRuleMeta(
    a: Logarithm,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Logarithm](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    // the order of the parameters is transposed intentionally
    GpuLogarithm(rhs, lhs)
}

case class SinRuleMeta(
    a: Sin,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Sin](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuSin(child)
}

case class SinhRuleMeta(
    a: Sinh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Sinh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuSinh(child)
}

case class CoshRuleMeta(
    a: Cosh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Cosh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuCosh(child)
}

case class CotRuleMeta(
    a: Cot,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Cot](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuCot(child)
}

case class TanhRuleMeta(
    a: Tanh,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Tanh](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuTanh(child)
}

case class TanRuleMeta(
    a: Tan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Tan](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuTan(child)
}

case class NormalizeNaNAndZeroRuleMeta(
    a: NormalizeNaNAndZero,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[NormalizeNaNAndZero](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuNormalizeNaNAndZero(child)
}

case class KnownFloatingPointNormalizedRuleMeta(
    a: KnownFloatingPointNormalized,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[KnownFloatingPointNormalized](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuKnownFloatingPointNormalized(child)
}

case class KnownNotNullRuleMeta(
    k: KnownNotNull,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[KnownNotNull](k, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuKnownNotNull(child)
}

case class DateDiffRuleMeta(
    a: DateDiff,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[DateDiff](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuDateDiff(lhs, rhs)
  }
}

case class DateAddIntervalRuleMeta(
    dateAddInterval: DateAddInterval,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[DateAddInterval](dateAddInterval, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    GpuOverrides.extractLit(dateAddInterval.interval).foreach { lit =>
      val intvl = lit.value.asInstanceOf[CalendarInterval]
      if (intvl.months != 0) {
        willNotWorkOnGpu("interval months isn't supported")
      }
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuDateAddInterval(lhs, rhs)
}

case class DateFormatClassRuleMeta(
    a: DateFormatClass,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnixTimeExprMeta[DateFormatClass](a, conf, p, r) {
  override def isTimeZoneSupported = true
  override protected def allowLegacyFormattingOnlyFormats: Boolean = true
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuDateFormatClass(lhs, rhs, strfFormat, a.timeZoneId)
}

case class ToUnixTimestampRuleMeta(
    a: ToUnixTimestamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnixTimeExprMeta[ToUnixTimestamp](a, conf, p, r) {
  // String type is not supported yet for non-UTC timezone.
  override def isTimeZoneSupported = true
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuToUnixTimestamp(lhs, rhs, sparkFormat, strfFormat, a.timeZoneId)
  }
}

case class UnixTimestampRuleMeta(
    a: UnixTimestamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnixTimeExprMeta[UnixTimestamp](a, conf, p, r) {
  override def isTimeZoneSupported = true
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuUnixTimestamp(lhs, rhs, sparkFormat, strfFormat, a.timeZoneId)
  }
}

case class HourRuleMeta(
    hour: Hour,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Hour](hour, conf, p, r) {
  override def isTimeZoneSupported = true
  override def convertToGpu(expr: Expression): GpuExpression = GpuHour(expr, hour.timeZoneId)
}

case class MinuteRuleMeta(
    minute: Minute,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Minute](minute, conf, p, r) {
  override def isTimeZoneSupported = true
  override def convertToGpu(expr: Expression): GpuExpression =
    GpuMinute(expr, minute.timeZoneId)
}

case class SecondRuleMeta(
    second: Second,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Second](second, conf, p, r) {
  override def isTimeZoneSupported = true
  override def convertToGpu(expr: Expression): GpuExpression =
    GpuSecond(expr, second.timeZoneId)
}

case class WeekDayRuleMeta(
    a: WeekDay,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[WeekDay](a, conf, p, r) {
  override def convertToGpu(expr: Expression): GpuExpression =
    GpuWeekDay(expr)
}

case class DayOfWeekRuleMeta(
    a: DayOfWeek,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[DayOfWeek](a, conf, p, r) {
  override def convertToGpu(expr: Expression): GpuExpression =
    GpuDayOfWeek(expr)
}

case class LastDayRuleMeta(
    a: LastDay,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[LastDay](a, conf, p, r) {
  override def convertToGpu(expr: Expression): GpuExpression =
    GpuLastDay(expr)
}


case class FromUnixTimeConstructorRuleMeta(
    a: FromUnixTime,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends FromUnixTimeMeta(a, conf, parent, r)

case class FromUTCTimestampConstructorRuleMeta(
    a: FromUTCTimestamp,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends FromUTCTimestampExprMeta(a, conf, parent, r)

case class ToUTCTimestampConstructorRuleMeta(
    a: ToUTCTimestamp,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ToUTCTimestampExprMeta(a, conf, parent, r)

case class MonthsBetweenConstructorRuleMeta(
    a: MonthsBetween,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends MonthsBetweenExprMeta(a, conf, parent, r)

case class TruncDateConstructorRuleMeta(
    a: TruncDate,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TruncDateExprMeta(a, conf, parent, r)

case class TruncTimestampConstructorRuleMeta(
    a: TruncTimestamp,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TruncTimestampExprMeta(a, conf, parent, r)
