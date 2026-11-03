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

import com.nvidia.spark.rapids.GpuOverrides._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.types._

case class PmodRuleMeta(
    a: Pmod,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Pmod](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.dataType match {
      case dt: DecimalType if dt.precision == DecimalType.MAX_PRECISION =>
        willNotWorkOnGpu("pmod at maximum decimal precision is not supported")
      case _ =>
    }
  }
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuPmod(lhs, rhs)
}

case class AddRuleMeta(
    a: Add,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[Add](a, conf, p, r) {
  private val ansiEnabled = SQLConf.get.ansiEnabled

  override def tagExprForGpu(): Unit = {
    // Check if this Add expression is in TRY mode context
    if (TryModeShim.isTryMode(a)) {
      willNotWorkOnGpu("try_add is not supported on GPU")
    }
  }

  override def tagSelfForAst(): Unit = {
    if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
      willNotWorkInAst("AST Addition does not support ANSI mode.")
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuAdd(lhs, rhs, ansiEnabled)(a.origin)
}

case class SubtractRuleMeta(
    a: Subtract,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[Subtract](a, conf, p, r) {
  private val ansiEnabled = SQLConf.get.ansiEnabled

  override def tagExprForGpu(): Unit = {
    // Check if this Subtract expression is in TRY mode context
    if (TryModeShim.isTryMode(a)) {
      willNotWorkOnGpu("try_subtract is not supported on GPU")
    }
  }

  override def tagSelfForAst(): Unit = {
    if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
      willNotWorkInAst("AST Subtraction does not support ANSI mode.")
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuSubtract(lhs, rhs, ansiEnabled)(a.origin)
}

case class AndRuleMeta(
    a: And,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[And](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuAnd(lhs, rhs)
}

case class OrRuleMeta(
    a: Or,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Or](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuOr(lhs, rhs)
}

case class EqualNullSafeRuleMeta(
    a: EqualNullSafe,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[EqualNullSafe](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuEqualNullSafe(lhs, rhs)
}

case class EqualToRuleMeta(
    a: EqualTo,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[EqualTo](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuEqualTo(lhs, rhs)
}

case class GreaterThanRuleMeta(
    a: GreaterThan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[GreaterThan](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuStringInstr.optimizeContains(GpuGreaterThan(lhs, rhs))
}

case class GreaterThanOrEqualRuleMeta(
    a: GreaterThanOrEqual,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[GreaterThanOrEqual](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuStringInstr.optimizeContains(GpuGreaterThanOrEqual(lhs, rhs))
}

case class InRuleMeta(
    in: In,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[In](in, conf, p, r) {
  private val allListItemsAreLiterals = in.list.forall(_.isInstanceOf[Literal])
  private lazy val dynamicListItems = in.list.zip(childExprs.tail).filterNot {
    case (expression, _) => expression.isInstanceOf[Literal]
  }
  private lazy val gpuDynamicExpressions = dynamicListItems.map(_._2.convertToGpu())

  override def tagExprForGpu(): Unit = {
    if (!allListItemsAreLiterals) {
      if (dynamicListItems.length > GpuIn.MAX_DYNAMIC_LIST_SIZE) {
        willNotWorkOnGpu(
          s"dynamic IN lists with more than ${GpuIn.MAX_DYNAMIC_LIST_SIZE} " +
            "non-literal expressions are not supported")
      } else if (!dynamicListItems.forall(_._1.deterministic)) {
        willNotWorkOnGpu("dynamic IN list expressions must be deterministic")
      } else if (!dynamicListItems.forall(_._2.canExprTreeBeReplaced)) {
        // CPU bridges are inserted after tagging, so verify the entire expression tree
        // before converting it to inspect side effects.
        willNotWorkOnGpu("dynamic IN list expressions must run entirely on GPU")
      } else if (gpuDynamicExpressions.exists {
          case gpuExpression: GpuExpression => gpuExpression.hasSideEffects
          case _ => false
        }) {
        willNotWorkOnGpu("dynamic IN list expressions with side effects are not supported")
      }
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    val gpuValue = childExprs.head.convertToGpu()
    if (allListItemsAreLiterals) {
      GpuInSet(gpuValue, in.list.asInstanceOf[Seq[Literal]].map(_.value),
        useInSetSemantics = false)
    } else {
      val literalValues = in.list.collect { case literal: Literal => literal.value }
      GpuIn(gpuValue, literalValues, gpuDynamicExpressions)
    }
  }
}

case class InSetRuleMeta(
    in: InSet,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[InSet](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuInSet(childExprs.head.convertToGpu(), in.hset.toSeq)
}

case class LessThanRuleMeta(
    a: LessThan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[LessThan](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuStringInstr.optimizeContains(GpuLessThan(lhs, rhs))
}

case class LessThanOrEqualRuleMeta(
    a: LessThanOrEqual,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[LessThanOrEqual](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuStringInstr.optimizeContains(GpuLessThanOrEqual(lhs, rhs))
}

case class PowRuleMeta(
    a: Pow,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryAstExprMeta[Pow](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuPow(lhs, rhs)
}

case class PivotFirstRuleMeta(
    pivot: PivotFirst,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ImperativeAggExprMeta[PivotFirst](pivot, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    pivot.pivotColumn.dataType match {
      // `StringType` is the UTF8_BINARY singleton, while `st` may be another
      // StringType instance whose collation differs from UTF8_BINARY in Spark 4.x.
      case st: StringType if st != StringType =>
        willNotWorkOnGpu(
          "PivotFirst does not support non-UTF8_BINARY string collations on the GPU")
      case _ =>
    }
    // If pivotColumnValues doesn't have distinct values, fall back to CPU
    if (pivot.pivotColumnValues.distinct.lengthCompare(pivot.pivotColumnValues.length) != 0) {
      willNotWorkOnGpu("PivotFirst does not work on the GPU when there are duplicate" +
          " pivot values provided")
    }
  }
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val Seq(pivotColumn, valueColumn) = childExprs
    GpuPivotFirst(pivotColumn, valueColumn, pivot.pivotColumnValues)
  }

  // Pivot does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class CountRuleMeta(
    count: Count,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[Count](count, conf, p, r) {
  // Spark Count agg returns Long and does not check Ansi mode and overflow
  override def needsAnsiCheck: Boolean = false

  override def tagAggForGpu(): Unit = {
    if (count.children.size > 1) {
      willNotWorkOnGpu("count of multiple columns not supported")
    }
  }
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuCount(childExprs)
}

case class MaxRuleMeta(
    max: Max,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[Max](max, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuMax(childExprs.head)

  // Max does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class MinRuleMeta(
    a: Min,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[Min](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuMin(childExprs.head)

  // Min does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class SumRuleMeta(
    a: Sum,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[Sum](a, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    val inputDataType = a.child.dataType
    checkAndTagFloatAgg(inputDataType, this.conf, this)

    // Check if this Sum expression is in TRY mode context
    if (TryModeShim.isTryMode(a)) {
      willNotWorkOnGpu("try_sum is not supported on GPU")
    }
  }

  override def needsAnsiCheck: Boolean = false

  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuSum(childExprs.head, a.dataType)
}

case class NthValueRuleMeta(
    a: NthValue,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[NthValue](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuNthValue(childExprs.head, a.offset, a.ignoreNulls)

  // nth does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class FirstRuleMeta(
    a: First,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[First](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuFirst(childExprs.head, a.ignoreNulls)

  // First does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class LastRuleMeta(
    a: Last,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[Last](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuLast(childExprs.head, a.ignoreNulls)

  // Last does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class MaxByRuleMeta(
    maxBy: MaxBy,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[MaxBy](maxBy, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    // Only two children (value expression, ordering expression)
    require(childExprs.length == 2)
    GpuMaxBy(childExprs.head, childExprs.last)
  }

  // MaxBy does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class MinByRuleMeta(
    minBy: MinBy,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[MinBy](minBy, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    // Only two children (value expression, ordering expression)
    require(childExprs.length == 2)
    GpuMinBy(childExprs.head, childExprs.last)
  }

  // MinBy does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class BRoundRuleMeta(
    a: BRound,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuBRoundMeta(a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.child.dataType match {
      case FloatType | DoubleType if !this.conf.isIncompatEnabled =>
        willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
            s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
      case _ => // NOOP
    }
  }
}

case class RoundRuleMeta(
    a: Round,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuRoundMeta(a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.child.dataType match {
      case FloatType | DoubleType if !this.conf.isIncompatEnabled =>
        willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
            s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
      case _ => // NOOP
    }
  }
}
