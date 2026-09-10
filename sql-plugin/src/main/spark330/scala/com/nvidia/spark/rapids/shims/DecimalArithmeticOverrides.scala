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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{BaseExprMeta, BinaryAstExprMeta, BinaryExprMeta, CastExprMeta, DecimalUtil, ExprChecks, ExprMeta, ExprRule, GpuCheckOverflow, GpuExpression, GpuPromotePrecision, LiteralExprMeta, TypeSig, UnaryExprMeta}
import com.nvidia.spark.rapids.GpuOverrides.expr

import org.apache.spark.sql.catalyst.expressions.{CastBase, CheckOverflow, Divide, Expression, IntegralDivide, Literal, Multiply, PromotePrecision, Remainder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{DecimalMultiplyChecks, GpuAnsi, GpuDecimalDivide, GpuDecimalMultiply, GpuDivide, GpuIntegralDivide, GpuMultiply, GpuRemainder}
import org.apache.spark.sql.types.{Decimal, DecimalType}

object DecimalArithmeticOverrides {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[PromotePrecision](
      "PromotePrecision before arithmetic operations between DecimalType data",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DECIMAL_128,
        TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[PromotePrecision](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuPromotePrecision(child)
      }),
    expr[CheckOverflow](
      "CheckOverflow after arithmetic operations between DecimalType data",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DECIMAL_128,
        TypeSig.DECIMAL_128),
      (a, conf, p, r) => new ExprMeta[CheckOverflow](a, conf, p, r) {
        // CheckOverflow prevents all bridge optimization in the tree. Spark's CPU
        // CheckOverflow evaluates the child with Scala Decimal/BigDecimal values and then
        // checks whether the final value fits the target DecimalType. The GPU path cannot
        // carry intermediate decimal values outside cuDF/GPU bounds; decimal arithmetic
        // overflow is checked in the GPU arithmetic expressions themselves, making
        // GpuCheckOverflow a wrapper around an already-checked value. Because the CPU and
        // GPU CheckOverflow contracts are not compatible, expression trees that contain
        // CheckOverflow cannot safely use CPU bridge optimization.
        override def preventsTreeBridgeOptimization: Boolean = true
        
        private[this] def extractOrigParam(expr: BaseExprMeta[_]): BaseExprMeta[_] =
          expr.wrapped match {
            case lit: Literal if lit.dataType.isInstanceOf[DecimalType] =>
              // Lets figure out if we can make the Literal value smaller
              val (newType, value) = lit.value match {
                case null =>
                  (DecimalType(0, 0), null)
                case dec: Decimal =>
                  val stripped = Decimal(dec.toJavaBigDecimal.stripTrailingZeros())
                  val p = stripped.precision
                  val s = stripped.scale
                  val t = if (s < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
                    // need to adjust to avoid errors about negative scale
                    DecimalType(p - s, 0)
                  } else {
                    DecimalType(p, s)
                  }
                  (t, stripped)
                case other =>
                  throw new IllegalArgumentException(s"Unexpected decimal literal value $other")
              }
              expr.asInstanceOf[LiteralExprMeta].withNewLiteral(Literal(value, newType))
            // Avoid unapply for PromotePrecision and Cast because it changes between Spark
            // versions
            case p: PromotePrecision if p.child.isInstanceOf[CastBase] &&
                p.child.dataType.isInstanceOf[DecimalType] =>
              val c = p.child.asInstanceOf[CastBase]
              val to = c.dataType.asInstanceOf[DecimalType]
              val fromType = DecimalUtil.optionallyAsDecimalType(c.child.dataType)
              fromType match {
                case Some(from) =>
                  val minScale = math.min(from.scale, to.scale)
                  val fromWhole = from.precision - from.scale
                  val toWhole = to.precision - to.scale
                  val minWhole = if (to.scale < from.scale) {
                    // If the scale is getting smaller in the worst case we need an
                    // extra whole part to handle rounding up.
                    math.min(fromWhole + 1, toWhole)
                  } else {
                    math.min(fromWhole, toWhole)
                  }
                  val newToType = DecimalType(minWhole + minScale, minScale)
                  if (newToType == from) {
                    // We can remove the cast totally
                    val castExpr = expr.childExprs.head
                    castExpr.childExprs.head
                  } else if (newToType == to) {
                    // The cast is already ideal
                    expr
                  } else {
                    val castExpr = expr.childExprs.head.asInstanceOf[CastExprMeta[_]]
                    castExpr.withToTypeOverride(newToType)
                  }
                case _ =>
                  expr
              }
            case _ => expr
          }
        private[this] lazy val binExpr = childExprs.head
        private[this] lazy val lhs = extractOrigParam(binExpr.childExprs.head)
        private[this] lazy val rhs = extractOrigParam(binExpr.childExprs(1))
        private[this] lazy val lhsDecimalType =
          DecimalUtil.asDecimalType(lhs.wrapped.asInstanceOf[Expression].dataType)
        private[this] lazy val rhsDecimalType =
          DecimalUtil.asDecimalType(rhs.wrapped.asInstanceOf[Expression].dataType)

        override def convertToGpuImpl(): GpuExpression = {
          // Prior to Spark 3.4.0
          // Division and Multiplication of Decimal types is a little odd. Spark will cast the
          // inputs to a common wider value where the scale is the max of the two input scales,
          // and the precision is max of the two input non-scale portions + the new scale. Then it
          // will do the divide or multiply as a BigDecimal value but lie about the return type.
          // Finally here in CheckOverflow it will reset the scale and check the precision so that
          // Spark knows it fits in the final desired result.
          // Here we try to strip out the extra casts, etc to get to as close to the original
          // query as possible. This lets us then calculate what CUDF needs to get the correct
          // answer, which in some cases is a lot smaller.

          a.child match {
            case _: Divide =>
              // GpuDecimalDivide includes the overflow check in it.
              GpuDecimalDivide(lhs.convertToGpu(), rhs.convertToGpu(), wrapped.dataType)
            case _: Multiply =>
              // GpuDecimal*Multiply includes the overflow check in it
              val intermediatePrecision =
                DecimalMultiplyChecks.nonRoundedIntermediatePrecision(lhsDecimalType,
                  rhsDecimalType, a.dataType)
              GpuDecimalMultiply(lhs.convertToGpu(), rhs.convertToGpu(), wrapped.dataType,
                useLongMultiply = intermediatePrecision > DType.DECIMAL128_MAX_PRECISION)
            case _ =>
              GpuCheckOverflow(childExprs.head.convertToGpu(),
                wrapped.dataType, wrapped.nullOnOverflow)
          }
        }
      }),
    expr[Multiply](
      "Multiplication",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric,
        TypeSig.cpuNumeric,
        ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
        ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryAstExprMeta[Multiply](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Check if this Multiply expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_multiply is not supported on GPU")
          }
        }

        override def tagSelfForAst(): Unit = {
          super.tagSelfForAst();
          if (SQLConf.get.ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("GPU AST multiplication does not support ANSI mode")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          a.dataType match {
            case _: DecimalType => throw new IllegalStateException(
              "Decimal Multiply should be converted in CheckOverflow")
            case _ =>
              GpuMultiply(lhs, rhs)(a.origin)
          }
        }
      }),
    expr[Divide](
      "Division",
      ExprChecks.binaryProject(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        ("lhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
            TypeSig.DOUBLE + TypeSig.DECIMAL_128),
        ("rhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
            TypeSig.DOUBLE + TypeSig.DECIMAL_128)),
      (a, conf, p, r) => new BinaryExprMeta[Divide](a, conf, p, r) {
        // Division of Decimal types is a little odd. To work around some issues with
        // what Spark does the tagging/checks are in CheckOverflow instead of here.
        override def tagExprForGpu(): Unit = {
          // Check if this Divide expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_divide is not supported on GPU")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          a.dataType match {
            case _: DecimalType =>
              throw new IllegalStateException("Internal Error: Decimal Divide operations " +
                  "should be converted to the GPU in the CheckOverflow rule")
            case _ =>
              GpuDivide(lhs, rhs)(a.origin)
          }
      }),
    expr[IntegralDivide](
      "Division with a integer result",
      ExprChecks.binaryProject(
        TypeSig.LONG, TypeSig.LONG,
        ("lhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128),
        ("rhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128)),
      (a, conf, p, r) => new BinaryExprMeta[IntegralDivide](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuIntegralDivide(lhs, rhs)(a.origin)
      }),
    expr[Remainder](
      "Remainder or modulo",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
        ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[Remainder](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Check if this Remainder expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_mod is not supported on GPU")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuRemainder(lhs, rhs)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
