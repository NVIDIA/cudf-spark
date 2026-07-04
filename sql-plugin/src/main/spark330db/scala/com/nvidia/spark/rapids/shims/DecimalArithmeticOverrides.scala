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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{BinaryAstExprMeta, DecimalUtil, ExprChecks, ExprRule, GpuExpression, TypeSig}
import com.nvidia.spark.rapids.GpuOverrides.expr

import org.apache.spark.sql.catalyst.expressions.{Divide, Expression, IntegralDivide, Multiply, Remainder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{DecimalIntegralDivideChecks, DecimalMultiplyChecks, DecimalRemainderChecks, GpuAnsi, GpuDecimalDivide, GpuDecimalMultiply, GpuDecimalRemainder, GpuDivide, GpuIntegralDecimalDivide, GpuIntegralDivide, GpuMultiply, GpuRemainder}
import org.apache.spark.sql.types.DecimalType

object DecimalArithmeticOverrides {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    // We don't override PromotePrecision or CheckOverflow for Spark 3.4
    Seq(
      expr[Multiply](
        "Multiplication",
        ExprChecks.binaryProjectAndAst(
          TypeSig.checkedArithmeticAstTypes + TypeSig.DECIMAL_128,
          TypeSig.gpuNumeric,
          TypeSig.cpuNumeric,
          ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
          ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
        (a, conf, p, r) => new BinaryAstExprMeta[Multiply](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled
          private val tryMode = TryModeShim.isTryMode(a)

          override def tagExprForGpu(): Unit = {
            if (tryMode && (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                !GpuAnsi.supportsAnsiArithmeticAst(a.dataType))) {
              willNotWorkOnGpu(
                "try_multiply supports integral types only when row IR JIT support is enabled")
            }
          }

          override def tagSelfForAst(): Unit = {
            a.dataType match {
              case _: DecimalType =>
                if (!this.conf.isProjectAstAnsiArithmeticEnabled) {
                  willNotWorkInAst("AST decimal multiplication requires row IR JIT support.")
                } else if (!DecimalMultiplyChecks.canUseAst(
                    a.left.dataType, a.right.dataType, a.dataType)) {
                  willNotWorkInAst(
                    "AST decimal multiplication requires an exact result without rounding or " +
                      "overflow.")
                }
              case _ => super.tagSelfForAst()
            }
            if (tryMode && (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                !GpuAnsi.supportsAnsiArithmeticAst(a.dataType))) {
              willNotWorkInAst("AST try_multiply requires integral row IR JIT support.")
            } else if (!tryMode && !ansiEnabled &&
                GpuAnsi.requiresRowIrArithmeticAst(a.dataType)) {
              willNotWorkInAst(
                "AST Byte/Short multiplication requires ANSI row IR JIT support.")
            }
            if (!tryMode && ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType) &&
                (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                    !GpuAnsi.supportsAnsiArithmeticAst(a.dataType))) {
              willNotWorkInAst("GPU AST multiplication does not support ANSI mode")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            lazy val lhsDecimalType =
              DecimalUtil.asDecimalType(lhs.dataType)
            lazy val rhsDecimalType =
              DecimalUtil.asDecimalType(rhs.dataType)

            a.dataType match {
              case d: DecimalType =>
                val intermediatePrecision =
                  DecimalMultiplyChecks.nonRoundedIntermediatePrecision(lhsDecimalType,
                    rhsDecimalType, d)
                GpuDecimalMultiply(lhs, rhs, d,
                  useLongMultiply = intermediatePrecision > DType.DECIMAL128_MAX_PRECISION)
              case _ =>
                GpuMultiply(lhs, rhs, ansiEnabled && !tryMode, tryMode)(a.origin)
            }
          }
        }),
      expr[Divide](
        "Division",
        ExprChecks.binaryProjectAndAst(
          TypeSig.DOUBLE,
          TypeSig.DOUBLE + TypeSig.DECIMAL_128,
          TypeSig.DOUBLE + TypeSig.DECIMAL_128,
          ("lhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
              TypeSig.DOUBLE + TypeSig.DECIMAL_128),
          ("rhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
              TypeSig.DOUBLE + TypeSig.DECIMAL_128)),
        (a, conf, p, r) => new BinaryAstExprMeta[Divide](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled
          private val tryMode = TryModeShim.isTryMode(a)
          private val failOnError = ansiEnabled && !tryMode
          private def leftInputIsSafe: Boolean =
            childExprs.head.canBePrecomputedForJoin

          override def tagExprForGpu(): Unit = {
            if (tryMode && (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                !GpuAnsi.supportsTrueDivideAst(
                  false, a.left.dataType, a.right.dataType))) {
              willNotWorkOnGpu(
                "try_divide supports DOUBLE inputs only when row IR JIT support is enabled")
            } else if (!failOnError && !leftInputIsSafe) {
              willNotWorkOnGpu(
                "non-ANSI division cannot eagerly evaluate a side-effecting left operand")
            }
          }

          override def tagSelfForAst(): Unit = {
            super.tagSelfForAst()
            if (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                !GpuAnsi.supportsTrueDivideAst(
                  failOnError, a.left.dataType, a.right.dataType)) {
              willNotWorkInAst(
                "AST true division supports non-ANSI DOUBLE inputs with row IR JIT support.")
            } else if (!leftInputIsSafe) {
              willNotWorkInAst(
                "AST true division cannot eagerly evaluate a side-effecting left operand.")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            a.dataType match {
              case d: DecimalType =>
                GpuDecimalDivide(lhs, rhs, d)
              case _ =>
                GpuDivide(lhs, rhs, failOnError)(a.origin)
            }
        }),
      expr[IntegralDivide](
        "Division with a integer result",
        ExprChecks.binaryProjectAndAst(
          TypeSig.LONG + TypeSig.DECIMAL_64,
          TypeSig.LONG, TypeSig.LONG,
          ("lhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128),
          ("rhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128)),
        (a, conf, p, r) => new BinaryAstExprMeta[IntegralDivide](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled

          override def tagSelfForAst(): Unit = {
            super.tagSelfForAst()
            if (!this.conf.isProjectAstAnsiArithmeticEnabled) {
              willNotWorkInAst("AST integral divide requires row IR JIT support.")
            } else if (a.left.dataType.isInstanceOf[DecimalType]) {
              if (!DecimalIntegralDivideChecks.canUseAst(
                  a.left.dataType, a.right.dataType, a.dataType)) {
                willNotWorkInAst(
                  "AST decimal integral divide requires identical inputs with precision at most 18.")
              }
            } else if (!GpuAnsi.supportsIntegralDivideAst(
                a.left.dataType, a.right.dataType)) {
              willNotWorkInAst("AST integral divide requires matching INT or LONG inputs.")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            if (lhs.dataType.isInstanceOf[DecimalType] && rhs.dataType.isInstanceOf[DecimalType]) {
              GpuIntegralDecimalDivide(lhs, rhs)
            } else {
              GpuIntegralDivide(lhs, rhs, ansiEnabled)(a.origin)
            }
        }),

      expr[Remainder](
        "Remainder or modulo",
        ExprChecks.binaryProjectAndAst(
          TypeSig.BYTE + TypeSig.SHORT + TypeSig.INT + TypeSig.LONG +
            TypeSig.FLOAT + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
          TypeSig.gpuNumeric, TypeSig.cpuNumeric,
          ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
          ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
        (a, conf, p, r) => new BinaryAstExprMeta[Remainder](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled
          private val tryMode = TryModeShim.isTryMode(a)
          private def leftInputIsSafe: Boolean =
            childExprs.head.canBePrecomputedForJoin

          override def tagExprForGpu(): Unit = {
            if (tryMode && (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                !GpuAnsi.supportsRemainderAst(a.left.dataType, a.right.dataType))) {
              willNotWorkOnGpu(
                "try_mod supports primitive numeric types only when row IR JIT support is enabled")
            } else if (tryMode && !leftInputIsSafe) {
              willNotWorkOnGpu("try_mod cannot eagerly evaluate a side-effecting left operand")
            }
          }

          override def tagSelfForAst(): Unit = {
            super.tagSelfForAst()
            a.dataType match {
              case _: DecimalType =>
                if (tryMode) {
                  willNotWorkInAst("AST decimal try_mod is not supported.")
                } else if (!this.conf.isProjectAstAnsiArithmeticEnabled) {
                  willNotWorkInAst("AST decimal remainder requires row IR JIT support.")
                } else if (!DecimalRemainderChecks.canUseAst(
                    a.left.dataType, a.right.dataType, a.dataType)) {
                  willNotWorkInAst(
                    "AST decimal remainder requires identical input and output types.")
                }
              case _ =>
                if (!this.conf.isProjectAstAnsiArithmeticEnabled ||
                    !GpuAnsi.supportsRemainderAst(a.left.dataType, a.right.dataType)) {
                  willNotWorkInAst("AST remainder requires row IR JIT support.")
                } else if (tryMode && !leftInputIsSafe) {
                  willNotWorkInAst(
                    "AST try_mod cannot eagerly evaluate a side-effecting left operand.")
                }
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            if (lhs.dataType.isInstanceOf[DecimalType] && rhs.dataType.isInstanceOf[DecimalType]) {
              GpuDecimalRemainder(lhs, rhs)
            } else {
              GpuRemainder(lhs, rhs, ansiEnabled && !tryMode)
            }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
