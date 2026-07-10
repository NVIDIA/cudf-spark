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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Add, BinaryExpression, Divide, Expression,
  Multiply, Subtract, TryEval}
import org.apache.spark.sql.rapids.{GpuAdd, GpuAnsi, GpuDivide, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.DoubleType

object TryEvalShims {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[TryEval](
      "TRY integral arithmetic and floating-point division",
      ExprChecks.projectAndAst(
        TypeSig.integral + TypeSig.DOUBLE,
        TypeSig.integral + TypeSig.DOUBLE,
        TypeSig.numericAndInterval,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", TypeSig.integral + TypeSig.DOUBLE, TypeSig.numericAndInterval))),
      (a, conf, p, r) => new ExprMeta[TryEval](a, conf, p, r) {
        override val isFoldableNonLitAllowed: Boolean = true

        private def operation: Option[BinaryExpression] = a.child match {
          case op: Add if op.failOnError &&
              GpuAnsi.supportsAnsiArithmeticAst(op.dataType) => Some(op)
          case op: Subtract if op.failOnError &&
              GpuAnsi.supportsAnsiArithmeticAst(op.dataType) => Some(op)
          case op: Multiply if op.failOnError &&
              GpuAnsi.supportsAnsiArithmeticAst(op.dataType) => Some(op)
          case op: Divide if op.failOnError && op.dataType == DoubleType &&
              op.left.dataType == DoubleType && op.right.dataType == DoubleType => Some(op)
          case _ => None
        }

        override val childExprs: Seq[BaseExprMeta[_]] = operation.toSeq.flatMap { op =>
          Seq(op.left, op.right).map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        }

        private def inputsAreSafe: Boolean =
          childExprs.forall(_.canBePrecomputedForJoin)

        override def tagExprForGpu(): Unit = {
          if (operation.isEmpty) {
            willNotWorkOnGpu(
              "only integral try_add/try_subtract/try_multiply and DOUBLE try_divide are supported")
          } else if (!this.conf.isProjectAstRowIrEnabled) {
            willNotWorkOnGpu("TRY arithmetic requires row IR JIT support")
          } else if (!inputsAreSafe) {
            willNotWorkOnGpu("TRY arithmetic cannot catch errors from its input expressions")
          }
        }

        override def tagSelfForAst(): Unit = {
          if (operation.isEmpty) {
            willNotWorkInAst(
              "only integral try_add/try_subtract/try_multiply and DOUBLE try_divide are supported")
          } else if (!this.conf.isProjectAstRowIrEnabled) {
            willNotWorkInAst("TRY arithmetic requires row IR JIT support")
          } else if (!inputsAreSafe) {
            willNotWorkInAst("TRY arithmetic cannot catch errors from its input expressions")
          }
        }

        override def convertToGpuImpl(): GpuExpression = {
          val Seq(lhs, rhs) = childExprs.map(_.convertToGpu())
          a.child match {
            case op: Add =>
              GpuAdd(lhs, rhs, failOnError = false, tryMode = true)(op.origin)
            case op: Subtract =>
              GpuSubtract(lhs, rhs, failOnError = false, tryMode = true)(op.origin)
            case op: Multiply =>
              GpuMultiply(lhs, rhs, failOnError = false, tryMode = true)(op.origin)
            case op: Divide =>
              GpuDivide(lhs, rhs, failOnError = false)(op.origin)
            case other =>
              throw new IllegalStateException(s"Unsupported expression under TryEval: $other")
          }
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
