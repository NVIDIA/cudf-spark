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
{"spark": "403"}
{"spark": "404"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Acosh, Asinh, Expression}
import org.apache.spark.sql.rapids.{GpuAcoshImproved, GpuAsinhImproved}

/**
 * Shared Spark-compatible hyperbolic expression overrides for shims where Spark's CPU
 * implementation avoids large-input overflow in the default ACOSH/ASINH formulas.
 */
trait HyperbolicExpressionShims extends SparkShims {
  abstract override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Acosh](
        "Inverse hyperbolic cosine",
        ExprChecks.mathUnaryWithAst,
        (a, conf, p, r) => new UnaryAstExprMeta[Acosh](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression =
            if (this.conf.includeImprovedFloat) {
              GpuAcoshImproved(child)
            } else {
              GpuAcosh(child)
            }

          override def tagSelfForAst(): Unit = {
            if (!this.conf.includeImprovedFloat) {
              // The compatibility expression needs conditional branches for domain and large-input
              // handling. AST does not express those branches, so fall back to the non-AST project.
              willNotWorkInAst("acosh is not AST compatible for this Spark version")
            }
          }
        }),
      GpuOverrides.expr[Asinh](
        "Inverse hyperbolic sine",
        ExprChecks.mathUnaryWithAst,
        (a, conf, p, r) => new UnaryAstExprMeta[Asinh](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression =
            if (this.conf.includeImprovedFloat || TypeUtilsShims.useImprovedAsinhByDefault) {
              GpuAsinhImproved(child)
            } else {
              GpuAsinh(child)
            }

          override def tagSelfForAst(): Unit = {
            if (!this.conf.includeImprovedFloat && !TypeUtilsShims.useImprovedAsinhByDefault) {
              // The compatibility expression needs conditional branches for preserving large
              // positive/negative behavior. AST cannot express that logic today.
              willNotWorkInAst("asinh is not AST compatible for this Spark version")
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
