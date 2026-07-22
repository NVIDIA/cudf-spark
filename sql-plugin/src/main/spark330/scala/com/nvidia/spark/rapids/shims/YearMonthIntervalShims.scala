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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
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
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids.shims.{GpuDivideYMInterval, GpuMultiplyYMInterval}

object YearMonthIntervalShims {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[MultiplyYMInterval](
      "Year-month interval * number",
      ExprChecks.binaryProject(
        TypeSig.YEARMONTH,
        TypeSig.YEARMONTH,
        ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[MultiplyYMInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiplyYMInterval(lhs, rhs)
      }),
    GpuOverrides.expr[DivideYMInterval](
      "Year-month interval / number",
      ExprChecks.binaryProject(
        TypeSig.YEARMONTH,
        TypeSig.YEARMONTH,
        ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[DivideYMInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivideYMInterval(lhs, rhs)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r))
    .toMap[Class[_ <: Expression], ExprRule[_ <: Expression]]
}
