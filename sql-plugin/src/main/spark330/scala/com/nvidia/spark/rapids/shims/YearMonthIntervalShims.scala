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
{"spark":["330","350","400","420"],"carryForward":"all"}
{"spark":["350db143","400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids.shims.{GpuDivideYMInterval, GpuMultiplyYMInterval}

object YearMonthIntervalShims {

  private class MultiplyYMIntervalMetaImpl(
      a: MultiplyYMInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends BinaryExprMeta[MultiplyYMInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiplyYMInterval(lhs, rhs)
      }

  private def createMultiplyYMIntervalMeta(
      a: MultiplyYMInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new MultiplyYMIntervalMetaImpl(a, conf, p, r)

  private class DivideYMIntervalMetaImpl(
      a: DivideYMInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends BinaryExprMeta[DivideYMInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivideYMInterval(lhs, rhs)
      }

  private def createDivideYMIntervalMeta(
      a: DivideYMInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new DivideYMIntervalMetaImpl(a, conf, p, r)
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[MultiplyYMInterval](
      "Year-month interval * number",
      ExprChecks.binaryProject(
        TypeSig.YEARMONTH,
        TypeSig.YEARMONTH,
        ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      createMultiplyYMIntervalMeta),
    GpuOverrides.expr[DivideYMInterval](
      "Year-month interval / number",
      ExprChecks.binaryProject(
        TypeSig.YEARMONTH,
        TypeSig.YEARMONTH,
        ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      createDivideYMIntervalMeta)
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r))
    .toMap[Class[_ <: Expression], ExprRule[_ <: Expression]]
}
