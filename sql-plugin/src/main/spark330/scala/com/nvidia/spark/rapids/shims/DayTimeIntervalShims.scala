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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
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
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.shims.{GpuDivideDTInterval, GpuMultiplyDTInterval}

object DayTimeIntervalShims {

  private class AbsMetaImpl(
      a: Abs,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends UnaryAstExprMeta[Abs](a, conf, p, r) {
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, ansiEnabled)
      }

  private def createAbsMeta(
      a: Abs,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new AbsMetaImpl(a, conf, p, r)

  private class MultiplyDTIntervalMetaImpl(
      a: MultiplyDTInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends BinaryExprMeta[MultiplyDTInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiplyDTInterval(lhs, rhs)
      }

  private def createMultiplyDTIntervalMeta(
      a: MultiplyDTInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new MultiplyDTIntervalMetaImpl(a, conf, p, r)

  private class DivideDTIntervalMetaImpl(
      a: DivideDTInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends BinaryExprMeta[DivideDTInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivideDTInterval(lhs, rhs)
      }

  private def createDivideDTIntervalMeta(
      a: DivideDTInterval,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new DivideDTIntervalMetaImpl(a, conf, p, r)
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    // TimeAdd moved to TimeAddShims to handle version differences
    TimeAddShims.exprs ++ Seq(
    GpuOverrides.expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.cpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes),
      createAbsMeta),
    GpuOverrides.expr[MultiplyDTInterval](
      "Day-time interval * number",
      ExprChecks.binaryProject(
        TypeSig.DAYTIME,
        TypeSig.DAYTIME,
        ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      createMultiplyDTIntervalMeta),
    GpuOverrides.expr[DivideDTInterval](
      "Day-time interval * operator",
      ExprChecks.binaryProject(
        TypeSig.DAYTIME,
        TypeSig.DAYTIME,
        ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      createDivideDTIntervalMeta)
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r))
    .toMap[Class[_ <: Expression], ExprRule[_ <: Expression]]
}
