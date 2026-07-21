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
{"spark": "400db173"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.shims.{GpuDivideDTInterval, GpuMultiplyDTInterval,
  GpuTimestampAddInterval}
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * DayTimeInterval shims for Spark 4.1.1+
 * TimeAdd was renamed to TimestampAddInterval in Spark 4.1.0
 */
object DayTimeIntervalShims {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.cpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes),
      (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, ansiEnabled)
      }),
    GpuOverrides.expr[MultiplyDTInterval](
      "Day-time interval * number",
      ExprChecks.binaryProject(
        TypeSig.DAYTIME,
        TypeSig.DAYTIME,
        ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[MultiplyDTInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiplyDTInterval(lhs, rhs)
      }),
    GpuOverrides.expr[DivideDTInterval](
      "Day-time interval / number",
      ExprChecks.binaryProject(
        TypeSig.DAYTIME,
        TypeSig.DAYTIME,
        ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
        ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[DivideDTInterval](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivideDTInterval(lhs, rhs)
      }),
    GpuOverrides.expr[TimestampAddInterval](
      "Adds interval to timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        // interval support DAYTIME column or CALENDAR literal
        ("interval", TypeSig.DAYTIME + TypeSig.lit(TypeEnum.CALENDAR)
            .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
            TypeSig.DAYTIME + TypeSig.CALENDAR)),
      (timeAdd, conf, p, r) => new BinaryExprMeta[TimestampAddInterval](timeAdd, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.extractLit(timeAdd.interval).foreach { lit =>
            lit.dataType match {
              case CalendarIntervalType =>
                val intvl = lit.value.asInstanceOf[CalendarInterval]
                if (intvl.months != 0) {
                  willNotWorkOnGpu("interval months isn't supported")
                }
              case _: DayTimeIntervalType => // Supported
            }
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimestampAddInterval(lhs, rhs)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
