/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, ExprMeta, GpuOverrides, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, Lag, Lead, Literal, OffsetWindowFunction}
import org.apache.spark.sql.types.IntegerType

/**
 * Spark 3.2-specific replacement for com.nvidia.spark.rapids.OffsetWindowFunctionMeta.
 * This is required primarily for two reasons:
 *   1. com.nvidia.spark.rapids.OffsetWindowFunctionMeta (compiled against Spark 3.0.x)
 *      fails class load in Spark 3.1.x. (`expr.input` is not recognized as an Expression.)
 *   2. The semantics of offsets in LAG() are reversed/negated in Spark 3.1.1.
 *      E.g. The expression `LAG(col, 5)` causes Lag.offset to be set to `-5`,
 *      as opposed to `5`, in prior versions of Spark.
 *   3. IGNORE NULL and RESPECT NULL was added in 3.2.0 and we need to fall back
 *      until this can be supported.
 * This class adjusts the LAG offset to use similar semantics to Spark 3.0.x.
 */
abstract class OffsetWindowFunctionMeta[INPUT <: OffsetWindowFunction] (
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {
  lazy val input: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.input, conf, Some(this))
  lazy val adjustedOffset: Expression = {
    expr match {
      case lag: Lag =>
        GpuOverrides.extractLit(lag.offset) match {
         case Some(Literal(offset: Int, IntegerType)) =>
            Literal(-offset, IntegerType)
         case _ =>
           throw new IllegalStateException(
             s"Only integer literal offsets are supported for LAG. Found:${lag.offset}")
        }
      case lead: Lead =>
        GpuOverrides.extractLit(lead.offset) match {
          case Some(Literal(offset: Int, IntegerType)) =>
            Literal(offset, IntegerType)
          case _ =>
            throw new IllegalStateException(
              s"Only integer literal offsets are supported for LEAD. Found:${lead.offset}")
        }
      case other =>
        throw new IllegalStateException(s"$other is not a supported window function")
    }
  }
  lazy val offset: BaseExprMeta[_] =
    GpuOverrides.wrapExpr(adjustedOffset, conf, Some(this))
  lazy val default: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.default, conf, Some(this))

  override val childExprs: Seq[BaseExprMeta[_]] = Seq(input, offset, default)

  override def tagExprForGpu(): Unit = {
    expr match {
      case lead: Lead =>
        if (lead.ignoreNulls) {
          willNotWorkOnGpu("IGNORE NULLS for lead is not currently supported")
        }
      case lag: Lag =>
        if (lag.ignoreNulls) {
          willNotWorkOnGpu("IGNORE NULLS for lag is not currently supported")
        }
      case other =>
        willNotWorkOnGpu( s"Only LEAD/LAG offset window functions are supported. Found: $other")
    }

    if (GpuOverrides.extractLit(expr.offset).isEmpty) { // Not a literal offset.
      willNotWorkOnGpu(
        s"Only integer literal offsets are supported for LEAD/LAG. Found: ${expr.offset}")
    }
  }
}
