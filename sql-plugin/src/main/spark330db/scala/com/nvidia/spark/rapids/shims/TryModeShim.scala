/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark":["330db","332db","350db143"],"carryForward":"none"}
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Add, Divide, EvalMode, Expression, Multiply, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}

object TryModeShim {
  /**
   * Check if an expression is in TRY mode.
   */
  def isTryMode(expr: Expression): Boolean = {
    val evalMode = expr match {
      case add: Add => add.evalMode
      case sub: Subtract => sub.evalMode
      case mul: Multiply => mul.evalMode
      case div: Divide => div.evalMode
      case mod: Remainder => mod.evalMode
      case avg: Average => avg.evalMode
      case sum: Sum => sum.evalMode
      case _ => throw new RuntimeException(s"Unsupported expression $expr in TRY mode")
    }
    evalMode == EvalMode.TRY
  }
}
