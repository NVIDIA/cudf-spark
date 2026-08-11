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
{"spark": "400db173"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Add, Divide, EvalMode, Expression, Multiply, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}

/**
 * Spark 4.1.0+ and Databricks 17.3 version where evalMode
 * changed to evalContext.evalMode for arithmetic.
 * See: https://github.com/apache/spark/commit/a96e9ca81518bff31b0089d459fe78804ca1aa38
 */
object TryModeShim {
  def isTryMode(expr: Expression): Boolean = {
    val evalMode = expr match {
      case add: Add => add.evalContext.evalMode
      case sub: Subtract => sub.evalContext.evalMode
      case mul: Multiply => mul.evalContext.evalMode
      case div: Divide => div.evalContext.evalMode
      case mod: Remainder => mod.evalContext.evalMode
      case avg: Average => avg.evalMode  // Average still uses evalMode directly as a parameter
      case sum: Sum => sum.evalContext.evalMode  // Sum uses evalContext.evalMode
      case _ => throw new RuntimeException(s"Unsupported expression $expr in TRY mode")
    }
    evalMode == EvalMode.TRY
  }
}
