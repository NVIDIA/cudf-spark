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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Expression

object TryModeShim {
  /**
   * Expression is wrapped under TryEval during query planning which is not supported on GPU.
   * Example: for try_add(col1, col2) it would be <TryEval> tryeval((col1#0 + col2#1))
   * So the return value from this function does not matter.
   */
  def isTryMode(expr: Expression): Boolean = {
    false
  }
}
