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
{"spark": "330db"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.catalyst.trees.{Origin, SQLQueryContext}

// Databricks 3.3.x back-ported SPARK-39175 and typed `Origin.context` as
// `SQLQueryContext` directly — same shape as Apache 3.4+.
object OriginContextShim {
  def queryContext(origin: Origin): SQLQueryContext = origin.context
  def contextSummary(origin: Origin): String = origin.context match {
    case null => ""
    case ctx => ctx.summary
  }
}
