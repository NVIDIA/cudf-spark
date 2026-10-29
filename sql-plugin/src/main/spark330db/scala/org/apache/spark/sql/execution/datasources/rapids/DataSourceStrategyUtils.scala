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
{"spark":["330db","332db","350db143"],"carryForward":"none"}
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.rapids

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

object DataSourceStrategyUtils {
  // Trampoline utility to access protected translateRuntimeFilterV2
  def translateRuntimeFilter(expr: Expression): Option[Predicate] =
    DataSourceV2Strategy.translateRuntimeFilterV2(expr)
}
