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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.adaptive.{QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

/** Utility methods for manipulating Catalyst classes involved in Adaptive Query Execution */
object AQEUtils {
  /** Return a new QueryStageExec reuse instance with updated output attributes */
  def newReuseInstance(sqse: ShuffleQueryStageExec, newOutput: Seq[Attribute]): QueryStageExec = {
    val reusedExchange = ReusedExchangeExec(newOutput, sqse.shuffle)
    QueryStageExecShims.createShuffleQueryStageExec(
      sqse, sqse.id, reusedExchange, sqse.originalPlan, sqse.isSparkExchange)
  }

  // Databricks 10.4 has an issue where if you turn off AQE it can still use it for
  // certain operations. This causes issues with the plugin so this is to work around
  // that.
  def isAdaptiveExecutionSupportedInSparkVersion(conf: SQLConf): Boolean = {
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
  }
}
