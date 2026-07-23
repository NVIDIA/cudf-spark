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
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{RowCountPlanVisitor, SparkPlanMeta}

import org.apache.spark.sql.execution.GlobalLimitExec

object GlobalLimitShims {

  /**
   * Estimate the number of rows for a GlobalLimitExec.
   */
  def visit(plan: SparkPlanMeta[GlobalLimitExec]): Option[BigInt] = {
    // offset is introduce in spark-3.4.0, and it ensures that offset >= 0. And limit can be -1,
    // such case happens only when we execute sql like 'select * from table offset 10'
    val offset = plan.wrapped.offset
    val limit = plan.wrapped.limit
    val sliced = if (limit >= 0) {
      Some(BigInt(limit - offset).max(0))
    } else {
      // limit can be -1, meaning no limit
      None
    }
    RowCountPlanVisitor.visit(plan.childPlans.head)
      .map { rowNum =>
        val remaining = (rowNum - offset).max(0)
        sliced.map(_.min(remaining)).getOrElse(remaining)
      }
      .orElse(sliced)
  }
}
