/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "341db"}
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuAlias, PlanShims}

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ResultQueryStageExec
import org.apache.spark.sql.types.DataType

class PlanShimsImpl extends PlanShims {
  def extractExecutedPlan(plan: SparkPlan): SparkPlan = plan match {
    case p: CommandResultExec => p.commandPhysicalPlan
    case q: ResultQueryStageExec => q.plan
    case _ => plan
  }

  def isAnsiCast(e: Expression): Boolean = AnsiCastShim.isAnsiCast(e)

  def isAnsiCastOptionallyAliased(e: Expression): Boolean = e match {
    case Alias(e, _) => isAnsiCast(e)
    case GpuAlias(e, _) => isAnsiCast(e)
    case e => isAnsiCast(e)
  }

  def extractAnsiCastTypes(e: Expression): (DataType, DataType) = {
    AnsiCastShim.extractAnsiCastTypes(e)
  }
}
