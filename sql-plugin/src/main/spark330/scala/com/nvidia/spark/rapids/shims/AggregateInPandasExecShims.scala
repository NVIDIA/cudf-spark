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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
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
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuOverrides, TypeSig}

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.AggregateInPandasExec

object AggregateInPandasExecShims {
  val execRule: Option[ExecRule[_ <: SparkPlan]] = Some(
    GpuOverrides.exec[AggregateInPandasExec](
      "The backend for an Aggregation Pandas UDF." +
        " This accelerates the data transfer between the Java process and the Python process." +
        " It also supports scheduling GPU resources for the Python process" +
        " when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r))
  )

  def isAggregateInPandasExec(plan: SparkPlan): Boolean = plan.isInstanceOf[AggregateInPandasExec]

  def getGroupingExpressions(plan: SparkPlan): Seq[NamedExpression] = {
    plan.asInstanceOf[AggregateInPandasExec].groupingExpressions
  }
}
