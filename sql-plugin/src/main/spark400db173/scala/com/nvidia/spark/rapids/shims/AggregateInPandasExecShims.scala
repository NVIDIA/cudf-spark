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
{"spark": "400db173"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuOverrides, TypeSig}

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.ArrowAggregatePythonExec

/**
 * AggregateInPandasExec was renamed to ArrowAggregatePythonExec in Spark 4.1.0 and Databricks 17.3.
 */
object AggregateInPandasExecShims {
  val execRule: Option[ExecRule[_ <: SparkPlan]] = Some(
    GpuOverrides.exec[ArrowAggregatePythonExec](
      "The backend for an Aggregation Pandas UDF." +
        " This accelerates the data transfer between the Java process and the Python process." +
        " It also supports scheduling GPU resources for the Python process" +
        " when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (aggPy, conf, p, r) => new GpuArrowAggregatePythonExecMeta(aggPy, conf, p, r))
  )

  def isAggregateInPandasExec(plan: SparkPlan): Boolean =
    plan.isInstanceOf[ArrowAggregatePythonExec]

  def getGroupingExpressions(plan: SparkPlan): Seq[NamedExpression] = {
    plan.asInstanceOf[ArrowAggregatePythonExec].groupingExpressions
  }
}
