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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.ArrowWindowPythonExec
import org.apache.spark.sql.rapids.execution.python.GpuWindowInPandasExecMetaBase

/**
 * Exec rules for ArrowWindowPythonExec (Spark 4.1+ - renamed from WindowInPandasExec).
 */
object WindowInPandasExecShims {
  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[ArrowWindowPythonExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            SparkShimImpl.getWindowExpressions(winPy).map(
              GpuOverrides.wrapExpr(_, this.conf, Some(this)))

          override def convertToGpu(): GpuExec = {
            val windowExprGpu = windowExpressions.map(_.convertToGpu())
            val partitionGpu = partitionSpec.map(_.convertToGpu())
            GpuWindowInPandasExec(
              windowExprGpu,
              partitionGpu,
              // leave ordering expression on the CPU, it's not used for GPU computation
              winPy.orderSpec,
              childPlans.head.convertIfNeeded()
            )(winPy.partitionSpec)
          }
        }).disabledByDefault("it only supports row based frame for now")
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }
}
