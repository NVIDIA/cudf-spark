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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.PythonMapInArrowExec
import org.apache.spark.sql.rapids.shims.GpuPythonMapInArrowExecMeta

object PythonMapInArrowExecShims {

  private def createPythonMapInArrowExecMeta(
      mapPy: PythonMapInArrowExec,
      conf: com.nvidia.spark.rapids.RapidsConf,
      p: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuPythonMapInArrowExecMeta(mapPy, conf, p, r)

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[PythonMapInArrowExec](
        "The backend for Map Arrow Iterator UDF. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        createPythonMapInArrowExecMeta)
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

}
