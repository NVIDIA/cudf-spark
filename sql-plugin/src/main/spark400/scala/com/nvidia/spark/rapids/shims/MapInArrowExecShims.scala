/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.MapInArrowExec
import org.apache.spark.sql.rapids.shims.GpuMapInArrowExecMeta

object PythonMapInArrowExecShims {

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[MapInArrowExec](
      "The backend for Map Arrow Iterator UDF. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (mapPy, conf, p, r) => new GpuMapInArrowExecMeta(mapPy, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

}