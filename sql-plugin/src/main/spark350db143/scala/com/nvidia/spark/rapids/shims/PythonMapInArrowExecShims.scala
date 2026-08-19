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
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.MapInArrowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.GpuPythonMapInArrowExecMeta
import org.apache.spark.sql.types.{BinaryType, StringType}

object PythonMapInArrowExecShims {

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[MapInArrowExec](
        "The backend for Map Arrow Iterator UDF. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (mapPy, conf, p, r) => new GpuPythonMapInArrowExecMeta(mapPy, conf, p, r) {
          override def tagPlanForGpu(): Unit = {
            super.tagPlanForGpu()
            if (SQLConf.get.getConf(SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES)) {

              val inputTypes = mapPy.child.schema.fields.map(_.dataType)
              val outputTypes = mapPy.output.map(_.dataType)

              val hasStringOrBinaryTypes = (inputTypes ++ outputTypes).exists(dataType =>
                TrampolineUtil.dataTypeExistsRecursively(dataType,
                  dt => dt == StringType || dt == BinaryType))

              if (hasStringOrBinaryTypes) {
                willNotWorkOnGpu(s"${SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key} is " +
                  s"enabled and the schema contains string or binary types. This is not " +
                  s"supported on the GPU.")
              }
            }
          }
      })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

}
