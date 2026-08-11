/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.execution.python.PythonUDFRunner

object WritePythonUDFUtils {
  def writeUDFs(
      dataOut: DataOutputStream,
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argOffsets: Array[Array[Int]],
      argNames: Option[Array[Array[Option[String]]]] = None,
      profiler: Option[String] = None): Unit = {
    if (argNames.isDefined) {
      // Support also send the argument name to Python from DB 143
      val argMetas = argOffsets.zip(argNames.get).map { case (idxs, names) =>
        idxs.zip(names).map { case (idx, name) =>
          ArgumentMetadata(idx, name)
        }
      }
      PythonUDFRunner.writeUDFs(dataOut, funcs.map(_._1), argMetas)
    } else {
      PythonUDFRunner.writeUDFs(dataOut, funcs.map(_._1), argOffsets)
    }
  }
}
