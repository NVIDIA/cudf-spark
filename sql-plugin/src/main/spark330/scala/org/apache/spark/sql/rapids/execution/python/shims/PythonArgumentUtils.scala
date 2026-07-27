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
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.python.GpuPythonArguments

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

object PythonArgumentUtils {

  /** Flatten all the arguments as a GpuPythonArguments. Almost the same as Spark */
  def flatten(args: Seq[Seq[Expression]]): GpuPythonArguments = {
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argOffsets = args.map { input =>
      input.map { e =>
        if (allInputs.exists(_.semanticEquals(e))) {
          allInputs.indexWhere(_.semanticEquals(e))
        } else {
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1
        }
      }.toArray
    }.toArray
    GpuPythonArguments(allInputs.toSeq, dataTypes.toSeq, argOffsets, None)
  }
}
