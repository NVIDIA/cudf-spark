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
{"spark": "350db143"}
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

package org.apache.spark.sql.rapids.execution.python.shims

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.python.{GpuArgumentMeta, GpuPythonArguments}

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedArgumentExpression}
import org.apache.spark.sql.types.DataType

/** The argument names will be sent to Python side from Spark 400 and DB-143 */
object PythonArgumentUtils {

  /** Flatten all the arguments as a GpuPythonArguments. Almost the same as Spark */
  def flatten(args: Seq[Seq[Expression]]): GpuPythonArguments = {
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argMetas = args.map { input =>
      input.map { e =>
        val (key, value) = e match {
          // No GPU version so far
          case NamedArgumentExpression(key, value) =>
            (Some(key), value)
          case _ =>
            (None, e)
        }
        if (allInputs.exists(_.semanticEquals(value))) {
          GpuArgumentMeta(allInputs.indexWhere(_.semanticEquals(value)), key)
        } else {
          allInputs += value
          dataTypes += value.dataType
          GpuArgumentMeta(allInputs.length - 1, key)
        }
      }.toArray
    }.toArray
    GpuPythonArguments(allInputs.toSeq, dataTypes.toSeq,
      argMetas.map(_.map(_.offset)), Some(argMetas.map(_.map(_.name))))
  }
}
