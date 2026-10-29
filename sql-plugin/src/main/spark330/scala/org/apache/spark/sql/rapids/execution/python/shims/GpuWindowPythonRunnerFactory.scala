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
{"spark":["330","350","400"],"carryForward":"all"}
{"spark":["330db","332db","350db143","400db173"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.rapids.execution.python.GpuArrowOutput
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Factory object to create Python runner for Window UDFs.
 * This shim allows different Spark versions to use different runner implementations.
 * 
 * In Spark 3.x and 4.0.x, the Python worker uses ArrowStreamPandasUDFSerializer for
 * SQL_WINDOW_AGG_PANDAS_UDF, which doesn't require grouped protocol markers.
 */
object GpuWindowPythonRunnerFactory {
  def createRunner(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      evalType: Int,
      argOffsets: Array[Array[Int]],
      pythonInSchema: StructType,
      timeZoneId: String,
      conf: Map[String, String],
      batchSize: Long,
      pythonOutSchema: StructType,
      argNames: Option[Array[Array[Option[String]]]],
      udfLogMaxEntries: Int = 0,
      udfLogLevel: String = "WARNING"
  ): GpuBasePythonRunner[ColumnarBatch] with GpuArrowOutput = {
    new GpuArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      pythonInSchema,
      timeZoneId,
      conf,
      batchSize,
      pythonOutSchema,
      argNames,
      udfLogMaxEntries = udfLogMaxEntries,
      udfLogLevel = udfLogLevel)
  }
}
