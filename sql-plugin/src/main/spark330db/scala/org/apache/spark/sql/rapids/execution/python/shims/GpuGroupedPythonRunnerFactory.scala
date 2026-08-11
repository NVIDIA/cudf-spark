/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuGroupedPythonRunnerFactory(
  conf: org.apache.spark.sql.internal.SQLConf,
  chainedFunc: Seq[(ChainedPythonFunctions, Long)],
  argOffsets: Array[Array[Int]],
  dedupAttrs: StructType,
  pythonOutputSchema: StructType,
  evalType: Int,
  argNames: Option[Array[Array[Option[String]]]] = None) {
  // Configs from DB runtime
  val maxBytes = conf.pandasZeroConfConversionGroupbyApplyMaxBytesPerSlice
  val zeroConfEnabled = conf.pandasZeroConfConversionGroupbyApplyEnabled
  val sessionLocalTimeZone = conf.sessionLocalTimeZone
  val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)

  def getRunner(): GpuBasePythonRunner[ColumnarBatch] = {
    if (zeroConfEnabled && maxBytes > 0L) {
      new GpuGroupUDFArrowPythonRunner(
        chainedFunc,
        evalType,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        // The whole group data should be written in a single call, so here is unlimited
        Int.MaxValue,
        pythonOutputSchema,
        argNames)
    } else {
      new GpuArrowPythonRunner(
        chainedFunc,
        evalType,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        Int.MaxValue,
        pythonOutputSchema,
        argNames)
    }
  }
}
