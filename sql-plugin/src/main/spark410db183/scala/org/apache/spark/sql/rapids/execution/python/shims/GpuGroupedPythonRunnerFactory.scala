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
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.rapids.execution.python.GpuArrowOutput
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
  private val maxBytes = conf.pandasZeroConfConversionGroupbyApplyMaxBytesPerSlice
  private val zeroConfEnabled = conf.pandasZeroConfConversionGroupbyApplyEnabled
  private val arrowBatchSlicingEnabled = conf.pythonArrowBatchSlicingEnabled
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)
  private val udfLogMaxEntries = conf.pythonUdfLogMaxEntries
  private val udfLogLevel = conf.pythonUdfLogLevel

  def getRunner(): GpuBasePythonRunner[ColumnarBatch] with GpuArrowOutput = {
    if (GpuGroupedPythonRunnerFactory.shouldUseGroupUdfRunner(
        zeroConfEnabled, maxBytes, arrowBatchSlicingEnabled)) {
      new GpuGroupUDFArrowPythonRunner(
        chainedFunc,
        evalType,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        Int.MaxValue,
        pythonOutputSchema,
        argNames,
        udfLogMaxEntries,
        udfLogLevel)
    } else {
      new GpuWindowArrowPythonRunner(
        chainedFunc,
        evalType,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        Int.MaxValue,
        pythonOutputSchema,
        argNames,
        udfLogMaxEntries,
        udfLogLevel)
    }
  }
}

object GpuGroupedPythonRunnerFactory {
  private[shims] def shouldUseGroupUdfRunner(
      zeroConfEnabled: Boolean,
      maxBytes: Long,
      arrowBatchSlicingEnabled: Boolean): Boolean = {
    // DBR 18.3 no longer uses Arrow batch slicing alone to select the grouped protocol.
    // Keep the argument here so the DBR 17.3 predicate cannot be restored accidentally.
    zeroConfEnabled && maxBytes > 0L
  }
}
