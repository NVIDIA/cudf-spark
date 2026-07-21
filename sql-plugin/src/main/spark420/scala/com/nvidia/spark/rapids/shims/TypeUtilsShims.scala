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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.NaNEquality

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.types.{DataType, NullType, NumericType}

/**
 * Reimplement the function `checkForNumericExpr` which has been removed since
 * Spark 3.4.0
 */
object TypeUtilsShims {
  def checkForNumericExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (dt.isInstanceOf[NumericType] || dt == NullType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$caller requires numeric types, not ${dt.catalogString}")
    }
  }

  // Spark 4.2 stores collect_set buffers in a way that treats all NaN values as one set entry.
  val collectSetFloatNanEquality: NaNEquality = NaNEquality.ALL_EQUAL

  def collectListIgnoreNulls(collectList: CollectList): Boolean = collectList.ignoreNulls

  // Spark 4.2 uses fdlibm asinh, so the default CPU behavior matches the stable cuDF kernel.
  val useImprovedAsinhByDefault: Boolean = true

  // These grouped aggregate iterator UDF eval types are new in Spark 4.2 and are planned by
  // ArrowAggregatePythonExec. The GPU runner does not implement their iterator protocol yet,
  // so tag them for CPU fallback. PythonEvalType is private[spark], so compare values directly.
  private val SQL_GROUPED_AGG_PANDAS_ITER_UDF = 217
  private val SQL_GROUPED_AGG_ARROW_ITER_UDF = 254

  def isUnsupportedArrowAggregatePythonEvalType(evalType: Int): Boolean = {
    evalType == SQL_GROUPED_AGG_PANDAS_ITER_UDF ||
      evalType == SQL_GROUPED_AGG_ARROW_ITER_UDF
  }
}
