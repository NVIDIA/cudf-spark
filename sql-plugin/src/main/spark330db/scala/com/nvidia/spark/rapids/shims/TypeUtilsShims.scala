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
{"spark":["330db","332db","350db143"],"carryForward":"none"}
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400"],"carryForward":"all"}
{"spark":["400db173"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.NaNEquality

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
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

  val collectSetFloatNanEquality: NaNEquality = NaNEquality.UNEQUAL

  // Pre-Spark 4.2 CollectSet stores child values directly in the agg buffer.
  def collectSetCpuBufferElementType(childType: DataType): DataType = childType

  def collectListIgnoreNulls(_collectList: CollectList): Boolean = true

  def collectSetIgnoreNulls(_collectSet: CollectSet): Boolean = true

  val useImprovedAsinhByDefault: Boolean = false

  def isUnsupportedArrowAggregatePythonEvalType(evalType: Int): Boolean = false
}
