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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Expression, GetJsonObject, JsonToStructs,
  NamedLambdaVariable, RegExpExtract, RegExpExtractAll, StringTranslate}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

object SparkShimImpl extends Spark420PlusShims {
  override protected def isBridgeCloneSafeStatefulExpression(expr: Expression): Boolean =
    expr match {
      case _: GetJsonObject | _: JsonToStructs | _: RegExpExtract | _: RegExpExtractAll |
          _: StringTranslate => true
      case _ => super.isBridgeCloneSafeStatefulExpression(expr)
    }

  override def canonicalizeArraySortComparator(expr: Expression): Expression = {
    // Spark 5 stores mutable evaluator state in NamedLambdaVariable's case-class value field.
    // Replace it with the stateless attribute before comparing comparator trees.
    expr.transformUp {
      case variable: NamedLambdaVariable => variable.toAttribute
    }.canonicalized
  }

  // Spark 5 changed exact-percentile interpolation. With floating infinities, its new formula can
  // produce NaN where the JNI implementation's earlier formula produces an infinity.
  override def isExactPercentileInputTypeSupported(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => false
    case _ => true
  }
}
