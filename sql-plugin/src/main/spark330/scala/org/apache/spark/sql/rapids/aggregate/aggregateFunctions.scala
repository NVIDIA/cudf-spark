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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.aggregate

import com.nvidia.spark.rapids.GpuCast
import com.nvidia.spark.rapids.window.{GpuWindowExpression, GpuWindowSpecDefinition}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.GpuDecimalDivide
import org.apache.spark.sql.types.DecimalType

abstract class GpuDecimalAverage(child: Expression, sumDataType: DecimalType, failOnError: Boolean)
    extends GpuDecimalAverageBase(child, sumDataType, failOnError) {

  // NOTE: this sets `failOnErrorOverride=false` in `GpuDivide` to force it not to throw
  // divide-by-zero exceptions, even when ansi mode is enabled in Spark.
  // This is to conform with Spark's behavior in the Average aggregate function.
  override lazy val evaluateExpression: Expression = {
    GpuCast(
      GpuDecimalDivide(sum, count, intermediateSparkDivideType, failOnError = failOnError,
        failOnDivideByZero = false), dataType, ansiMode = failOnError)()
  }

  // Window
  // Replace average with SUM/COUNT. This lets us run average in running window mode without
  // recreating everything that would have to go into doing the SUM and the COUNT here.
  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    val count = GpuWindowExpression(GpuCount(Seq(child)), spec)
    val sum = GpuWindowExpression(GpuSum(child, sumDataType,
      failOnErrorOverride = failOnError), spec)
    GpuCast(
      GpuDecimalDivide(sum, count, intermediateSparkDivideType, failOnError = failOnError,
        failOnDivideByZero = false), dataType, ansiMode = failOnError)()
  }
}
