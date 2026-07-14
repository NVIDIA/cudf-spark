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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuCast, GpuColumnVector, GpuExpression}
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Handle the new Expression added in Spark-3.3.1+.
 * Casting a numeric value as another numeric type in store assignment. It captures the arithmetic
 * exception thrown by Cast and shows a relevant error message.
 * The current implementation in Spark creates a child of type Cast with AnsiEnabled set to true.
 *
 * The GPU equivalent columnar evaluation is a delegation to the child's columnar evaluation which
 * is a GpuCast. In order to match Spark's Exception, we need to catch the Exception passing it to
 * QueryExecutionError.
 * The calculation of the sideEffect is delegated to the child Expression "GpuCast".
 */
case class GpuCheckOverflowInTableInsert(child: GpuCast, columnName: String)
  extends ShimUnaryExpression with GpuExpression {

  override def dataType: DataType = child.dataType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    try {
      child.columnarEval(batch)
    } catch {
      // map SparkArithmeticException to SparkArithmeticException("CAST_OVERFLOW_IN_TABLE_INSERT")
      case _: SparkArithmeticException =>
        throw QueryExecutionErrors.castingCauseOverflowErrorInTableInsert(
          child.child.dataType,
          dataType,
          columnName)
    }
  }

  override def toString: String = child.toString

  override def sql: String = child.sql
}
