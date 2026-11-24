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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.TestStats

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, LiteralExpressionSuite}
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types.{ObjectType, UserDefinedType}

class RapidsLiteralExpressionSuite extends LiteralExpressionSuite
  with RapidsTestsTrait {

  override def rapidsCheckExpression(
      origExpr: Expression,
      expected: Any,
      inputRow: InternalRow): Unit = origExpr.dataType match {
    case udt: UserDefinedType[_] =>
      // Keep the UDT expression and schema. Its external deserializer is not a lossless
      // representation of Literal.default: ExamplePointUDT turns an empty array into a
      // random point. Compare the original expected value before that deserialization.
      shouldNotFallback()
      val resultDF = _spark.range(0, 1).select(Column(origExpr))
      assert(resultDF.schema.fields.length == 1)
      assert(resultDF.schema.fields.head.dataType == udt)
      val qe = resultDF.queryExecution
      val result = try {
        qe.toRdd.map(_.copy()).collect()
      } finally {
        // Internal-row collection bypasses Dataset.collect's query-listener wrapper.
        ExecutionPlanCaptureCallback.captureIfNeeded(qe)
      }
      TestStats.testUnitNumber += 1
      assert(result.length == 1)
      val actual = result.head.get(0, udt.sqlType)
      val catalystExpected = CatalystTypeConverters.convertToCatalyst(expected)
      assert(checkResult(actual, catalystExpected, udt.sqlType, origExpr.nullable),
        s"Incorrect internal UDT evaluation: $origExpr, " +
          s"actual: $actual, expected: $catalystExpected")
      logInfo("UDT literal checked as internal rows; native GPU UDT coverage is not claimed.")

    case _: ObjectType =>
      // ObjectType is a JVM-only literal representation, not a DataFrame row type.
      // Use the original Spark expression helper without changing its type or expected value.
      shouldNotFallback()
      TestStats.testUnitNumber += 1
      super[LiteralExpressionSuite].checkEvaluation(origExpr, expected, inputRow)
      logInfo("ObjectType literal checked by the original CPU helper; no GPU execution claimed.")

    case _ =>
      super.rapidsCheckExpression(origExpr, expected, inputRow)
  }
}
