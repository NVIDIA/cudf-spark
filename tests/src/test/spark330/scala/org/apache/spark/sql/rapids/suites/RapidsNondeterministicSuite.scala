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

import com.nvidia.spark.rapids.{GpuMonotonicallyIncreasingID, GpuProjectExec,
  GpuSparkPartitionID, TestStats}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, InputFileName,
  MonotonicallyIncreasingID, NondeterministicSuite, SparkPartitionID}
import org.apache.spark.sql.rapids.GpuInputFileName
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

class RapidsNondeterministicSuite extends NondeterministicSuite
  with RapidsTestsTrait {

  override def rapidsCheckExpression(
      origExpr: Expression,
      expected: Any,
      inputRow: InternalRow): Unit = {
    val nativeClass: Option[Class[_]] = origExpr match {
      case _: MonotonicallyIncreasingID => Some(classOf[GpuMonotonicallyIncreasingID])
      case _: SparkPartitionID => Some(classOf[GpuSparkPartitionID])
      case _: InputFileName => Some(classOf[GpuInputFileName])
      case _ => None
    }
    nativeClass match {
      case Some(expectedNativeClass) =>
        // The original helper initializes these expressions at partition index 0 and
        // evaluates one row. Preserve that setup before selecting the expression.
        val resultDF = _spark.range(0, 1, 1, 1).select(Column(origExpr))
        // Dataset.collect also delivers this executed plan to the inherited audit listener.
        val result = resultDF.collect()
        TestStats.testUnitNumber += 1
        assert(result.length == 1)
        val actual = CatalystTypeConverters.createToCatalystConverter(origExpr.dataType)(
          result.head.get(0))
        val catalystExpected = CatalystTypeConverters.convertToCatalyst(expected)
        assert(checkResult(actual, catalystExpected, origExpr.dataType, origExpr.nullable),
          s"Incorrect single-partition evaluation: $origExpr, " +
            s"actual: $actual, expected: $catalystExpected")
        val projects = resultDF.queryExecution.executedPlan.collect {
          case project: GpuProjectExec => project
        }
        assert(projects.exists(_.expressions.exists { expression =>
          expression.find(expectedNativeClass.isInstance).nonEmpty
        }), s"Expected native ${expectedNativeClass.getName} in the executed GPU project")
        TestStats.offloadRapidsUnitNumber += 1

      case None =>
        super.rapidsCheckExpression(origExpr, expected, inputRow)
    }
  }
}
