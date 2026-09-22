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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.command.{
  DSV2CharVarcharDDLTestSuite,
  FileSourceCharVarcharDDLTestSuite
}
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsFileSourceCharVarcharDDLTestSuite
    extends FileSourceCharVarcharDDLTestSuite with RapidsSQLTestsTrait {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    val isGpuCtasMetadataTest =
      testName == "SPARK-33901: ctas should should not change table's schema" ||
        testName == "SPARK-37160: CREATE TABLE AS SELECT with CHAR_AS_VARCHAR"
    if (isGpuCtasMetadataTest) {
      super.test(testName, testTags: _*) {
        ExecutionPlanCaptureCallback.startCapture()
        try {
          testFun
          val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
          assert(plans.exists(ExecutionPlanCaptureCallback.contains(
            _, "GpuDataWritingCommandExec")),
            s"Did not capture a GPU CTAS write plan:\n${plans.mkString("\n")}")
        } finally {
          ExecutionPlanCaptureCallback.endCapture()
        }
      }
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}

class RapidsDSV2CharVarcharDDLTestSuite
    extends DSV2CharVarcharDDLTestSuite with RapidsSQLTestsTrait
