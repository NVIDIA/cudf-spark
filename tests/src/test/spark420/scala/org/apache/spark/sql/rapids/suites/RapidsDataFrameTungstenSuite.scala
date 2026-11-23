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
{"spark":["420"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.{GpuExec, ShimLoaderTemp}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.DataFrameTungstenSuite
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsDataFrameTungstenSuite extends DataFrameTungstenSuite with RapidsSQLTestsTrait {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      ExecutionPlanCaptureCallback.startCapture()
      try {
        testFun
        val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        val plan = plans.lastOption.getOrElse(fail(s"No executed query captured in $testName"))
        val planHelper = ShimLoaderTemp.newAdaptiveSparkPlanHelperShim()
        val gpuNodes = planHelper.collect(plan) { case gpu: GpuExec => gpu }
        assert(gpuNodes.nonEmpty, s"No GPU execution in $testName:\n$plan")
        if (testName == "access only some column of the all of columns") {
          // The last query asserts the filtered count; the earlier count only warms the cache.
          ExecutionPlanCaptureCallback.assertContains(plan, "GpuFilterExec")
        }
        info(s"GPU execution verified for $testName: " +
          gpuNodes.map(_.getClass.getSimpleName).distinct.mkString(", "))
      } finally {
        ExecutionPlanCaptureCallback.endCapture()
      }
    }
  }
}
