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
package org.apache.spark.sql.rapids.utils

import com.nvidia.spark.rapids.{GpuExec, ShimLoaderTemp}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

/** Verifies a representative inherited action while retaining the suite's auxiliary checks. */
trait RapidsGpuPlanTest extends RapidsSQLTestsTrait {
  protected def gpuTestName: String
  protected def gpuOperator: Class[_ <: GpuExec]

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      if (testName == gpuTestName) {
        ExecutionPlanCaptureCallback.startCapture()
        try {
          testFun
          val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
          val helper = ShimLoaderTemp.newAdaptiveSparkPlanHelperShim()
          val operators = plans.toSeq.flatMap { plan =>
            helper.collect(plan) { case gpu: GpuExec if gpuOperator.isInstance(gpu) => gpu }
          }
          assert(operators.nonEmpty,
            s"No ${gpuOperator.getSimpleName} executed in $testName:\n${plans.mkString("\n")}")
          info(s"GPU execution verified for $testName: ${gpuOperator.getSimpleName}")
        } finally {
          ExecutionPlanCaptureCallback.endCapture()
        }
      } else {
        testFun
      }
    }
  }
}
