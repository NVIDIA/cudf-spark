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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.GpuExec
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

/** Verifies GPU execution of the upstream scenario selected by the version-delta audit. */
trait RapidsGpuPlanTest extends RapidsSQLTestsTrait with AdaptiveSparkPlanHelper {
  protected def gpuTestNames: Set[String]
  protected def gpuOperator: Class[_ <: GpuExec]
  protected def requireGpuForEveryAnswer: Boolean = false
  protected def gpuExpressions: Seq[Class[_ <: Expression]] = Seq.empty

  private var checkingGpuTest = false
  private val answerPlans = ArrayBuffer.empty[SparkPlan]

  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val actual = df
    super.checkAnswer(actual, expectedAnswer)
    if (checkingGpuTest) {
      // Keep the actual result plan, excluding any reference action used to build expectedAnswer.
      answerPlans += actual.queryExecution.executedPlan
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      if (gpuTestNames.contains(testName)) {
        answerPlans.clear()
        checkingGpuTest = true
        ExecutionPlanCaptureCallback.startCapture()
        try {
          testFun
          val captured = ExecutionPlanCaptureCallback.getResultsWithTimeout()
          // Tests using count()/collect() directly have no checkAnswer result to retain.
          val plans = if (answerPlans.nonEmpty) answerPlans.toSeq else captured.toSeq
          val operatorsByPlan = plans.map { plan =>
            collectWithSubqueries(plan) {
              case gpu: GpuExec if gpuOperator.isInstance(gpu) => gpu
            }
          }
          val operators = operatorsByPlan.flatten
          if (requireGpuForEveryAnswer) {
            assert(operatorsByPlan.forall(_.nonEmpty),
              s"GPU execution missing from a result query in $testName")
          }
          assert(operators.nonEmpty,
            s"No ${gpuOperator.getSimpleName} executed in $testName:\n${plans.mkString("\n")}")
          gpuExpressions.foreach { expressionClass =>
            assert(operators.exists(_.expressions.exists(_.exists(expressionClass.isInstance))),
              s"No ${expressionClass.getSimpleName} executed in $testName")
          }
          info(s"GPU delta verified for $testName: ${gpuOperator.getSimpleName}" +
            gpuExpressions.map(_.getSimpleName).mkString(" [", ", ", "]"))
        } finally {
          checkingGpuTest = false
          answerPlans.clear()
          ExecutionPlanCaptureCallback.endCapture()
        }
      } else {
        testFun
      }
    }
  }
}
