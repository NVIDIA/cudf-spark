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

import java.time.Period

import org.apache.spark.sql.{ApproxCountDistinctForIntervalsQuerySuite, Dataset}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproxCountDistinctForIntervals
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsApproxCountDistinctForIntervalsQuerySuite
  extends ApproxCountDistinctForIntervalsQuerySuite with RapidsSQLTestsTrait {
  test("RAPIDS interval NDV retains the target CPU aggregate") {
    import testImplicits._

    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val input = Seq(Period.ofMonths(100), Period.ofMonths(200), Period.ofMonths(300))
        .toDF("value")
      def query = {
        val relation = input.logicalPlan
        val aggregate = ApproxCountDistinctForIntervals(
          relation.output.head, CreateArray((0 to 5).map(i => Literal(i / 10))))
          .toAggregateExpression()
        Dataset.ofRows(spark, Aggregate(Nil, Seq(Alias(aggregate, "ndv")()), relation))
      }
      withSQLConf("spark.rapids.sql.enabled" -> "false") {
        val cpu = query
        val expected = cpu.collect().toSeq
        withSQLConf("spark.rapids.sql.enabled" -> "true") {
          val gpu = query
          checkAnswer(gpu, expected)
          assert(gpu.schema == cpu.schema)
          val aggregates = gpu.queryExecution.executedPlan.collect {
            case aggregate: ObjectHashAggregateExec => aggregate
          }
          assert(aggregates.exists(_.aggregateExpressions.exists(
            _.aggregateFunction.isInstanceOf[ApproxCountDistinctForIntervals])),
            s"Expected interval NDV in a CPU aggregate: ${gpu.queryExecution.executedPlan}")
        }
      }
    }
  }
}
