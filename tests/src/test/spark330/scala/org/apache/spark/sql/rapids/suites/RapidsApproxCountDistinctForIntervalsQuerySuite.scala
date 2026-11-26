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

import java.time.{Duration, Period}

import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec}

import org.apache.spark.sql.{ApproxCountDistinctForIntervalsQuerySuite, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproxCountDistinctForIntervals
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsApproxCountDistinctForIntervalsQuerySuite
  extends ApproxCountDistinctForIntervalsQuerySuite with RapidsSQLTestsTrait {
  testRapids("SPARK-37138: ANSI interval NDV falls back after GPU input") {
    import testImplicits._

    val table = "rapids_approx_count_distinct_for_ansi_intervals"
    withTable(table) {
      Seq((100, 100L), (200, 250L), (200, 250L), (300, 300L), (600, 600L))
        .map { case (months, seconds) => (Period.ofMonths(months), Duration.ofSeconds(seconds)) }
        .toDF("months", "duration").createOrReplaceTempView(table)

      def query: DataFrame = {
        val relation = spark.table(table).logicalPlan
        val zeroEndpoints = Seq.fill(6)(Literal(0))
        val bounds = Seq(0, 200, 200, 350, 500)

        def ndv(column: Int, endpoints: Seq[Literal], name: String): Alias = {
          val function = ApproxCountDistinctForIntervals(
            relation.output(column), CreateArray(endpoints))
          Alias(function.toAggregateExpression(), name)()
        }

        val aggregates = Seq(
          ndv(0, zeroEndpoints, "months_zero_width"),
          ndv(1, zeroEndpoints, "duration_zero_width"),
          ndv(0, bounds.map(Literal(_)), "months_ndv"),
          ndv(1, bounds.map(bound => Literal(bound * MICROS_PER_SECOND)), "duration_ndv"))
        Dataset.ofRows(spark, Aggregate(Nil, aggregates, relation))
      }

      // Keep the original zero-width bucket checks. The other buckets also cover in-range
      // duplicates, empty ranges, and out-of-range values in both interval types.
      val expected = Row(Seq.fill(5)(1L), Seq.fill(5)(1L),
        Seq(2L, 1L, 1L, 0L), Seq(1L, 1L, 2L, 0L))
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          "spark.rapids.sql.enabled" -> "false") {
        val cpu = query
        val cpuRows = cpu.collect().toSeq
        assert(cpuRows == Seq(expected))
        withSQLConf("spark.rapids.sql.enabled" -> "true") {
          val gpu = query
          assert(gpu.collect().toSeq == cpuRows)
          assert(gpu.schema == cpu.schema)
          assert(getExecutedPlan(gpu).exists {
            case aggregate: ObjectHashAggregateExec =>
              aggregate.aggregateExpressions.size == expected.length &&
                aggregate.aggregateExpressions.forall { expr =>
                  expr.mode == Partial &&
                    expr.aggregateFunction.isInstanceOf[ApproxCountDistinctForIntervals]
                } && aggregate.child.exists {
                  case transition: GpuColumnarToRowExec =>
                    transition.child.exists(_.isInstanceOf[GpuExec])
                  case _ => false
                }
            case _ => false
          }, s"Expected GPU input to the partial CPU interval NDV aggregate: " +
            s"${gpu.queryExecution.executedPlan}")
        }
      }
    }
  }
}
