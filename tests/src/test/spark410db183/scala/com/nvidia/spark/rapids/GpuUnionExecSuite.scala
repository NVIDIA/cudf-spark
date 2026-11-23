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
{"spark":["410db183"],"carryForward":"none"}
{"spark":["411","420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.exchange.REPARTITION_BY_NUM
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class GpuUnionExecSuite extends SparkQueryCompareTestSuite {
  private val conf = new SparkConf()
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  test("GpuUnionExec keeps its partitioning-aware decision when SQLConf changes") {
    withGpuSparkSession({ spark =>
      val left = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k")
      val right = spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k")
      val planned = left.repartition(4, col("k"))
        .union(right.repartition(4, col("k"))).groupBy("k").count()

      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val plan = planned.queryExecution.executedPlan
        val unions = plan.collect { case union: GpuUnionExec => union }
        assertResult(1, s"GPU unions in:\n$plan")(unions.size)
        assert(!unions.head.isPlainUnion,
          s"expected a partitioning-aware GPU union, got:\n$plan")
        assert(!unions.head.outputPartitioning.isInstanceOf[UnknownPartitioning],
          s"expected the GPU union to report concrete partitioning, got:\n$plan")

        val shuffles = plan.collect { case shuffle: GpuShuffleExchangeExec => shuffle }
        assertResult(2, s"GPU shuffles in:\n$plan")(shuffles.size)
        assert(shuffles.forall(_.shuffleOrigin == REPARTITION_BY_NUM),
          s"expected only the two explicit repartitions, got " +
            s"${shuffles.map(_.shuffleOrigin)}")
      }

      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
        val actual = planned.collect()
          .map(row => row.getLong(0) -> row.getLong(1))
          .sortBy(_._1)
          .toSeq
        assertResult((0L until 5L).map(_ -> 8L))(actual)
      }
    }, conf)
  }
}
