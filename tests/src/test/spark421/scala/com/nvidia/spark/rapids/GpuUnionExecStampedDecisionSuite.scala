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
{"spark":["421"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

class GpuUnionExecStampedDecisionSuite extends SparkQueryCompareTestSuite {
  private val conf = new SparkConf()
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    .set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "false")

  test("GpuUnionExec honors Spark's stamped partitioning decision during AQE") {
    withGpuSparkSession({ spark =>
      val left = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k")
      val right = spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k")
      val planned = left.repartition(4, col("k"))
        .union(right.repartition(4, col("k")))
        .groupBy("k")
        .count()

      // Spark 4.2.1 stamps the UnionExec decision during physical preparation. Earlier
      // releases retain the upstream behavior of consulting the live SQLConf during AQE.
      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        planned.queryExecution.executedPlan
      }

      ExecutionPlanCaptureCallback.startCapture()
      try {
        withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
          val actual = planned.collect()
            .map(row => row.getLong(0) -> row.getLong(1))
            .sortBy(_._1)
            .toSeq
          assertResult((0L until 5L).map(_ -> 8L))(actual)
        }

        val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        val planHelper = ShimLoaderTemp.newAdaptiveSparkPlanHelperShim()
        val gpuUnions = plans.flatMap { plan =>
          planHelper.collect(plan) { case union: GpuUnionExec => union }
        }
        assert(gpuUnions.nonEmpty,
          s"Expected a GpuUnionExec in adaptive plans:\n${plans.mkString("\n")}")
        assert(gpuUnions.forall(!_.isPlainUnion),
          s"Expected the stamped partitioning-aware decision:\n${gpuUnions.mkString("\n")}")
        assert(gpuUnions.forall(
          !_.outputPartitioning.isInstanceOf[UnknownPartitioning]),
          s"Expected concrete union partitioning:\n${gpuUnions.mkString("\n")}")
        assert(gpuUnions.forall(_.outputPartitioning.numPartitions == 4),
          s"Expected four preserved union partitions:\n${gpuUnions.mkString("\n")}")
      } finally {
        ExecutionPlanCaptureCallback.endCapture()
      }
    }, conf)
  }
}
