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

import com.nvidia.spark.rapids.{GpuProjectExec, GpuRowToColumnarExec}

import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.sources.PrunedScanSuite

class RapidsPrunedScanSuite extends PrunedScanSuite with RapidsSQLTestsTrait {
  private case class PruningObservation(columns: Seq[String], numOutputRows: Long)

  // Adapted from Spark 3.3 PrunedScanSuite lines 112-154. The inherited helper executes the CPU
  // RowDataSourceScanExec directly instead of executing the complete query-level GPU plan.
  // https://github.com/apache/spark/blob/v3.3.0/sql/core/src/test/scala/org/apache/spark/sql/
  // sources/PrunedScanSuite.scala#L112-L154
  testRapidsPruning("SELECT * FROM oneToTenPruned", "a", "b")
  testRapidsPruning("SELECT a, b FROM oneToTenPruned", "a", "b")
  testRapidsPruning("SELECT b, a FROM oneToTenPruned", "b", "a")
  testRapidsPruning("SELECT b, b FROM oneToTenPruned", "b")
  testRapidsPruning("SELECT a FROM oneToTenPruned", "a")
  testRapidsPruning("SELECT b FROM oneToTenPruned", "b")
  testRapidsPruning("SELECT a, rand(7) FROM oneToTenPruned WHERE a > 5", "a")
  testRapidsPruning("SELECT a FROM oneToTenPruned WHERE rand(11) > 0.5", "a")
  testRapidsPruning("SELECT a, rand(7) FROM oneToTenPruned WHERE rand(11) > 0.5", "a")
  testRapidsPruning("SELECT a, rand(7) FROM oneToTenPruned WHERE b > 5", "a", "b")

  private def testRapidsPruning(sqlString: String, expectedColumns: String*): Unit = {
    testRapids(s"Columns output ${expectedColumns.mkString(",")}: $sqlString") {
      spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, false)
      try {
        val (cpuObservation, _) = executeQueryAndObservePruning(sqlString, rapidsEnabled = false)
        val (gpuObservation, gpuPlan) =
          executeQueryAndObservePruning(sqlString, rapidsEnabled = true)

        assert(cpuObservation.columns === expectedColumns)
        assert(cpuObservation.numOutputRows > 0,
          s"Expected the complete CPU plan to consume source rows for $sqlString")
        assert(gpuObservation === cpuObservation,
          s"GPU and CPU pruning observations differ for $sqlString")
        assertGpuQueryPlan(gpuPlan)
      } finally {
        spark.conf.set(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.defaultValue.get)
      }
    }
  }

  private def executeQueryAndObservePruning(
      sqlString: String,
      rapidsEnabled: Boolean): (PruningObservation, SparkPlan) = {
    val rapidsSqlEnabledKey = "spark.rapids.sql.enabled"
    val originalRapidsEnabled = spark.conf.get(rapidsSqlEnabledKey)
    spark.conf.set(rapidsSqlEnabledKey, rapidsEnabled)
    try {
      val query = sql(sqlString).selectExpr("*", "1 AS __rapids_probe")
      val queryExecution = query.queryExecution
      val plan = queryExecution.executedPlan
      val sourceScan = plan.collect {
        case scan: DataSourceScanExec => scan
      } match {
        case Seq(scan) => scan
        case _ => fail(s"Expected exactly one DataSourceScanExec\n$queryExecution")
      }

      // A row-width mismatch fails while the complete plan converts source rows into a batch.
      query.collect()
      (PruningObservation(
        sourceScan.output.map(_.name),
        sourceScan.metrics("numOutputRows").value), plan)
    } finally {
      spark.conf.set(rapidsSqlEnabledKey, originalRapidsEnabled)
    }
  }

  private def assertGpuQueryPlan(plan: SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[GpuRowToColumnarExec]).nonEmpty,
      s"Expected GpuRowToColumnarExec above the CPU data source scan:\n$plan")
    assert(plan.find(_.isInstanceOf[GpuProjectExec]).nonEmpty,
      s"Expected GpuProjectExec in the query-level plan:\n$plan")
  }
}
