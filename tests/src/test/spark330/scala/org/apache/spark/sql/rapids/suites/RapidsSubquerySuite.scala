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

import org.apache.spark.sql.{Row, SubquerySuite}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, GpuScalarSubquery}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait


/**
 * RAPIDS GPU tests for Subquery operations.
 *
 * This test suite validates Subquery operation execution on GPU.
 * It extends the original Spark SubquerySuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/SubquerySuite.scala
 * Test count: 88 tests
 *
 * Migration notes:
 * - SubquerySuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper,
 *   so we use RapidsSQLTestsTrait
 * - This test suite covers:
 *   - Scalar subqueries
 *   - IN/NOT IN subqueries
 *   - EXISTS/NOT EXISTS subqueries
 *   - Correlated subqueries
 *   - Subquery reuse and optimization
 *   - Nested subqueries
 */
class RapidsSubquerySuite
  extends SubquerySuite
  with RapidsSQLTestsTrait {
  // All 88 tests from SubquerySuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific Subquery configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "SPARK-27279: Reuse Subquery"
  // Original test: SubquerySuite.scala lines 1377-1408
  testRapids("SPARK-27279: Reuse Subquery", DisableAdaptiveExecution("reuse is dynamic in AQE")) {
    Seq(true, false).foreach { reuse =>
      withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> reuse.toString) {
        val df = sql(
          """
            |SELECT (SELECT avg(key) FROM testData) + (SELECT avg(key) FROM testData)
            |FROM testData
            |LIMIT 1
          """.stripMargin)

        var countSubqueryExec = 0
        var countReuseSubqueryExec = 0
        df.queryExecution.executedPlan.transformAllExpressions {
          case s @ GpuScalarSubquery(_: SubqueryExec, _) =>
            countSubqueryExec = countSubqueryExec + 1
            s
          case s @ GpuScalarSubquery(_: ReusedSubqueryExec, _) =>
            countReuseSubqueryExec = countReuseSubqueryExec + 1
            s
        }

        if (reuse) {
          assert(countSubqueryExec == 1, "Subquery reusing not working correctly")
          assert(countReuseSubqueryExec == 1, "Subquery reusing not working correctly")
        } else {
          assert(countSubqueryExec == 2, "expect 2 SubqueryExec when not reusing")
          assert(countReuseSubqueryExec == 0,
            "expect 0 ReusedSubqueryExec when not reusing")
        }
      }
    }
  }

  // Original: SubquerySuite.scala (Spark v3.3.0) lines 1321-1339.
  // GPU plan replaces FileSourceScanExec with GpuFileSourceScanExec, so
  // the original WholeStageCodegen→ColumnarToRow→InputAdapter→
  // FileSourceScanExec match never fires. Verify the same intent on the
  // GPU node: a single GpuFileSourceScanExec whose partitionFilters
  // contain a subquery (i.e. partition pruning was pushed into the
  // scan), and dynamicallySelectedPartitions narrowed runtime reads to
  // p=0 only — directly mirroring the CPU test's
  // FileScanRDD.filePartitions check.
  // https://github.com/NVIDIA/spark-rapids/issues/14172
  testRapids("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))

      val plan = df.queryExecution.executedPlan
      val gpuScans = collect(plan) { case s: GpuFileSourceScanExec => s }
      // Pick the scan whose partitionFilters contain a subquery (pushdown target)
      // rather than asserting on total scan count, so the test stays robust if
      // additional file scans (e.g. table `b` inside the scalar subquery) become
      // GpuFileSourceScanExec as GPU coverage expands.
      val fs = gpuScans.find(_.partitionFilters.exists(ExecSubqueryExpression.hasSubquery))
        .getOrElse(fail(s"expected a GpuFileSourceScanExec with a subquery in partitionFilters " +
          s"(pushdown failed); found ${gpuScans.size} GPU scan(s) total"))
      val readFiles = fs.dynamicallySelectedPartitions.flatMap(_.files)
      assert(readFiles.nonEmpty, "no files selected after dynamic pruning")
      assert(readFiles.forall(_.getPath.toString.contains("p=0")),
        s"expected only p=0 files, got ${readFiles.map(_.getPath).mkString(", ")}")
    }
  }

  // GPU-specific test for "SPARK-36280"
  // Original test: SubquerySuite.scala lines 1883-1905
  testRapids("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 USING parquet AS SELECT id AS a, id AS b, id AS c FROM range(10)")
      sql("CREATE TABLE t2 USING parquet AS SELECT id AS x, id AS y FROM range(8)")
      val df = sql(
        """
          |SELECT *
          |FROM   t1
          |WHERE  a IN (SELECT x
          |             FROM   (SELECT x AS x,
          |                            RANK() OVER (PARTITION BY x ORDER BY SUM(y) DESC) AS ranking
          |                     FROM   t2
          |                     GROUP  BY x) tmp1
          |             WHERE  ranking <= 5)
          |""".stripMargin)

      df.collect()

      // Check for GPU shuffle exchange
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: GpuShuffleExchangeExecBase => s
      }
      assert(exchanges.size === 1, s"Expected 1 shuffle exchange, got ${exchanges.size}")
    }
  }
}
