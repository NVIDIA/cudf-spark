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

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.DataFrameSuite
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for DataFrame operations.
 *
 * This test suite validates DataFrame operation execution on GPU.
 * It extends the original Spark DataFrameSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/DataFrameSuite.scala
 * Test count: 198 tests
 *
 * Migration notes:
 * - DataFrameSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper,
 *   so we use RapidsSQLTestsTrait
 * - This is a comprehensive DataFrame test suite covering:
 *   - DataFrame creation and conversion
 *   - Column operations and transformations
 *   - Filtering, sorting, grouping
 *   - Joins and unions
 *   - Aggregations and window functions
 *   - UDFs and encoders
 *   - Caching and persistence
 *   - Schema operations
 */
class RapidsDataFrameSuite
  extends DataFrameSuite
  with RapidsSQLTestsTrait {

  // All 198 tests from DataFrameSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific DataFrame configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "reuse exchange"
  // Original test: DataFrameSuite.scala lines 2053-2076
  // Change to use GPU class name
  testRapids("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      checkAnswer(join, df)
      
      // GPU uses Gpu* versions of Exchange operators
      import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
      
      // Check for GPU shuffle exchange
      val shuffleExchanges = collect(join.queryExecution.executedPlan) {
        case e if e.getClass.getName.contains("GpuShuffleExchange") => true
      }
      assert(shuffleExchanges.size === 1,
        s"Expected 1 shuffle exchange, got ${shuffleExchanges.size}")
      
      assert(
        collect(join.queryExecution.executedPlan) {
          case _: ReusedExchangeExec => true }.size === 1)
      
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      
      val shuffleExchanges2 = collect(join2.queryExecution.executedPlan) {
        case e if e.getClass.getName.contains("GpuShuffleExchange") => true
      }
      assert(shuffleExchanges2.size == 1)
      
      val broadcastExchanges = collect(join2.queryExecution.executedPlan) {
        case e if e.getClass.getName.contains("GpuBroadcastExchange") => true
      }
      assert(broadcastExchanges.size === 1)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case _: ReusedExchangeExec => true }.size == 4)
    }
  }

  // GPU-specific test for "SPARK-27439: Explain result should match collected result
  // after view change"
  // Original test: DataFrameSuite.scala lines 2557-2579
  testRapids("SPARK-27439: Explain result should match collected result after view change") {
    withTempView("test", "test2", "tmp") {
      spark.range(10).createOrReplaceTempView("test")
      spark.range(5).createOrReplaceTempView("test2")
      spark.sql("select * from test").createOrReplaceTempView("tmp")
      val df = spark.sql("select * from tmp")
      spark.sql("select * from test2").createOrReplaceTempView("tmp")

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.explain(extended = true)
      }
      checkAnswer(df, spark.range(10).toDF)
      val output = captured.toString
      assert(output.contains(
        """== Parsed Logical Plan ==
          |'Project [*]
          |+- 'UnresolvedRelation [tmp]""".stripMargin))
      // change to GpuRange
      assert(output.contains(
        """== Physical Plan ==
          |GpuColumnarToRow false
          |+- GpuRange (0, 10, step=1, splits=2)""".stripMargin))
    }
  }
}
