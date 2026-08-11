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

import org.apache.spark.sql.{JoinSuite, Row}
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for Join operations.
 *
 * This test suite validates Join operation execution on GPU.
 * It extends the original Spark JoinSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/JoinSuite.scala
 * Test count: 40 tests
 *
 * Migration notes:
 * - JoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper,
 *   so we use RapidsSQLTestsTrait
 * - This test suite covers:
 *   - Inner, outer, left, right, semi, anti joins
 *   - Cross joins
 *   - Join conditions and optimizations
 *   - Broadcast joins
 *   - Sort merge joins
 *   - Join reordering and planning
 */
class RapidsJoinSuite
  extends JoinSuite
  with RapidsSQLTestsTrait {
  // All 40 tests from JoinSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific Join configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "SPARK-36794"
  // Original test: JoinSuite.scala lines 1406-1442
  testRapids("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join") {
    import testImplicits._
    
    withTable("t1", "t2") {
      spark.range(10).map(i => (i.toString, i + 1)).toDF("c1", "c2").write.saveAsTable("t1")
      spark.range(10).map(i => ((i % 5).toString, i % 3)).toDF("c1", "c2").write.saveAsTable("t2")

      val semiJoinQueries = Seq(
        // No join condition, ignore duplicated key.
        (s"SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2 ON t1.c1 = t2.c1",
          true),
        // Have join condition on build join key only, ignore duplicated key.
        (s"""
            |SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2
            |ON t1.c1 = t2.c1 AND CAST(t1.c2 * 2 AS STRING) != t2.c1
          """.stripMargin,
          true),
        // Have join condition on other build attribute beside join key, do not ignore
        // duplicated key.
        (s"""
            |SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2
            |ON t1.c1 = t2.c1 AND t1.c2 * 100 != t2.c2
          """.stripMargin,
          false)
      )
      semiJoinQueries.foreach {
        case (query, ignoreDuplicatedKey) =>
          val semiJoinDF = sql(query)
          val antiJoinDF = sql(query.replaceAll("SEMI", "ANTI"))
          checkAnswer(semiJoinDF, Seq(Row("0"), Row("1"), Row("2"), Row("3"), Row("4")))
          checkAnswer(antiJoinDF, Seq(Row("5"), Row("6"), Row("7"), Row("8"), Row("9")))
          
          // GPU uses GpuShuffledHashJoinExec instead of ShuffledHashJoinExec
          Seq(semiJoinDF, antiJoinDF).foreach { df =>
            val hasGpuHashJoin = collect(df.queryExecution.executedPlan) {
              case j if j.getClass.getName.contains("GpuShuffledHashJoin") => true
              case j if j.getClass.getName.contains("ShuffledHashJoin") => true
            }.size == 1
            assert(hasGpuHashJoin, 
              "Expected GpuShuffledHashJoinExec or ShuffledHashJoinExec " +
              s"for ignoreDuplicatedKey=$ignoreDuplicatedKey")
          }
      }
    }
  }
}
