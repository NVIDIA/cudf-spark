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

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuBuildSide}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.joins.BroadcastJoinSuiteBase
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuBroadcastNestedLoopJoinExec}
import org.apache.spark.sql.rapids.utils.{RapidsSQLTestsBaseTrait, RapidsTestsBaseTrait}
import org.apache.spark.sql.rapids.utils.RapidsTestConstants.RAPIDS_TEST

trait RapidsBroadcastJoinSuiteBase extends BroadcastJoinSuiteBase with RapidsTestsBaseTrait {
  import testImplicits._

  protected def testRapids(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    test(RAPIDS_TEST + testName, testTag: _*)(testFun)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }

  override def beforeAll(): Unit = {
    // Do the SparkFunSuite setup that BroadcastJoinSuiteBase.beforeAll() would normally do.
    // Calling super.beforeAll() here would create Spark's local-cluster session before RAPIDS
    // configs are applied.
    System.setProperty(IS_TESTING.key, "true")
    if (enableAutoThreadAudit) {
      doThreadPreAudit()
    }

    val conf = RapidsSQLTestsBaseTrait.nativeSparkConf(new SparkConf(), warehouse)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("testing")
      .config(conf)
      .getOrCreate()
  }

  testRapids("broadcast hint uses GpuBroadcastHashJoinExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
      val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
      val joined = df1.join(broadcast(df2), Seq("key"), "inner")

      checkAnswer(joined, Seq(Row(1, "4", "1"), Row(2, "2", "2")))

      val joins = joined.queryExecution.executedPlan.collect {
        case join: GpuBroadcastHashJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastHashJoinExec in plan:\n${joined.queryExecution.executedPlan}")
    }
  }

  testRapids("SPARK-23192: broadcast hint should be retained after using the cached data") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      try {
        val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
        val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
        df2.cache()
        val joined = df1.join(broadcast(df2), Seq("key"), "inner")

        checkAnswer(joined, Seq(Row(1, "4", "1"), Row(2, "2", "2")))

        val joins = collect(joined.queryExecution.executedPlan) {
          case join: GpuBroadcastHashJoinExec => join
        }
        assert(joins.size === 1,
          s"Expected one GpuBroadcastHashJoinExec in plan:\n" +
            joined.queryExecution.executedPlan)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  testRapids("SPARK-23214: cached data should not carry extra hint info") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      try {
        val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
        val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
        broadcast(df2).cache()

        val joined = df1.join(df2, Seq("key"), "inner")

        checkAnswer(joined, Seq(Row(1, "4", "1"), Row(2, "2", "2")))

        val cachedScans = collect(joined.queryExecution.executedPlan) {
          case scan: GpuInMemoryTableScanExec => scan
        }
        assert(cachedScans.size === 1,
          s"Expected one GpuInMemoryTableScanExec in plan:\n" +
            joined.queryExecution.executedPlan)

        val broadcastJoins = collect(joined.queryExecution.executedPlan) {
          case join: GpuBroadcastHashJoinExec => join
        }
        assert(broadcastJoins.isEmpty,
          s"Did not expect GpuBroadcastHashJoinExec in plan:\n" +
            joined.queryExecution.executedPlan)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  testRapids("SPARK-37742: join planning shouldn't read invalid InMemoryRelation stats") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10") {
      try {
        val df1 = Seq(1).toDF("key")
        val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
        df2.persist()
        df2.queryExecution.toRdd

        val joined = df1.join(df2, Seq("key"), "inner")

        checkAnswer(joined, Seq(Row(1, "1")))

        val cachedScans = collect(joined.queryExecution.executedPlan) {
          case scan: GpuInMemoryTableScanExec => scan
        }
        assert(cachedScans.size === 1,
          s"Expected one GpuInMemoryTableScanExec in plan:\n" +
            joined.queryExecution.executedPlan)

        val broadcastJoins = collect(joined.queryExecution.executedPlan) {
          case join: GpuBroadcastHashJoinExec => join
        }
        assert(broadcastJoins.isEmpty,
          s"Did not expect GpuBroadcastHashJoinExec in plan:\n" +
            joined.queryExecution.executedPlan)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }


  testRapids("Shouldn't change broadcast join buildSide if user clearly specified") {
    def assertGpuBroadcastHashJoinBuildSide(
        sqlText: String,
        expectedBuildSide: GpuBuildSide,
        expectedRows: Int): Unit = {
      val df = sql(sqlText)
      val rows = df.collect()
      assert(rows.length === expectedRows)

      val joins = collect(df.queryExecution.executedPlan) {
        case join: GpuBroadcastHashJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastHashJoinExec in plan:\n" +
          df.queryExecution.executedPlan)
      assert(joins.head.buildSide === expectedBuildSide,
        s"Unexpected build side in plan:\n" + df.queryExecution.executedPlan)
    }

    def assertGpuBroadcastNestedLoopJoinBuildSide(
        sqlText: String,
        expectedBuildSide: GpuBuildSide,
        expectedRows: Int): Unit = {
      val df = sql(sqlText)
      val rows = df.collect()
      assert(rows.length === expectedRows)

      val joins = collect(df.queryExecution.executedPlan) {
        case join: GpuBroadcastNestedLoopJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastNestedLoopJoinExec in plan:\n" +
          df.queryExecution.executedPlan)
      assert(joins.head.gpuBuildSide === expectedBuildSide,
        s"Unexpected build side in plan:\n" + df.queryExecution.executedPlan)
    }

    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value")
        .createTempView("t2")

      assertGpuBroadcastHashJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.key = t2.key",
        GpuBuildLeft,
        3)
      assertGpuBroadcastHashJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 LEFT JOIN t2 ON t1.key = t2.key",
        GpuBuildRight,
        3)
      assertGpuBroadcastHashJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 RIGHT JOIN t2 ON t1.key = t2.key",
        GpuBuildLeft,
        3)
      assertGpuBroadcastHashJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1) */ * FROM t1 JOIN t2 ON t1.key = t2.key",
        GpuBuildLeft,
        3)
      assertGpuBroadcastHashJoinBuildSide(
        "SELECT /*+ MAPJOIN(t2) */ * FROM t1 JOIN t2 ON t1.key = t2.key",
        GpuBuildRight,
        3)

      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // Full outer BNLJ and preserved-side outer builds remain CPU-only in RAPIDS.
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 JOIN t2",
          GpuBuildLeft,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 LEFT JOIN t2",
          GpuBuildRight,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 RIGHT JOIN t2",
          GpuBuildLeft,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT /*+ MAPJOIN(t1) */ * FROM t1 JOIN t2",
          GpuBuildLeft,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT /*+ MAPJOIN(t2) */ * FROM t1 JOIN t2",
          GpuBuildRight,
          6)
      }
    }
  }

  testRapids("Shouldn't bias towards build right if user didn't specify") {
    def assertGpuBroadcastHashJoinBuildSide(
        sqlText: String,
        expectedBuildSide: GpuBuildSide,
        expectedRows: Int): Unit = {
      val df = sql(sqlText)
      val rows = df.collect()
      assert(rows.length === expectedRows)

      val joins = collect(df.queryExecution.executedPlan) {
        case join: GpuBroadcastHashJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastHashJoinExec in plan:\n" +
          df.queryExecution.executedPlan)
      assert(joins.head.buildSide === expectedBuildSide,
        s"Unexpected build side in plan:\n" + df.queryExecution.executedPlan)
    }

    def assertGpuBroadcastNestedLoopJoinBuildSide(
        sqlText: String,
        expectedBuildSide: GpuBuildSide,
        expectedRows: Int): Unit = {
      val df = sql(sqlText)
      val rows = df.collect()
      assert(rows.length === expectedRows)

      val joins = collect(df.queryExecution.executedPlan) {
        case join: GpuBroadcastNestedLoopJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastNestedLoopJoinExec in plan:\n" +
          df.queryExecution.executedPlan)
      assert(joins.head.gpuBuildSide === expectedBuildSide,
        s"Unexpected build side in plan:\n" + df.queryExecution.executedPlan)
    }

    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value")
        .createTempView("t2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t1 JOIN t2 ON t1.key = t2.key",
          GpuBuildLeft,
          3)
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t2 JOIN t1 ON t1.key = t2.key",
          GpuBuildRight,
          3)
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t1 LEFT JOIN t2 ON t1.key = t2.key",
          GpuBuildRight,
          3)
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t2 LEFT JOIN t1 ON t1.key = t2.key",
          GpuBuildRight,
          3)
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t1 RIGHT JOIN t2 ON t1.key = t2.key",
          GpuBuildLeft,
          3)
        assertGpuBroadcastHashJoinBuildSide(
          "SELECT * FROM t2 RIGHT JOIN t1 ON t1.key = t2.key",
          GpuBuildLeft,
          3)
      }

      withSQLConf(
          SQLConf.CROSS_JOINS_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        // Full outer BNLJ remains CPU-only in RAPIDS.
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t1 JOIN t2",
          GpuBuildLeft,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t2 JOIN t1",
          GpuBuildRight,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t1 LEFT JOIN t2",
          GpuBuildRight,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t2 LEFT JOIN t1",
          GpuBuildRight,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t1 RIGHT JOIN t2",
          GpuBuildLeft,
          6)
        assertGpuBroadcastNestedLoopJoinBuildSide(
          "SELECT * FROM t2 RIGHT JOIN t1",
          GpuBuildLeft,
          6)
      }
    }
  }

  testRapids("broadcast non-equi join uses GpuBroadcastNestedLoopJoinExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = Seq(1, 2).toDF("left")
      val df2 = Seq(1, 3).toDF("right")
      val joined = df1.join(broadcast(df2), $"left" < $"right")

      checkAnswer(joined, Seq(Row(1, 3), Row(2, 3)))

      val joins = joined.queryExecution.executedPlan.collect {
        case join: GpuBroadcastNestedLoopJoinExec => join
      }
      assert(joins.size === 1,
        s"Expected one GpuBroadcastNestedLoopJoinExec in plan:\n" +
          joined.queryExecution.executedPlan)
    }
  }

}

class RapidsBroadcastJoinSuite
  extends RapidsBroadcastJoinSuiteBase
  with DisableAdaptiveExecutionSuite

class RapidsBroadcastJoinSuiteAE
  extends RapidsBroadcastJoinSuiteBase
  with EnableAdaptiveExecutionSuite
