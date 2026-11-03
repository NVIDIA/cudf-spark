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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.sources.{
  DisableUnnecessaryBucketedScanSuite,
  DisableUnnecessaryBucketedScanWithoutHiveSupportSuite,
  DisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE
}

private[suites] trait RapidsDisableUnnecessaryBucketedScanTests {
  self: DisableUnnecessaryBucketedScanSuite with RapidsSQLTestsTrait =>

  import testImplicits._

  private lazy val rapidsDf1 =
    (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
  private lazy val rapidsDf2 =
    (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k").as("df2")

  private def checkDisableBucketedScanRapids(
      query: String,
      expectedNumScanWithAutoScanEnabled: Int,
      expectedNumScanWithAutoScanDisabled: Int): Unit = {

    def checkNumBucketedScan(df: DataFrame, expectedNumBucketedScans: Int): Unit = {
      val plan = df.queryExecution.executedPlan
      val bucketedScans = plan.collect {
        case scan: GpuFileSourceScanExec if scan.bucketedScan => scan
        case scan: FileSourceScanExec if scan.bucketedScan => scan
      }
      assert(bucketedScans.length == expectedNumBucketedScans,
        s"Expected $expectedNumBucketedScans bucketed scans, found " +
          s"${bucketedScans.length}:\n$plan")
    }

    withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
      val autoScanEnabledQuery = sql(query)
      checkNumBucketedScan(autoScanEnabledQuery, expectedNumScanWithAutoScanEnabled)
      val result = autoScanEnabledQuery.collect()

      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {
        val autoScanDisabledQuery = sql(query)
        checkNumBucketedScan(autoScanDisabledQuery, expectedNumScanWithAutoScanDisabled)
        checkAnswer(autoScanDisabledQuery, result)
      }
    }
  }

  testRapids("SPARK-32859: disable unnecessary bucketed table scan - basic test") {
    withTable("t1", "t2", "t3") {
      rapidsDf1.write.format("parquet").bucketBy(8, "i").saveAsTable("t1")
      rapidsDf2.write.format("parquet").bucketBy(8, "i").saveAsTable("t2")
      rapidsDf2.write.format("parquet").bucketBy(4, "i").saveAsTable("t3")

      Seq(
        // Read bucketed table
        ("SELECT * FROM t1", 0, 1),
        ("SELECT i FROM t1", 0, 1),
        ("SELECT j FROM t1", 0, 0),
        // Filter on bucketed column
        ("SELECT * FROM t1 WHERE i = 1", 0, 1),
        // Filter on non-bucketed column
        ("SELECT * FROM t1 WHERE j = 1", 0, 1),
        // Join with same buckets
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 2, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 2, 2),
        // Join with different buckets
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 1, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 1, 2),
        // Join on non-bucketed column
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 1, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 1, 2),
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        // Aggregate on bucketed column
        ("SELECT SUM(i) FROM t1 GROUP BY i", 1, 1),
        // Aggregate on non-bucketed column
        ("SELECT SUM(i) FROM t1 GROUP BY j", 0, 1),
        ("SELECT j, SUM(i), COUNT(j) FROM t1 GROUP BY j", 0, 1)
      ).foreach { case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
        checkDisableBucketedScanRapids(
          query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  testRapids("SPARK-32859: disable unnecessary bucketed table scan - multiple joins test") {
    withTable("t1", "t2", "t3") {
      rapidsDf1.write.format("parquet").bucketBy(8, "i").saveAsTable("t1")
      rapidsDf2.write.format("parquet").bucketBy(8, "i").saveAsTable("t2")
      rapidsDf2.write.format("parquet").bucketBy(4, "i").saveAsTable("t3")

      Seq(
        // Multiple joins on bucketed columns
        ("""
           |SELECT /*+ broadcast(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.i AND t2.i = t3.i
           |""".stripMargin, 0, 3),
        ("""
           |SELECT /*+ broadcast(t1) merge(t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.i AND t2.i = t3.i
           |""".stripMargin, 2, 3),
        ("""
           |SELECT /*+ merge(t1) broadcast(t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.i AND t2.i = t3.i
           |""".stripMargin, 2, 3),
        ("""
           |SELECT /*+ merge(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.i AND t2.i = t3.i
           |""".stripMargin, 2, 3),
        // Multiple joins on non-bucketed columns
        ("""
           |SELECT /*+ broadcast(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.j AND t2.j = t3.i
           |""".stripMargin, 0, 3),
        ("""
           |SELECT /*+ merge(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.i = t2.j AND t2.j = t3.i
           |""".stripMargin, 1, 3),
        ("""
           |SELECT /*+ merge(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
           |ON t1.j = t2.j AND t2.j = t3.j
           |""".stripMargin, 0, 3)
      ).foreach { case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
        checkDisableBucketedScanRapids(
          query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  testRapids(
    "SPARK-32859: disable unnecessary bucketed table scan - multiple bucketed columns test") {
    withTable("t1", "t2", "t3") {
      rapidsDf1.write.format("parquet").bucketBy(8, "i", "j").saveAsTable("t1")
      rapidsDf2.write.format("parquet").bucketBy(8, "i", "j").saveAsTable("t2")
      rapidsDf2.write.format("parquet").bucketBy(4, "i", "j").saveAsTable("t3")

      Seq(
        // Filter on bucketed columns
        ("SELECT * FROM t1 WHERE i = 1", 0, 1),
        ("SELECT * FROM t1 WHERE i = 1 AND j = 1", 0, 1),
        // Join on bucketed columns
        ("""
           |SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i AND t1.j = t2.j
           |""".stripMargin, 0, 2),
        ("""
           |SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i AND t1.j = t2.j
           |""".stripMargin, 2, 2),
        ("""
           |SELECT /*+ merge(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i AND t1.j = t3.j
           |""".stripMargin, 1, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 0, 2),
        // Aggregate on bucketed columns
        ("SELECT i, j, COUNT(*) FROM t1 GROUP BY i, j", 1, 1),
        ("SELECT i, COUNT(i) FROM t1 GROUP BY i", 0, 0),
        ("SELECT i, COUNT(j) FROM t1 GROUP BY i", 0, 1)
      ).foreach { case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
        checkDisableBucketedScanRapids(
          query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  testRapids("SPARK-32859: disable unnecessary bucketed table scan - other operators test") {
    withTable("t1", "t2", "t3") {
      rapidsDf1.write.format("parquet").bucketBy(8, "i").saveAsTable("t1")
      rapidsDf2.write.format("parquet").bucketBy(8, "i").saveAsTable("t2")
      rapidsDf1.write.format("parquet").saveAsTable("t3")

      Seq(
        // Operator with interesting partition not in sub-plan
        ("""
           |SELECT t1.i FROM t1
           |UNION ALL
           |(SELECT t2.i FROM t2 GROUP BY t2.i)
           |""".stripMargin, 1, 2),
        // Non-allowed operator in sub-plan
        ("""
           |SELECT COUNT(*)
           |FROM (SELECT t1.i FROM t1 UNION ALL SELECT t2.i FROM t2)
           |GROUP BY i
           |""".stripMargin, 2, 2),
        // Multiple Exchange operators in sub-plan
        ("""
           |SELECT j, SUM(i), COUNT(*) FROM t1 GROUP BY j
           |DISTRIBUTE BY j
           |""".stripMargin, 0, 1),
        ("""
           |SELECT j, COUNT(*)
           |FROM (SELECT i, j FROM t1 DISTRIBUTE BY i, j)
           |GROUP BY j
           |""".stripMargin, 0, 1),
        // No bucketed table scan in plan
        ("""
           |SELECT j, COUNT(*)
           |FROM (SELECT t1.j FROM t1 JOIN t3 ON t1.j = t3.j)
           |GROUP BY j
           |""".stripMargin, 0, 0)
      ).foreach { case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
        checkDisableBucketedScanRapids(
          query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  testRapids("SPARK-33075: not disable bucketed table scan for cached query") {
    withTable("t1") {
      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
        rapidsDf1.write.format("parquet").bucketBy(8, "i").saveAsTable("t1")
        spark.catalog.cacheTable("t1")
        assertCached(spark.table("t1"))

        val partitioning = stripAQEPlan(spark.table("t1").queryExecution.executedPlan)
          .outputPartitioning
        assert(partitioning match {
          case HashPartitioning(Seq(column: AttributeReference), 8) if column.name == "i" => true
          case _ => false
        })

        val aggregateQuery = sql("SELECT SUM(i) FROM t1 GROUP BY i")
        checkAnswer(aggregateQuery, Seq(Row(0L), Row(10L), Row(20L), Row(30L), Row(40L)))
        val aggregateQueryPlan = aggregateQuery.queryExecution.executedPlan
        val shuffles = collect(aggregateQueryPlan) {
          case shuffle: GpuShuffleExchangeExecBase => shuffle
          case shuffle: ShuffleExchangeExec => shuffle
        }
        assert(shuffles.isEmpty,
          s"Expected no CPU or GPU shuffle exchange for cached bucketed data:\n$aggregateQueryPlan")
      }
    }
  }

  testRapids("Aggregates with no groupby over tables having 1 BUCKET, return multiple rows") {
    withTable("t1") {
      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
        sql(
          """
            |CREATE TABLE t1 (id BIGINT, event_date DATE)
            |USING PARQUET
            |CLUSTERED BY (id)
            |INTO 1 BUCKETS
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE t1 VALUES(1.23, cast("2021-07-07" as date))
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE t1 VALUES(2.28, cast("2021-08-08" as date))
            |""".stripMargin)
        val df = spark.sql("select sum(id) from t1 where id is not null")
        assert(df.count == 1)
        checkDisableBucketedScanRapids(
          query = "SELECT SUM(id) FROM t1 WHERE id is not null",
          expectedNumScanWithAutoScanEnabled = 1,
          expectedNumScanWithAutoScanDisabled = 1)
      }
    }
  }
}

class RapidsDisableUnnecessaryBucketedScanWithoutHiveSupportSuite
  extends DisableUnnecessaryBucketedScanWithoutHiveSupportSuite
  with RapidsSQLTestsTrait
  with RapidsDisableUnnecessaryBucketedScanTests

class RapidsDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE
  extends DisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE
  with RapidsSQLTestsTrait
