/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.time.LocalDateTime

import org.apache.spark.sql.{CachedTableSuite, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsCachedTableSuite extends CachedTableSuite with RapidsSQLTestsTrait {
  import testImplicits._

  private def getNumInMemoryRelations(ds: Dataset[_]): Int = {
    val plan = ds.queryExecution.withCachedData
    var sum = plan.collect { case _: InMemoryRelation => 1 }.sum
    plan.transformAllExpressions {
      case e: SubqueryExpression =>
        sum += e.plan.collect { case _: InMemoryRelation => 1 }.sum
        e
    }
    sum
  }

  private def getNumGpuInMemoryTablesInSubquery(plan: SparkPlan): Int = {
    plan.expressions.flatMap(_.collect {
      case sub: ExecSubqueryExpression => getNumGpuInMemoryTablesRecursively(sub.plan)
    }).sum
  }

  private def getNumGpuInMemoryTablesRecursively(plan: SparkPlan): Int = {
    collect(plan) {
      case gpuTable: GpuInMemoryTableScanExec =>
        getNumGpuInMemoryTablesRecursively(gpuTable.relation.cachedPlan) +
          getNumGpuInMemoryTablesInSubquery(gpuTable) + 1
      case p =>
        getNumGpuInMemoryTablesInSubquery(p)
    }.sum
  }

  testRapids("InMemoryRelation statistics") {
    sql("CACHE TABLE testData")

    val cachedRelations = spark.table("testData").queryExecution.withCachedData.collect {
      case cached: InMemoryRelation => cached
    }
    assert(cachedRelations.size === 1)

    val cached = cachedRelations.head
    val stats = cached.stats
    assert(stats.sizeInBytes > 0)
    assert(stats.sizeInBytes === cached.cacheBuilder.sizeInBytesStats.value)
    assert(stats.rowCount.contains(cached.cacheBuilder.rowCountStats.value))
    assert(stats.rowCount.contains(100L))

    val df = spark.table("testData").where("key = 1").select("value")
    checkAnswer(df, Row("1"))

    val hasGpuCacheScan = collect(df.queryExecution.executedPlan) {
      case p if p.getClass.getName.contains("GpuInMemoryTableScanExec") => p
    }.nonEmpty
    assert(hasGpuCacheScan,
      "Expected GpuInMemoryTableScanExec in plan:\n" + df.queryExecution.executedPlan)
  }

  testRapids("SPARK-19993 subquery with cached underlying relation") {
    withTempView("t1") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      spark.catalog.cacheTable("t1")

      val sqlText =
        """
          |SELECT * FROM t1
          |WHERE
          |NOT EXISTS (SELECT * FROM t1)
        """.stripMargin
      val ds = sql(sqlText)
      assert(getNumInMemoryRelations(ds) === 2)

      val cachedDs = sql(sqlText).cache()
      checkAnswer(cachedDs, Nil)

      val numGpuCacheScans =
        getNumGpuInMemoryTablesRecursively(cachedDs.queryExecution.executedPlan)
      assert(numGpuCacheScans === 3,
        "Expected three recursive GpuInMemoryTableScanExec nodes in plan:\n" +
          cachedDs.queryExecution.executedPlan)
    }
  }

  testRapids("SPARK-36120: Support cache/uncache table with TimestampNTZ type") {
    val tableName = "ntzCache"
    withTable(tableName) {
      sql(s"CACHE TABLE $tableName AS SELECT TIMESTAMP_NTZ'2021-01-01 00:00:00'")
      assert(spark.catalog.isCached(tableName))

      val df = spark.table(tableName)
      checkAnswer(df, Row(LocalDateTime.parse("2021-01-01T00:00:00")))

      val cachedRelations = df.queryExecution.withCachedData.collect {
        case cached: InMemoryRelation => cached
      }
      assert(cachedRelations.size === 1)

      val cached = cachedRelations.head
      val stats = cached.stats
      assert(stats.sizeInBytes > 0)
      assert(stats.sizeInBytes === cached.cacheBuilder.sizeInBytesStats.value)
      assert(stats.rowCount.contains(cached.cacheBuilder.rowCountStats.value))
      assert(stats.rowCount.contains(1L))

      sql(s"UNCACHE TABLE $tableName")
      assert(!spark.catalog.isCached(tableName))
    }
  }
}