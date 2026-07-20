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

import org.apache.spark.sql.{DataFrame, DatasetCacheSuite, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.storage.StorageLevel

class RapidsDatasetCacheSuite extends DatasetCacheSuite with RapidsSQLTestsTrait {
  private def getSingleInMemoryRelation(df: DataFrame): InMemoryRelation = {
    val cachedRelations = df.queryExecution.withCachedData.collect {
      case cached: InMemoryRelation => cached
    }
    assert(cachedRelations.size === 1,
      "Expected one InMemoryRelation in plan:\n" + df.queryExecution.withCachedData)
    cachedRelations.head
  }

  private def getFirstInMemoryRelation(df: DataFrame): InMemoryRelation = {
    df.queryExecution.withCachedData.collectFirst {
      case cached: InMemoryRelation => cached
    }.getOrElse {
      fail("Expected an InMemoryRelation in plan:\n" + df.queryExecution.withCachedData)
    }
  }

  private def cacheScanCount(plan: SparkPlan): Int = {
    plan.collect {
      case _: InMemoryTableScanExec => 1
      case _: GpuInMemoryTableScanExec => 1
    }.sum
  }

  private def assertGpuCacheScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) {
      case scan if scan.getClass.getName.contains("GpuInMemoryTableScanExec") => scan
    }
    assert(scans.nonEmpty,
      "Expected GpuInMemoryTableScanExec in plan:\n" + plan)
  }

  testRapids("SPARK-24613 Cache with UDF could not be matched with subsequent dependent caches") {
    val udf1 = udf { x: Long => x + 1L }
    val df = spark.range(0, 10).toDF("a").withColumn("b", udf1(col("a")))
    val df2 = df.agg(sum(df("b")).as("sum_b"))

    df.cache()
    df.count()
    df2.cache()

    val cachedDf2 = getSingleInMemoryRelation(df2)
    assert(cacheScanCount(cachedDf2.cacheBuilder.cachedPlan) === 1,
      "Expected df2 cache plan to depend on the df cache:\n" +
        cachedDf2.cacheBuilder.cachedPlan)

    checkAnswer(df2, Row(55L))
    assertGpuCacheScan(df2)

    df2.unpersist(blocking = true)
    df.unpersist(blocking = true)
  }

  testRapids("SPARK-24596 Non-cascading Cache Invalidation - verify cached data reuse") {
    val udfEvalCount = spark.sparkContext.longAccumulator("SPARK-24596 UDF evaluations")
    val expensiveUDF = udf { x: Long =>
      udfEvalCount.add(1L)
      x
    }
    val df = spark.range(0, 5).toDF("a")
    val df1 = df.withColumn("b", expensiveUDF(col("a")))
    val df2 = df1.groupBy(col("a")).agg(sum(col("b")).as("sum_b"))
    val df3 = df.agg(sum(col("a")).as("sum_a"))

    df1.cache()
    df2.cache()
    checkAnswer(df2, (0L until 5L).map(i => Row(i, i)))
    val evalsAfterDf2 = udfEvalCount.value
    assert(evalsAfterDf2 > 0)
    df3.cache()

    val cachedDf2 = getSingleInMemoryRelation(df2)
    assert(cacheScanCount(cachedDf2.cacheBuilder.cachedPlan) === 1,
      "Expected df2 cache plan to depend on the df1 cache:\n" +
        cachedDf2.cacheBuilder.cachedPlan)

    df1.unpersist(blocking = true)

    assert(df1.storageLevel === StorageLevel.NONE)

    val df4 = df1.groupBy(col("a")).agg(sum(col("b")).as("sum_b")).agg(sum("sum_b").as("total"))
    assertCached(df4)
    checkAnswer(df4, Row(10L))
    assert(udfEvalCount.value === evalsAfterDf2,
      "Expected df4 to reuse the loaded df2 cache without re-evaluating the UDF")
    assertGpuCacheScan(df4)

    val df5 = df.agg(sum(col("a")).as("sum_a")).filter(col("sum_a") > 1)
    assertCached(df5)
    checkAnswer(df5, Row(10L))
  }

  testRapids("SPARK-26708 Cache data and cached plan should stay consistent") {
    val df = spark.range(0, 5).toDF("a")
    val df1 = df.withColumn("b", col("a") + 1)
    val df2 = df.filter(col("a") > 1)

    df.cache()
    df1.cache()
    df1.collect()
    df2.cache()

    val cachedDf1 = getSingleInMemoryRelation(df1)
    val df1InnerPlan = cachedDf1.cacheBuilder.cachedPlan
    assert(cacheScanCount(df1InnerPlan) === 1,
      "Expected df1 cache plan to depend on the df cache:\n" + df1InnerPlan)

    val cachedDf2 = getSingleInMemoryRelation(df2)
    assert(cacheScanCount(cachedDf2.cacheBuilder.cachedPlan) === 1,
      "Expected df2 cache plan to initially depend on the df cache:\n" +
        cachedDf2.cacheBuilder.cachedPlan)

    df.unpersist(blocking = true)

    val df1Limit = df1.limit(2)
    val df1LimitInnerPlan = getFirstInMemoryRelation(df1Limit).cacheBuilder.cachedPlan
    assert(df1LimitInnerPlan == df1InnerPlan,
      "Expected loaded df1 cache to keep the original cached plan")
    checkAnswer(df1Limit, Row(0L, 1L) :: Row(1L, 2L) :: Nil)
    assertGpuCacheScan(df1Limit)

    val df2Limit = df2.limit(2)
    val df2LimitInnerPlan = getFirstInMemoryRelation(df2Limit).cacheBuilder.cachedPlan
    assert(cacheScanCount(df2LimitInnerPlan) === 0,
      "Expected unloaded df2 cache to be re-cached without depending on df cache:\n" +
        df2LimitInnerPlan)
    checkAnswer(df2Limit, Row(2L) :: Row(3L) :: Nil)
    assertGpuCacheScan(df2Limit)
  }
}
