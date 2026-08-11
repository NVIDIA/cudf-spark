/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.filecache

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class FileCacheIntegrationSuite extends SparkQueryCompareTestSuite {
  import com.nvidia.spark.rapids.GpuMetric._

  private val FILE_SPLITS_PARQUET = "file-splits.parquet"
  private val FILE_SPLITS_ORC = "file-splits.orc"
  private val MAP_OF_STRINGS_PARQUET = "map_of_strings.snappy.parquet"
  private val SCHEMA_CANT_PRUNE_ORC = "schema-cant-prune.orc"

  def isFileCacheEnabled(conf: SparkConf): Boolean = {
    // File cache only supported on Spark 3.2+
    assumeSpark320orLater
    conf.getBoolean("spark.rapids.filecache.enabled", false)
  }

  test("filecache metrics v1 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "parquet")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      // Read a cluster of columns, skipping ranges
      var df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
          .select("orig_channel", "orig_interest_rate", "orig_loan_term", "first_pay_date",
            "num_borrowers", "dti", "borrower_credit_score", "zip", "quarter")
      df.collect()
      var gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      // read same cluster of columns, should be a full hit
      var fullHitFunc: () => Option[SparkPlan] = () => {
        val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
          .select("orig_channel", "orig_interest_rate", "orig_loan_term", "first_pay_date",
            "num_borrowers", "dti", "borrower_credit_score", "zip", "quarter")
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
      // read entire table, should be a partial hit
      df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsPartialHit(gpuScan.get.metrics)
      // read entire table again, should be a full hit
      fullHitFunc = () => {
        val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
    }, conf)
  }

  test("filecache metrics v2 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromParquet(MAP_OF_STRINGS_PARQUET)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      val fullHitFunc: () => Option[SparkPlan] = () => {
        val df = frameFromParquet(MAP_OF_STRINGS_PARQUET)(spark)
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
    }, conf)
  }

  test("filecache metrics v1 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "orc")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      // Read a cluster of columns, skipping ranges
      var df = frameFromOrc(FILE_SPLITS_ORC)(spark)
          .select("orig_channel", "orig_interest_rate", "orig_loan_term", "first_pay_date",
            "num_borrowers", "dti", "borrower_credit_score", "zip", "quarter")
      df.collect()
      var gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      // read same cluster of columns, should be a full hit
      var fullHitFunc: () => Option[SparkPlan] = () => {
        val df = frameFromOrc(FILE_SPLITS_ORC)(spark)
          .select("orig_channel", "orig_interest_rate", "orig_loan_term", "first_pay_date",
            "num_borrowers", "dti", "borrower_credit_score", "zip", "quarter")
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
      // read entire table, should be a partial hit
      df = frameFromOrc(FILE_SPLITS_ORC)(spark)
      df.collect()
      gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsPartialHit(gpuScan.get.metrics)
      // read entire table again, should be a full hit
      fullHitFunc = () => {
        val df = frameFromOrc(FILE_SPLITS_ORC)(spark)
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
    }, conf)
  }

  test("filecache metrics v2 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromOrc(SCHEMA_CANT_PRUNE_ORC)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      val fullHitFunc: () => Option[SparkPlan] = () => {
        val df = frameFromOrc(SCHEMA_CANT_PRUNE_ORC)(spark)
        df.collect()
        df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      }
      checkMetricsFullHit(fullHitFunc)
    }, conf)
  }

  testSparkResultsAreEqual("ORC cached row index",
    frameFromOrc(FILE_SPLITS_ORC)) { df =>
          df.select("loan_id", "orig_interest_rate").where("loan_id > 123")
  }

  private def checkMetricsFullMiss(metrics: Map[String, SQLMetric]): Unit = {
    assertResult(0)(metrics(FILECACHE_FOOTER_HITS).value)
    assertResult(0)(metrics(FILECACHE_FOOTER_HITS_SIZE).value)
    assertResult(1)(metrics(FILECACHE_FOOTER_MISSES).value)
    assert(metrics(FILECACHE_FOOTER_MISSES_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_HITS).value)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_HITS_SIZE).value)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES_SIZE).value > 0)
    assert(metrics.contains(FILECACHE_FOOTER_READ_TIME))
    assert(metrics.contains(FILECACHE_DATA_RANGE_READ_TIME))
  }

  private def checkMetricsFullHit(func: () => Option[SparkPlan]): Unit = {
    var metrics: Map[String, SQLMetric] = func().get.metrics
    var attempts = 0
    while (attempts < 10 && metrics(FILECACHE_DATA_RANGE_MISSES).value > 0) {
      Thread.sleep(1000)
      metrics = func().get.metrics
      attempts += 1
    }
    assert(metrics(FILECACHE_FOOTER_HITS).value > 0)
    assert(metrics(FILECACHE_FOOTER_HITS_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES).value)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES_SIZE).value)
    assert(metrics(FILECACHE_DATA_RANGE_HITS).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_HITS_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_MISSES).value)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_MISSES_SIZE).value)
    assert(metrics.contains(FILECACHE_FOOTER_READ_TIME))
    assert(metrics.contains(FILECACHE_DATA_RANGE_READ_TIME))
  }

  private def checkMetricsPartialHit(metrics: Map[String, SQLMetric]): Unit = {
    assertResult(1)(metrics(FILECACHE_FOOTER_HITS).value)
    assert(metrics(FILECACHE_FOOTER_HITS_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES).value)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES_SIZE).value)
    assert(metrics(FILECACHE_DATA_RANGE_HITS).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_HITS_SIZE).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES_SIZE).value > 0)
    assert(metrics.contains(FILECACHE_FOOTER_READ_TIME))
    assert(metrics.contains(FILECACHE_DATA_RANGE_READ_TIME))
  }
}
