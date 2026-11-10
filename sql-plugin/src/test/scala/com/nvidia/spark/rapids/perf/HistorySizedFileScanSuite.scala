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

package com.nvidia.spark.rapids.perf

import java.nio.file.Files
import java.time.Duration

import com.nvidia.spark.history.MetricStores
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * When a DSv2 file scan is queued for recording: every time its exec runs, inside a SQL execution.
 * Needs a local SparkSession.
 */
class HistorySizedFileScanSuite extends AnyFunSuite {

  private val policy = HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5))

  private class HistoryParquetScan(
      spark: SparkSession, index: InMemoryFileIndex, schema: StructType)
    extends ParquetScan(spark, spark.sessionState.newHadoopConf(), index, schema, schema,
      StructType(Nil), Array.empty, CaseInsensitiveStringMap.empty())
      with HistorySizedFileScan

  /** Runs `body` with a scan over real Parquet files; history is on unless `historyOn` is false. */
  private def withScan(historyOn: Boolean = true)(
      body: (SparkSession, HistorySizedFileScan) => Unit): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val dir = Files.createTempDirectory("history-sized-scan")
    val registration = MetricStores.install(new FakeMetricStore)
    HistoryObservations.start()
    if (historyOn) {
      ScanSplitHeuristic.enable(policy)
    }
    try {
      val out = new Path(dir.toUri.toString, "t")
      spark.range(0, 1000).write.parquet(out.toString)
      val schema = spark.read.parquet(out.toString).schema
      val index = new InMemoryFileIndex(spark, Seq(out), Map.empty, None)
      val scan = new HistoryParquetScan(spark, index, schema)
      scan.planInputPartitions() // the exec's inputRDD plans the partitions first
      body(spark, scan)
    } finally {
      ScanSplitHeuristic.disable()
      HistoryObservations.shutdown()
      registration.close()
      spark.stop()
    }
  }

  private def inExecution(spark: SparkSession, id: Long)(body: => Unit): Unit = {
    spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, id.toString)
    try {
      body
    } finally {
      spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    }
  }

  test("a scan is queued under its SQL execution, and again each time it executes") {
    withScan() { (spark, scan) =>
      inExecution(spark, 7L) {
        scan.registerForExecution(spark.sparkContext)
      }
      assert(HistoryObservations.trackedCount == 1)
      HistoryObservations.listener.onOtherEvent(SparkListenerSQLExecutionEnd(7L, 0L))
      assert(HistoryObservations.trackedCount == 0, "the ended execution is drained")
      // The same plan runs again in a later execution: it is queued again.
      inExecution(spark, 8L) {
        scan.registerForExecution(spark.sparkContext)
      }
      assert(HistoryObservations.trackedCount == 1)
    }
  }

  test("a scan registered outside any SQL execution is not tracked") {
    withScan() { (spark, scan) =>
      scan.registerForExecution(spark.sparkContext)
      assert(HistoryObservations.trackedCount == 0)
    }
  }

  test("a scan is not queued while history is off") {
    withScan(historyOn = false) { (spark, scan) =>
      inExecution(spark, 9L) {
        scan.registerForExecution(spark.sparkContext)
      }
      assert(HistoryObservations.trackedCount == 0)
    }
  }

  test("the exec helper ignores a scan that has no history support") {
    withScan() { (spark, _) =>
      inExecution(spark, 10L) {
        HistorySizedFileScan.registerForExecution("not a history scan", spark.sparkContext)
      }
      assert(HistoryObservations.trackedCount == 0)
    }
  }
}
