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

package org.apache.spark.sql.rapids

import java.nio.file.Files
import java.time.Duration
import java.util.Collections

import com.nvidia.spark.history.{DimValue, MetricStores, Observation}
import com.nvidia.spark.rapids.perf.{FakeMetricStore, HistoryPolicy, HistorySizedFileScan,
  ScanExpansionRatio, ScanSplitHeuristic}
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FilePartition, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * `HistorySizedFileScan.partitions` against Spark's own `FileScan.partitions`, on real Parquet
 * files. Needs a local SparkSession.
 */
class ScanSplitResizeSuite extends AnyFunSuite {

  /** A Spark Parquet scan whose partitions come from our override. */
  private class HistoryParquetScan(
      spark: SparkSession, index: InMemoryFileIndex, schema: StructType)
    extends ParquetScan(spark, spark.sessionState.newHadoopConf(), index, schema, schema,
      StructType(Nil), Array.empty, CaseInsensitiveStringMap.empty())
      with HistorySizedFileScan

  private def plan(scan: ParquetScan): Seq[(Int, Seq[(String, Long, Long)])] =
    scan.planInputPartitions().toSeq.map { p =>
      val fp = p.asInstanceOf[FilePartition]
      (fp.index, fp.files.toSeq.map(f => (f.filePath.toString, f.start, f.length)))
    }

  private def withScans(
      extraConf: (String, String)*)(body: (SparkSession, Path, StructType) => Unit): Unit = {
    val builder = SparkSession.builder().master("local[2]")
      .config("spark.sql.files.maxPartitionBytes", "20000")
      .config("spark.sql.files.openCostInBytes", "0")
      .config("spark.rapids.sql.history.scanSplit.minBytes", "100")
    val spark = extraConf.foldLeft(builder)((b, kv) => b.config(kv._1, kv._2)).getOrCreate()
    val dir = Files.createTempDirectory("scan-split-partitions")
    try {
      val out = new Path(dir.toUri.toString, "t")
      spark.range(0, 60000).selectExpr("id", "cast(id as string) s").repartition(3)
        .write.parquet(out.toString)
      body(spark, out, spark.read.parquet(out.toString).schema)
    } finally {
      spark.stop()
    }
  }

  private def newIndex(spark: SparkSession, out: Path) =
    new InMemoryFileIndex(spark, Seq(out), Map.empty, None)

  private def newSparkScan(spark: SparkSession, out: Path, schema: StructType) =
    ParquetScan(spark, spark.sessionState.newHadoopConf(), newIndex(spark, out), schema, schema,
      StructType(Nil), Array.empty, CaseInsensitiveStringMap.empty())

  test("with history on but nothing recorded the partitions are Spark's own") {
    withScans() { (spark, out, schema) =>
      val registration = MetricStores.install(new FakeMetricStore)
      ScanSplitHeuristic.enable(HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5)))
      try {
        val sparkPlan = plan(newSparkScan(spark, out, schema))
        val ours = plan(new HistoryParquetScan(spark, newIndex(spark, out), schema))
        assert(sparkPlan.size > 3, "files must be cut into several partitions")
        assert(ours == sparkPlan)
      } finally {
        ScanSplitHeuristic.disable()
        registration.close()
      }
    }
  }

  test("with history off the partitions are Spark's own") {
    withScans() { (spark, out, schema) =>
      assert(!ScanSplitHeuristic.isEnabled)
      assert(plan(new HistoryParquetScan(spark, newIndex(spark, out), schema)) ==
        plan(newSparkScan(spark, out, schema)))
    }
  }

  test("a recorded ratio cuts the files at the learned split") {
    withScans() { (spark, out, schema) =>
      val store = new FakeMetricStore
      store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
      val index = newIndex(spark, out)
      val key = ScanSplitHeuristic.scanKey(None, index.rootPaths).toOption.get
      // 1 GiB target batch / 1e7 = 107 bytes, above the configured 100 byte floor.
      store.record(new Observation(ScanExpansionRatio.metric,
        Collections.singletonMap(ScanExpansionRatio.dimension, DimValue.of(key)), 1e7,
        System.currentTimeMillis() - 1000))
      val registration = MetricStores.install(store)
      ScanSplitHeuristic.enable(HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5)))
      try {
        val sparkPlan = plan(newSparkScan(spark, out, schema))
        val ours = plan(new HistoryParquetScan(spark, index, schema))
        val chunks = ours.flatMap(_._2)
        assert(chunks.nonEmpty && chunks.forall(_._3 <= 107L), "chunks are at most the split")
        assert(chunks.map(_._3).sum == sparkPlan.flatMap(_._2).map(_._3).sum, "all bytes planned")
        assert(chunks.size > sparkPlan.flatMap(_._2).size, "smaller split means more chunks")
        assert(ours.map(_._1) == ours.indices, "partition indexes are 0..n-1")
      } finally {
        ScanSplitHeuristic.disable()
        registration.close()
      }
    }
  }

  test("minPartitionNum follows Spark's file scan parallelism") {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    try {
      assert(ScanSplitHeuristic.minPartitionNum(spark) ==
        spark.sparkContext.defaultParallelism.toLong)
      spark.conf.set("spark.sql.leafNodeDefaultParallelism", "40")
      assert(ScanSplitHeuristic.minPartitionNum(spark) == 40L)
      spark.conf.set("spark.sql.files.minPartitionNum", "500")
      assert(ScanSplitHeuristic.minPartitionNum(spark) == 500L)
    } finally {
      spark.stop()
    }
  }
}
