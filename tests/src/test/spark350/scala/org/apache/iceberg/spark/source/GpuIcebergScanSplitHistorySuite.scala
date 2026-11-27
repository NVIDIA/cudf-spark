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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["400"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * GPU reads of Iceberg scans regrouped by scan split history return the same rows as CPU reads.
 * History settings are startup-only, so the suite runs its own GPU session with them on.
 */
class GpuIcebergScanSplitHistorySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val dir = Files.createTempDirectory("iceberg-split-history").toFile

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", new java.io.File(dir, "wh").toURI.toString)
      .set("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.rapids.sql.history.metrics.provider", "local")
      .set("spark.rapids.sql.history.metrics.local.path",
        new java.io.File(dir, "history.db").getAbsolutePath)
      .set("spark.rapids.sql.history.scanSplit.enabled", "true")
      // The test tables are small; let history size them.
      .set("spark.rapids.sql.history.scanSplit.minBytes", "16k")
    spark = SparkSession.builder().config(conf).getOrCreate()
    onCpu {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
      Seq("t" -> "", "mor" -> ", 'format-version'='2', 'write.delete.mode'='merge-on-read'")
        .foreach { case (name, props) =>
          spark.sql(s"CREATE TABLE local.db.$name (id bigint, p int, pad string) USING iceberg " +
            s"PARTITIONED BY (p) TBLPROPERTIES ('read.split.open-file-cost'='1'$props)")
          (0 until 3).foreach { _ =>
            spark.range(0, 30000).selectExpr("id", "cast(id % 4 as int) as p",
              "repeat(cast(id as string), 8) as pad")
              .repartition(4).writeTo(s"local.db.$name").append()
          }
        }
      spark.sql("DELETE FROM local.db.mor WHERE id % 7 = 0")
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    TrampolineUtil.cleanupAnyExistingSession()
    FileUtils.deleteDirectory(dir)
  }

  private def onCpu[T](body: => T): T = {
    spark.conf.set("spark.rapids.sql.enabled", "false")
    try body finally spark.conf.set("spark.rapids.sql.enabled", "true")
  }

  private def rows(sql: String): Seq[Row] =
    spark.sql(sql).collect().toSeq.sortBy(_.getLong(0))

  private def partitions(sql: String): Int = spark.sql(sql).rdd.getNumPartitions

  /** Runs `sql` on GPU until history changes its partition count, or a few tries pass. */
  private def gpuPartitionsAfterHistory(sql: String, icebergParts: Int): Int = {
    rows(sql) // records the scan's ratio when the query ends
    var parts = icebergParts
    var tries = 0
    while (parts == icebergParts && tries < 20) {
      Thread.sleep(250) // the recording is written after the query-end event
      parts = partitions(sql)
      tries += 1
    }
    parts
  }

  test("a regrouped GPU Iceberg scan returns the same rows as the CPU") {
    val sql = "SELECT id, p, pad FROM local.db.t WHERE p < 3"
    assert(spark.sql(sql).queryExecution.executedPlan.toString.contains("GpuBatchScan"))
    val icebergParts = onCpu(partitions(sql))
    val gpuParts = gpuPartitionsAfterHistory(sql, icebergParts)
    assert(gpuParts != icebergParts, s"not regrouped: $icebergParts partitions")
    assert(rows(sql) == onCpu(rows(sql)))
  }

  test("a GPU Iceberg scan with delete files keeps Iceberg's groups and the CPU's rows") {
    val sql = "SELECT id, p, pad FROM local.db.mor WHERE p < 3"
    val icebergParts = onCpu(partitions(sql))
    assert(gpuPartitionsAfterHistory(sql, icebergParts) == icebergParts)
    assert(rows(sql) == onCpu(rows(sql)))
  }
}
