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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.perf.PlanKey
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Regrouping a stock Iceberg scan's planned tasks by a learned split: the same files and bytes in
 * new groups, and every case that must keep Iceberg's own groups. The learned split is injected,
 * so no history store is needed. A hadoop `SparkCatalog`, not the plugin's catalog, is used.
 */
class GpuSparkBatchRegroupSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouse = Files.createTempDirectory("iceberg-regroup").toFile
  private val kb = 1024L

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("GpuSparkBatchRegroupSuite")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", warehouse.toURI.toString)
      .set("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.adaptive.enabled", "false")
    spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    // Files weigh their size in the groups, not Iceberg's 4 MiB per-file default.
    createTable("t", "'read.split.open-file-cost'='1'")
    createTable("mor", "'read.split.open-file-cost'='1', 'format-version'='2', " +
      "'write.delete.mode'='merge-on-read'")
    Seq("t", "mor").foreach { name =>
      (0 until 3).foreach { _ =>
        spark.range(0, 30000)
          .selectExpr("id", "cast(id % 4 as int) as p", "uuid() as pad")
          .repartition(4)
          .writeTo(s"local.db.$name").append()
      }
    }
    spark.sql("DELETE FROM local.db.mor WHERE id % 7 = 0")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    TrampolineUtil.cleanupAnyExistingSession()
    FileUtils.deleteDirectory(warehouse)
  }

  private def createTable(name: String, props: String): Unit =
    spark.sql(s"CREATE TABLE local.db.$name (id bigint, p int, pad string) USING iceberg " +
      s"PARTITIONED BY (p) TBLPROPERTIES ($props)")

  private def scanOf(df: DataFrame): Scan =
    df.queryExecution.sparkPlan.collectFirst { case b: BatchScanExec => b.scan }.get

  private def partitionsOf(scan: Scan): Array[InputPartition] =
    GpuSparkScanAccess.toBatch(scan).planInputPartitions()

  private def listedOf(scan: Scan): Long =
    GpuSparkScanAccess.dataBytes(GpuSparkScanAccess.tasks(scan))

  private def regroup(scan: Scan, decide: (Long, Long) => Long) =
    GpuSparkBatch.regroup(scan, GpuSparkScanAccess.toBatch(scan), listedOf(scan), decide)

  private def groupsOf(parts: Array[InputPartition]) =
    parts.toSeq.map(p => GpuSparkScanAccess.taskGroup(p))

  private def cpuTasks(parts: Array[InputPartition]) =
    parts.toSeq.flatMap(p => GpuSparkScanAccess.taskGroup(p).tasks().asScala)
      .map(_.asFileScanTask())

  /**
   * Per data file, where its pieces end (their lengths plus the first start; splits begin at a
   * file's first row group, not at byte 0), and the delete files it is read with.
   */
  private def coverage(tasks: Seq[org.apache.iceberg.FileScanTask]) = {
    val ends = tasks.groupBy(_.file().location()).map { case (file, pieces) =>
      file -> (pieces.map(_.length()).sum + pieces.map(_.start()).min)
    }
    val deletes = tasks.groupBy(_.file().location()).map { case (file, pieces) =>
      file -> pieces.flatMap(_.deletes().asScala.map(_.location())).toSet
    }
    (ends, deletes)
  }

  test("regrouped partitions read exactly the planned files, bytes and locations") {
    val split = 32 * kb
    for ((name, df) <- Seq(
        "plain" -> spark.table("local.db.t"),
        "locality" -> spark.read.option("locality", "true").table("local.db.t"))) {
      val scan = scanOf(df)
      val parts = partitionsOf(scan)
      val regrouped = regroup(scan, (_, _) => split).get
      assert(regrouped.length > parts.length, s"$name: ${parts.length} -> ${regrouped.length}")
      assert(coverage(cpuTasks(regrouped)) == coverage(cpuTasks(parts)), name)
      // No group is bigger than the split, except a lone piece that is itself that big.
      assert(groupsOf(regrouped).forall(g => g.sizeBytes() <= split || g.tasks().size() == 1),
        name)
      if (name == "locality") {
        assert(parts.exists(_.preferredLocations().nonEmpty), "fixture has no locations")
        assert(regrouped.forall(_.preferredLocations().nonEmpty), name)
      }
    }
  }

  test("the split compared against is the one Iceberg grouped by, after its adaptive sizing") {
    val scan = scanOf(spark.table("local.db.t"))
    val icebergSplit = GpuSparkScanAccess.splitSize(scan)
    // The fixture is small enough for adaptive sizing to shrink the configured split.
    assert(icebergSplit < GpuSparkScanAccess.readConf(scan).splitSize())
    val atIcebergSplit = GpuSparkScanAccess.planInputPartitions(scan,
      GpuSparkScanAccess.toBatch(scan), icebergSplit)
    assert(groupsOf(atIcebergSplit).map(_.sizeBytes()) ==
      groupsOf(partitionsOf(scan)).map(_.sizeBytes()))
  }

  test("a filtered scan is sized from the bytes it reads after pruning") {
    val seen = scala.collection.mutable.Map.empty[String, (Long, Long)]
    def decide(key: String)(listed: Long, icebergSplit: Long): Long = {
      seen(key) = (listed, icebergSplit)
      math.max(16 * kb, listed / 4)
    }
    val full = scanOf(spark.table("local.db.t"))
    val one = scanOf(spark.sql("SELECT * FROM local.db.t WHERE p = 1"))
    val fullParts = regroup(full, decide("full")).get
    val oneParts = regroup(one, decide("one")).get
    assert(seen("one")._1 < seen("full")._1 / 2, s"$seen")
    assert(seen("full")._2 == GpuSparkScanAccess.splitSize(full))
    // The learned split follows the bytes read, so the two scans got different splits.
    assert(cpuTasks(oneParts).map(_.file().location()).toSet.size <
      cpuTasks(fullParts).map(_.file().location()).toSet.size)
    assert(groupsOf(oneParts).map(_.sizeBytes()).max < groupsOf(fullParts).map(_.sizeBytes()).max)
  }

  test("Iceberg's groups are kept when there is nothing to regroup by or it is not safe") {
    val notAsked: (Long, Long) => Long = (_, _) => fail("the decision must not be asked")
    def table = scanOf(spark.table("local.db.t"))
    val cases: Seq[(String, () => Option[Array[InputPartition]])] = Seq(
      "no learned split" -> (() => regroup(table, (_, split) => split)),
      "a failing decision" -> (() =>
        regroup(table, (_, _) => throw new IllegalStateException("history store down"))),
      "an explicit split-size read option" -> (() => regroup(
        scanOf(spark.read.option("split-size", (64 * kb).toString).table("local.db.t")),
        notAsked)),
      "a metadata table" -> (() => regroup(scanOf(spark.table("local.db.t.files")), notAsked)),
      "delete files" -> (() => {
        val scan = scanOf(spark.table("local.db.mor"))
        assert(GpuSparkScanAccess.hasDeletes(GpuSparkScanAccess.tasks(scan)),
          "fixture has no delete files")
        regroup(scan, notAsked)
      }),
      "a scan grouped by partition key" -> (() =>
        withSqlConf("spark.sql.iceberg.planning.preserve-data-grouping" -> "true") {
          val scan = table
          assert(!GpuSparkScanAccess.groupingKeyType(scan).fields().isEmpty,
            "fixture does not group by partition key")
          regroup(scan, notAsked)
        }))
    for ((name, run) <- cases) {
      assert(run().isEmpty, name)
    }
  }

  test("with scan-split history off the GPU scan has no key and records nothing") {
    val gpuScan = new GpuSparkBatchQueryScan(scanOf(spark.table("local.db.t")),
      new RapidsConf(Map.empty[String, String]), false)
    assert(gpuScan.historyKey().isEmpty)
    assert(gpuScan.splitTotals.isEmpty)
  }

  private def withSqlConf[T](pairs: (String, String)*)(body: => T): T = {
    val old = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try body finally old.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None) => spark.conf.unset(k)
    }
  }

  private def keyOf(df: DataFrame): PlanKey = {
    val scan = scanOf(df)
    IcebergPlanKey.of(GpuSparkScanAccess.table(scan).name(), scan.readSchema(),
      GpuSparkScanAccess.filterExpressions(scan).asScala.toSeq, Nil).get
  }

  test("a real scan's pushed filters key by their shape and values, in any order") {
    val t = spark.table("local.db.t")
    val first = keyOf(t.where("p = 1 and id > 5"))
    assert(keyOf(t.where("id > 5 and p = 1")) == first)
    val otherDay = keyOf(t.where("p = 2 and id > 5"))
    assert(otherDay.plan == first.plan && otherDay.literals != first.literals)
    assert(keyOf(t.where("p = 1 and id > 5").select("id")).plan != first.plan)
    assert(keyOf(t.where("p = 1 and id < 5")).plan != first.plan)
  }
}
