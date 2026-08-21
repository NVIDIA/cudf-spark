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

// scalastyle:off println
// Existing console output in this file is intentional. New code should prefer logging.

package com.nvidia.spark.rapids.timezone

import java.io.File
import java.time.LocalDate
import java.util.UUID

import com.nvidia.spark.rapids.{RapidsConf, RapidsReaderType, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.Arm.withResourceIfAllowed
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{
  DateColumnVector, LongColumnVector, StructColumnVector}
import org.apache.orc.{CompressionKind, OrcFile, TypeDescription}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, FileUtils, SparkSession}
import org.apache.spark.sql.functions.{col, hash, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

/**
 * Opt-in performance coverage for ORC legacy-date rebasing.
 *
 * Usage:
 * {{
 *   mvn package -pl tests -am -Dbuildver=330 \
 *     -DwildcardSuites=com.nvidia.spark.rapids.timezone.OrcDateRebasePerfSuite \
 *     -DargLine="-DenableOrcDateRebasePerf=true -DorcDatePerfRows=10485760"
 * }}
 */
class OrcDateRebasePerfSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {

  private case class PerfCase(name: String, path: Path, column: String)

  private val enablePerfTest = java.lang.Boolean.getBoolean("enableOrcDateRebasePerf")
  private val numRows = java.lang.Long.getLong(
    "orcDatePerfRows", 10L * 1024L * 1024L).longValue()
  private val warmupRounds = java.lang.Integer.getInteger(
    "orcDatePerfWarmupRounds", 1).intValue()
  private val measuredRounds = java.lang.Integer.getInteger(
    "orcDatePerfMeasuredRounds", 5).intValue()

  private val baseDir = new File(System.getProperty("java.io.tmpdir"),
    s"tmp_OrcDateRebasePerfSuite_${UUID.randomUUID()}")
  private val legacyFile = new File(baseDir, "legacy.orc")
  private val prolepticFile = new File(baseDir, "proleptic.orc")

  private val legacyStartDay = LocalDate.of(1000, 1, 1).toEpochDay
  private val legacyDayCount = LocalDate.of(1500, 12, 31).toEpochDay - legacyStartDay + 1L
  private val modernStartDay = LocalDate.of(2000, 1, 1).toEpochDay
  private val modernDayCount = 3653L

  private def sparkConf(): SparkConf = {
    new SparkConf()
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.orc.impl", "native")
      .set("spark.sql.orc.aggregatePushdown", "false")
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "orc")
      .set(RapidsConf.ORC_READER_TYPE.key, RapidsReaderType.PERFILE.toString)
      .set(RapidsConf.CHUNKED_READER.key, "false")
  }

  private def writeOrcFile(file: File, writerUsedProlepticGregorian: Boolean): Unit = {
    val schema = TypeDescription.createStruct()
      .addField("id", TypeDescription.createLong())
      .addField("legacy_date", TypeDescription.createDate())
      .addField("modern_date", TypeDescription.createDate())
      .addField("nested", TypeDescription.createStruct()
        .addField("nested_date", TypeDescription.createDate()))
    val conf = new Configuration()
    val options = OrcFile.writerOptions(conf)
      .setSchema(schema)
      .compress(CompressionKind.NONE)
      .setProlepticGregorian(writerUsedProlepticGregorian)
    val path = new Path(file.getCanonicalPath)

    withResourceIfAllowed(OrcFile.createWriter(path, options)) { writer =>
      val batch = schema.createRowBatch()
      val ids = batch.cols(0).asInstanceOf[LongColumnVector]
      val legacyDates = batch.cols(1).asInstanceOf[DateColumnVector]
      val modernDates = batch.cols(2).asInstanceOf[DateColumnVector]
      val nestedDates = batch.cols(3).asInstanceOf[StructColumnVector]
        .fields(0).asInstanceOf[DateColumnVector]
      var rowId = 0L
      while (rowId < numRows) {
        batch.reset()
        legacyDates.setUsingProlepticCalendar(true)
        modernDates.setUsingProlepticCalendar(true)
        nestedDates.setUsingProlepticCalendar(true)
        val rowsInBatch = math.min(batch.getMaxSize.toLong, numRows - rowId).toInt
        var rowIndex = 0
        while (rowIndex < rowsInBatch) {
          val currentId = rowId + rowIndex
          ids.vector(rowIndex) = currentId
          legacyDates.vector(rowIndex) = legacyStartDay + currentId % legacyDayCount
          modernDates.vector(rowIndex) = modernStartDay + currentId % modernDayCount
          nestedDates.vector(rowIndex) = legacyStartDay + currentId % legacyDayCount
          rowIndex += 1
        }
        batch.size = rowsInBatch
        writer.addRowBatch(batch)
        rowId += rowsInBatch
      }
    }

    withResourceIfAllowed(OrcFile.createReader(path, OrcFile.readerOptions(conf))) { reader =>
      assert(reader.getCompressionKind === CompressionKind.NONE)
      assert(reader.writerUsedProlepticGregorian() === writerUsedProlepticGregorian)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (enablePerfTest) {
      assert(numRows > 0, "orcDatePerfRows must be positive")
      assert(warmupRounds >= 0, "orcDatePerfWarmupRounds must not be negative")
      assert(measuredRounds > 0, "orcDatePerfMeasuredRounds must be positive")
      assert(baseDir.mkdirs(), s"failed to create $baseDir")
      writeOrcFile(legacyFile, writerUsedProlepticGregorian = false)
      writeOrcFile(prolepticFile, writerUsedProlepticGregorian = true)
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      if (baseDir.exists()) {
        FileUtils.deleteRecursively(baseDir)
      }
    }
  }

  private def aggregate(spark: SparkSession, perfCase: PerfCase): DataFrame = {
    spark.read.orc(perfCase.path.toString)
      .agg(sum(hash(col(perfCase.column))).alias("hash_sum"))
  }

  private def elapsedNanos(spark: SparkSession, perfCase: PerfCase): Long = {
    val query = aggregate(spark, perfCase)
    val start = System.nanoTime()
    val result = query.collect()
    val elapsed = System.nanoTime() - start
    assert(result.length === 1 && !result.head.isNullAt(0),
      s"${perfCase.name} did not fully consume ${perfCase.column}")
    elapsed
  }

  private def rowsPerSecond(elapsedNanos: Double): Double = {
    numRows.toDouble * 1000000000.0 / elapsedNanos
  }

  private def median(sortedNanos: Seq[Long]): Double = {
    val middle = sortedNanos.length / 2
    if (sortedNanos.length % 2 == 0) {
      (sortedNanos(middle - 1).toDouble + sortedNanos(middle).toDouble) / 2.0
    } else {
      sortedNanos(middle).toDouble
    }
  }

  private def runCase(spark: SparkSession, perfCase: PerfCase): Unit = {
    ExecutionPlanCaptureCallback.assertContains(
      aggregate(spark, perfCase), "GpuFileSourceScanExec")

    (1 to warmupRounds).foreach { round =>
      val nanos = elapsedNanos(spark, perfCase)
      val elapsedMs = nanos.toDouble / 1000000.0
      val throughput = rowsPerSecond(nanos.toDouble)
      println(f"ORC_DATE_REBASE_PERF_RUN,case=${perfCase.name},phase=warmup," +
        f"round=$round,rows=$numRows,elapsed_ms=$elapsedMs%.3f," +
        f"rows_per_sec=$throughput%.3f")
    }

    val measured = (1 to measuredRounds).map { round =>
      val nanos = elapsedNanos(spark, perfCase)
      val elapsedMs = nanos.toDouble / 1000000.0
      val throughput = rowsPerSecond(nanos.toDouble)
      println(f"ORC_DATE_REBASE_PERF_RUN,case=${perfCase.name},phase=measured," +
        f"round=$round,rows=$numRows,elapsed_ms=$elapsedMs%.3f," +
        f"rows_per_sec=$throughput%.3f")
      nanos
    }.sorted

    val medianNanos = median(measured)
    val p95Index = math.ceil(measured.length * 0.95).toInt - 1
    val p95Nanos = measured(p95Index).toDouble
    val medianMs = medianNanos / 1000000.0
    val p95Ms = p95Nanos / 1000000.0
    val medianThroughput = rowsPerSecond(medianNanos)
    println(f"ORC_DATE_REBASE_PERF_SUMMARY,case=${perfCase.name},rows=$numRows," +
      f"measured_rounds=$measuredRounds,median_ms=$medianMs%.3f,p95_ms=$p95Ms%.3f," +
      f"rows_per_sec=$medianThroughput%.3f")
  }

  test("ORC date rebase performance") {
    assume(enablePerfTest,
      "set -DenableOrcDateRebasePerf=true to run the ORC date rebase benchmark")

    val cases = Seq(
      PerfCase("legacy_file_legacy_date", new Path(legacyFile.getCanonicalPath), "legacy_date"),
      PerfCase("legacy_file_modern_date", new Path(legacyFile.getCanonicalPath), "modern_date"),
      PerfCase("legacy_file_id_control", new Path(legacyFile.getCanonicalPath), "id"),
      PerfCase("proleptic_file_legacy_date_fast_path",
        new Path(prolepticFile.getCanonicalPath), "legacy_date"),
      PerfCase("proleptic_file_nested_date_fast_path",
        new Path(prolepticFile.getCanonicalPath), "nested.nested_date"))

    withGpuSparkSession(spark => cases.foreach(runCase(spark, _)), sparkConf())
  }
}
// scalastyle:on println
