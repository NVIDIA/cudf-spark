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
import java.util.TimeZone

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.FileUtils
import org.apache.spark.sql.functions._

/**
 * Performance test for ORC timezone rebasing (cross-timezone reads).
 *
 * Writes an ORC file with the JVM default timezone set to a writer timezone, then reads it
 * with the JVM default timezone set to a reader timezone, measuring CPU vs GPU elapsed time.
 *
 * ORC encodes the writer timezone in the stripe footer, and uses the JVM default timezone
 * as the reader timezone, so we must manipulate TimeZone.setDefault().
 *
 * Note: TimeZone must be set INSIDE the withCpuSparkSession/withGpuSparkSession lambda,
 * because resetSparkSessionConf restores spark.sql.session.timeZone to the original value
 * (UTC), which also resets TimeZone.getDefault().
 *
 * Usage:
 *   mvn test -Dbuildver=xxx \
 *     -DwildcardSuites=com.nvidia.spark.rapids.timezone.OrcTimezonePerfSuite \
 *     -DargLine="-DenableOrcTimeZonePerf=true \
 *                -DorcPerfWriterTZ=America/Los_Angeles \
 *                -DorcPerfReaderTZ=UTC \
 *                -DorcPerfRows=10000000"
 */
class OrcTimezonePerfSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {

  private val originalTimeZone = TimeZone.getDefault
  private val enablePerfTest = java.lang.Boolean.getBoolean("enableOrcTimeZonePerf")
  private val writerTZ = System.getProperty("orcPerfWriterTZ", "America/Los_Angeles")
  private val readerTZ = System.getProperty("orcPerfReaderTZ", "UTC")
  private val numRows = java.lang.Long.getLong("orcPerfRows", 1024L * 1024L * 10L).longValue()
  private val baseEpochSeconds = 946684800L // 2000-01-01 00:00:00 UTC
  private val warmupRounds = 1
  private val measuredRounds = 5

  // Use a per-run unique path under java.io.tmpdir to avoid cross-run interference
  // when multiple test runs share the same host (e.g., CI matrix).
  private val path = System.getProperty("java.io.tmpdir") + "/tmp_OrcTimezonePerfSuite_" +
    java.util.UUID.randomUUID().toString

  private def setSessionTimeZone(spark: SparkSession, tzId: String): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone(tzId))
    spark.conf.set("spark.sql.session.timeZone", tzId)
  }

  private def orcConf(): SparkConf = {
    new SparkConf()
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.orc.impl", "native")
      .set("spark.sql.sources.useV1SourceList", "orc")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (enablePerfTest) {
      withCpuSparkSession(spark => {
        setSessionTimeZone(spark, writerTZ)
        spark.range(numRows)
          .selectExpr("id",
            s"cast($baseEpochSeconds + abs(hash(id)) % " +
              s"(34 * 365 * 86400) as timestamp) as ts")
          .write.mode("overwrite").orc(path)
      }, orcConf())

      val dir = new File(path)
      val orcFiles = dir.listFiles().filter(_.getName.endsWith(".orc"))
      val totalMB = orcFiles.map(_.length()).sum / 1024 / 1024
      println(s"ORC data: ${orcFiles.length} files, $totalMB MB, $numRows rows")
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      if (enablePerfTest) {
        FileUtils.deleteRecursively(new File(path))
      }
    } finally {
      TimeZone.setDefault(originalTimeZone)
    }
  }

  private def readOrcAgg(spark: SparkSession): DataFrame = {
    spark.read.orc(path).select(col("ts"))
      .agg(sum(hash(col("ts"))).alias("hash_sum"))
  }

  private def readOrcSummary(spark: SparkSession): DataFrame = {
    spark.read.orc(path).agg(
      count(col("id")).alias("row_count"),
      min(col("ts")).alias("min_ts"),
      max(col("ts")).alias("max_ts"))
  }

  /**
   * Run warmup + measured rounds for both CPU and GPU, print per-round and mean times.
   * Returns (cpuMean, gpuMean).
   */
  private def runAndRecordTime(testName: String, jvmTZ: String): (Double, Double) = {
    val conf = orcConf()
    val totalRounds = warmupRounds + measuredRounds

    println(s"\n=== $testName (writer=$writerTZ, reader=$jvmTZ) ===")
    println("round,type,elapsedMs")

    val cpuTimes = (1 to totalRounds).map { i =>
      val start = System.nanoTime()
      withCpuSparkSession(spark => {
        setSessionTimeZone(spark, jvmTZ)
        readOrcAgg(spark).collect()
      }, conf)
      val ms = (System.nanoTime() - start) / 1000000L
      println(s"$i,CPU,$ms")
      ms
    }.drop(warmupRounds)

    val gpuTimes = (1 to totalRounds).map { i =>
      val start = System.nanoTime()
      withGpuSparkSession(spark => {
        setSessionTimeZone(spark, jvmTZ)
        readOrcAgg(spark).collect()
      }, conf)
      val ms = (System.nanoTime() - start) / 1000000L
      println(s"$i,GPU,$ms")
      ms
    }.drop(warmupRounds)

    val cpuMean = cpuTimes.sum.toDouble / cpuTimes.length
    val gpuMean = gpuTimes.sum.toDouble / gpuTimes.length
    val speedup = cpuMean / gpuMean
    println(f"$testName: CPU mean=$cpuMean%.0f ms, GPU mean=$gpuMean%.0f ms, " +
      f"speedup=$speedup%.2fx")
    (cpuMean, gpuMean)
  }

  test("ORC cross-timezone read perf") {
    assume(enablePerfTest)

    // Verify CPU and GPU produce the same results
    val cpuSummary = withCpuSparkSession(spark => {
      setSessionTimeZone(spark, readerTZ)
      readOrcSummary(spark).collect()
    }, orcConf())
    val gpuSummary = withGpuSparkSession(spark => {
      setSessionTimeZone(spark, readerTZ)
      readOrcSummary(spark).collect()
    }, orcConf())
    println(s"CPU summary: ${cpuSummary.mkString(", ")}")
    println(s"GPU summary: ${gpuSummary.mkString(", ")}")
    assert(cpuSummary.sameElements(gpuSummary),
      "CPU and GPU ORC summaries did not match")

    // Verify GPU plan uses RAPIDS ORC scan
    val gpuPlan = withGpuSparkSession(spark => {
      setSessionTimeZone(spark, readerTZ)
      readOrcAgg(spark).queryExecution.executedPlan.toString()
    }, orcConf())
    assert(gpuPlan.contains("GpuFileSourceScanExec") ||
      gpuPlan.contains("GpuFileGpuScan"),
      s"GPU plan does not use RAPIDS ORC scan:\n$gpuPlan")

    val (crossCpuMean, crossGpuMean) = runAndRecordTime("cross-tz", readerTZ)

    // Baseline: same-timezone read (no timezone conversion)
    val (baseCpuMean, baseGpuMean) = runAndRecordTime("same-tz-baseline", writerTZ)

    println(f"\nTZ conversion overhead - CPU: ${crossCpuMean - baseCpuMean}%.0f ms")
    println(f"TZ conversion overhead - GPU: ${crossGpuMean - baseGpuMean}%.0f ms")
  }
}
// scalastyle:on println
