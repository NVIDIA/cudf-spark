/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.timezone

import java.io.File
import java.time.Instant

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.timezone._
import org.apache.spark.sql.timezone.TimeZonePerfUtils
import org.apache.spark.sql.types._

/**
 * A simple test performance framework for non-UTC features.
 * Usage:
 *
 *   argLine="-DTZs=Asia/Shanghai,Japan -DenableTimeZonePerf=true" \
 *   mvn test -Dbuildver=320 -DwildcardSuites=com.nvidia.spark.rapids.timezone.TimeZonePerfSuite
 * Note:
 *   Generate a Parquet file with 6 columns:
 *     - c_ts: timestamp column
 *     - c_long_of_ts: long value which is microseconds
 *     - c_date: date column
 *     - c_int_of_date:int value which is days from 1970-01-01
 *     - c_long_of_ts_seconds: long values of seconds from epoch
 *     - c_str_for_cast: strings for cast to timestamp, formats are yyyy, yyyy-mm, ...
 *     - c_str_of_ts: strings with format: yyyy-MM-dd HH:mm:ss
 *    Each column is high duplicated.
 *    The generated file is highly compressed since we expect both CPU and GPU can scan quickly.
 *    When testing operators, we need to add in a max/count aggregator to reduce the result data.
 */
class TimeZonePerfSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {
  // perf test is disabled by default since it's a long running time in UT.
  private val enablePerfTest = java.lang.Boolean.getBoolean("enableTimeZonePerf")
  private val timeZoneStrings = System.getProperty("TZs", "Asia/Shanghai")

  // rows for perf test
  private val numRows: Long = 1024L * 1024L * 10L

  private val zones = timeZoneStrings.split(",")

  private val path = "/tmp/tmp_TimeZonePerfSuite"

  /**
   * Create a Parquet file to test
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (enablePerfTest) {
      // create a parquet file for testing
      withCpuSparkSession(
        spark => createDF(spark).write.mode("overwrite").parquet(path))
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (enablePerfTest) {
      // delete the parquet file
      FileUtils.deleteRecursively(new File(path))
    }
  }

  val year1980 = Instant.parse("1980-01-01T00:00:00Z").getEpochSecond * 1000L * 1000L
  val year2000 = Instant.parse("2000-01-01T00:00:00Z").getEpochSecond * 1000L * 1000L
  val year2030 = Instant.parse("2030-01-01T00:00:00Z").getEpochSecond * 1000L * 1000L

  private val stringsForCast = Array(
    "2000",
    "2000-01",
    "2000-01-02",
    "2000-01-02 03:04:05",
    "2000-01-02 03:04:05Iran")

  private val regularStrings = Array(
    "1970-01-01 00:00:00",
    "2000-01-02 00:00:00",
    "2030-01-02 00:00:00")

  /**
   * create a data frame with schema:
   * - c_ts: timestamp column
   * - c_long_of_ts: long value which is microseconds
   * - c_date: date column
   * - c_int_of_date:int value which is days from 1970-01-01
   * - c_str_for_cast: strings for cast to timestamp, formats are yyyy, yyyy-mm, ...
   * - c_str_of_ts: strings with format: yyyy-MM-dd HH:mm:ss
   */
  def createDF(spark: SparkSession): DataFrame = {
    val id = col("id")
    val tsArray = Array[Long](year1980, year2000, year2030)
    val secondsArray = tsArray.map(e => e / 1000000L)
    val dateArray = Array[Int](0, 100, 200)
    val columns = Array[Column](
      TimeZonePerfUtils.createColumn(id, TimestampType, TsGenFunc(tsArray)).alias("c_ts"),
      TimeZonePerfUtils.createColumn(id, LongType, TsGenFunc(tsArray)).alias("c_long_of_ts"),
      TimeZonePerfUtils.createColumn(id, DateType, DateGenFunc(dateArray)).alias("c_date"),
      TimeZonePerfUtils.createColumn(id, LongType, TsGenFunc(secondsArray))
          .alias("c_long_of_ts_seconds"),
      TimeZonePerfUtils.createColumn(id, IntegerType, DateGenFunc(dateArray))
          .alias("c_int_of_date"),
      TimeZonePerfUtils.createColumn(id, StringType, StringGenFunc(stringsForCast))
          .alias("c_str_for_cast"),
      TimeZonePerfUtils.createColumn(id, StringType, StringGenFunc(regularStrings))
          .alias("c_str_of_ts")
    )
    val range = spark.range(numRows)
    range.select(columns: _*)
  }

  /**
   * Run 6 rounds for both Cpu and Gpu,
   * but only print the elapsed times for the last 5 rounds.
   */
  def runAndRecordTime(
      testName: String,
      func: (SparkSession, String) => DataFrame,
      conf: SparkConf = new SparkConf()): Any = {

    if (!enablePerfTest) {
      // by default skip perf test
      return None
    }
    println(s"test,type,zone,used MS")
    for (zoneStr <- zones) {
      // run 6 rounds, but ignore the first round.
      val elapses = (1 to 6).map { i =>
        // run on Cpu
        val startOnCpu = System.nanoTime()
        withCpuSparkSession(
          spark => func(spark, zoneStr).collect(),
          // set session time zone
          conf.set("spark.sql.session.timeZone", zoneStr))
        val endOnCpu = System.nanoTime()
        val elapseOnCpuMS = (endOnCpu - startOnCpu) / 1000000L
        if (i != 1) {
          println(s"$testName,Cpu,$zoneStr,$elapseOnCpuMS")
        }

        // run on Gpu
        val startOnGpu = System.nanoTime()
        withGpuSparkSession(
          spark => func(spark, zoneStr).collect(),
          // set session time zone
          conf.set("spark.sql.session.timeZone", zoneStr))
        val endOnGpu = System.nanoTime()
        val elapseOnGpuMS = (endOnGpu - startOnGpu) / 1000000L
        if (i != 1) {
          println(s"$testName,Gpu,$zoneStr,$elapseOnGpuMS")
          (elapseOnCpuMS, elapseOnGpuMS)
        } else {
          (0L, 0L) // skip the first round
        }
      }
      val meanCpu = elapses.map(_._1).sum / 5.0
      val meanGpu = elapses.map(_._2).sum / 5.0
      val speedup = meanCpu.toDouble / meanGpu.toDouble
      println(f"$testName, $zoneStr: mean cpu time: $meanCpu%.2f ms, " +
          f"mean gpu time: $meanGpu%.2f ms, speedup: $speedup%.2f x")
    }
  }

  test("test from_utc_timestamp") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.max( // use max to reduce the result data
        functions.from_utc_timestamp(functions.col("c_ts"), zone)
      ))
    }

    runAndRecordTime("from_utc_timestamp", perfTest)
  }

  test("test to_utc_timestamp") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.to_utc_timestamp(functions.col("c_ts"), zone)
      ))
    }

    runAndRecordTime("to_utc_timestamp", perfTest)
  }

  test("test hour") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.hour(functions.col("c_ts"))
      ))
    }

    runAndRecordTime("hour",
      perfTest)
  }

  test("test minute") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.minute(functions.col("c_ts"))
      ))
    }

    runAndRecordTime("minute",
      perfTest)
  }

  test("test second") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.second(functions.col("c_ts"))
      ))
    }

    runAndRecordTime("second",
      perfTest)
  }

  test("test unix_timestamp") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.unix_timestamp(functions.col("c_str_of_ts"))
      ))
    }

    runAndRecordTime("unix_timestamp",
      perfTest)
  }

  test("test from_unixtime") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.from_unixtime(functions.col("c_long_of_ts_seconds"))
      ))
    }

    runAndRecordTime("from_unixtime",
      perfTest)
  }

  test("test date_format") {
    assume(enablePerfTest)

    // cache time zone DB in advance
    GpuTimeZoneDB.cacheDatabase()
    Thread.sleep(5L)

    def perfTest(spark: SparkSession, zone: String): DataFrame = {
      spark.read.parquet(path).select(functions.count(
        functions.date_format(functions.col("c_ts"), "yyyy-MM-dd HH:mm:ss")
      ))
    }

    runAndRecordTime("date_format",
      perfTest)
  }
}
