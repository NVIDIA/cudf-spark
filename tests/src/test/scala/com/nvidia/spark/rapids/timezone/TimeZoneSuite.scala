/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import java.time._
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToInstant
import org.apache.spark.sql.rapids.TimeZoneDB
import org.apache.spark.sql.types._

class TimeZoneSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {
  private val useGPU = true
  private val testAllTimezones = false
  private val testAllYears = false

  private var zones = Seq.empty[String]

  /**
   * create timestamp column vector
   */
  def createColumnVector(epochSeconds: Array[Long]): ColumnVector = {
    val rows = epochSeconds.length
    withResource(HostColumnVector.builder(DType.TIMESTAMP_MICROSECONDS, rows)) { builder =>
      var idx = 0
      while (idx < rows) {
        // convert seconds to micro seconds
        builder.append(epochSeconds(idx) * 1000000L)
        idx += 1
      }
      withResource(builder.build()) { b =>
        b.copyToDevice()
      }
    }
  }

  /**
   * create date column vector
   */
  def createDateColumnVector(epochDays: Array[LocalDate]): ColumnVector = {
    val rows = epochDays.length
    withResource(HostColumnVector.builder(DType.TIMESTAMP_DAYS, rows)) { builder =>
      var idx = 0
      while (idx < rows) {
        builder.append(epochDays(idx).toEpochDay.toInt)
        idx += 1
      }
      withResource(builder.build()) { b =>
        b.copyToDevice()
      }
    }
  }

  /**
   * create Spark data frame, schema is [(ts_long: long)]
   *
   * @return
   */
  def createLongDF(spark: SparkSession, epochSeconds: Array[Long]): DataFrame = {
    val data = epochSeconds.map(l => Row(l))
    val schema = StructType(Array(StructField("ts_long", LongType)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * create Spark data frame, schema is [(date_int: Int)]
   *
   * @return
   */
  def createDateDF(spark: SparkSession, epochDays: Array[LocalDate]): DataFrame = {
    val data = epochDays.map(d => Row(d))
    val schema = StructType(Array(StructField("date_col", DateType)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * assert timestamp result with Spark result
   */
  def assertTimestampRet(actualRet: ColumnVector, sparkRet: Seq[Row], input: ColumnVector): Unit = {
    withResource(actualRet.copyToHost()) { host =>
      withResource(input.copyToHost()) { hostInput =>
        assert(actualRet.getRowCount == sparkRet.length)
        for (i <- 0 until actualRet.getRowCount.toInt) {
          val sparkInstant = sparkRet(i).getInstant(0)
          val sparkMicro = sparkInstant.getEpochSecond * 1000000L + sparkInstant.getNano / 1000L
          if (hostInput.getType == DType.TIMESTAMP_DAYS) {
            assert(host.getLong(i) == sparkMicro,
              s"for ${hostInput.getInt(i)} " +
                s"${microsToInstant(host.getLong(i))} != ${microsToInstant(sparkMicro)}")

          } else {
            assert(host.getLong(i) == sparkMicro,
              s"for ${hostInput.getLong(i)} (${microsToInstant(hostInput.getLong(i))}) " +
                s"${microsToInstant(host.getLong(i))} != ${microsToInstant(sparkMicro)}")
          }
        }
      }
    }
  }

  /**
   * assert date result with Spark result
   */
  def assertDateRet(actualRet: ColumnVector, sparkRet: Seq[Row]): Unit = {
    withResource(actualRet.copyToHost()) { host =>
      assert(actualRet.getRowCount == sparkRet.length)
      for (i <- 0 until actualRet.getRowCount.toInt) {
        val epochDay = sparkRet(i).getLocalDate(0).toEpochDay
        assert(host.getInt(i) == epochDay)
      }
    }
  }

  /**
   * Get all epoch seconds every 15 minutes in [startYear, endYear].
   */
  def getEpochSeconds(startYear: Int, endYear: Int): Array[Long] = {
    val s = Instant.parse("%04d-01-01T00:00:00z".format(startYear)).getEpochSecond
    val e = Instant.parse("%04d-01-01T00:00:00z".format(endYear)).getEpochSecond
    val epochSeconds = mutable.ArrayBuffer[Long]()
    for (epoch <- s until e by TimeUnit.MINUTES.toSeconds(15)) {
      epochSeconds += epoch
    }
    // Note: in seconds, not micro seconds
    epochSeconds.toArray
  }

  /**
   * Get all LocalDate in [startYear, endYear].
   */
  def getEpochDays(startYear: Int, endYear: Int): Array[LocalDate] = {
    val s = LocalDate.of(startYear, 1, 1).toEpochDay.toInt
    val e = LocalDate.of(endYear, 1, 1).toEpochDay.toInt
    val epochDays = mutable.ArrayBuffer[LocalDate]()
    for (epoch <- s until e) {
      epochDays += LocalDate.ofEpochDay(epoch)
    }
    epochDays.toArray
  }

  def testFromUtcTimeStampToTimestamp(epochSeconds: Array[Long], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createLongDF(spark, epochSeconds).createOrReplaceTempView("tab")
        // refer to https://spark.apache.org/docs/latest/api/sql/#from_utc_timestamp
        // first cast(long as timestamp), it does not timezone aware.
        val query = s"select from_utc_timestamp(cast(ts_long as timestamp), '$zoneStr') from tab"
        spark.sql(query).collect()
      },
      new SparkConf()
          // from_utc_timestamp's 2nd parameter is timezone, so here use UTC is OK.
          .set("spark.sql.session.timeZone", "UTC")
          // by setting this, the Spark output for datetime type is java.time.Instant instead
          // of java.sql.Timestamp, it's convenient to compare result via java.time.Instant
          .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    withResource(createColumnVector(epochSeconds)) { inputCv =>
      val actualRet = if (useGPU) {
        GpuTimeZoneDB.fromUtcTimestampToTimestamp(
          inputCv,
          ZoneId.of(zoneStr))
      } else {
        TimeZoneDB.fromUtcTimestampToTimestamp(
          inputCv,
          ZoneId.of(zoneStr))
      }
      withResource(actualRet) { _ =>
        assertTimestampRet(actualRet, sparkRet, inputCv)
      }
    }

  }

  def testFromTimestampToUtcTimestamp(epochSeconds: Array[Long], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createLongDF(spark, epochSeconds).createOrReplaceTempView("tab")
        // refer to https://spark.apache.org/docs/latest/api/sql/#to_utc_timestamp
        // first cast(long as timestamp), it does not timezone aware.
        val query = s"select to_utc_timestamp(cast(ts_long as timestamp), '$zoneStr') from tab"
        spark.sql(query).collect()
      },
      new SparkConf()
          // to_utc_timestamp's 2nd parameter is timezone, so here use UTC is OK.
          .set("spark.sql.session.timeZone", "UTC")
          // by setting this, the Spark output for datetime type is java.time.Instant instead
          // of java.sql.Timestamp, it's convenient to compare result via java.time.Instant
          .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    withResource(createColumnVector(epochSeconds)) { inputCv =>
      val actualRet = if (useGPU) {
        GpuTimeZoneDB.fromTimestampToUtcTimestamp(
          inputCv,
          ZoneId.of(zoneStr))
      } else {
        TimeZoneDB.fromTimestampToUtcTimestamp(
          inputCv,
          ZoneId.of(zoneStr))
      }
      withResource(actualRet) { _ =>
        assertTimestampRet(actualRet, sparkRet, inputCv)
      }
    }

  }

  def testFromTimestampToDate(epochSeconds: Array[Long], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createLongDF(spark, epochSeconds).createOrReplaceTempView("tab")
        // cast timestamp to date
        // first cast(long as timestamp), it's not timezone aware.
        spark.sql(s"select cast(cast(ts_long as timestamp) as date) from tab").collect()
      },
      new SparkConf()
          // cast(timestamp as date) will use this timezone
          .set("spark.sql.session.timeZone", zoneStr)
          // by setting this, the Spark output for date type is java.time.LocalDate instead
          // of java.sql.Date, it's convenient to compare result
          .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    val actualRet = withResource(createColumnVector(epochSeconds)) { inputCv =>
      TimeZoneDB.fromTimestampToDate(
        inputCv,
        ZoneId.of(zoneStr))
    }

    withResource(actualRet) { _ =>
      assertDateRet(actualRet, sparkRet)
    }
  }

  def testFromDateToTimestamp(epochDays: Array[LocalDate], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createDateDF(spark, epochDays).createOrReplaceTempView("tab")
        // cast timestamp to date
        // first cast(long as timestamp), it's not timezone aware.
        spark.sql(s"select cast(date_col as Timestamp) from tab").collect()
      },
      new SparkConf()
          // cast(date as timestamp) will use this timezone
          .set("spark.sql.session.timeZone", zoneStr)
          // by setting this, the Spark output for date type is java.time.LocalDate instead
          // of java.sql.Date, it's convenient to compare result
          .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    withResource(createDateColumnVector(epochDays)) { inputCv =>
      val actualRet = TimeZoneDB.fromDateToTimestamp(
        inputCv,
        ZoneId.of(zoneStr))
      withResource(actualRet) { _ =>
        assertTimestampRet(actualRet, sparkRet, inputCv)
      }
    }

  }

  def selectWithRepeatZones: Seq[String] = {
    val mustZones = Array[String]("Asia/Shanghai", "America/Los_Angeles")
    val repeatZones = ZoneId.getAvailableZoneIds.asScala.toList.filter { z =>
      val rules = ZoneId.of(z).getRules
      !(rules.isFixedOffset || rules.getTransitionRules.isEmpty) && !mustZones.contains(z)
    }
    scala.util.Random.shuffle(repeatZones)
    repeatZones.slice(0, 2) ++ mustZones
  }

  def selectTimeZones: Seq[String] = {
    val mustZones = Array[String]("Asia/Shanghai", "America/Sao_Paulo")
    if (testAllTimezones) {
      val nonRepeatZones = ZoneId.getAvailableZoneIds.asScala.toList.filter { z =>
        val rules = ZoneId.of(z).getRules
        // remove this line when we support repeat rules
        (rules.isFixedOffset || rules.getTransitionRules.isEmpty) && !mustZones.contains(z)
      }
      scala.util.Random.shuffle(nonRepeatZones)
      nonRepeatZones.slice(0, 2) ++ mustZones
    } else {
      mustZones
    }
  }

  override def beforeAll(): Unit = {
    zones = selectTimeZones
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (useGPU) {
      GpuTimeZoneDB.shutdown()
    }
  }

  test("test timestamp to utc timestamp") {
    for (zoneStr <- zones) {
      // iterate years
      val startYear = if (testAllYears) 1 else 1899
      val endYear = if (testAllYears) 9999 else 2030
      for (year <- startYear until endYear by 7) {
        val epochSeconds = getEpochSeconds(year, year + 1)
        testFromTimestampToUtcTimestamp(epochSeconds, zoneStr)
      }
    }
  }

  test("test utc timestamp to timestamp") {
    for (zoneStr <- zones) {
      // iterate years
      val startYear = if (testAllYears) 1 else 1899
      val endYear = if (testAllYears) 9999 else 2030
      for (year <- startYear until endYear by 7) {
        val epochSeconds = getEpochSeconds(year, year + 1)
        testFromUtcTimeStampToTimestamp(epochSeconds, zoneStr)
      }
    }
  }

  test("test timestamp to date") {
    for (zoneStr <- zones) {
      // iterate years
      val startYear = if (testAllYears) 1 else 1899
      val endYear = if (testAllYears) 9999 else 2030
      for (year <- startYear until endYear by 7) {
        val epochSeconds = getEpochSeconds(year, year + 1)
         testFromTimestampToDate(epochSeconds, zoneStr)
      }
    }
  }

  test("test date to timestamp") {
    for (zoneStr <- zones) {
      // iterate years
      val startYear = if (testAllYears) 1 else 1899
      val endYear = if (testAllYears) 9999 else 2030
      val epochDays = getEpochDays(startYear, endYear)
      testFromDateToTimestamp(epochDays, zoneStr)
    }
  }

}
