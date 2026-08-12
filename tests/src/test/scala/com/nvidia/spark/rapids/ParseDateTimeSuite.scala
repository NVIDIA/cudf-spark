/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.sql.{Date, Timestamp}
import java.time.{ZonedDateTime, ZoneId}
import java.util.TimeZone

import scala.collection.mutable.ListBuffer

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class ParseDateTimeSuite extends SparkQueryCompareTestSuite with BeforeAndAfterEach {

  private val CORRECTED_TIME_PARSER_POLICY: SparkConf = new SparkConf()
    .set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")
    .set("spark.sql.ansi.enabled", "false")

  private val LEGACY_TIME_PARSER_POLICY_CONF: SparkConf = new SparkConf()
    .set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "LEGACY")
    .set(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key, "true")
    .set("spark.sql.ansi.enabled", "false")

  override def beforeEach(): Unit = {
    GpuOverrides.removeAllListeners()
  }

  override def afterEach(): Unit = {
    GpuOverrides.removeAllListeners()
  }

  testGpuFallback("to_date dd/MM/yy (fall back to CPU for two-digit years)",
      "GetTimestamp",
      datesAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")
        .set(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key, "true")
        .set("spark.sql.ansi.enabled", "false"),
      // The ProjectExec is needed only if the gpu cpu bridge is disabled.
      execsAllowedNonGpu = Seq("ProjectExec", "Alias", "GetTimestamp", 
        "UnixTimestamp", "ShuffleExchangeExec")) {
    // until we fix https://github.com/NVIDIA/spark-rapids/issues/2118 we need to fall
    // back to CPU when parsing two-digit years
    df => df.withColumn("c1", to_date(col("c0"), "dd/MM/yy"))
  }


  testSparkResultsAreEqual("to_date yyyy-MM-dd",
      datesAsStrings,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_date yyyy-MM-dd LEGACY",
    datesAsStrings,
    conf = LEGACY_TIME_PARSER_POLICY_CONF) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_date yyyy/MM/dd LEGACY",
    datesAsStrings,
    conf = LEGACY_TIME_PARSER_POLICY_CONF) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy/MM/dd"))
  }

  testSparkResultsAreEqual("to_date dd/MM/yyyy",
      datesAsStrings,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "dd/MM/yyyy"))
  }

  testSparkResultsAreEqual("to_date dd/MM/yyyy LEGACY",
    datesAsStrings,
    conf = LEGACY_TIME_PARSER_POLICY_CONF) {
    df => df.withColumn("c1", to_date(col("c0"), "dd/MM/yyyy"))
  }

  testSparkResultsAreEqual("to_date MM/dd/yyyy",
      datesAsStrings,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "MM/dd/yyyy"))
  }

  testSparkResultsAreEqual("to_date yyyy/MM",
    datesAsStrings,
    conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy/MM"))
  }

  testSparkResultsAreEqual("to_date parse date",
      dates,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_date parse timestamp",
      timestamps,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_timestamp yyyy-MM-dd",
      timestampsAsStrings,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_timestamp(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_timestamp dd/MM/yyyy",
      timestampsAsStrings,
      conf = CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", to_timestamp(col("c0"), "dd/MM/yyyy"))
  }

  testSparkResultsAreEqual("to_date default pattern",
    datesAsStrings,
    CORRECTED_TIME_PARSER_POLICY
        // All of the dates being parsed are valid for all of the versions of Spark supported.
        .set(RapidsConf.HAS_EXTENDED_YEAR_VALUES.key, "false")
        .set("spark.sql.ansi.enabled", "false")) {
    df => df.withColumn("c1", to_date(col("c0")))
  }

  testSparkResultsAreEqual("unix_timestamp parse date",
      timestampsAsStrings,
      CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("unix_timestamp parse yyyy/MM",
    timestampsAsStrings,
    CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy/MM"))
  }

  testSparkResultsAreEqual("to_unix_timestamp parse yyyy/MM",
    timestampsAsStrings,
    CORRECTED_TIME_PARSER_POLICY) {
    df => {
      df.createOrReplaceTempView("df")
      val spark = getActiveSession
      spark.sql("SELECT c0, to_unix_timestamp(c0, 'yyyy/MM') FROM df")
    }
  }

  testSparkResultsAreEqual("unix_timestamp parse timestamp",
      timestampsAsStrings,
      CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss"))
  }

  testSparkResultsAreEqual("unix_timestamp parse yyyy-MM-dd HH:mm:ss LEGACY",
    timestampsAsStrings,
    LEGACY_TIME_PARSER_POLICY_CONF) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss"))
  }

  testSparkResultsAreEqual("unix_timestamp parse yyyy/MM/dd HH:mm:ss LEGACY",
    timestampsAsStrings,
    LEGACY_TIME_PARSER_POLICY_CONF) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy/MM/dd HH:mm:ss"))
  }

  testGpuFallback("unix_timestamp parse timestamp millis (fall back to CPU)",
      "UnixTimestamp",
      timestampsAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")
        .set("spark.sql.ansi.enabled", "false"),
      // The ProjectExec is needed only if the gpu cpu bridge is disabled.
      execsAllowedNonGpu = Seq("ProjectExec", "Alias", "UnixTimestamp")) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss.SSS"))
  }

  testSparkResultsAreEqual("unix_timestamp parse timestamp default pattern",
      timestampsAsStrings,
      CORRECTED_TIME_PARSER_POLICY) {
    df => df.withColumn("c1", unix_timestamp(col("c0")))
  }

  testGpuFallback("fall back to CPU when policy is LEGACY and unsupported format is used",
      "UnixTimestamp",
      timestampsAsStrings,
      LEGACY_TIME_PARSER_POLICY_CONF,
      repart = 1,
      // The ProjectExec is needed only if the gpu cpu bridge is disabled.
      execsAllowedNonGpu = Seq("ProjectExec", "Alias", "UnixTimestamp")) {
    df => {
      val formatString = "u" // we do not support this legacy format on GPU
      df.withColumn("c1", unix_timestamp(col("c0"), formatString))
    }
  }

  test("unsupported format") {

    // capture plans
    val plans = new ListBuffer[SparkPlanMeta[SparkPlan]]()
    GpuOverrides.addListener(
        (plan: SparkPlanMeta[SparkPlan], _: SparkPlan, _: Seq[Optimization]) => {
      plans.append(plan)
    })

    val e = intercept[IllegalArgumentException] {
      val df = withGpuSparkSession(spark => {
        datesAsStrings(spark)
          .repartition(2)
          .withColumn("c1", to_date(col("c0"), "F"))
      }, CORRECTED_TIME_PARSER_POLICY)
      df.collect()
    }
    // With the bridge feature, the error message might be about bridge expressions
    // instead of ProjectExec, so we check for both possibilities
    assert(e.getMessage.contains("Part of the plan is not columnar") ||
      e.getMessage.contains("GpuCpuBridgeExpression contains disallowed CPU expressions"))

    val planStr = plans.last.toString
    assert(planStr.contains("Failed to convert Unsupported character: F"))
    // make sure we aren't suggesting enabling INCOMPATIBLE_DATE_FORMATS for something we
    // can never support
    assert(!planStr.contains(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key))
  }

  test("literals: ensure time literals are correct") {
    val conf = new SparkConf()
    val df = withGpuSparkSession(spark => {
      spark.sql("SELECT current_date(), current_timestamp(), now() FROM RANGE(1, 10)")
    }, conf)

    val times = df.collect()
    val systemCurrentTime = System.currentTimeMillis()
    val res = times.forall(time => {
      val diffDate = systemCurrentTime - time.getDate(0).getTime()
      val diffTimestamp = systemCurrentTime - time.getTimestamp(1).getTime()
      val diffNow = systemCurrentTime - time.getTimestamp(2).getTime()
      diffDate.abs <= 8.64E7 & diffTimestamp.abs <= 1000 & diffNow.abs <= 1000
    })

    assert(res)
  }

  test("literals: ensure time literals are correct with different timezones") {
    testTimeWithDiffTimezones("Asia/Shanghai", "America/New_York")
    testTimeWithDiffTimezones("Asia/Shanghai", "UTC")
    testTimeWithDiffTimezones("UTC", "Asia/Shanghai")
  }

  private[this] def testTimeWithDiffTimezones(sessionTZStr: String, systemTZStr: String) = {
    withTimeZones(sessionTimeZone = sessionTZStr, systemTimeZone = systemTZStr) { conf =>
      val df = withGpuSparkSession(spark => {
        spark.sql("SELECT current_date(), current_timestamp(), now() FROM RANGE(1, 10)")
      }, conf)

      val times = df.collect()
      val zonedDateTime = ZonedDateTime.now(ZoneId.of("America/New_York"))
      val res = times.forall(time => {
        val diffDate = zonedDateTime.toLocalDate.toEpochDay - time.getLocalDate(0).toEpochDay
        val diffTimestamp =
          zonedDateTime.toInstant.getNano - time.getInstant(1).getNano
        val diffNow =
          zonedDateTime.toInstant.getNano - time.getInstant(2).getNano
        // For date, at most 1 day difference when execution is crossing two days
        // For timestamp or now, it should be less than 1 second allowing Spark's execution
        diffDate.abs <= 1 & diffTimestamp.abs <= 1E9 & diffNow.abs <= 1E9
      })
      assert(res)
    }
  }

  private def withTimeZones(sessionTimeZone: String,
      systemTimeZone: String)(f: SparkConf => Unit): Unit = {
    val conf = new SparkConf()
    conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sessionTimeZone)
    conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, "true")
    val originTimeZone = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(systemTimeZone))
      f(conf)
    } finally {
      TimeZone.setDefault(originTimeZone)
    }
  }

  // just show the failures so we don't have to manually parse all
  // the output to find which ones failed
  override def compareResults(
      sort: Boolean,
      floatEpsilon: Double,
      fromCpu: Array[Row],
      fromGpu: Array[Row]): Unit = {
    assert(fromCpu.length === fromGpu.length)

    val failures = fromCpu.zip(fromGpu).zipWithIndex.filterNot {
      case ((cpu, gpu), _) => super.compare(cpu, gpu, 0.0001)
    }

    if (failures.nonEmpty) {
      val str = failures.map {
        case ((cpu, gpu), i) =>
          s"""
             |[#$i] CPU: $cpu
             |[#$i] GPU: $gpu
             |
             |""".stripMargin
      }.mkString("\n")
      fail(s"Mismatch between CPU and GPU for the following rows:\n$str")
    }
  }

  private def dates(spark: SparkSession) = {
    import spark.implicits._
    dateValues.toDF("c0")
  }

  private def timestamps(spark: SparkSession) = {
    import spark.implicits._
    tsValues.toDF("c0")
  }

  private def timestampsAsStrings(spark: SparkSession) = {
    import spark.implicits._
    timestampValues.toDF("c0")
  }

  private def datesAsStrings(spark: SparkSession) = {
    import spark.implicits._
    val values = Seq(
      DateUtils.EPOCH,
      DateUtils.NOW,
      DateUtils.TODAY,
      DateUtils.YESTERDAY,
      DateUtils.TOMORROW
    ) ++ timestampValues
    values.toDF("c0")
  }

  private val singleDigits = Seq("1999-1-1 ",
    "1999-1-1 11",
    "1999-1-1 11:",
    "1999-1-1 11:5",
    "1999-1-1 11:59",
    "1999-1-1 11:59:",
    "1999-1-1 11:59:5",
    "1999-1-1 11:59:59",
    "1999-1-1",
    "1999-1-1 ",
    "1999-1-1 1",
    "1999-1-1 1:",
    "1999-1-1 1:2",
    "1999-1-1 1:2:",
    "1999-1-1 1:2:3",
    "1999-1-1 1:2:3.",
    "1999-1-1 1:12:3.",
    "1999-1-1 11:12:3.",
    "1999-1-1 11:2:3.",
    "1999-1-1 11:2:13.",
    "1999-1-1 1:2:3.4",
    "12-01-1999",
    "01-12-1999",
    "1999-12")

  private val timestampValues = Seq(
    "",
    "null",
    null,
    "\n",
    "1999-12-31 11",
    "1999-12-31 11:",
    "1999-12-31 11:5",
    "1999-12-31 11:59",
    "1999-12-31 11:59:",
    "1999-12-31 11:59:5",
    "1999-12-31 11:59:59",
    "  1999-12-31 11:59:59",
    "\t1999-12-31 11:59:59",
    "\t1999-12-31 11:59:59\n",
    "1999-12-31 11:59:59.",
    "1999-12-31 11:59:59.9",
    " 1999-12-31 11:59:59.9",
    "1999-12-31 11:59:59.99",
    "1999-12-31 11:59:59.999",
    "1999-12-31 11:59:59.9999",
    "1999-12-31 11:59:59.99999",
    "1999-12-31 11:59:59.999999",
    "1999-12-31 11:59:59.9999999",
    "1999-12-31 11:59:59.99999999",
    "1999-12-31 11:59:59.999999999",
    "31/12/1999",
    "31/12/1999 11:59:59.999",
    "1999-12-3",
    "1999-12-31",
    "1999/12/31",
    "1999-12",
    "1999/12",
    "1975/06",
    "1975/06/18",
    "1975/06/18 06:48:57",
    "1999-12-29\n",
    "\t1999-12-30",
    " \n1999-12-31",
    "1999/12/31",
    "2001- 1-1",
    "2001-1- 1",
    "2001- 1- 1",
    "2000-3-192",
    // interesting test cases from fuzzing that original triggered differences between CPU and GPU
    "2000-1- 099\n305\n 7-390--.0:-",
    "2000-7-7\n9\t8568:\n",
    "2000-\t5-2.7.44584.9935") ++ singleDigits ++ singleDigits.map(_.replace('-', '/'))

  private val dateValues = Seq(
    Date.valueOf("2020-07-24"),
    Date.valueOf("2020-07-25"),
    Date.valueOf("1999-12-31"))

  private val tsValues = Seq(
    Timestamp.valueOf("2015-07-24 10:00:00.3"),
    Timestamp.valueOf("2015-07-25 02:02:02.2"),
    Timestamp.valueOf("1999-12-31 11:59:59.999")
  )

}
