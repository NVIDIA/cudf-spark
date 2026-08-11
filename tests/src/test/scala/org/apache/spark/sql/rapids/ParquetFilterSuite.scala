/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import java.sql.{Date, Timestamp}

import com.nvidia.spark.rapids.{GpuFilterExec, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

class ParquetFilterSuite extends SparkQueryCompareTestSuite {

  def stripSparkFilter(spark: SparkSession, df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
      case GpuFilterExec(_, child) => child
    }
    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }

  def withAllDatasources(code: => Unit): Unit = {
    // test data source v1 and v2
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      code
    }
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      code
    }
  }

  def withAllDevicePair(func: (Boolean, Boolean) => Unit): Unit = {
    for {
      (writeGpu, readGpu) <- Seq((true, true), (true, false), (false, true), (false, false))
    } func(writeGpu, readGpu)
  }

  def testRangePartitioningPpd(spark: SparkSession, writeDf: DataFrame,
      partCol: String, predicate: Column, length: Int)(
      writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllDatasources {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          "spark.rapids.sql.test.enabled" -> writeGpu.toString,
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf.repartitionByRange(math.max(1, length / 128), col(partCol))
              .write
              .parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
          "spark.rapids.sql.test.enabled" -> readGpu.toString,
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val df = spark.read.parquet(path.getAbsolutePath).filter(predicate)
          // Here, we strip the Spark side filter and check the actual results from Parquet.
          val actual = stripSparkFilter(spark, df).collect().length
          assert(actual > 1 && actual < length)
        }
      }
    }
  }

  def testOutOfRangePpd(spark: SparkSession, writeDf: DataFrame, predicate: Column)(
      writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllDatasources {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          "spark.rapids.sql.test.enabled" -> writeGpu.toString,
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf.coalesce(1).write.parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
          "spark.rapids.sql.test.enabled" -> readGpu.toString,
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val df = spark.read.parquet(path.getAbsolutePath).filter(predicate)
          // Here, we strip the Spark side filter and check the actual results from Parquet.
          val actual = stripSparkFilter(spark, df).collect().length
          assert(actual == 0)
        }
      }
    }
  }

  test("Parquet filter pushdown - boolean") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df1 = (1 to 1024).map(i => i % 500 == 0).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df1, "a", {col("a") === true}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df1, {col("a").isNull}))
      val df2 = (1 to 1024).map(i => false).toDF("a")
      withAllDevicePair(testOutOfRangePpd(spark, df2, {col("a") === true}))
    })
  }

  test("Parquet filter pushdown - byte") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 127).map(_.toByte).toDF("a")
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - smallint") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toShort).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - int") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - long") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toLong).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - float") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toFloat).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - double") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toDouble).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - string") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toString).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === "500"}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", 
          {col("a").startsWith("10")}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
    })
  }

  test("Parquet filter pushdown - decimal") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => BigDecimal.valueOf(i)).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") === 500}, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", {col("a") > 1000}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a") === 0}))
    })
  }

  test("Parquet filter pushdown - date") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => Date.valueOf("1970-01-01").toLocalDate.plusDays(i))
          .toDF("a")
      val equalsPredicate = {col("a") === Date.valueOf("1970-01-01").toLocalDate.plusDays(500)}
      val greaterThanPredicate = {col("a") > Date.valueOf("1970-01-01").toLocalDate.plusDays(1000)}
      val outOfRangePredicate = 
          {col("a") === Date.valueOf("1970-01-01").toLocalDate.plusDays(2000)}
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", equalsPredicate, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", greaterThanPredicate, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, outOfRangePredicate))
    })
  }

  test("Parquet filter pushdown - timestamp") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => new Timestamp(i)).toDF("a")
      val equalsPredicate = {col("a") === Timestamp.valueOf("1970-01-01 00:00:00.500")}
      val greaterThanPredicate = {col("a") > Timestamp.valueOf("1970-01-01 00:00:01.000")}
      val outOfRangePredicate = {col("a") === Timestamp.valueOf("1970-01-01 00:00:02.000")}
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", equalsPredicate, 1024))
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", greaterThanPredicate, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, {col("a").isNull}))
      withAllDevicePair(testOutOfRangePpd(spark, df, outOfRangePredicate))
    })
  }

  def testDotsInNamePpd(spark: SparkSession, writeDf: DataFrame, predicate: String)(
      writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllDatasources {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false",
          "spark.rapids.sql.test.enabled" -> writeGpu.toString,
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf.write.parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false",
          "spark.rapids.sql.test.enabled" -> readGpu.toString,
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val readBack = spark.read.parquet(path.getAbsolutePath).where(predicate)
          assert(readBack.count() == 1)
        }
      }
    }
  }

  test("SPARK-31026: Parquet predicate pushdown for fields having dots in the names") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df1 = Seq(Some(1), None).toDF("col.dots")
      withAllDevicePair(testDotsInNamePpd(spark, df1, "`col.dots` IS NOT NULL"))
      val df2 = (1 to 1024).toDF("col.dots")
      withAllDevicePair(testRangePartitioningPpd(spark, df2, "`col.dots`", 
          {col("`col.dots`") === 500}, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df2, {col("`col.dots`") === 0}))
    })
  }
}
