/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.time.Period

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType, YearMonthIntervalType}

/**
 * Spark stores year month interval as int, the value is 12 * year + month
 * Currently Pyspark does not have year month type, please see
 *   https://github.com/apache/spark/blob/branch-3.3/python/pyspark/sql/types.py
 * Put this test suite into Python tests module after Pyspark supports year month interval type.
 */
class IntervalArithmeticSuite extends SparkQueryCompareTestSuite {

  private def getDfForNoOverflow(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Period is the external type of year-month type
    Seq((Period.ofYears(500).plusMonths(1), Period.ofYears(600).plusMonths(6)),
      (Period.ofYears(500).plusMonths(2), Period.ofYears(600).plusMonths(11)))
        .toDF("c_year_month1", "c_year_month2")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Add",
    spark => getDfForNoOverflow(spark)) {
    df => df.selectExpr("c_year_month1 + c_year_month2")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Add overflow",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(24), Period.ofYears(Integer.MAX_VALUE / 12)))
          .toDF("c_year_month1", "c_year_month2")
    }
  ) {
    df => df.selectExpr("c_year_month1 + c_year_month2")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Add overflow, ansi mode",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(24), Period.ofYears(Integer.MAX_VALUE / 12)))
          .toDF("c_year_month1", "c_year_month2")
    },
    new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")
  ) {
    df => df.selectExpr("c_year_month1 + c_year_month2")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Subtract",
    spark => getDfForNoOverflow(spark)) {
    df => df.selectExpr("c_year_month1 - c_year_month2")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Subtract overflow",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofYears(Integer.MIN_VALUE / 12), Period.ofYears(2)))
          .toDF("c_year_month1", "c_year_month2")
    }
  ) {
    df => df.selectExpr("c_year_month1 - c_year_month2")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Subtract overflow, ansi mode",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofYears(Integer.MIN_VALUE / 12), Period.ofYears(2)))
          .toDF("c_year_month1", "c_year_month2")
    },
    new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")
  ) {
    df => df.selectExpr("c_year_month1 - c_year_month2")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Abs",
    spark => getDfForNoOverflow(spark)) {
    df => df.selectExpr("abs(c_year_month1)")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: abs overflow",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(Integer.MIN_VALUE)))
          .toDF("c_year_month1")
    }
  ) {
    df => df.selectExpr("abs(c_year_month1)")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: abs overflow, ansi mode",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(Integer.MIN_VALUE)))
          .toDF("c_year_month1")
    },
    new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")
  ) {
    df => df.selectExpr("abs(c_year_month1)")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Minus",
    spark => getDfForNoOverflow(spark)) {
    df => df.selectExpr("-(c_year_month1)")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Minus overflow",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(Integer.MIN_VALUE)))
          .toDF("c_year_month1")
    }
  ) {
    df => df.selectExpr("-(c_year_month1)")
  }

  testBothCpuGpuExpectedException[SparkException](
    "test year month interval arithmetic: Minus overflow, ansi mode",
    e => e.getMessage.contains("ArithmeticException"),
    spark => {
      import spark.implicits._
      Seq((Period.ofMonths(Integer.MIN_VALUE)))
          .toDF("c_year_month1")
    },
    new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")
  ) {
    df => df.selectExpr("-(c_year_month1)")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Positive",
    spark => {
      val data = Seq(Row(Period.ofYears(100)))
      val schema = StructType(Seq(StructField("c_day_time1", YearMonthIntervalType())))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("+c_day_time1")
  }

  testSparkResultsAreEqual(
    "test year month interval arithmetic: Positive, AST",
    spark => {
      val data = Seq(Row(Period.ofYears(100)))
      val schema = StructType(Seq(StructField("c_year_month1", YearMonthIntervalType())))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    new SparkConf().set(RapidsConf.ENABLE_PROJECT_AST.key, "true"),
    existClasses = "GpuProjectAstExec",
    nonExistClasses = "GpuProjectExec"
  ) {
    df => {
      df.selectExpr("+c_year_month1")
    }
  }
}
