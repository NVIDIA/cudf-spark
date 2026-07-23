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
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import java.time.{Duration, Period}

import scala.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class IntervalMultiplySuite extends SparkQueryCompareTestSuite {
  private def intervalArithmeticError(e: Exception): Boolean = {
    val msg = Option(e.getMessage).getOrElse("")
    val full = s"${e.getClass.getName}: $msg"
    full.contains("ArithmeticException") ||
      msg.contains("ARITHMETIC_OVERFLOW") ||
      msg.contains("input is infinite or NaN") ||
      msg.contains("Has NaN")
  }

  testSparkResultsAreEqual(
    "test year-month interval * integer num, normal case",
    spark => {
      val data = Seq(
        Row(Period.ofMonths(5), 3L, 3),
        Row(Period.ofMonths(5), null, null),
        Row(null, 5L, 5),
        Row(Period.ofMonths(6), 0L, 0),
        Row(Period.ofMonths(0), 6L, 6),
        Row(Period.ofMonths(0), Long.MinValue, Int.MinValue),
        Row(Period.ofMonths(0), Long.MaxValue, Int.MaxValue)
      )
      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_l", LongType),
        StructField("c_i", IntegerType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df =>
      df.selectExpr(
        "c_ym * c_l", "c_ym * c_i", "c_ym * 15", "c_ym * 15L", "c_ym * 1.15f", "c_ym * 1.15d")
  }

  testSparkResultsAreEqual(
    "test year-month interval * integer num, normal case 2",
    spark => {
      val data = Seq(
        Row(1.toByte, 1.toShort, 1, 1L, 1.5f, 1.5d)
      )
      val schema = StructType(Seq(
        StructField("c_b", ByteType),
        StructField("c_s", ShortType),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)))

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df =>
      df.selectExpr(
        "interval '5' month * c_b",
        "interval '5' month * c_s",
        "interval '5' month * c_i",
        "interval '5' month * c_l",
        "interval '5' month * c_f",
        "interval '5' month * c_d"
      )
  }

  testSparkResultsAreEqual("test year-month interval * float num, normal case",
    spark => {
      val r = new Random(0)
      val data = (0 until 1024).map(i => {
        val sign = if (r.nextBoolean()) 1 else -1
        if (i % 10 == 0) {
          Row(Period.ofMonths(r.nextInt(1024)), Float.MinPositiveValue, Double.MinPositiveValue)
        } else {
          Row(Period.ofMonths(r.nextInt(1024)), sign * r.nextFloat(), sign * r.nextDouble())
        }
      })

      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("c_ym * c_f", "c_ym * c_d")
  }

  def testOverflowMultipyInt(testCaseName: String, month: Int, num: Int): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(month)))
        val schema = StructType(Seq(StructField("c1", YearMonthIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 * $num")
    }
  }

  testOverflowMultipyInt("test year-month interval * int overflow, case 1", Int.MaxValue, 2)
  testOverflowMultipyInt("test year-month interval * int overflow, case 2", 3, Int.MaxValue / 2)
  testOverflowMultipyInt("test year-month interval * int overflow, case 3", Int.MinValue, 2)
  testOverflowMultipyInt("test year-month interval * int overflow, case 4", -1, Int.MinValue)
  testOverflowMultipyInt("test year-month interval * int overflow, case 5", Int.MinValue, -1)


  def testOverflowMultipyLong(testCaseName: String, month: Int, num: Long): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(month)))
        val schema = StructType(Seq(StructField("c1", YearMonthIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 * ${num}L")
    }
  }

  testOverflowMultipyLong(
    "test year-month interval * long overflow case 1", -1, Long.MinValue)
  testOverflowMultipyLong(
    "test year-month interval * long overflow case 2", Int.MaxValue, -2)
  testOverflowMultipyLong(
    "test year-month interval * long overflow case 3", 1, Int.MaxValue.toLong + 1L)
  testOverflowMultipyLong(
    "test year-month interval * long overflow case 4", -1, Int.MinValue.toLong)
  testOverflowMultipyLong(
    "test year-month interval * long overflow case 5", -1, Long.MinValue)
  testOverflowMultipyLong(
    "test year-month interval * long overflow case 6", 2, Long.MaxValue)

  def testOverflowMultipyLong2(testCaseName: String, month: Int, num: Long): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(month), num))
        val schema = StructType(Seq(
          StructField("c1", YearMonthIntervalType()),
          StructField("c2", LongType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowMultipyLong2(
    "test year-month interval * long overflow case 21", -1, Long.MinValue)
  testOverflowMultipyLong2(
    "test year-month interval * long overflow case 22", Int.MinValue, -1L)
  testOverflowMultipyLong2(
    "test year-month interval * long overflow case 23", 1, Int.MinValue.toLong - 1L)
  testOverflowMultipyLong2(
    "test year-month interval * long overflow case 24", 2, Int.MinValue.toLong + 1L)
  testOverflowMultipyLong2(
    "test year-month interval * long overflow case 25", -2, Long.MaxValue)

  def testOverflowMultipyFloat(testCaseName: String, month: Int, num: Float): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(month), num))
        val schema = StructType(Seq(
          StructField("c1", YearMonthIntervalType()),
          StructField("c2", FloatType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowMultipyFloat("test year-month interval * float overflow case 1", 1, Float.NaN)
  testOverflowMultipyFloat(
    "test year-month interval * float overflow case 2", 1, Float.PositiveInfinity)
  testOverflowMultipyFloat(
    "test year-month interval * float overflow case 3", 1, Float.NegativeInfinity)
  testOverflowMultipyFloat(
    "test year-month interval * float overflow case 4", 2, Long.MinValue.toFloat)
  testOverflowMultipyFloat(
    "test year-month interval * float overflow case 5", 2, Long.MaxValue.toFloat)

  def testOverflowMultipyDouble(testCaseName: String, month: Int, num: Double): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(month), num))
        val schema = StructType(Seq(
          StructField("c1", YearMonthIntervalType()),
          StructField("c2", DoubleType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowMultipyDouble("test year-month interval * double overflow case 1", 1, Double.NaN)
  testOverflowMultipyDouble(
    "test year-month interval * double overflow case 2", 1, Double.PositiveInfinity)
  testOverflowMultipyDouble(
    "test year-month interval * double overflow case 3", 1, Double.NegativeInfinity)
  testOverflowMultipyDouble(
    "test year-month interval * double overflow case 4", 2, Long.MinValue.toDouble)
  testOverflowMultipyDouble(
    "test year-month interval * double overflow case 5", 2, Long.MaxValue.toDouble)


//   The following are day-time test cases

  def testOverflowDTMultipyInt(testCaseName: String, microSeconds: Long, num: Int): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val d = Duration.ofSeconds(microSeconds / 1000000, microSeconds % 1000000 * 1000)
        val data = Seq(Row(d))
        val schema = StructType(Seq(StructField("c1", DayTimeIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 * $num")
    }
  }

  testOverflowDTMultipyInt("test day-time interval * int overflow, case 1", Long.MaxValue, 2)
  testOverflowDTMultipyInt("test day-time interval * int overflow, case 2", Long.MaxValue / 2, 3)
  testOverflowDTMultipyInt("test day-time interval * int overflow, case 3", Long.MinValue, 2)
  testOverflowDTMultipyInt("test day-time interval * int overflow, case 4", Long.MinValue, -1)
  testOverflowDTMultipyInt("test day-time interval * int overflow, case 5", Long.MinValue, -2)


  def testOverflowDTMultipyLong(testCaseName: String, microSeconds: Long, num: Long): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val d = Duration.ofSeconds(microSeconds / 1000000, microSeconds % 1000000 * 1000)
        val data = Seq(Row(d))
        val schema = StructType(Seq(StructField("c1", DayTimeIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 * ${num}L")
    }
  }

  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 1", 2, Long.MaxValue)
  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 2", 3, Long.MinValue)
  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 3", Long.MinValue, -1)
  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 4", -1L, Long.MinValue)
  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 5", 2, Long.MaxValue)
  testOverflowDTMultipyLong(
    "test day-time interval * long overflow case 6", -2, Long.MaxValue)

  def testOverflowDTMultipyLong2(testCaseName: String, microSeconds: Long, num: Long): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val d = Duration.ofSeconds(microSeconds / 1000000, microSeconds % 1000000 * 1000)
        val data = Seq(Row(d, num))
        val schema = StructType(Seq(
          StructField("c1", DayTimeIntervalType()),
          StructField("c2", LongType)))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 21", -1L, Long.MinValue)
  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 22", Long.MinValue, -1L)
  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 23", Long.MinValue, 2)
  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 24", -2L, Long.MinValue)
  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 25", 2, Long.MaxValue)
  testOverflowDTMultipyLong2(
    "test day-time interval * long overflow case 26", -3, Long.MaxValue)


  def testOverflowDTMultipyFloat(testCaseName: String, microSeconds: Long, num: Float): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val d = Duration.ofSeconds(microSeconds / 1000000, microSeconds % 1000000 * 1000)
        val data = Seq(Row(d, num))
        val schema = StructType(Seq(
          StructField("c1", DayTimeIntervalType()),
          StructField("c2", FloatType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowDTMultipyFloat(
    "test day-time interval * float overflow case 1", 1, Float.NaN)
  testOverflowDTMultipyFloat(
    "test day-time interval * float overflow case 2", 1, Float.PositiveInfinity)
  testOverflowDTMultipyFloat(
    "test day-time interval * float overflow case 3", 1, Float.NegativeInfinity)
  testOverflowDTMultipyFloat(
    "test day-time interval * float overflow case 4", -1, Long.MinValue.toFloat)
  testOverflowDTMultipyFloat(
    "test day-time interval * float overflow case 5", 2, Long.MaxValue.toFloat)

  def testOverflowDTMultipyDouble(testCaseName: String, microSeconds: Long, num: Double): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName,
      intervalArithmeticError,
      spark => {
        val d = Duration.ofSeconds(microSeconds / 1000000, microSeconds % 1000000 * 1000)
        val data = Seq(Row(d, num))
        val schema = StructType(Seq(
          StructField("c1", DayTimeIntervalType()),
          StructField("c2", DoubleType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr("c1 * c2")
    }
  }

  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 1", 1, Double.NaN)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 2", 1, Double.PositiveInfinity)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 3", 1, Double.NegativeInfinity)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 4", 1, Float.NegativeInfinity)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 5", -1, Long.MinValue.toDouble)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 6", 2, Long.MaxValue.toDouble)
  testOverflowDTMultipyDouble(
    "test day-time interval * double overflow case 7", 3, Long.MinValue.toDouble)

  testSparkResultsAreEqual(
    "test day-time interval * integer num, normal case 2",
    spark => {
      val data = Seq(
        Row(Duration.ofSeconds(1), 1.toByte, 1.toShort, 1, 1L, 1.5f, 1.5d)
      )
      val schema = StructType(Seq(
        StructField("c_dt", DayTimeIntervalType()),
        StructField("c_b", ByteType),
        StructField("c_s", ShortType),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)))

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df =>
      df.selectExpr(
        "interval '5' second * c_b",
        "interval '5' second * c_s",
        "interval '5' second * c_i",
        "interval '5' second * c_l",
        "interval '5' second * c_f",
        "interval '5' second * c_d",
        "c_dt * 1.5f",
        "c_dt * 1.5d",
        "c_dt * 5L",
        "c_dt * 5"
      )
  }
}
