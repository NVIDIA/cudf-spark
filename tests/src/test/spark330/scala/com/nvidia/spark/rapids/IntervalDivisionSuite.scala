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

import java.time.Period

import scala.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// Currently Pyspark does not support YearMonthIntervalType,
// TODO move this to the integration test module, see issue
//  https://github.com/NVIDIA/spark-rapids/issues/5212
class IntervalDivisionSuite extends SparkQueryCompareTestSuite {
  private def intervalArithmeticError(e: Exception): Boolean = {
    val msg = Option(e.getMessage).getOrElse("")
    val full = s"${e.getClass.getName}: $msg"
    full.contains("ArithmeticException") ||
      msg.contains("INTERVAL_DIVIDED_BY_ZERO") ||
      msg.contains("ARITHMETIC_OVERFLOW") ||
      msg.contains("input is infinite or NaN") ||
      msg.contains("Has NaN")
  }

  testSparkResultsAreEqual(
    "test year-month interval / num, normal case",
    spark => {
      val data = Seq(
        Row(Period.ofMonths(5), 3.toByte, 2.toShort, 1, 4L, 2.5f, 2.6d),
        Row(Period.ofMonths(5), null, null, null, null, null, null),
        Row(null, 3.toByte, 3.toShort, 3, 3L, 1.1f, 2.7d),
        Row(Period.ofMonths(0), 3.toByte, 3.toShort, 3, 3L, 3.1f, 3.4d),
        Row(Period.ofMonths(0), Byte.MinValue, Short.MinValue, Int.MinValue, Long.MinValue,
          Float.MinValue, Double.MinValue),
        Row(Period.ofMonths(0),
          Byte.MaxValue, Short.MaxValue, Int.MaxValue, Long.MaxValue,
          Float.MaxValue, Double.MaxValue),
        Row(Period.ofMonths(7), 4.toByte, 3.toShort, 2, 1L,
          Float.NegativeInfinity, Double.NegativeInfinity),
        Row(Period.ofMonths(7), 6.toByte, 5.toShort, 4, 3L,
          Float.PositiveInfinity, Double.PositiveInfinity)
      )
      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_b", ByteType),
        StructField("c_s", ShortType),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr(
        "c_ym / c_b", "c_ym / c_s", "c_ym / c_i", "c_ym / c_l", "c_ym / c_f", "c_ym / c_d",
        "c_ym / cast('15' as byte)", "c_ym / cast('15' as short)", "c_ym / 15",
        "c_ym / 15L", "c_ym / 15.1f", "c_ym / 15.1d",
        "interval '55' month / c_b",
        "interval '15' month / c_s",
        "interval '25' month / c_i",
        "interval '15' month / c_l",
        "interval '25' month / c_f",
        "interval '35' month / c_d")
  }

  testSparkResultsAreEqual("test year-month interval / float num, normal case",
    spark => {
      val numRows = 1024
      val r = new Random(0)

      def getSign: Int = if (r.nextBoolean()) 1 else -1

      val data = (0 until numRows).map(i => {
        Row(Period.ofMonths(getSign * r.nextInt(Int.MaxValue)),
          getSign * (r.nextFloat() + 10f), // add 10 to overflow
          getSign * (r.nextDouble() + 10d))
      })

      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("c_ym / c_f", "c_ym / c_d")
  }

  testSparkResultsAreEqual("test year-month interval / int num, normal case",
    spark => {
      val numRows = 1024
      val r = new Random(0)

      def getSign: Int = if (r.nextBoolean()) 1 else -1

      val data = (0 until numRows).map(i => {
        Row(Period.ofMonths(getSign * r.nextInt(Int.MaxValue)),
          getSign * r.nextInt(1024 * 1024) + 1, // add 1 to avoid dividing 0
          (getSign * r.nextInt(1024 * 1024) + 1).toLong)
      })

      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("c_ym / c_i", "c_ym / c_l")
  }

  // both gpu and cpu will throw ArithmeticException
  def testDivideYMOverflow(testCaseName: String, months: Int, num: Any): Unit = {
    testBothCpuGpuExpectedException[Exception](testCaseName + ", cv / scalar",
      intervalArithmeticError,
      spark => {
        val data = Seq(Row(Period.ofMonths(months)))
        val schema = StructType(Seq(
          StructField("c1", YearMonthIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df =>
        num match {
          case b: Byte => df.selectExpr(s"c1 / cast('$b' as Byte)")
          case s: Short => df.selectExpr(s"c1 / cast('$s' as Short)")
          case i: Int => df.selectExpr(s"c1 / $i")
          case l: Long => df.selectExpr(s"c1 / ${l}L")
          case f: Float if f.isNaN => df.selectExpr("c1 / cast('NaN' as float)")
          case f: Float if f.equals(Float.PositiveInfinity) =>
            df.selectExpr("c1 / cast('Infinity' as float)")
          case f: Float if f.equals(Float.NegativeInfinity) =>
            df.selectExpr("c1 / cast('-Infinity' as float)")
          case f: Float => df.selectExpr(s"c1 / ${f}f")
          case d: Double if d.isNaN => df.selectExpr("c1 / cast('NaN' as double)")
          case d: Double if d.equals(Double.PositiveInfinity) =>
            df.selectExpr("c1 / cast('Infinity' as double)")
          case d: Double if d.equals(Double.NegativeInfinity) =>
            df.selectExpr("c1 / cast('-Infinity' as double)")
          case d: Double => df.selectExpr(s"c1 / ${d}d")
          case _ => fail(s"Unsupported num type ${num.getClass}")
        }
    }

    testBothCpuGpuExpectedException[Exception](testCaseName + ", cv / cv",
      intervalArithmeticError,
      spark => {
        // Period is the external type of year-month type
        val (data, schema) = num match {
          case b: Byte => (Seq(Row(Period.ofMonths(months), b)), StructType(Seq(
            StructField("c1", YearMonthIntervalType()),
            StructField("c2", ByteType)
          )))
          case s: Short => (Seq(Row(Period.ofMonths(months), s)), StructType(Seq(
            StructField("c1", YearMonthIntervalType()),
            StructField("c2", ShortType)
          )))
          case i: Int => (Seq(Row(Period.ofMonths(months), i)), StructType(Seq(
            StructField("c1", YearMonthIntervalType()),
            StructField("c2", IntegerType)
          )))
          case l: Long =>
            (Seq(Row(Period.ofMonths(months), l)), StructType(Seq(
              StructField("c1", YearMonthIntervalType()),
              StructField("c2", LongType)
            )))
          case f: Float => (Seq(Row(Period.ofMonths(months), f)), StructType(Seq(
            StructField("c1", YearMonthIntervalType()),
            StructField("c2", FloatType)
          )))
          case d: Double => (Seq(Row(Period.ofMonths(months), d)), StructType(Seq(
            StructField("c1", YearMonthIntervalType()),
            StructField("c2", DoubleType)
          )))
          case _ =>
            fail(s"Unsupported num type ${num.getClass}")
        }

        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 / c2")
    }

    testBothCpuGpuExpectedException[Exception](testCaseName + ", scalar / cv",
      intervalArithmeticError,
      spark => {
        val (data, schema) = num match {
          case b: Byte => (Seq(Row(b)), StructType(Seq(StructField("c1", ByteType))))
          case s: Short => (Seq(Row(s)), StructType(Seq(StructField("c1", ShortType))))
          case i: Int => (Seq(Row(i)), StructType(Seq(StructField("c1", IntegerType))))
          case l: Long => (Seq(Row(l)), StructType(Seq(StructField("c1", LongType))))
          case f: Float => (Seq(Row(f)), StructType(Seq(StructField("c1", FloatType))))
          case d: Double => (Seq(Row(d)), StructType(Seq(StructField("c1", DoubleType))))
          case _ => fail(s"Unsupported num type ${num.getClass}")
        }
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"interval '$months' month / c1")
    }
  }

  // divide by 0
  testDivideYMOverflow("year-month divide 0.toByte", 1, 0.toByte)
  testDivideYMOverflow("year-month divide 0.toShort", 1, 0.toShort)
  testDivideYMOverflow("year-month divide 0", 1, 0)
  testDivideYMOverflow("year-month divide 0L", 1, 0.toLong)
  testDivideYMOverflow("year-month divide 0.0f", 1, 0.0f)
  testDivideYMOverflow("year-month divide 0.0d", 1, 0.0d)
  testDivideYMOverflow("year-month divide -0.0f", 1, -0.0f)
  testDivideYMOverflow("year-month divide -0.0d", 1, -0.0d)

  // NaN
  testDivideYMOverflow("year-month divide Float.NaN", 1, Float.NaN)
  testDivideYMOverflow("year-month divide Double.NaN", 1, Double.NaN)
  // 0.0 / 0.0 = NaN
  testDivideYMOverflow("year-month 0 divide 0.0f", 0, 0.0f)
  testDivideYMOverflow("year-month 0 divide 0.0d", 0, 0.0d)

  // divide float/double overflow
  testDivideYMOverflow("year-month divide overflow 1", Int.MaxValue, 0.1f)
  testDivideYMOverflow("year-month divide overflow 2", Int.MaxValue, 0.1d)
  testDivideYMOverflow("year-month divide overflow 3", Int.MinValue, 0.1f)
  testDivideYMOverflow("year-month divide overflow 4", Int.MinValue, 0.1d)

  testDivideYMOverflow("year-month divide overflow 5", Int.MinValue, -1)
}
