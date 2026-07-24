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
import java.util.Locale

import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, IntegerType, LongType, MapType, ShortType, StructField, StructType, YearMonthIntervalType => YM}

/**
 * Can not put this suite to Pyspark test cases
 * because of currently Pyspark not have year-month type.
 * See: https://github.com/apache/spark/blob/branch-3.3/python/pyspark/sql/types.py
 * Should move the year-month scala test cases to the integration test module,
 * filed an issue to track: https://github.com/NVIDIA/spark-rapids/issues/5212
 */
class IntervalCastSuite extends SparkQueryCompareTestSuite {
  private def castOverflowError(e: Exception): Boolean = {
    Option(e.getMessage).exists(_.toLowerCase(Locale.ROOT).contains("overflow"))
  }

  testSparkResultsAreEqual(
    "test cast year-month to integral",
    spark => {
      val data = (-128 to 127).map(i => Row(Period.ofMonths(i)))
      val schema = StructType(Seq(StructField("c_ym", YM())))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr("cast(c_ym as long)", "cast(c_ym as integer)", "cast(c_ym as short)",
        "cast(c_ym as byte)")
  }

  testSparkResultsAreEqual(
    "test cast integral to year-month",
    spark => {
      val data = (-128 to 127).map(i => Row(i.toLong, i, i.toShort, i.toByte))
      val schema = StructType(Seq(StructField("c_l", LongType),
        StructField("c_i", IntegerType),
        StructField("c_s", ShortType),
        StructField("c_b", ByteType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr("cast(c_l as interval month)", "cast(c_i as interval year)",
        "cast(c_s as interval year)", "cast(c_b as interval month)")
  }

  val toIntegralOverflowPairs = Array(
    (Byte.MinValue - 1, "byte"),
    (Byte.MaxValue + 1, "byte"),
    (Short.MinValue - 1, "short"),
    (Short.MinValue - 1, "short"))
  var testLoop = 1
  toIntegralOverflowPairs.foreach { case (months, toType) =>
    testBothCpuGpuExpectedException[Exception](
      s"test cast year-month to integral, overflow $testLoop",
      castOverflowError,
      spark => {
        val data = Seq(Row(Period.ofMonths(months)))
        val schema = StructType(Seq(StructField("c_ym", YM())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }) {
      df =>
        df.selectExpr(s"cast(c_ym as $toType)")
    }
    testLoop += 1
  }

  val toYMOverflows = Array(
    (Int.MinValue / 12 - 1, IntegerType, "year"),
    (Int.MaxValue / 12 + 1, IntegerType, "year"),
    (Int.MinValue - 1L, LongType, "month"),
    (Int.MaxValue + 1L, LongType, "month"),
    (Int.MinValue / 12 - 1L, LongType, "year"),
    (Int.MaxValue / 12 + 1L, LongType, "year"))
  testLoop = 1
  toYMOverflows.foreach { case (integral, integralType, toField) =>
    testBothCpuGpuExpectedException[Exception](
      s"test cast integral to year-month, overflow $testLoop",
      castOverflowError,
      spark => {
        val data = Seq(Row(integral))
        val schema = StructType(Seq(StructField("c_integral", integralType)))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }) {
      df =>
        df.selectExpr(s"cast(c_integral as interval $toField)")
    }
    testLoop += 1
  }

  testSparkResultsAreEqual(
    "test cast struct(integral, integral) to struct(year-month, year-month)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(
          Row(i.toLong, i.toLong),
          Row(i, i),
          Row(i.toShort, i.toShort),
          Row(i.toByte, i.toByte))
      }
      val schema = StructType(Seq(
        StructField("c_l", StructType(
          Seq(StructField("c1", LongType), StructField("c2", LongType)))),
        StructField("c_i", StructType(
          Seq(StructField("c1", IntegerType), StructField("c2", IntegerType)))),
        StructField("c_s", StructType(
          Seq(StructField("c1", ShortType), StructField("c2", ShortType)))),
        StructField("c_b", StructType(
          Seq(StructField("c1", ByteType), StructField("c2", ByteType))))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_l").cast(StructType(
          Seq(StructField("c1", YM(YM.YEAR, YM.YEAR)), StructField("c2", YM(YM.MONTH, YM.MONTH))))),
        f.col("c_i").cast(StructType(
          Seq(StructField("c1", YM(YM.YEAR, YM.YEAR)), StructField("c2", YM(YM.MONTH, YM.MONTH))))),
        f.col("c_s").cast(StructType(
          Seq(StructField("c1", YM(YM.YEAR, YM.YEAR)), StructField("c2", YM(YM.MONTH, YM.MONTH))))),
        f.col("c_b").cast(StructType(
          Seq(StructField("c1", YM(YM.YEAR, YM.YEAR)), StructField("c2", YM(YM.MONTH, YM.MONTH))))))
  }

  testSparkResultsAreEqual(
    "test cast array(integral) to array(year-month)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(
          Seq(i.toLong, i.toLong),
          Seq(i, i),
          Seq(i.toShort, i.toShort),
          Seq(i.toByte, i.toByte))
      }
      val schema = StructType(Seq(
        StructField("c_l", ArrayType(LongType)),
        StructField("c_i", ArrayType(IntegerType)),
        StructField("c_s", ArrayType(ShortType)),
        StructField("c_b", ArrayType(ByteType))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_l").cast(ArrayType(YM(YM.YEAR, YM.YEAR))),
        f.col("c_i").cast(ArrayType(YM(YM.MONTH, YM.MONTH))),
        f.col("c_s").cast(ArrayType(YM(YM.YEAR, YM.YEAR))),
        f.col("c_b").cast(ArrayType(YM(YM.MONTH, YM.MONTH))))
  }

  testSparkResultsAreEqual(
    "test cast map(integral, integral) to map(year-month, year-month)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(
          Map((i.toLong, i.toLong)),
          Map((i, i)),
          Map((i.toShort, i.toShort)),
          Map((i.toByte, i.toByte)))
      }
      val schema = StructType(Seq(
        StructField("c_l", MapType(LongType, LongType)),
        StructField("c_i", MapType(IntegerType, IntegerType)),
        StructField("c_s", MapType(ShortType, ShortType)),
        StructField("c_b", MapType(ByteType, ByteType))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_l").cast(MapType(YM(YM.YEAR, YM.YEAR), YM(YM.YEAR, YM.YEAR))),
        f.col("c_i").cast(MapType(YM(YM.MONTH, YM.MONTH), YM(YM.MONTH, YM.MONTH))),
        f.col("c_s").cast(MapType(YM(YM.YEAR, YM.YEAR), YM(YM.YEAR, YM.YEAR))),
        f.col("c_b").cast(MapType(YM(YM.MONTH, YM.MONTH), YM(YM.MONTH, YM.MONTH))))
  }

  testSparkResultsAreEqual(
    "test cast struct(year-month, year-month) to struct(integral, integral)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(Row(Period.ofMonths(i), Period.ofMonths(i)))
      }
      val schema = StructType(Seq(StructField("c_ym", StructType(Seq(
        StructField("c1", YM(YM.MONTH, YM.MONTH)),
        StructField("c2", YM(YM.MONTH, YM.MONTH)))))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_ym").cast(StructType(
          Seq(StructField("c1", LongType), StructField("c2", LongType)))),
        f.col("c_ym").cast(StructType(
          Seq(StructField("c1", IntegerType), StructField("c2", IntegerType)))),
        f.col("c_ym").cast(StructType(
          Seq(StructField("c1", ShortType), StructField("c2", ShortType)))),
        f.col("c_ym").cast(StructType(
          Seq(StructField("c1", ByteType), StructField("c2", ByteType)))))
  }

  testSparkResultsAreEqual(
    "test cast array(year-month) to array(integral)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(Seq(Period.ofMonths(i), Period.ofMonths(i)))
      }
      val schema = StructType(Seq(StructField("c_ym", ArrayType(YM(YM.MONTH, YM.MONTH)))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_ym").cast(ArrayType(LongType)),
        f.col("c_ym").cast(ArrayType(IntegerType)),
        f.col("c_ym").cast(ArrayType(ShortType)),
        f.col("c_ym").cast(ArrayType(ByteType)))
  }

  testSparkResultsAreEqual(
    "test cast map(year-month, year-month) to map(integral, integral)",
    spark => {
      val data = (-128 to 127).map { i =>
        Row(Map((Period.ofMonths(i), Period.ofMonths(i))))
      }
      val schema = StructType(Seq(StructField("c_ym",
        MapType(YM(YM.MONTH, YM.MONTH), YM(YM.MONTH, YM.MONTH)))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.select(
        f.col("c_ym").cast(MapType(LongType, LongType)),
        f.col("c_ym").cast(MapType(IntegerType, IntegerType)),
        f.col("c_ym").cast(MapType(ShortType, ShortType)),
        f.col("c_ym").cast(MapType(ByteType, ByteType)))
  }

  testSparkResultsAreEqual(
    "test cast(ym as (byte or short)) side effect",
    spark => {
      val data = (-128 to 127).map { i =>
        val boolean = if (i % 2 == 0) true else false
        val sideEffectValue = Period.ofMonths(Int.MaxValue)
        val ymValue = if (boolean) sideEffectValue else Period.ofMonths(i)
        Row(boolean, ymValue)
      }
      val schema = StructType(Seq(StructField("c_b", BooleanType),
        StructField("c_ym", YM(YM.MONTH, YM.MONTH))))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      // when `c_b` is true, the `c_ym` is Period.ofMonths(Int.MaxValue)
      // cast(Period.ofMonths(Int.MaxValue) as byte) will overflow
      df.selectExpr("if(c_b, cast(0 as byte), cast(c_ym as byte))",
        "if(c_b, cast(0 as short), cast(c_ym as short))")
  }

  testSparkResultsAreEqual(
    "test cast((long or int) as year-month) side effect",
    spark => {
      val data = (-128 to 127).map { i =>
        val boolean = if (i % 2 == 0) true else false
        val sideEffectLongValue = Long.MaxValue
        val sideEffectIntValue = Int.MaxValue
        Row(boolean,
          if (boolean) sideEffectLongValue else i.toLong,
          if (boolean) sideEffectIntValue else i
        )
      }
      val schema = StructType(Seq(StructField("c_b", BooleanType),
        StructField("c_l", LongType),
        StructField("c_i", IntegerType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr("if(c_b, interval 0 month, cast(c_l as interval month))",
        "if(c_b, interval 0 year, cast(c_i as interval year))")
  }
}
