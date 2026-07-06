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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class TimestampSuite extends SparkQueryCompareTestSuite {
  val ansiConf = new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")

  private def getOverflowLong(spark: SparkSession): DataFrame = {
    val data = Seq(Row((Long.MaxValue / 1000000) + 1), Row(-(Long.MaxValue / 1000000) - 1))
    val schema = StructType(Array(StructField("a", LongType)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  private def getOverflowDouble(spark: SparkSession): DataFrame = {
    val data = Seq(
      Row((Long.MaxValue / 1000000 + 100).toDouble),
      Row((-(Long.MaxValue / 1000000) - 100).toDouble))
    val schema = StructType(Array(StructField("a", DoubleType)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   *
   * This test case failed on Pyspark:
   * $SPARK_HOME/bin/pyspark
   * >>> import math
   * >>> data = [(math.floor((9223372036854775807 / 1000000) + 1), )]
   * >>> df = spark.createDataFrame(data, "a long")
   * >>> df.selectExpr("cast(a as timestamp)").collect()
   * Traceback (most recent call last):
   * ......
   * ValueError: year -290308 is out of range
   *
   */
  // TODO, blocked by a follow on issue: https://github.com/NVIDIA/spark-rapids/issues/5606
  if (false) {
    testSparkResultsAreEqual("test cast overflowed long seconds to max seconds",
      spark => getOverflowLong(spark)) {
      df => df.repartition(1).selectExpr("cast(a as timestamp)")
    }

    // non ansi mode, should get SECONDS.toMicros(t) which will be truncated to
    // the max long micro seconds
    testSparkResultsAreEqual("test cast overflowed long seconds to max seconds",
      spark => getOverflowLong(spark)) {
      df => df.repartition(1).selectExpr("cast(a as timestamp)")
    }

    // ansi mode, should get SECONDS.toMicros(t) which will be truncated to
    // the max long micro seconds
    testSparkResultsAreEqual("test cast overflowed long seconds to max seconds, ansi mode",
      spark => getOverflowLong(spark),
      conf = ansiConf) {
      df => df.repartition(1).selectExpr("cast(a as timestamp)")
    }

    // non ansi mode, should get `(double * 1000000L).toLong`
    testSparkResultsAreEqual("test overflowed float to timestamp",
      spark => getOverflowDouble(spark)) {
      df => df.selectExpr("cast(a as timestamp)")
    }
  }
}
