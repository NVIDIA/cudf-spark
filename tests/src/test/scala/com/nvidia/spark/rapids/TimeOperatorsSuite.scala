/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.lang.RuntimeException

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

class TimeOperatorsSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Test from_unixtime", datesPostEpochDf) {
    frame => frame.select(from_unixtime(col("dates")))
  }

  testSparkResultsAreEqual("Test from_unixtime with pattern dd/mm/yyyy", datesPostEpochDf) {
    frame => frame.select(from_unixtime(col("dates"),"dd/MM/yyyy"))
  }

  testSparkResultsAreEqual(
      "Test from_unixtime with alternative month and two digit year", datesPostEpochDf,
      conf = new SparkConf().set(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key, "true")) {
    frame => frame.select(from_unixtime(col("dates"),"dd/LL/yy HH:mm:ss.SSSSSS"))
  }

  // some cases for timestamp_seconds not covered by integration tests
  testSparkResultsAreEqual(
      "Test timestamp_seconds from Large Double type", doubleTimestampSecondsDf) {
    frame => frame.select(timestamp_seconds(col("doubles")))
  }

  testSparkResultsAreEqual(
      "Test timestamp_seconds from Large Decimal type", decimalTimestampSecondsDf) {
    frame => frame.select(timestamp_seconds(col("decimals")))
  }

  testSparkResultsAreEqual(
      "Test timestamp_seconds from large Long type", longTimestampSecondsDf) {
    frame => frame.select(timestamp_seconds(col("longs")))
  }

  testSparkResultsAreEqual(
      "Test timestamp_millis from large Long type", longTimestampMillisDf) {
    frame => frame.selectExpr("timestamp_millis(longs)")
  }

  testSparkResultsAreEqual(
      "Test timestamp_micros from large Long type", longTimestampMicrosDf) {
    frame => frame.selectExpr("timestamp_micros(longs)")
  }

  testBothCpuGpuExpectedException[RuntimeException](
    "Test timestamp_micros from long near Long.minValue: long overflow",
    e => if (isSpark400OrLater) {
           e.getMessage.contains("EXPRESSION_DECODING_FAILED")
         } else {
           e.getMessage.contains("ArithmeticException")
         },
    longTimestampMicrosLongOverflowDf) {
    frame => frame.selectExpr("timestamp_micros(longs)")
  }
}
