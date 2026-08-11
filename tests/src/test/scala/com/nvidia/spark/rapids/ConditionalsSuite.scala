/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class ConditionalsSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set("spark.sql.ansi.enabled", "true")
    .set(RapidsConf.ENABLE_REGEXP.key, "true")

  testSparkResultsAreEqual("CASE WHEN test all branches", testData, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{1,3}\\z' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{4,6}\\z' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN first branch always true", testData2, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{1,3}\\z' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{4,6}\\z' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN second branch always true", testData2, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{4,6}\\z' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{1,3}\\z' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN else condition always true", testData2, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{4,6}\\z' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{7,9}\\z' THEN CAST(a AS INT) + 123 " +
        "ELSE CAST(a AS INT) END"))
  }

  testSparkResultsAreEqual("CASE WHEN first or second branch is true", testData3, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{1,3}\\z' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{4,6}\\z' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN with null predicate values on first branch",
    testData3, conf) { df =>
    df.withColumn("test", expr(
      "CASE " +
        "WHEN char_length(a) < 4 THEN CAST(a AS INT) " +
        "WHEN char_length(a) < 7 THEN CAST(a AS INT) + 123 " +
        "WHEN char_length(a) IS NULL THEN -999 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN with null predicate values after first branch",
      testData3, conf) { df =>
    assume(isUnicodeEnabled())
    df.withColumn("test", expr(
      "CASE " +
        "WHEN char_length(a) IS NULL THEN -999 " +
        "WHEN char_length(a) < 4 THEN CAST(a AS INT) " +
        "WHEN char_length(a) < 7 THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  private def testData(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      "123",
      "123456",
      "123456789",
      null,
      "99999999999999999999"
    ).toDF("a").repartition(2)
  }

  private def testData2(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      null,
      "123",
      "456"
    ).toDF("a").repartition(2)
  }

  private def testData3(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      null,
      "123",
      "123456"
    ).toDF("a").repartition(2)
  }

  private def isUnicodeEnabled(): Boolean = {
    Charset.defaultCharset().name() == "UTF-8"
  }
}
