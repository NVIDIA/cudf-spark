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
{"spark":["330","350"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.time.Period

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Can not put this suite to Pyspark test cases
 * because of currently Pyspark not have year-month type.
 * See: https://github.com/apache/spark/blob/branch-3.3/python/pyspark/sql/types.py
 * Should move the year-month scala test cases to the integration test module,
 * filed an issue to track: https://github.com/NVIDIA/spark-rapids/issues/5212
 */
class SampleSuite extends SparkQueryCompareTestSuite {
  private def getDfForNoOverflow(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq((0 until 1024).map(i => Period.ofYears(i))).toDF("c1")
  }

  testSparkResultsAreEqual("test sample day-time",
    spark => getDfForNoOverflow(spark)) {
    df => df.sample(0.5, 1)
  }

  testSparkResultsAreEqual("test sample day-time with replacement",
    spark => getDfForNoOverflow(spark)) {
    df => df.sample(true, 0.5, 1)
  }
}
