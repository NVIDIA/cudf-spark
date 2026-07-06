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

import java.io.File
import java.time.{Duration, Period}

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Can not put this suite to cache_test.py because of currently Pyspark not have year-month type.
 * See: https://github.com/apache/spark/blob/branch-3.3/python/pyspark/sql/types.py
 */
class IntervalSuite extends SparkQueryCompareTestSuite {

  /**
   * Set environment variable: "spark.sql.cache.serializer"
   * "spark.sql.cache.serializer" is a static SQL configuration, can NOT set/unset it.
   * Should specify it as environment variable when running the MVN, e.g:
   * SPARK_CONF="spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer" mvn test
   * This method is a hack code to set environment variable in the test code. it's a workaround.
   * Should move this test suite to cache_test.py after Pyspark supports year-month type
   */
  private def setPCBS() = {
    val env = System.getenv
    // use reflect to set environment variable
    val field = System.getenv.getClass.getDeclaredField("m")
    field.setAccessible(true)
    val envMap = field.get(env).asInstanceOf[java.util.Map[String, String]]
    envMap.put("SPARK_CONF",
      "spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer")
  }

  private def getDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Period is the external type of year-month type
    // Duration is the external type of day-time type
    Seq((1, Period.ofYears(1).plusMonths(1), Duration.ofDays(1).plusSeconds(1)),
      (2, Period.ofYears(2).plusMonths(2), Duration.ofDays(2).plusSeconds(2)))
        .toDF("c_interval", "c_year_month", "c_day_type")
  }

  test("test CPU cache interval by PCBS") {
    setPCBS()
    withCpuSparkSession(spark => {
      getDF(spark).cache().count()
    })
  }

  test("test GPU cache interval by PCBS") {
    setPCBS()
    withGpuSparkSession(spark => {
      getDF(spark).cache().count()
    })
  }

  test("test GPU cache interval when reading/writing parquet") {
    setPCBS()

    val tempFile = File.createTempFile("pcbs", ".parquet")
    try {
      withGpuSparkSession(spark => {
        // invoke convertInternalRowToCachedBatch
        getDF(spark).coalesce(1).write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      })

      withGpuSparkSession(spark => {
        val df = spark.read.parquet(tempFile.getAbsolutePath)
        // invoke convertColumnarBatchToCachedBatch
        df.cache().count()
      })
    } finally {
      tempFile.delete()
    }
  }

  // CPU write a parquet, then test the reading between CPU and GPU
  test("test ANSI interval read") {
    val tmpFile = File.createTempFile("interval", ".parquet")
    try {
      withCpuSparkSession(spark => getDF(spark).coalesce(1)
          .write.mode("overwrite").parquet(tmpFile.getAbsolutePath))
      val c = withCpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath)
          .collect())
      val g = withGpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath)
          .collect())
      assert(compare(g, c))
    } finally {
      tmpFile.delete()
    }
  }

  // GPU write a parquet, then test the reading between CPU and GPU
  test("test ANSI interval write") {
    val tmpFile = File.createTempFile("interval", ".parquet")
    try {
      withGpuSparkSession(spark => getDF(spark).coalesce(1)
          .write.mode("overwrite").parquet(tmpFile.getAbsolutePath))
      val c = withCpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      val g = withGpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      assert(compare(g, c))
    } finally {
      tmpFile.delete()
    }
  }

  // Write 2 files with CPU and GPU separately, then read the 2 files with CPU and compare
  testSparkWritesAreEqual(
    "test ANSI interval read/write",
    getDF,
    (df, path) => df.coalesce(1).write.mode("overwrite").parquet(path),
    (spark, path) => spark.read.parquet(path)
  )
}
