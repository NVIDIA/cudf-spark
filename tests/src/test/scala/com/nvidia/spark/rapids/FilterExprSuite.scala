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

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types.Decimal

class FilterExprSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("filter with decimal literals", mixedDf(_), repart = 0) { df =>
    df.select(col("doubles"), col("decimals"),
      lit(BigDecimal(0L)).as("BigDec0"),
      lit(BigDecimal(123456789L, 6)).as("BigDec1"),
      lit(BigDecimal(-2.12314e-8)).as("BigDec2"))
      .filter(col("doubles").gt(3.0))
      .select("BigDec0", "BigDec1", "doubles", "decimals")
  }

  testSparkResultsAreEqual("filter with decimal columns", mixedDf(_), repart = 0) { df =>
    df.filter(col("ints") > 90)
      .filter(col("decimals").isNotNull)
      .select("ints", "strings", "decimals")
  }

  test("filter with In/InSet") {
    val dir = Files.createTempDirectory("spark-rapids-test").toFile
    val path = new File(dir, s"InSet-${System.currentTimeMillis()}.parquet").getAbsolutePath
    try {
      withCpuSparkSession(spark => mixedDf(spark).write.parquet(path), new SparkConf())
      val createDF = (ss: SparkSession) => ss.read.parquet(path)
      val fun = (df: DataFrame) => df
          // In
          .filter(col("strings").isin("A", "B", "C", "d"))
          .filter(col("decimals").isin(Decimal("1.2"), Decimal("2.1")))
          // outBound values will be removed by UnwrapCastInBinaryComparison
          .filter(col("ints").isin(90L, 95L, Int.MaxValue.toLong + 1))
          // InSet (spark.sql.optimizer.inSetConversionThreshold = 10 by default)
          .filter(col("longs").isin(100L to 1200L: _*))
          .filter(col("doubles").isin(1 to 15: _*))
      val conf = new SparkConf()
      val (fromCpu, fromGpu) = runOnCpuAndGpu(createDF, fun, conf, repart = 0)
      compareResults(false, 0.0, fromCpu, fromGpu)
    } finally {
      dir.delete()
    }
  }
}
