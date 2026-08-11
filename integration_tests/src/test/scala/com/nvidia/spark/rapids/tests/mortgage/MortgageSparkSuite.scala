/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.mortgage

import com.nvidia.spark.rapids.ShimLoader
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MortgageSparkSuite extends AnyFunSuite {

  /**
   * This is intentionally a def rather than a val so that scalatest uses the correct value (from
   * this class or the derived class) when registering tests.
   */
  def adaptiveQueryEnabled = false

  lazy val  session: SparkSession = {
    var builder = SparkSession.builder
      .master("local[2]")
      .appName("MortgageTests")
      .config("spark.sql.adaptive.enabled", adaptiveQueryEnabled)
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.explain", "ALL")
      .config("spark.rapids.sql.test.enabled", false)
      .config("spark.rapids.sql.incompatibleOps.enabled", true)
    val rapidsShuffle = ShimLoader.getRapidsShuffleManagerClass
    val prop = System.getProperty("rapids.shuffle.manager.override", "false")
    if (prop.equalsIgnoreCase("true")) {
      println("RAPIDS SHUFFLE MANAGER ACTIVE")
      // configure the cache-only shuffle manager (disable transport)
      builder = builder
          .config("spark.shuffle.manager", rapidsShuffle)
          .config("spark.rapids.shuffle.mode", "CACHE_ONLY")
    } else {
      println("RAPIDS SHUFFLE MANAGER INACTIVE")
    }
    builder.getOrCreate()
  }

  test("extract mortgage data") {
    val df = Run.csv(
      session,
      getClass.getClassLoader.getResource("Performance_2007Q3.txt_0").getPath,
      getClass.getClassLoader.getResource("Acquisition_2007Q3.txt").getPath
    ).sort(col("loan_id"), col("monthly_reporting_period"))

    assert(df.count() === 10000)
  }

  test("convert data to parquet") {
    ReadPerformanceCsv(session,
      getClass.getClassLoader.getResource("Performance_2007Q3.txt_0").getPath)
      .write.mode("overwrite").parquet("target/test_output/perf")

    ReadAcquisitionCsv(session,
      getClass.getClassLoader.getResource("Acquisition_2007Q3.txt").getPath)
      .write.mode("overwrite").parquet("target/test_output/acq")
  }

  test("run on parquet data") {
    val df = Run.parquet(
      session,
      getClass.getClassLoader.getResource("parquet_perf").getPath,
      getClass.getClassLoader.getResource("parquet_acq").getPath
    ).sort(col("loan_id"), col("monthly_reporting_period"))

    assert(df.count() === 10000)
  }

  test("compute some basic aggregates") {
    val df = SimpleAggregates.csv(
      session,
      getClass.getClassLoader.getResource("Performance_2007Q3.txt_0").getPath,
      getClass.getClassLoader.getResource("Acquisition_2007Q3.txt").getPath
    )

    assert(df.count() === 1660)
  }

  test("compute aggregates with percentiles") {
    val df = AggregatesWithPercentiles.csv(
      session,
      getClass.getClassLoader.getResource("Performance_2007Q3.txt_0").getPath
    )

    assert(df.count() === 177)
  }

  test("compute aggregates with joins") {
    val df = AggregatesWithJoin.csv(
      session,
      getClass.getClassLoader.getResource("Performance_2007Q3.txt_0").getPath,
      getClass.getClassLoader.getResource("Acquisition_2007Q3.txt").getPath
    )

    assert(df.count() === 177)
  }
}

