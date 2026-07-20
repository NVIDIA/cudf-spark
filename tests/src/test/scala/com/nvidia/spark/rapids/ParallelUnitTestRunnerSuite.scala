/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.nio.file.Files

import org.apache.hadoop.fs.FileUtil
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkContextAccessor}
import org.apache.spark.sql.SparkSession

class ParallelUnitTestRunnerSuite extends AnyFunSuite {
  test("cleanup worker state stops Spark sessions and contexts") {
    val tmpDir = Files.createTempDirectory("parallel-unit-test-runner")
    val warehouseDir = tmpDir.resolve("spark-warehouse")
    val sparkConf = new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local[1]")
        .set("spark.driver.host", "localhost")
        .set("spark.sql.warehouse.dir", warehouseDir.toString)
        .set("spark.ui.enabled", "false")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    try {
      assert(!spark.sparkContext.isStopped)
      assert(SparkSession.getActiveSession.contains(spark))
      assert(SparkSession.getDefaultSession.contains(spark))

      ParallelUnitTestRunner.cleanupWorkerState(tmpDir)

      assert(spark.sparkContext.isStopped)
      assert(SparkContextAccessor.getActive.isEmpty)
      assert(SparkSession.getActiveSession.isEmpty)
      assert(SparkSession.getDefaultSession.isEmpty)
    } finally {
      if (!spark.sparkContext.isStopped) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      FileUtil.fullyDelete(tmpDir.toFile)
    }
  }

  test("cleanup worker state stops a standalone Spark context") {
    val tmpDir = Files.createTempDirectory("parallel-unit-test-runner-context")
    val sparkConf = new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local[1]")
        .set("spark.driver.host", "localhost")
        .set("spark.ui.enabled", "false")
    val sparkContext = new SparkContext(sparkConf)

    try {
      assert(SparkContextAccessor.getActive.contains(sparkContext))

      ParallelUnitTestRunner.cleanupWorkerState(tmpDir)

      assert(sparkContext.isStopped)
      assert(SparkContextAccessor.getActive.isEmpty)
    } finally {
      if (!sparkContext.isStopped) {
        sparkContext.stop()
      }
      FileUtil.fullyDelete(tmpDir.toFile)
    }
  }
}
