/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class CudfComparisonTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("UDF vs. RapidsUDF Comparison Test")
      .master("local[4]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.memory.gpu.pool", "NONE")
      .config("spark.rapids.sql.mode", "explainOnly")
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  def registerRapidsUDF(spark: SparkSession, udfName: String): Unit = {
    spark.udf.register(udfName, new ArraySegmentSumRapidsUDF())
  }

  test("UDF vs RapidsUDF") {
    val testDF = UnitTest.createTestData(spark).repartition(1)

    // Run CPU UDF
    UnitTest.registerUDF(spark, "array_segment_sum_cpu")
    val cpuResultDF = UnitTest.executeUDF(spark, "array_segment_sum_cpu", testDF)
    UnitTest.verifyUDFResults(cpuResultDF, testDF)

    // Run RapidsUDF
    registerRapidsUDF(spark, "array_segment_sum_gpu")
    val gpuResultDF = UnitTest.executeUDF(spark, "array_segment_sum_gpu", testDF)
    UnitTest.verifyUDFResults(gpuResultDF, testDF)
    TestUtils.assertDataFrameEquals(actual = gpuResultDF, expected = cpuResultDF)
  }
}
