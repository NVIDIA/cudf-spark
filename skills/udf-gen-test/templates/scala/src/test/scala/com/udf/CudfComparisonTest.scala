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
      .config("spark.rapids.sql.explain", "NONE")
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  /** TODO: Register the RapidsUDF with Spark. */
  def registerRapidsUDF(spark: SparkSession, udfName: String): Unit = ???

  test("UDF vs RapidsUDF") {
    // Repartition down to 2 tasks to ensure we exercise multi-row columns.
    val testDF = UnitTest.createTestData(spark).repartition(2)

    // Run CPU UDF
    UnitTest.registerUDF(spark, "placeholder_udf_name")
    val cpuResultDF = UnitTest.executeUDF(spark, "placeholder_udf_name", testDF)
    UnitTest.assertUDFResults(cpuResultDF, testDF)

    // Run RapidsUDF
    registerRapidsUDF(spark, "placeholder_rapids_udf_name")
    val gpuResultDF = UnitTest.executeUDF(spark, "placeholder_rapids_udf_name", testDF)
    UnitTest.assertUDFResults(gpuResultDF, testDF)

    // Compare
    TestUtils.assertDataFrameEquals(actual = gpuResultDF, expected = cpuResultDF)
  }

  /**
   * TODO: If UnitTest adds extra tests beyond the main result checks, add
   * corresponding comparison tests here. Each case should run the same input
   * through the CPU UDF and the RapidsUDF, apply equivalent assertions to both
   * outputs, and compare the RapidsUDF output against the CPU output.
   */
}
