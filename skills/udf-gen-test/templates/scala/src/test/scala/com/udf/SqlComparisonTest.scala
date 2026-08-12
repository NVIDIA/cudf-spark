/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SqlComparisonTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("UDF vs. SQL Comparison Test")
      .master("local[4]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.skipGpuArchitectureCheck", "true")
      .config("spark.rapids.sql.mode", "explainOnly")
      .config("spark.sql.adaptive.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("UDF vs SQL expression") {
    // Repartition down to 2 tasks to ensure we exercise multi-row columns.
    val testDF = UnitTest.createTestData(spark).repartition(2)

    // Run CPU UDF
    UnitTest.registerUDF(spark, "placeholder_udf_name")
    val udfResultDF = UnitTest.executeUDF(spark, "placeholder_udf_name", testDF)
    UnitTest.assertUDFResults(udfResultDF, testDF)

    // Read and execute SQL expression
    testDF.createOrReplaceTempView("test_table")
    val sqlSource = scala.io.Source.fromFile("src/main/resources/placeholder_udf_name.sql")
    val sqlContent = try sqlSource.mkString finally sqlSource.close()
    val sqlResultDF = spark.sql(sqlContent)
    UnitTest.assertUDFResults(sqlResultDF, testDF)

    // Compare results
    TestUtils.assertDataFrameEquals(actual = sqlResultDF, expected = udfResultDF)

    // Verify GPU compatibility
    SparkUtils.assertPlanRunsOnGpu(sqlResultDF)
  }

  /**
   * TODO: If UnitTest adds extra tests beyond the main result checks, add
   * corresponding comparison tests here. Each case should run the same input
   * through the CPU UDF and the SQL expression, apply equivalent assertions to
   * both outputs, and compare the SQL output against the CPU output.
   */
}
