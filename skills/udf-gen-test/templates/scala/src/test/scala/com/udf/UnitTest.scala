/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

object UnitTest extends Assertions {
  /**
   * TODO: Create a test DataFrame with diverse test cases including edge cases
   * (at least 10+ cases).
   *
   * Example:
   * {{{
   *   val schema = StructType(Seq(
   *     StructField("id", IntegerType, nullable = false),
   *     StructField("credit_score", IntegerType, nullable = true)
   *   ))
   *   val testData = Seq(
   *     Row(1, 800),
   *     Row(2, 550),
   *     Row(3, null),
   *     // ...
   *   )
   *   spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
   * }}}
   */
  def createTestData(spark: SparkSession): DataFrame = ???

  /**
   * TODO: Register the UDF with Spark.
   *
   * Examples:
   * {{{
   *   spark.udf.register(udfName, new CalculateRiskUDF())   // Scala UDF
   *   spark.udf.register(udfName, new FormatPhoneUDF(), StringType)   // Java UDF
   *   spark.sql(s"CREATE TEMPORARY FUNCTION $udfName AS 'com.udf.IntegerMultiplyBy2UDF'")   // Hive UDF
   * }}}
   */
  def registerUDF(spark: SparkSession, udfName: String): Unit = ???

  /**
   * TODO: Execute the UDF on the test DataFrame and return the result.
   *
   * Example:
   * {{{
   *   testDF.createOrReplaceTempView("test_table")
   *   spark.sql(s"SELECT *, $udfName(credit_score) AS risk_level FROM test_table")
   * }}}
   */
  def executeUDF(spark: SparkSession, udfName: String, testDF: DataFrame): DataFrame = ???

  /**
   * TODO: Assert the UDF results match expectations.
   *
   * Example:
   * {{{
   *   val results = resultDF.collect().sortBy(_.getAs[Int]("id"))
   *   assert(results(0).getAs[String]("risk_level") === "LOW")
   *   assert(results(1).getAs[String]("risk_level") === "MEDIUM")
   *   assert(results(2).getAs[String]("risk_level") === "UNKNOWN")
   * }}}
   */
  def assertUDFResults(resultDF: DataFrame, testDF: DataFrame): Unit = ???
}

class UnitTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("UDF Unit Test")
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

  test("UDF produces correct results") {
    // Repartition down to 2 tasks to ensure we exercise multi-row columns.
    val testDF = UnitTest.createTestData(spark).repartition(2)

    UnitTest.registerUDF(spark, "placeholder_udf_name")
    val resultDF = UnitTest.executeUDF(spark, "placeholder_udf_name", testDF)

    UnitTest.assertUDFResults(resultDF, testDF)
  }
}
