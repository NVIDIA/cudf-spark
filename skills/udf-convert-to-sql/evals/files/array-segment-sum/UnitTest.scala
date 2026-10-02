/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

object UnitTest extends Assertions {

  private def jl(v: Long): JLong = JLong.valueOf(v)
  private def i(v: Int): JInt = JInt.valueOf(v)

  // (id, vals, start, len, expected) — expected hand-computed against the UDF contract.
  private val cases: Seq[(Int, Seq[JLong], JInt, JInt, JLong)] = Seq(
    // Bounded slice cases
    (10, Seq(jl(1), jl(2), jl(3), jl(4), jl(5)), i(1), i(2), jl(5)),
    (11, Seq(jl(1), jl(2), jl(3), jl(4), jl(5)), i(0), i(5), jl(15)),
    (12, Seq(jl(1), jl(2), jl(3), jl(4), jl(5)), i(3), i(10), jl(9)),
    (13, Seq(jl(1), jl(2), jl(3)), i(2), i(1), jl(3)),
    // Out-of-range and non-positive length cases
    (14, Seq(jl(10), jl(20), jl(30)), i(-1), i(2), jl(0)),
    (15, Seq(jl(10), jl(20), jl(30)), i(3), i(2), jl(0)),
    (16, Seq(jl(10), jl(20), jl(30)), i(5), i(2), jl(0)),
    (17, Seq(jl(10), jl(20), jl(30)), i(1), i(0), jl(0)),
    (18, Seq(jl(10), jl(20), jl(30)), i(1), i(-3), jl(0)),
    // Null elements are skipped while summing
    (19, Seq(jl(5), null, jl(7), null, jl(9)), i(0), i(5), jl(21)),
    (20, Seq(jl(5), null, jl(7)), i(1), i(1), jl(0)),
    // Zero and negative values are ordinary elements
    (21, Seq(jl(0), jl(4), jl(-6), jl(8)), i(0), i(4), jl(6)),
    (22, Seq(jl(-5), jl(0), jl(5)), i(0), i(2), jl(-5)),
    // null / empty handling
    (40, null, i(0), i(1), null),
    (41, Seq(jl(1), jl(2), jl(3)), null, i(1), null),
    (42, Seq(jl(1), jl(2), jl(3)), i(1), null, null),
    (43, null, null, null, null),
    (44, Seq.empty[JLong], i(0), i(1), jl(0)),
    (45, Seq.empty[JLong], i(3), i(2), jl(0)),
    (46, Seq.empty[JLong], i(3), null, null),
    (47, Seq(null, null, null), i(0), i(3), jl(0))
  )

  private val expectedById: Map[Int, JLong] =
    cases.map { case (id, _, _, _, exp) => id -> exp }.toMap

  def createTestData(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("vals", ArrayType(LongType, containsNull = true), nullable = true),
      StructField("start", IntegerType, nullable = true),
      StructField("len", IntegerType, nullable = true)
    ))
    val rows = cases.map { case (id, vals, start, len, _) => Row(id, vals, start, len) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  def registerUDF(spark: SparkSession, udfName: String): Unit = {
    spark.udf.register(udfName, new ArraySegmentSumUDF())
  }

  def executeUDF(spark: SparkSession, udfName: String, testDF: DataFrame): DataFrame = {
    testDF.createOrReplaceTempView("test_table")
    spark.sql(s"SELECT id, $udfName(vals, start, len) AS result FROM test_table")
  }

  def verifyUDFResults(resultDF: DataFrame, testDF: DataFrame): Unit = {
    val byId = resultDF.collect().map(r => r.getAs[Int]("id") -> r).toMap
    for ((id, exp) <- expectedById) {
      val r = byId(id)
      val actual: JLong =
        if (r.isNullAt(r.fieldIndex("result"))) null else JLong.valueOf(r.getAs[Long]("result"))
      assert(actual === exp, s"id=$id expected=$exp actual=$actual")
    }
  }
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
    val testDF = UnitTest.createTestData(spark).repartition(1)
    UnitTest.registerUDF(spark, "array_segment_sum")
    val resultDF = UnitTest.executeUDF(spark, "array_segment_sum", testDF)
    UnitTest.verifyUDFResults(resultDF, testDF)
  }
}
