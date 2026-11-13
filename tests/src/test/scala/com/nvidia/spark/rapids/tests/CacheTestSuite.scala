/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite, TestUtils}
import com.nvidia.spark.rapids.FuzzerUtils._

import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types.{ArrayType, ByteType, CalendarIntervalType, DataType, IntegerType, LongType, MapType, NullType, StringType}

class CacheTestSuite extends SparkQueryCompareTestSuite {
  type DfGenerator = SparkSession => DataFrame

  private val pcbsClassName = "com.nvidia.spark.ParquetCachedBatchSerializer"

  test("interval") {
    testCache { spark: SparkSession =>
      val schema = createSchema(CalendarIntervalType)
      generateDataFrame(spark, schema)
    }
  }

  test("map(interval)") {
    testCache(getMapWithDataDF(CalendarIntervalType))
  }

  test("struct(interval)") {
    testCache(getIntervalStructDF(CalendarIntervalType))
    testCache(getIntervalStructDF1(CalendarIntervalType))
  }

  test("array(interval)") {
    testCache(getArrayDF(CalendarIntervalType))
  }

  test("array(map(integer, struct(string, byte, interval)))") {
    testCache(getDF(CalendarIntervalType))
  }

  test("array(array(map(array(long), struct(interval))))") {
    testCache(getMultiNestedDF(CalendarIntervalType))
  }

  test("null") {
    testCache { spark: SparkSession =>
      val schema = createSchema(NullType)
      generateDataFrame(spark, schema)
    }
  }

  test("array(null)") {
    testCache(getArrayDF(NullType))
  }

  test("map(null)") {
    testCache(getMapWithDataDF(NullType))
  }

  test("struct(null)") {
    testCache(getIntervalStructDF(NullType))
    testCache(getIntervalStructDF1(NullType))
  }

  test("array(map(integer, struct(string, byte, null)))") {
    testCache(getDF(NullType))
  }

  test("array(array(map(array(long), struct(null))))") {
    testCache(getMultiNestedDF(NullType))
  }

  Seq(
    "struct" ->
      "array(named_struct('s', cast(id as string)), cast(null as struct<s:string>))",
    "array" ->
      "array(array(named_struct('s', cast(id as string))), cast(null as array<struct<s:string>>))",
    "map" ->
      "array(map('s', cast(id as string)), cast(null as map<string,string>))"
  ).foreach { case (nestedType, nestedExpr) =>
    test(s"array with null $nestedType element and unsupported column") {
      testCacheWithPCBS { spark: SparkSession =>
        spark.range(2).selectExpr(
          "id",
          "make_interval(0, 0, 0, 0, 0, 0, id) as iv",
          s"$nestedExpr as a")
      }
    }
  }

/** Helper functions  */

  def testCache(f: SparkSession => DataFrame): Unit = {
    withCpuSparkSession(checkCache(_, f))
  }

  private def testCacheWithPCBS(f: SparkSession => DataFrame): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    TestUtils.clearCachedBatchSerializer()
    val spark = getBuilder()
      .master("local[1]")
      .config(RapidsConf.SQL_ENABLED.key, "false")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.sql.cache.serializer", pcbsClassName)
      .appName("CacheTestSuite with PCBS")
      .getOrCreate()
    try {
      checkCache(spark, f, Some(pcbsClassName))
    } finally {
      TrampolineUtil.cleanupAnyExistingSession()
      TestUtils.clearCachedBatchSerializer()
    }
  }

  private def checkCache(
      spark: SparkSession,
      f: SparkSession => DataFrame,
      expectedSerializer: Option[String] = None): Unit = {
    val df = f(spark)
    val regularValues = df.selectExpr("*").collect()
    val cachedDf = df.selectExpr("*").cache()
    try {
      expectedSerializer.foreach { expected =>
        val cachedData = spark.sharedState.cacheManager.lookupCachedData(cachedDf)
          .getOrElse(fail("DataFrame was not registered with the cache manager"))
        assertResult(expected)(
          cachedData.cachedRepresentation.cacheBuilder.serializer.getClass.getName)
      }
      val cachedValues = cachedDf.collect()
      assert(compare(regularValues, cachedValues), "Cached values differ from uncached values")
    } finally {
      cachedDf.unpersist()
    }
  }

  def getArrayDF(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema = createSchema(ArrayType(dataType))
      generateDataFrame(spark, schema)
  }

  def getMapWithDataDF(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema =
        createSchema(StringType, ArrayType(
          createSchema(StringType, StringType)),
          MapType(StringType, StringType),
          MapType(IntegerType, dataType))
      generateDataFrame(spark, schema)
  }

  def getIntervalStructDF(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema =
        createSchema(
          createSchema(dataType, StringType, dataType))
      generateDataFrame(spark, schema)
  }

  def getIntervalStructDF1(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema =
        createSchema(createSchema(IntegerType, IntegerType), dataType)
      generateDataFrame(spark, schema)
  }

  def getMultiNestedDF(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema =
        createSchema(ArrayType(
          createSchema(ArrayType(
            createSchema(MapType(ArrayType(LongType),
              createSchema(dataType)))))))
      generateDataFrame(spark, schema)
  }

  def getDF(dataType: DataType): DfGenerator = {
    spark: SparkSession =>
      val schema =
        createSchema(ArrayType(
          createSchema(MapType(IntegerType,
            createSchema(StringType, ByteType, dataType)))))
      generateDataFrame(spark, schema)
  }
}
