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

package com.nvidia.spark.rapids.tests

import com.nvidia.spark.rapids.FuzzerUtils._
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types.{ArrayType, ByteType, CalendarIntervalType, DataType, IntegerType, LongType, MapType, NullType, StringType}

class CacheTestSuite extends SparkQueryCompareTestSuite {
  type DfGenerator = SparkSession => DataFrame

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

/** Helper functions  */

  def testCache(f: SparkSession => DataFrame): Unit = {
    val df = withCpuSparkSession(f)
    val regularValues = df.selectExpr("*").collect()
    val cachedValues = df.selectExpr("*").cache().collect()
    compare(regularValues, cachedValues)
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
