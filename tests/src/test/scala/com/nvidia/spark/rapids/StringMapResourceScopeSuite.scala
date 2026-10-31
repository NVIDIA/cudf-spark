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

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.types._

class StringMapResourceScopeSuite extends SparkQueryCompareTestSuite {
  private def checkProjection(
      name: String,
      schema: StructType,
      rows: Seq[Row],
      expressions: Seq[String],
      conf: SparkConf = new SparkConf()): Unit = {
    testSparkResultsAreEqualWithCapture(name,
      spark => spark.createDataFrame(rows.asJava, schema),
      conf = conf.set("spark.sql.session.timeZone", "UTC")) {
      _.selectExpr(expressions: _*)
    } { (_, gpuPlan) =>
      ExecutionPlanCaptureCallback.assertContains(gpuPlan, classOf[GpuProjectExec].getName)
      if (expressions.exists(_.startsWith("regexp_extract_all("))) {
        ExecutionPlanCaptureCallback.assertContains(gpuPlan, "GpuRegExpExtractAll")
      }
      if (expressions.exists(_.startsWith("str_to_map("))) {
        ExecutionPlanCaptureCallback.assertContains(gpuPlan, "GpuStringToMap")
      }
      if (expressions.exists(_.startsWith("transform_keys("))) {
        ExecutionPlanCaptureCallback.assertContains(gpuPlan, "GpuTransformKeys")
      }
    }
  }

  private val mapSchema = new StructType().add("a", MapType(IntegerType, IntegerType))
  private val mapRows = Seq(Row(Map(1 -> 2, 3 -> null)), Row(Map.empty[Int, Int]), Row(null))

  Seq(Seq(Row("a"), Row("b"), Row("ab"), Row(""), Row(null)),
      Seq(Row("ccc"), Row(""), Row(null))).zipWithIndex.foreach { case (rows, index) =>
    checkProjection(s"regex captures preserve empty and null lists: $index",
      new StructType().add("a", StringType), rows,
      Seq("regexp_extract_all(a, '(a)|(b)', 2)"),
      new SparkConf().set("spark.rapids.sql.regexp.enabled", "true"))
  }

  checkProjection("string map values survive closing the split view",
    new StructType().add("a", StringType),
    Seq(Row("a:1,b:2"), Row("a"), Row(""), Row(null)),
    Seq("str_to_map(a, ',', ':')", "str_to_map(a, '[,]', '[:]')"),
    new SparkConf().set("spark.rapids.sql.regexp.enabled", "true"))

  checkProjection("string map entries without value delimiters retain null values",
    new StructType().add("a", StringType),
    Seq(Row("a"), Row("b"), Row(""), Row(null)),
    Seq("str_to_map(a, ',', ':')"))

  Seq("EXCEPTION", "LAST_WIN").foreach { policy =>
    checkProjection(s"transformed map keys own their deduplicated data: $policy",
      mapSchema, mapRows,
      Seq("transform_keys(a, (k, v) -> k + 1)"),
      new SparkConf().set("spark.sql.mapKeyDedupPolicy", policy))
  }

  checkProjection("transformed map duplicate keys use the last value",
    mapSchema, mapRows,
    Seq("transform_keys(a, (k, v) -> 1)"),
    new SparkConf().set("spark.sql.mapKeyDedupPolicy", "LAST_WIN"))

  testBothCpuGpuExpectedException[Exception]("transformed map keys retain duplicate-key errors",
    _.getMessage.contains("Duplicate map key"),
    spark => spark.createDataFrame(mapRows.asJava, mapSchema),
    conf = new SparkConf().set("spark.sql.mapKeyDedupPolicy", "EXCEPTION")) {
    _.selectExpr("transform_keys(a, (k, v) -> 1)")
  }

  testBothCpuGpuExpectedException[Exception]("transformed map keys reject null keys",
    _.getMessage.contains("Cannot use null as map key"),
    spark => spark.createDataFrame(mapRows.asJava, mapSchema)) {
    _.selectExpr("transform_keys(a, (k, v) -> cast(null as int))")
  }
}
