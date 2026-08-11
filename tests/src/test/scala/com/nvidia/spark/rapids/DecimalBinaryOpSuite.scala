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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{avg, sum}
import org.apache.spark.sql.rapids.{GpuEqualTo, GpuGreaterThan, GpuGreaterThanOrEqual, GpuLessThan, GpuLessThanOrEqual}
import org.apache.spark.sql.types.{DataTypes, Decimal, DecimalType}

class DecimalBinaryOpSuite extends GpuExpressionTestSuite {
  private val schema = FuzzerUtils.createSchema(Seq(
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 3),
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 2)))
  private val litValue = Decimal(12345.678)
  private val lit = GpuLiteral(litValue, DecimalType(DType.DECIMAL32_MAX_PRECISION, 3))
  private val leftExpr =
    GpuBoundReference(0, schema.head.dataType, nullable = true)(NamedExpression.newExprId, "left")
  private val rightExpr =
    GpuBoundReference(1, schema(1).dataType, nullable = true)(NamedExpression.newExprId, "right")

  private val schemaSame = FuzzerUtils.createSchema(Seq(
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 3),
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 3)))
  private val leftExprSame = GpuBoundReference(0, schemaSame.head.dataType,
    nullable = true)(NamedExpression.newExprId, "left")
  private val rightExprSame = GpuBoundReference(1, schemaSame(1).dataType,
    nullable = true)(NamedExpression.newExprId, "right")

  test("GpuEqualTo") {
    val expectedFun = (l: Decimal, r: Decimal) => Option(l == r)
    checkEvaluateGpuBinaryExpression(GpuEqualTo(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFun, schema)
    checkEvaluateGpuBinaryExpression(GpuEqualTo(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFun, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x == litValue)
    checkEvaluateGpuUnaryExpression(GpuEqualTo(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue == x)
    checkEvaluateGpuUnaryExpression(GpuEqualTo(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuGreaterThan") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l > r)
    checkEvaluateGpuBinaryExpression(GpuGreaterThan(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)

    val expectedFunVS = (x: Decimal) => Option(x > litValue)
    checkEvaluateGpuUnaryExpression(GpuGreaterThan(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue > x)
    checkEvaluateGpuUnaryExpression(GpuGreaterThan(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuGreaterThanOrEqual") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l >= r)
    checkEvaluateGpuBinaryExpression(GpuGreaterThanOrEqual(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)
    checkEvaluateGpuBinaryExpression(GpuGreaterThanOrEqual(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x >= litValue)
    checkEvaluateGpuUnaryExpression(GpuGreaterThanOrEqual(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue >= x)
    checkEvaluateGpuUnaryExpression(GpuGreaterThanOrEqual(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuLessThan") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l < r)
    checkEvaluateGpuBinaryExpression(GpuLessThan(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)

    val expectedFunVS = (x: Decimal) => Option(x < litValue)
    checkEvaluateGpuUnaryExpression(GpuLessThan(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue < x)
    checkEvaluateGpuUnaryExpression(GpuLessThan(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuLessThanOrEqual") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l <= r)
    checkEvaluateGpuBinaryExpression(GpuLessThanOrEqual(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)
    checkEvaluateGpuBinaryExpression(GpuLessThanOrEqual(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x <= litValue)
    checkEvaluateGpuUnaryExpression(GpuLessThanOrEqual(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue <= x)
    checkEvaluateGpuUnaryExpression(GpuLessThanOrEqual(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  // https://github.com/NVIDIA/spark-rapids/issues/6076
  testSparkResultsAreEqual("SPARK-24957: average with decimal followed by " +
      "aggregation returning wrong result", decimals,
      assumeCondition = ignoreAnsi("https://github.com/NVIDIA/spark-rapids/issues/5114")) {
    df => df.groupBy("text").agg(avg("number").as("avg_res"))
        .groupBy("text").agg(sum("avg_res"))
  }
}