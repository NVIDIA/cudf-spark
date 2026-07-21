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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}

class GpuProjectAstJitSuite extends AnyFunSuite {
  private def reference(ordinal: Int, dataType: org.apache.spark.sql.types.DataType) =
    AttributeReference(s"c$ordinal", dataType, nullable = true)()

  private def alias(expression: GpuExpression, name: String) = GpuAlias(expression, name)()

  test("project AST JIT is disabled by default") {
    assert(!new RapidsConf(Map.empty[String, String]).isProjectAstJitEnabled)
  }

  test("project AST JIT supports non-ANSI integral add and multiply") {
    val left = reference(0, LongType)
    val right = reference(1, LongType)
    val expression = alias(
      GpuMultiply(
        GpuAdd(left, right, failOnError = false)(),
        right,
        failOnError = false)(),
      "result")

    val wrapped = GpuAstJitExpression.wrapProjectExpressions(List(expression))
    val jit = wrapped.head.asInstanceOf[GpuAlias].child.asInstanceOf[GpuAstJitExpression]
    assert(jit.child.isInstanceOf[GpuMultiply])
    assert(jit.child.find(_.isInstanceOf[GpuAstJitExpression]).isEmpty)
  }

  test("project AST JIT wraps maximal nested subtrees independently") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(
      GpuSubtract(
        GpuAdd(left, right, failOnError = false)(),
        GpuMultiply(left, right, failOnError = false)(),
        failOnError = false)(),
      "result")

    val wrapped = GpuAstJitExpression.wrapProjectExpressions(List(expression))
    val subtract = wrapped.head.asInstanceOf[GpuAlias].child.asInstanceOf[GpuSubtract]
    assert(subtract.left.asInstanceOf[GpuAstJitExpression].child.isInstanceOf[GpuAdd])
    assert(subtract.right.asInstanceOf[GpuAstJitExpression].child.isInstanceOf[GpuMultiply])
  }

  test("project AST JIT excludes ANSI and floating point arithmetic") {
    val intLeft = reference(0, IntegerType)
    val intRight = reference(1, IntegerType)
    val floatLeft = reference(0, FloatType)
    val floatRight = reference(1, FloatType)

    val ansiAdd = alias(GpuAdd(intLeft, intRight, failOnError = true)(), "ansi_sum")
    val floatMultiply = alias(
      GpuMultiply(floatLeft, floatRight, failOnError = false)(), "float_product")

    val wrapped = GpuAstJitExpression.wrapProjectExpressions(List(ansiAdd, floatMultiply))
    assert(wrapped.forall(_.find(_.isInstanceOf[GpuAstJitExpression]).isEmpty))
  }
}
