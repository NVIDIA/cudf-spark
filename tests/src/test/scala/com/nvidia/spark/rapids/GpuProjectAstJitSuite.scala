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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuGreatest, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}

class GpuProjectAstJitSuite extends AnyFunSuite {
  private def reference(ordinal: Int, dataType: org.apache.spark.sql.types.DataType) =
    AttributeReference(s"c$ordinal", dataType, nullable = true)()

  private def alias(expression: GpuExpression, name: String) = GpuAlias(expression, name)()

  private def jitExpressions(expressions: Seq[Expression]): Seq[GpuAstJitExpression] = {
    expressions.flatMap(_.collect {
      case expression: GpuAstJitExpression => expression
    })
  }

  private def projectConf(
      tiered: Boolean = true,
      jit: Boolean = true,
      legacy: Boolean = false): SQLConf = {
    val conf = new SQLConf()
    conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, tiered.toString)
    conf.setConfString(RapidsConf.ENABLE_PROJECT_AST_JIT.key, jit.toString)
    conf.setConfString(RapidsConf.ENABLE_PROJECT_AST.key, legacy.toString)
    conf
  }

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
    assert(GpuProjectAstExpressionBase.extractTopLevel(wrapped.head).contains(jit))
  }

  test("project AST JIT only wraps a fully supported top-level expression") {
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
    assert(jitExpressions(wrapped).isEmpty)
    assert(subtract.left.isInstanceOf[GpuAdd])
    assert(subtract.right.isInstanceOf[GpuMultiply])
  }

  test("project CSE exposes a shared supported expression to JIT") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    val shared = GpuAdd(left, right, failOnError = false)()
    val expressions = Seq(
      GpuProjectAstExpression.wrap(
        alias(GpuSubtract(shared, third, failOnError = false)(), "legacy")),
      alias(GpuGreatest(Seq(shared, fourth)), "regular"))

    val tiered = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      expressions, Seq(left, right, third, fourth), projectConf())

    assertResult(2)(tiered.exprTiers.size)
    assertResult(Seq(1, 0))(tiered.exprTiers.map(jitExpressions(_).size))
    assert(jitExpressions(tiered.exprTiers.head).head.child.isInstanceOf[GpuAdd])
    assert(GpuProjectAstExpressionBase.extractTopLevel(tiered.exprTiers.last.head)
      .exists(_.isInstanceOf[GpuProjectAstExpression]))
    assert(GpuProjectAstExpressionBase.extractTopLevel(tiered.exprTiers.last(1)).isEmpty)
    val finalTierReferences = tiered.exprTiers.last.flatMap(_.collect {
      case reference: GpuBoundReference if reference.name.startsWith("tiered_input_") => reference
    })
    assertResult(2)(finalTierReferences.size)
    assertResult(1)(finalTierReferences.map(_.exprId).distinct.size)
  }

  test("project JIT takes precedence while unsupported legacy AST falls back") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val jitCandidate = GpuProjectAstExpression.wrap(
      alias(GpuAdd(left, right, failOnError = false)(), "jit"))
    val legacyCandidate = GpuProjectAstExpression.wrap(
      alias(GpuSubtract(left, right, failOnError = false)(), "legacy"))

    val tiered = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(jitCandidate, legacyCandidate), Seq(left, right), projectConf(legacy = true))
    val outputs = tiered.exprTiers.last

    assert(GpuProjectAstExpressionBase.extractTopLevel(outputs.head)
      .exists(_.isInstanceOf[GpuAstJitExpression]))
    assert(GpuProjectAstExpressionBase.extractTopLevel(outputs(1))
      .exists(_.isInstanceOf[GpuProjectAstExpression]))
  }

  test("only the project binder selects the JIT backend") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")
    val conf = projectConf()

    val generic = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), conf)
    val project = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), conf)

    assert(jitExpressions(generic.exprTiers.flatten).isEmpty)
    assertResult(1)(jitExpressions(project.exprTiers.flatten).size)
  }

  test("the generic binder preserves only the preselected JIT dataflow") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val shared = GpuAdd(left, right, failOnError = false)()
    val preselected = GpuAstJitExpression.wrapProjectExpressions(List(
      alias(shared, "jit"))).head
    val firstUse = alias(GpuSubtract(shared, left, failOnError = false)(), "first_use")
    val secondUse = alias(GpuSubtract(shared, right, failOnError = false)(), "second_use")
    val unrelated = alias(GpuMultiply(left, right, failOnError = false)(), "unrelated")

    val generic = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      Seq(preselected, firstUse, secondUse, unrelated), Seq(left, right), projectConf())

    assertResult(2)(generic.exprTiers.size)
    withClue(generic.exprTiers.mkString("\n")) {
      assertResult(Seq(1, 0))(generic.exprTiers.map(jitExpressions(_).size))
    }
    assert(jitExpressions(generic.exprTiers.head).head.child.isInstanceOf[GpuAdd])
    assert(generic.exprTiers.last.forall(
      GpuProjectAstExpressionBase.extractTopLevel(_).isEmpty))
    val sharedReferences = generic.exprTiers.last.flatMap(_.collect {
      case reference: GpuBoundReference if reference.name.startsWith("tiered_input_") =>
        reference
    })
    assertResult(3)(sharedReferences.size)
    assertResult(1)(sharedReferences.map(_.exprId).distinct.size)
  }

  test("the project binder respects a disabled JIT setting") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")

    val project = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), projectConf(jit = false))

    assert(jitExpressions(project.exprTiers.flatten).isEmpty)
  }

  test("project JIT remains available when tiered projection is disabled") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")

    val project = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), projectConf(tiered = false))

    assertResult(1)(project.exprTiers.size)
    assertResult(1)(jitExpressions(project.exprTiers.head).size)
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

  test("project AST JIT does not compile a literal or pass-through") {
    val input = reference(0, IntegerType)
    val expressions = List(
      alias(GpuLiteral(7, IntegerType), "literal"),
      GpuAlias(input, "pass_through")())

    val wrapped = GpuAstJitExpression.wrapProjectExpressions(expressions)

    assert(jitExpressions(wrapped).isEmpty)
  }
}
