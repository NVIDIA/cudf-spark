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

import ai.rapids.cudf.Table
import ai.rapids.cudf.ast.{AstExpression, CompiledExpression}
import com.nvidia.spark.rapids.ProjectAstTestUtils.{collectExpressions, tierReferences}
import org.mockito.Mockito.{doThrow, mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuGreatest, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}
import org.apache.spark.util.TaskCompletionListener

class GpuProjectAstJitSuite extends AnyFunSuite {
  private def reference(ordinal: Int, dataType: org.apache.spark.sql.types.DataType) =
    AttributeReference(s"c$ordinal", dataType, nullable = true)()

  private def alias(expression: GpuExpression, name: String) = GpuAlias(expression, name)()

  private def mockCompiledChild(compiled: CompiledExpression): GpuExpression = {
    val child = mock(classOf[GpuExpression])
    val ast = mock(classOf[AstExpression])
    when(child.convertToAst(Int.MaxValue)).thenReturn(ast)
    when(ast.compile()).thenReturn(compiled)
    child
  }

  private def mockJitExpression(compiled: CompiledExpression): GpuAstJitExpression =
    GpuAstJitExpression(mockCompiledChild(compiled))

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
    val wrappedAgain = GpuAstJitExpression.wrapProjectExpressions(wrapped)
    assert(jit.child.isInstanceOf[GpuMultiply])
    assert(jit.child.find(_.isInstanceOf[GpuAstJitExpression]).isEmpty)
    assert(GpuAstJitExpression.extractTopLevel(wrapped.head).contains(jit))
    assert(wrappedAgain.head eq wrapped.head)
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
    assert(wrapped.forall(GpuAstJitExpression.extractTopLevel(_).isEmpty))
    assert(subtract.left.isInstanceOf[GpuAdd])
    assert(subtract.right.isInstanceOf[GpuMultiply])
  }

  test("project CSE exposes a shared supported expression to JIT") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    val shared = GpuAdd(left, right, failOnError = false)()
    // [AST((left+right)-third) AS legacy, greatest(left+right, fourth) AS regular]
    val expressions = Seq(
      GpuProjectAstExpression.wrap(
        alias(GpuSubtract(shared, third, failOnError = false)(), "legacy")),
      alias(GpuGreatest(Seq(shared, fourth)), "regular"))

    val tiered = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      expressions, Seq(left, right, third, fourth), projectConf())

    // after CSE:
    // tier 0: [AST_JIT(left+right) AS t1]
    // tier 1: [AST(t1-third) AS legacy, greatest(t1, fourth) AS regular]
    val jitExpressionTiers = tiered.exprTiers.map(collectExpressions[GpuAstJitExpression])
    assertResult(Seq(1, 0))(jitExpressionTiers.map(_.size))
    assert(jitExpressionTiers.head.head.child.isInstanceOf[GpuAdd])
    assert(GpuProjectAstExpression.extractTopLevel(tiered.exprTiers.last.head).isDefined)
    assert(GpuProjectAstExpression.extractTopLevel(tiered.exprTiers.last(1)).isEmpty)
    // Final references: [t1, t1] (distinct: {t1}).
    val finalTierReferences = tiered.exprTiers.last.flatMap(tierReferences)
    assertResult(2)(finalTierReferences.size)
    assertResult(1)(finalTierReferences.map(_.exprId).distinct.size)
  }

  test("final JIT explanation includes a shared expression selected in an earlier tier") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    val shared = GpuAdd(left, right, failOnError = false)()
    val expressions = Seq(
      alias(GpuSubtract(shared, third, failOnError = false)(), "first"),
      alias(GpuSubtract(shared, fourth, failOnError = false)(), "second"))

    val tiered = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      expressions, Seq(left, right, third, fourth), projectConf())
    val all = GpuAstJitExpression.explainFinalSelections(tiered.exprTiers, all = true)

    assert(all.contains("TIER 0"), all)
    assert(all.contains("AST_JIT"), all)
    assert(all.contains("final backend: Project AST JIT"), all)
    assert(all.contains("TIER 1"), all)
    assert(all.contains("final backend: the regular GPU projection"), all)
    assertResult("")(
      GpuAstJitExpression.explainFinalSelections(tiered.exprTiers, all = false))
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

    assert(GpuAstJitExpression.extractTopLevel(outputs.head).isDefined)
    assert(GpuProjectAstExpression.extractTopLevel(outputs(1)).isDefined)
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

    assert(collectExpressions[GpuAstJitExpression](generic.exprTiers.flatten).isEmpty)
    assertResult(1)(collectExpressions[GpuAstJitExpression](project.exprTiers.flatten).size)
  }

  test("the project binder respects a disabled JIT setting") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")

    val project = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), projectConf(jit = false))

    assert(collectExpressions[GpuAstJitExpression](project.exprTiers.flatten).isEmpty)
  }

  test("project JIT remains available when tiered projection is disabled") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")

    val project = GpuBindReferences.bindGpuProjectReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), projectConf(tiered = false))

    assertResult(1)(project.exprTiers.size)
    assertResult(1)(collectExpressions[GpuAstJitExpression](project.exprTiers.head).size)
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

    assert(wrapped.forall(GpuAstJitExpression.extractTopLevel(_).isEmpty))
  }

  test("final JIT explanation reports only unselected JIT candidates by default") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val add = alias(GpuAdd(left, right, failOnError = false)(), "jit")
    val subtract = alias(
      GpuSubtract(
        GpuAdd(left, right, failOnError = false)(),
        third,
        failOnError = false)(),
      "regular")
    val jit = GpuAstJitExpression.wrapProjectExpressions(List(add)).head
    val legacy = GpuProjectAstExpression.wrap(subtract)
    val selections = Seq(Seq(jit, legacy, subtract))

    val all = GpuAstJitExpression.explainFinalSelections(selections, all = true)
    assert(all.contains("final backend: Project AST JIT"), all)
    assert(all.contains("final backend: legacy Project AST"), all)
    assert(all.contains("final backend: the regular GPU projection"), all)
    assertResult("")(
      GpuAstJitExpression.explainFinalSelections(Seq(Seq(jit)), all = false))

    val notOnGpu = GpuAstJitExpression.explainFinalSelections(selections, all = false)
    assert(!notOnGpu.contains("final backend: Project AST JIT"), notOnGpu)
    assert(notOnGpu.contains("final backend: legacy Project AST"), notOnGpu)
    assert(notOnGpu.contains("final backend: the regular GPU projection"), notOnGpu)
  }

  test("project AST JIT keeps its compiled expression across retry") {
    val child = mock(classOf[GpuExpression])
    val ast = mock(classOf[AstExpression])
    val compiled = mock(classOf[CompiledExpression])
    when(child.convertToAst(Int.MaxValue)).thenReturn(ast)
    when(ast.compile()).thenReturn(compiled)
    val jit = GpuAstJitExpression(child)

    TestUtils.withMockTaskContext() {
      jit.checkpoint()
      jit.restore()
      jit.checkpoint()
      verify(ast, times(1)).compile()
      verify(compiled, times(0)).close()
    }
    verify(compiled, times(1)).close()
  }

  test("project AST JIT cleans up when task completion registration fails") {
    val registrationFailure = new RuntimeException("task completion registration failed")
    val closeFailure = new RuntimeException("compiled expression close failed")
    val taskContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0) {
      override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext =
        throw registrationFailure
    }
    val compiled = mock(classOf[CompiledExpression])
    doThrow(closeFailure).when(compiled).close()
    val jit = mockJitExpression(compiled)

    TestUtils.withTaskContext(taskContext) {
      val thrown = intercept[RuntimeException] {
        jit.checkpoint()
      }
      assert(thrown eq registrationFailure)
      assertResult(Seq(closeFailure))(thrown.getSuppressed.toSeq)
      jit.close()
      verify(compiled, times(1)).close()
    }
  }

  test("project AST JIT rejects a cleanup callback consumed during registration") {
    val taskContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0) {
      override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
        listener.onTaskCompletion(this)
        this
      }
    }
    val compiled = mock(classOf[CompiledExpression])
    val jit = mockJitExpression(compiled)

    TestUtils.withTaskContext(taskContext) {
      val thrown = intercept[IllegalStateException] {
        jit.checkpoint()
      }
      assertResult("Task completed while registering the AST JIT cleanup callback")(
        thrown.getMessage)
      jit.close()
      verify(compiled, times(1)).close()
    }
  }

  test("legacy project AST rejects a cleanup callback consumed during registration") {
    val taskContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0) {
      override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
        listener.onTaskCompletion(this)
        this
      }
    }
    val compiled = mock(classOf[CompiledExpression])
    val astExpression = GpuProjectAstExpression(mockCompiledChild(compiled))

    TestUtils.withTaskContext(taskContext) {
      val thrown = intercept[IllegalStateException] {
        astExpression.computeColumn(mock(classOf[Table]))
      }
      assertResult("Task completed while registering the Project AST cleanup callback")(
        thrown.getMessage)
      astExpression.close()
      verify(compiled, times(1)).close()
    }
  }
}
