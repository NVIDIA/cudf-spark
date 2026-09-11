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

import ai.rapids.cudf.{ColumnVector, DType, Table}
import ai.rapids.cudf.ast.{AstExpression, AstJitProgram, CompiledExpression}
import com.nvidia.spark.rapids.ProjectAstTestUtils.collectExpressions
import org.mockito.Mockito.{doThrow, mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression,
  NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuGreatest, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}
import org.apache.spark.util.TaskCompletionListener

class GpuProjectAstJitSuite extends AnyFunSuite {
  private def reference(ordinal: Int, dataType: org.apache.spark.sql.types.DataType) =
    AttributeReference(s"c$ordinal", dataType, nullable = true)()

  private def alias(expression: GpuExpression, name: String) = GpuAlias(expression, name)()

  private def mockCompiledChild(
      compiled: CompiledExpression,
      jit: Boolean = false): GpuExpression = {
    val child = mock(classOf[GpuExpression])
    val ast = mock(classOf[AstExpression])
    when(child.convertToAst(Int.MaxValue)).thenReturn(ast)
    if (jit) {
      when(ast.compileJit()).thenReturn(compiled)
    } else {
      when(ast.compile()).thenReturn(compiled)
    }
    child
  }

  private def mockJitExpression(compiled: CompiledExpression): GpuAstJitExpression =
    GpuAstJitExpression(mockCompiledChild(compiled, jit = true))

  private def mockTable(nullable: Boolean, otherNullable: Boolean = false): Table = {
    val referencedColumn = mock(classOf[ColumnVector])
    when(referencedColumn.getType).thenReturn(DType.INT32)
    when(referencedColumn.hasValidityVector).thenReturn(nullable)
    val otherColumn = mock(classOf[ColumnVector])
    when(otherColumn.getType).thenReturn(DType.INT32)
    when(otherColumn.hasValidityVector).thenReturn(otherNullable)
    val table = mock(classOf[Table])
    when(table.getNumberOfColumns).thenReturn(2)
    when(table.getColumn(0)).thenReturn(referencedColumn)
    when(table.getColumn(1)).thenReturn(otherColumn)
    table
  }

  private def projectConf(
      tiered: Boolean = true,
      jit: Boolean = true,
      legacy: Boolean = false,
      maxGroupOps: Int = Int.MaxValue,
      maxGroupOutputs: Int = Int.MaxValue): SQLConf = {
    val conf = new SQLConf()
    conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, tiered.toString)
    conf.setConfString(RapidsConf.ENABLE_PROJECT_AST_JIT.key, jit.toString)
    conf.setConfString(RapidsConf.ENABLE_PROJECT_AST.key, legacy.toString)
    conf.setConfString(RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OPS.key, maxGroupOps.toString)
    conf.setConfString(RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OUTPUTS.key, maxGroupOutputs.toString)
    conf
  }

  private def bindProject(
      expressions: Seq[Expression],
      input: Seq[Attribute],
      conf: SQLConf): GpuTieredProject = {
    GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      expressions, input, conf, enableAstJit = RapidsConf.ENABLE_PROJECT_AST_JIT.get(conf))
  }

  test("project AST JIT is disabled by default") {
    assert(!new RapidsConf(Map.empty[String, String]).isProjectAstJitEnabled)
  }

  test("project AST JIT group safety guards use conservative defaults") {
    val conf = new SQLConf()

    assertResult(384)(RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OPS.get(conf))
    assertResult(32)(RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OUTPUTS.get(conf))
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

  test("project wave exports a shared supported expression to JIT") {
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

    val tiered = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf())

    // after wave planning:
    // tier 0: [AST_JIT(left+right) AS t1]
    // tier 1: [AST(t1-third) AS legacy, greatest(t1, fourth) AS regular]
    val jitExpressionTiers = tiered.exprTiers.map(collectExpressions[GpuAstJitExpression])
    assertResult(Seq(1, 0))(jitExpressionTiers.map(_.size))
    assert(jitExpressionTiers.head.head.child.isInstanceOf[GpuAdd])
    assert(GpuProjectAstExpression.extractTopLevel(tiered.exprTiers.last.head).isDefined)
    assert(GpuProjectAstExpression.extractTopLevel(tiered.exprTiers.last(1)).isEmpty)
    // Final references: [t1, t1] (distinct: {t1}).
    val waveExprId = tiered.exprTiers.head.collectFirst {
      case alias: GpuAlias if GpuAstJitExpression.extractTopLevel(alias).isDefined => alias.exprId
    }.get
    val finalTierReferences = collectExpressions[GpuBoundReference](tiered.exprTiers.last)
        .filter(_.exprId == waveExprId)
    assertResult(2)(finalTierReferences.size)
    assertResult(1)(finalTierReferences.map(_.exprId).distinct.size)
  }

  test("same-wave JIT roots keep their shared subtree inside one group") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    def shared = GpuAdd(left, right, failOnError = false)()
    val expressions = Seq(
      alias(GpuMultiply(shared, third, failOnError = false)(), "first"),
      alias(GpuMultiply(shared, fourth, failOnError = false)(), "second"))

    val tiered = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf())
    val jitTiers = tiered.exprTiers.map(_.flatMap(GpuAstJitExpression.extractTopLevel))

    assertResult(1)(tiered.exprTiers.size)
    assertResult(Seq(2))(jitTiers.map(_.size))
    assert(jitTiers.head.forall(_.child.isInstanceOf[GpuMultiply]))
    assert(jitTiers.head.forall(_.child.find(_.isInstanceOf[GpuAdd]).isDefined))
    assertResult(1)(jitTiers.head.map(_.groupId).distinct.size)
  }

  test("max outputs splits one JIT wave into multiple groups") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expressions = Seq(
      alias(GpuAdd(left, right, failOnError = false)(), "sum"),
      alias(GpuMultiply(left, right, failOnError = false)(), "product"))

    val tiered = bindProject(
      expressions, Seq(left, right), projectConf(maxGroupOutputs = 1))
    val jitExpressions = tiered.exprTiers.head.flatMap(GpuAstJitExpression.extractTopLevel)

    assertResult(1)(tiered.exprTiers.size)
    assertResult(2)(jitExpressions.size)
    assertResult(2)(jitExpressions.map(_.groupId).distinct.size)
  }

  test("max operations preserves a shared subtree only when the group fits") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    def shared = GpuAdd(left, right, failOnError = false)()
    val expressions = Seq(
      alias(GpuMultiply(shared, third, failOnError = false)(), "first"),
      alias(GpuMultiply(shared, fourth, failOnError = false)(), "second"))

    val together = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf(maxGroupOps = 3))
    val split = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf(maxGroupOps = 2))
    val togetherJit = together.exprTiers.head.flatMap(GpuAstJitExpression.extractTopLevel)
    val splitJit = split.exprTiers.head.flatMap(GpuAstJitExpression.extractTopLevel)

    assertResult(1)(togetherJit.map(_.groupId).distinct.size)
    assertResult(2)(splitJit.map(_.groupId).distinct.size)
  }

  test("an individually oversized JIT root uses single-output AST JIT") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(
      GpuMultiply(
        GpuAdd(left, right, failOnError = false)(),
        right,
        failOnError = false)(),
      "result")

    val tiered = bindProject(
      Seq(expression), Seq(left, right), projectConf(maxGroupOps = 1))

    assertResult(1)(tiered.exprTiers.size)
    assertResult(1)(collectExpressions[GpuAstJitExpression](tiered.exprTiers.head).size)
  }

  test("JIT wave exports a shared subtree only for its regular consumer") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    def shared = GpuAdd(left, right, failOnError = false)()
    val expressions = Seq(
      alias(GpuMultiply(shared, third, failOnError = false)(), "jit"),
      alias(GpuGreatest(Seq(shared, fourth)), "regular"))

    val tiered = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf())
    val jitTiers = tiered.exprTiers.map(_.flatMap(GpuAstJitExpression.extractTopLevel))

    assertResult(Seq(2, 0))(jitTiers.map(_.size))
    assertResult(1)(jitTiers.head.count(_.child.isInstanceOf[GpuMultiply]))
    assertResult(1)(jitTiers.head.count(_.child.isInstanceOf[GpuAdd]))
    assertResult(2)(tiered.exprTiers.last.size)
    assert(collectExpressions[GpuGreatest](tiered.exprTiers.last).nonEmpty)
  }

  test("JIT and regular dependencies form three waves") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val third = reference(2, IntegerType)
    val fourth = reference(3, IntegerType)
    def shared = GpuAdd(left, right, failOnError = false)()
    def regular = GpuGreatest(Seq(shared, third))
    val expressions = Seq(
      alias(GpuMultiply(shared, fourth, failOnError = false)(), "early"),
      alias(regular, "regular"),
      alias(GpuMultiply(regular, fourth, failOnError = false)(), "late"))

    val tiered = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf())
    val jitTiers = tiered.exprTiers.map(_.flatMap(GpuAstJitExpression.extractTopLevel))

    assertResult(3)(tiered.exprTiers.size)
    assertResult(Seq(2, 0, 1))(jitTiers.map(_.size))
    assertResult(1)(jitTiers.head.count(_.child.isInstanceOf[GpuAdd]))
    assertResult(1)(jitTiers.head.count(_.child.isInstanceOf[GpuMultiply]))
    assert(jitTiers.last.head.child.isInstanceOf[GpuMultiply])
    assert(collectExpressions[GpuGreatest](tiered.exprTiers(1)).nonEmpty)
  }

  test("unsupported root uses maximal JIT children in an earlier wave") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(
      GpuSubtract(
        GpuAdd(left, right, failOnError = false)(),
        GpuMultiply(left, right, failOnError = false)(),
        failOnError = false)(),
      "result")

    val tiered = bindProject(
      Seq(expression), Seq(left, right), projectConf())
    val jitTiers = tiered.exprTiers.map(_.flatMap(GpuAstJitExpression.extractTopLevel))

    assertResult(2)(tiered.exprTiers.size)
    assertResult(Seq(2, 0))(jitTiers.map(_.size))
    assert(collectExpressions[GpuSubtract](tiered.exprTiers.last).nonEmpty)
  }

  test("JIT planner keeps non-deterministic arithmetic on the regular backend") {
    val expression = alias(
      GpuAdd(
        GpuMonotonicallyIncreasingID(),
        GpuLiteral(1L, LongType),
        failOnError = false)(),
      "result")

    val tiered = bindProject(
      Seq(expression), Seq.empty, projectConf())

    assertResult(1)(tiered.exprTiers.size)
    assert(collectExpressions[GpuAstJitExpression](tiered.exprTiers.head).isEmpty)
    assert(collectExpressions[GpuMonotonicallyIncreasingID](tiered.exprTiers.head).nonEmpty)
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

    val tiered = bindProject(
      expressions, Seq(left, right, third, fourth), projectConf())
    val all = GpuAstJitExpression.explainFinalSelections(tiered.exprTiers, all = true)

    assert(all.contains("TIER 0"), all)
    assert(all.contains("AST_JIT"), all)
    assert(all.contains("final backend: AST JIT"), all)
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

    val tiered = bindProject(
      Seq(jitCandidate, legacyCandidate), Seq(left, right), projectConf(legacy = true))
    val outputs = tiered.exprTiers.last

    assert(GpuAstJitExpression.extractTopLevel(outputs.head).isDefined)
    assert(GpuProjectAstExpression.extractTopLevel(outputs(1)).isDefined)
  }

  test("tiered binding only selects JIT when explicitly enabled") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")
    val conf = projectConf()

    val generic = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      Seq(expression), Seq(left, right), conf)
    val project = bindProject(
      Seq(expression), Seq(left, right), conf)

    assert(collectExpressions[GpuAstJitExpression](generic.exprTiers.flatten).isEmpty)
    assertResult(1)(collectExpressions[GpuAstJitExpression](project.exprTiers.flatten).size)
  }

  test("the Project caller respects a disabled JIT setting") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expression = alias(GpuAdd(left, right, failOnError = false)(), "result")

    val project = bindProject(
      Seq(expression), Seq(left, right), projectConf(jit = false))

    assert(collectExpressions[GpuAstJitExpression](project.exprTiers.flatten).isEmpty)
  }

  test("project JIT remains available when tiered projection is disabled") {
    val left = reference(0, IntegerType)
    val right = reference(1, IntegerType)
    val expressions = Seq(
      alias(GpuAdd(left, right, failOnError = false)(), "sum"),
      alias(GpuMultiply(left, right, failOnError = false)(), "product"))

    val project = bindProject(
      expressions, Seq(left, right), projectConf(tiered = false))

    assertResult(1)(project.exprTiers.size)
    assertResult(2)(collectExpressions[GpuAstJitExpression](project.exprTiers.head).size)
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
    assert(all.contains("final backend: AST JIT"), all)
    assert(all.contains("final backend: AST\n"), all)
    assert(all.contains("final backend: the regular GPU projection"), all)
    assertResult("")(
      GpuAstJitExpression.explainFinalSelections(Seq(Seq(jit)), all = false))

    val notOnGpu = GpuAstJitExpression.explainFinalSelections(selections, all = false)
    assert(!notOnGpu.contains("final backend: AST JIT"), notOnGpu)
    assert(notOnGpu.contains("final backend: AST\n"), notOnGpu)
    assert(notOnGpu.contains("final backend: the regular GPU projection"), notOnGpu)
  }

  test("project AST JIT keeps its compiled expression across retry") {
    val child = mock(classOf[GpuExpression])
    val ast = mock(classOf[AstExpression])
    val compiled = mock(classOf[CompiledExpression])
    when(child.convertToAst(Int.MaxValue)).thenReturn(ast)
    when(ast.compileJit()).thenReturn(compiled)
    val jit = GpuAstJitExpression(child)

    TestUtils.withMockTaskContext() {
      jit.checkpoint()
      jit.restore()
      jit.checkpoint()
      verify(ast, times(1)).compileJit()
      verify(compiled, times(0)).close()
    }
    verify(compiled, times(1)).close()
  }

  test("project AST JIT reuses a schema-compatible program") {
    val ownerCompiled = mock(classOf[CompiledExpression])
    val siblingCompiled = mock(classOf[CompiledExpression])
    val firstProgram = mock(classOf[AstJitProgram])
    val secondProgram = mock(classOf[AstJitProgram])
    val programs = Iterator(firstProgram, secondProgram)
    var compileCount = 0
    val boundReference = GpuBoundReference(0, IntegerType, nullable = true)(
      NamedExpression.newExprId, "c0")
    val owner = new GpuAstJitExpression(boundReference) {
      override protected def compileAst(ast: AstExpression): CompiledExpression = ownerCompiled

      override protected def compileJitProgram(
          table: Table,
          expressions: Array[CompiledExpression]): AstJitProgram = {
        assertResult(Seq(ownerCompiled, siblingCompiled))(expressions.toSeq)
        compileCount += 1
        programs.next()
      }
    }
    val sibling = new GpuAstJitExpression(boundReference) {
      override protected def compileAst(ast: AstExpression): CompiledExpression = siblingCompiled
    }
    val group = Seq(owner, sibling)
    val firstSchema = mockTable(nullable = false)
    val compatibleSchema = mockTable(nullable = false)
    val changedUnreferencedSchema = mockTable(nullable = false, otherNullable = true)
    val nullableSchema = mockTable(nullable = true)

    TestUtils.withMockTaskContext() {
      assert(owner.getJitProgram(group, firstSchema) eq firstProgram)
      assert(owner.getJitProgram(group, compatibleSchema) eq firstProgram)
      assert(owner.getJitProgram(group, changedUnreferencedSchema) eq firstProgram)
      assertResult(1)(compileCount)

      assert(owner.getJitProgram(group, nullableSchema) eq secondProgram)
      assertResult(2)(compileCount)
      verify(firstProgram, times(1)).close()
      verify(secondProgram, times(0)).close()
    }
    verify(secondProgram, times(1)).close()
    verify(ownerCompiled, times(1)).close()
    verify(siblingCompiled, times(1)).close()
  }

  test("project AST JIT uses singleton programs when multi-output is disabled") {
    val first = GpuAstJitExpression(mock(classOf[GpuExpression]), groupId = 1)
    val second = GpuAstJitExpression(mock(classOf[GpuExpression]), groupId = 1)
    val third = GpuAstJitExpression(mock(classOf[GpuExpression]), groupId = 2)
    val expressions = Seq(first, second, third)

    val multiOutputGroups = GpuAstJitExpression.executionGroups(
      expressions, multiOutputEnabled = true)
    assertResult(Seq(Seq(first, second), Seq(third)))(multiOutputGroups)

    val singletonGroups = GpuAstJitExpression.executionGroups(
      expressions, multiOutputEnabled = false)
    assertResult(Seq(Seq(first), Seq(second), Seq(third)))(singletonGroups)
  }

  test("project AST JIT reuses a singleton program") {
    val compiled = mock(classOf[CompiledExpression])
    val program = mock(classOf[AstJitProgram])
    val boundReference = GpuBoundReference(0, IntegerType, nullable = true)(
      NamedExpression.newExprId, "c0")
    val jit = new GpuAstJitExpression(boundReference) {
      override protected def compileAst(ast: AstExpression): CompiledExpression = compiled

      override protected def compileJitProgram(
          table: Table,
          expressions: Array[CompiledExpression]): AstJitProgram = {
        assertResult(Seq(compiled))(expressions.toSeq)
        program
      }
    }

    TestUtils.withMockTaskContext() {
      assert(jit.getJitProgram(Seq(jit), mockTable(nullable = false)) eq program)
      assert(jit.getJitProgram(Seq(jit), mockTable(nullable = false)) eq program)
      verify(program, times(0)).close()
    }
    verify(program, times(1)).close()
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
      assertResult("Task completed while registering compiled expression cleanup callback")(
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
      assertResult("Task completed while registering compiled expression cleanup callback")(
        thrown.getMessage)
      astExpression.close()
      verify(compiled, times(1)).close()
    }
  }
}
