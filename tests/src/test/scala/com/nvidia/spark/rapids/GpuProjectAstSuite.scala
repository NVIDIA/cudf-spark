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

import org.mockito.Mockito.mock
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply, GpuShiftLeft}
import org.apache.spark.sql.types.{IntegerType, LongType}

class GpuProjectAstSuite extends AnyFunSuite {
  test("AST plan copy preserves JIT error sites") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val sum = GpuAlias(GpuAdd(a, b, failOnError = true)(), "sum")()
    val errorSites = List(List(AstJitErrorSite(AstJitErrorKind.Add, sum.origin)))
    val child = mock(classOf[SparkPlan])
    val replacement = mock(classOf[SparkPlan])
    val plan = GpuProjectAstExec(List(sum), child)(errorSites)

    val copied = plan.withNewChildren(Seq(replacement)).asInstanceOf[GpuProjectAstExec]

    assert(copied.child eq replacement)
    assert(copied.astJitErrorSites == errorSites)
  }

  test("AST output planning excludes pass-through expressions") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val passB = GpuAlias(b, "pass_b")()
    val sum = GpuAlias(GpuAdd(a, b, failOnError = false)(), "sum")()
    val passA = GpuAlias(a, "pass_a")()
    val product = GpuAlias(GpuMultiply(a, b, failOnError = false)(), "product")()
    val duplicateB = GpuAlias(b, "duplicate_b")()
    val expressions = Seq(passB, sum, passA, product, duplicateB)
    val sumErrorSite = AstJitErrorSite(AstJitErrorKind.Add, sum.origin)
    val productErrorSite = AstJitErrorSite(AstJitErrorKind.Multiply, product.origin)

    val plan = GpuProjectAstExec.planOutputs(
      expressions, Seq(Seq.empty, Seq(sumErrorSite), Seq.empty, Seq(productErrorSite), Seq.empty))

    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstInputColumn(0),
      GpuProjectAstExec.AstComputedColumn(1),
      GpuProjectAstExec.AstInputColumn(1)))
    assert(plan.expressionsToCompute == Seq(sum, product))
    assert(plan.errorSitesToCompute == Seq(Seq(sumErrorSite), Seq(productErrorSite)))
    assert(!plan.allPassThrough)
  }

  test("AST output planning preserves nested error sites") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val sum = GpuAdd(a, b, failOnError = true)()
    val product = GpuMultiply(sum, b, failOnError = true)()
    val nested = GpuAlias(product, "nested")()
    val errorSites = Seq(
      AstJitErrorSite(AstJitErrorKind.Multiply, product.origin),
      AstJitErrorSite(AstJitErrorKind.Add, sum.origin))

    val plan = GpuProjectAstExec.planOutputs(Seq(nested), Seq(errorSites))

    assert(plan.errorSitesToCompute == Seq(errorSites))
  }

  test("AST output planning uses JIT only for row IR outputs") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val legacySum = GpuAlias(GpuAdd(a, b, failOnError = false)(), "sum")()
    val rowIrShift = GpuAlias(GpuShiftLeft(a, GpuLiteral(1, IntegerType)), "shift")()

    val plan = GpuProjectAstExec.planOutputs(
      Seq(legacySum, rowIrShift), Seq(Seq.empty, Seq.empty))

    assert(plan.backendsToCompute == Seq(
      GpuProjectAstExec.LegacyAstBackend,
      GpuProjectAstExec.RowIrJitBackend))
  }

  test("AST output planning reuses equivalent computed expressions") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val sum = GpuAlias(GpuAdd(a, b, failOnError = false)(), "sum")()
    val product = GpuAlias(GpuMultiply(a, b, failOnError = false)(), "product")()
    val reversedSum = GpuAlias(GpuAdd(b, a, failOnError = false)(), "reversed_sum")()
    val duplicateSum = GpuAlias(GpuAdd(a, b, failOnError = false)(), "duplicate_sum")()
    val passB = GpuAlias(b, "pass_b")()
    val expressions = Seq(sum, passB, product, reversedSum, duplicateSum)

    val plan = GpuProjectAstExec.planOutputs(
      expressions, Seq.fill(expressions.size)(Seq.empty[AstJitErrorSite]))

    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstComputedColumn(1),
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstComputedColumn(0)))
    assert(plan.expressionsToCompute == Seq(sum, product))
    assert(plan.errorSitesToCompute == Seq(Seq.empty, Seq.empty))
  }

  test("AST output planning projects top-level literals without JIT") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val literal = GpuAlias(GpuLiteral(7L, LongType), "literal")()
    val duplicateLiteral = GpuAlias(GpuLiteral(7L, LongType), "duplicate_literal")()
    val nestedLiteral = GpuAlias(
      GpuAdd(a, GpuLiteral(1L, LongType), failOnError = false)(), "nested_literal")()
    val forcedErrorLiteral = GpuAlias(GpuLiteral(9L, LongType), "error_literal")()
    val errorSite = AstJitErrorSite(AstJitErrorKind.Add, forcedErrorLiteral.origin)
    val expressions = Seq(a, literal, duplicateLiteral, nestedLiteral, forcedErrorLiteral)

    val plan = GpuProjectAstExec.planOutputs(
      expressions, Seq(Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq(errorSite)))

    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstInputColumn(0),
      GpuProjectAstExec.AstLiteralColumn(0),
      GpuProjectAstExec.AstLiteralColumn(0),
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstComputedColumn(1)))
    assert(plan.literalsToProject == Seq(literal))
    assert(plan.expressionsToCompute == Seq(nestedLiteral, forcedErrorLiteral))
    assert(plan.errorSitesToCompute == Seq(Seq.empty, Seq(errorSite)))
    assert(!plan.allPassThrough)
  }

  test("AST output planning does not reuse fallible or nondeterministic expressions") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val checkedSum = GpuAlias(GpuAdd(a, b, failOnError = true)(), "checked_sum")()
    val duplicateCheckedSum =
      GpuAlias(GpuAdd(a, b, failOnError = true)(), "duplicate_checked_sum")()
    val firstErrorSite = AstJitErrorSite(AstJitErrorKind.Add, checkedSum.origin)
    val secondErrorSite = AstJitErrorSite(AstJitErrorKind.Add, duplicateCheckedSum.origin)
    val nondeterministic = GpuAlias(GpuMonotonicallyIncreasingID(), "id")()
    val duplicateNondeterministic = GpuAlias(GpuMonotonicallyIncreasingID(), "duplicate_id")()
    val expressions =
      Seq(checkedSum, duplicateCheckedSum, nondeterministic, duplicateNondeterministic)

    val plan = GpuProjectAstExec.planOutputs(
      expressions, Seq(Seq(firstErrorSite), Seq(secondErrorSite), Seq.empty, Seq.empty))

    assert(plan.outputSources == expressions.indices.map(GpuProjectAstExec.AstComputedColumn))
    assert(plan.expressionsToCompute == expressions)
    assert(plan.errorSitesToCompute ==
      Seq(Seq(firstErrorSite), Seq(secondErrorSite), Seq.empty, Seq.empty))

    val uncheckedPlan = GpuProjectAstExec.planOutputs(
      Seq(checkedSum, duplicateCheckedSum), Seq(Seq.empty, Seq.empty))
    assert(uncheckedPlan.expressionsToCompute == Seq(checkedSum, duplicateCheckedSum))
  }

  test("AST output planning rejects error sites on pass-through expressions") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val passA = GpuAlias(a, "pass_a")()
    val errorSite = AstJitErrorSite(AstJitErrorKind.Add, passA.origin)

    val error = intercept[IllegalArgumentException] {
      GpuProjectAstExec.planOutputs(Seq(passA), Seq(Seq(errorSite)))
    }

    assert(error.getMessage.contains("Pass-through AST outputs cannot have error sites"))
  }

  test("AST output planning recognizes an all-pass-through project") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val expressions = Seq(GpuAlias(b, "b")(), GpuAlias(a, "a")(), GpuAlias(b, "b2")())

    val plan = GpuProjectAstExec.planOutputs(
      expressions, Seq.fill(expressions.size)(Seq.empty[AstJitErrorSite]))

    assert(plan.allPassThrough)
    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstInputColumn(0),
      GpuProjectAstExec.AstInputColumn(1)))
    assert(plan.expressionsToCompute.isEmpty)
    assert(plan.errorSitesToCompute.isEmpty)
    assert(plan.literalsToProject.isEmpty)
  }
}
