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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply}
import org.apache.spark.sql.types.LongType

class GpuProjectAstSuite extends AnyFunSuite {
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
      expressions, Seq(None, Some(sumErrorSite), None, Some(productErrorSite), None))

    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstInputColumn(0),
      GpuProjectAstExec.AstComputedColumn(1),
      GpuProjectAstExec.AstInputColumn(1)))
    assert(plan.expressionsToCompute == Seq(sum, product))
    assert(plan.errorSitesToCompute == Seq(Some(sumErrorSite), Some(productErrorSite)))
    assert(!plan.allPassThrough)
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

    val plan = GpuProjectAstExec.planOutputs(expressions, Seq.fill(expressions.size)(None))

    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstComputedColumn(1),
      GpuProjectAstExec.AstComputedColumn(0),
      GpuProjectAstExec.AstComputedColumn(0)))
    assert(plan.expressionsToCompute == Seq(sum, product))
    assert(plan.errorSitesToCompute == Seq(None, None))
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
      expressions, Seq(Some(firstErrorSite), Some(secondErrorSite), None, None))

    assert(plan.outputSources == expressions.indices.map(GpuProjectAstExec.AstComputedColumn))
    assert(plan.expressionsToCompute == expressions)
    assert(plan.errorSitesToCompute ==
      Seq(Some(firstErrorSite), Some(secondErrorSite), None, None))

    val uncheckedPlan = GpuProjectAstExec.planOutputs(
      Seq(checkedSum, duplicateCheckedSum), Seq(None, None))
    assert(uncheckedPlan.expressionsToCompute == Seq(checkedSum, duplicateCheckedSum))
  }

  test("AST output planning rejects error sites on pass-through expressions") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val passA = GpuAlias(a, "pass_a")()
    val errorSite = AstJitErrorSite(AstJitErrorKind.Add, passA.origin)

    val error = intercept[IllegalArgumentException] {
      GpuProjectAstExec.planOutputs(Seq(passA), Seq(Some(errorSite)))
    }

    assert(error.getMessage.contains("Pass-through AST outputs cannot have error sites"))
  }

  test("AST output planning recognizes an all-pass-through project") {
    val a = GpuBoundReference(0, LongType, nullable = true)(NamedExpression.newExprId, "a")
    val b = GpuBoundReference(1, LongType, nullable = true)(NamedExpression.newExprId, "b")
    val expressions = Seq(GpuAlias(b, "b")(), GpuAlias(a, "a")(), GpuAlias(b, "b2")())

    val plan = GpuProjectAstExec.planOutputs(expressions, Seq.fill(expressions.size)(None))

    assert(plan.allPassThrough)
    assert(plan.outputSources == Seq(
      GpuProjectAstExec.AstInputColumn(1),
      GpuProjectAstExec.AstInputColumn(0),
      GpuProjectAstExec.AstInputColumn(1)))
    assert(plan.expressionsToCompute.isEmpty)
    assert(plan.errorSitesToCompute.isEmpty)
  }
}
