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

import ai.rapids.cudf.{ColumnVector, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.Retryable

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

object GpuAstJitExpression {
  private def canUseAstJit(expression: GpuExpression): Boolean =
    GpuBatchUtils.isFixedWidth(expression.dataType) &&
      expression.supportsAstJit && expression.containsAstJitOperator

  private def asAstJit(child: GpuExpression): Option[GpuAstJitExpression] = child match {
    case jitExpression: GpuAstJitExpression => Some(jitExpression)
    case astExpression: GpuProjectAstExpression => asAstJit(astExpression.child)
    case other if canUseAstJit(other) => Some(GpuAstJitExpression(other))
    case _ => None
  }

  private[rapids] def wrapTierExpression(expression: Expression): Expression = expression match {
    case alias @ GpuAlias(child: GpuExpression, _) =>
      asAstJit(child).map(GpuProjectAstExpressionBase.replaceChild(alias, _)).getOrElse(alias)
    case other => other
  }

  private[rapids] def wrapProjectExpressions(
      expressions: List[NamedExpression]): List[NamedExpression] = {
    expressions.map(wrapTierExpression(_).asInstanceOf[NamedExpression])
  }

  /** Extracts a Project AST JIT wrapper after unwrapping any top-level aliases. */
  private[rapids] def extractTopLevel(expression: Expression): Option[GpuAstJitExpression] =
    GpuProjectAstExpressionBase.extractTopLevel(expression).collect {
      case jitExpression: GpuAstJitExpression => jitExpression
    }

  private def hasJitCandidate(expression: Expression): Boolean = expression.find {
    case gpuExpression: GpuExpression =>
      gpuExpression.supportsAstJit && gpuExpression.containsAstJitOperator
    case _ => false
  }.isDefined

  private def finalBackend(expression: Expression): String = {
    GpuProjectAstExpressionBase.extractTopLevel(expression) match {
      case Some(_: GpuAstJitExpression) => "Project AST JIT"
      case Some(_: GpuProjectAstExpression) => "legacy Project AST"
      case _ => "the regular GPU projection"
    }
  }

  private[rapids] def explainFinalSelections(
      expressionTiers: Seq[Seq[Expression]],
      all: Boolean): String = {
    expressionTiers.zipWithIndex.flatMap { case (expressions, tier) =>
      val explanations = expressions.iterator.collect {
        case expression if all ||
            (extractTopLevel(expression).isEmpty && hasJitCandidate(expression)) =>
          s"    $expression final backend: ${finalBackend(expression)}\n"
      }.mkString
      if (explanations.nonEmpty) {
        Some(s"  TIER $tier\n$explanations")
      } else {
        None
      }
    }.mkString
  }
}

case class GpuAstJitExpression(child: GpuExpression)
    extends GpuProjectAstExpressionBase with Retryable {

  override protected def backendName: String = "AST JIT"

  override protected def compileNvtxId: NvtxId = NvtxRegistry.COMPILE_AST_JIT

  override protected def computeNvtxId: NvtxId = NvtxRegistry.PROJECT_AST_JIT

  override protected def evaluate(
      compiled: CompiledExpression,
      table: Table): ColumnVector = compiled.computeColumnJit(table)

  override def toString: String = s"AST_JIT($child)"

  override def checkpoint(): Unit = {
    getCompiledExpression
  }

  // Compiled ASTs are immutable and remain valid across retry attempts.
  override def restore(): Unit = ()
}
