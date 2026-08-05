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
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuAstJitExpression {
  private def canUseAstJit(expression: GpuExpression): Boolean =
    GpuBatchUtils.isFixedWidth(expression.dataType) &&
      expression.supportsAstJit && expression.containsAstJitOperator

  private[rapids] def wrapTierExpression(expression: Expression): Expression = expression match {
    case alias @ GpuAlias(astExpression: GpuProjectAstExpression, _)
        if canUseAstJit(astExpression.child) =>
      GpuProjectAstExpression.replaceChild(alias, GpuAstJitExpression(astExpression.child))
    case alias @ GpuAlias(child: GpuExpression, _) if canUseAstJit(child) =>
      GpuProjectAstExpression.replaceChild(alias, GpuAstJitExpression(child))
    case other => other
  }

  private[rapids] def wrapProjectExpressions(
      expressions: List[NamedExpression]): List[NamedExpression] = {
    expressions.map(wrapTierExpression(_).asInstanceOf[NamedExpression])
  }

  private[rapids] def contains(expression: Expression): Boolean =
    expression.find(_.isInstanceOf[GpuAstJitExpression]).isDefined
}

case class GpuAstJitExpression(child: GpuExpression)
    extends ShimUnaryExpression with GpuProjectAstExpressionBase
        with Retryable with AutoCloseable {

  @transient private[this] var compiledExpression: CompiledExpression = _
  @transient private[this] var completionRegistered = false

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def disableTieredProjectCombine: Boolean = true

  override def toString: String = s"AST_JIT($child)"

  override def checkpoint(): Unit = {
    getCompiledExpression
  }

  override def restore(): Unit = {
    // The existing task callback closes the expression recompiled after a retry.
    closeCompiledExpression()
  }

  override def close(): Unit = closeCompiledExpression()

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectAstExpression.tableFromBatch(batch)) { table =>
      computeColumn(table)
    }
  }

  private[rapids] override def computeColumn(table: Table): GpuColumnVector =
    closeOnExcept(getCompiledExpression.computeColumnJit(table)) { result =>
      GpuColumnVector.from(result, dataType)
    }

  private def getCompiledExpression: CompiledExpression = synchronized {
    if (compiledExpression == null) {
      compiledExpression = child.convertToAst(Int.MaxValue)
        .compile()
    }
    if (!completionRegistered) {
      Option(TaskContext.get()).foreach { taskContext =>
        completionRegistered = true
        try {
          onTaskCompletion(taskContext) {
            closeAtTaskCompletion()
          }
          if (!completionRegistered) {
            throw new IllegalStateException(
              "Task completed while registering the AST JIT cleanup callback")
          }
        } catch {
          case t: Throwable =>
            completionRegistered = false
            closeCompiledExpression(t)
            throw t
        }
      }
    }
    compiledExpression
  }

  private def closeAtTaskCompletion(): Unit = synchronized {
    completionRegistered = false
    closeCompiledExpression()
  }

  private def closeCompiledExpression(error: Throwable = null): Unit = synchronized {
    val toClose = compiledExpression
    compiledExpression = null
    Option(toClose).foreach(_.safeClose(error))
  }
}
