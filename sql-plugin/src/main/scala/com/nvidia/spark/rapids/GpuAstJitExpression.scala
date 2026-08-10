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
import com.nvidia.spark.rapids.GpuMetric.OP_TIME_LEGACY
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
    case alias @ GpuAlias(_: GpuAstJitExpression, _) => alias
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
    GpuProjectAstExpressionBase.extractTopLevel(expression)
      .exists(_.isInstanceOf[GpuAstJitExpression])
}

case class GpuAstJitExpression(child: GpuExpression)
    extends ShimUnaryExpression with GpuProjectAstExpressionBase
        with GpuMetricsInjectable with Retryable with AutoCloseable {

  @transient private[this] var compiledExpression: CompiledExpression = _
  @transient private[this] var completionRegistered = false
  private[this] var opTime: GpuMetric = NoopMetric

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"AST_JIT($child)"

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    opTime = metrics.getOrElse(OP_TIME_LEGACY, NoopMetric)
  }

  override def checkpoint(): Unit = {
    getCompiledExpression
  }

  // Compiled ASTs are immutable and remain valid across retry attempts.
  override def restore(): Unit = ()

  override def close(): Unit = closeCompiledExpression()

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectAstExpression.tableFromBatch(batch)) { table =>
      computeColumn(table)
    }
  }

  private[rapids] override def computeColumn(table: Table): GpuColumnVector = {
    val compiled = getCompiledExpression
    NvtxIdWithMetrics(NvtxRegistry.PROJECT_AST, opTime) {
      closeOnExcept(compiled.computeColumnJit(table)) { result =>
        GpuColumnVector.from(result, dataType)
      }
    }
  }

  private def getCompiledExpression: CompiledExpression = synchronized {
    if (compiledExpression == null) {
      compiledExpression = NvtxIdWithMetrics(NvtxRegistry.COMPILE_ASTS, opTime) {
        // Force every bound reference to the left table; Project AST has one input table.
        child.convertToAst(Int.MaxValue).compile()
      }
    }
    if (!completionRegistered) {
      Option(TaskContext.get()).foreach { taskContext =>
        completionRegistered = true
        try {
          onTaskCompletion(taskContext) {
            clearRegistrationAndClose()
          }
          if (!completionRegistered) {
            throw new IllegalStateException(
              "Task completed while registering the AST JIT cleanup callback")
          }
        } catch {
          case t: Throwable =>
            clearRegistrationAndClose(t)
            throw t
        }
      }
    }
    compiledExpression
  }

  private def clearRegistrationAndClose(error: Throwable = null): Unit =
    closeCompiledExpression(error, clearRegistration = true)

  private def closeCompiledExpression(
      error: Throwable = null,
      clearRegistration: Boolean = false): Unit = {
    val toClose = synchronized {
      if (clearRegistration) {
        completionRegistered = false
      }
      val current = compiledExpression
      compiledExpression = null
      current
    }
    Option(toClose).foreach(_.safeClose(error))
  }
}
