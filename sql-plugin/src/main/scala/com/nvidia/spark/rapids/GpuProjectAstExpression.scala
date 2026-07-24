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

import scala.annotation.tailrec

import ai.rapids.cudf.{Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.OP_TIME_LEGACY
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.catalyst.expressions.{
  GpuEquivalentExpressions, GpuExpressionEquals}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuProjectAstExpression {
  private[rapids] def wrap(expression: NamedExpression): NamedExpression = {
    expression match {
      case alias @ GpuAlias(child: GpuExpression, name) =>
        GpuAlias(GpuProjectAstExpression(child), name)(
          alias.exprId, alias.qualifier, alias.explicitMetadata)
      case other => other
    }
  }

  @tailrec
  private[rapids] def extractTopLevel(expression: Expression): Option[GpuProjectAstExpression] = {
    expression match {
      case alias: GpuAlias => extractTopLevel(alias.child)
      case astExpression: GpuProjectAstExpression => Some(astExpression)
      case _ => None
    }
  }

  private def unwrap(expression: Expression): Expression = expression match {
    case alias @ GpuAlias(astExpression: GpuProjectAstExpression, name) =>
      GpuAlias(astExpression.child, name)(
        alias.exprId, alias.qualifier, alias.explicitMetadata)
    case astExpression: GpuProjectAstExpression => astExpression.child
    case other => other
  }

  private def rewrap(expression: Expression): Expression = expression match {
    case namedExpression: NamedExpression => wrap(namedExpression)
    case gpuExpression: GpuExpression => GpuProjectAstExpression(gpuExpression)
    case other => other
  }

  private[rapids] def buildExprTiers(
      expressions: Seq[Expression],
      conf: SQLConf): Seq[Seq[Expression]] = {
    val astOutputs = expressions.map(extractTopLevel)
    val astSubexpressions = astOutputs.flatten.flatMap { astExpression =>
      astExpression.child.collect {
        case gpuExpression: GpuExpression => GpuExpressionEquals(gpuExpression)
      }
    }.toSet
    // CSE must see through the marker so AST and non-AST outputs can share the same tiers.
    val unwrapped = expressions.map(unwrap)
    val replaced = if (RapidsConf.ENABLE_COMBINED_EXPRESSIONS.get(conf)) {
      GpuEquivalentExpressions.replaceMultiExpressions(unwrapped, conf)
    } else {
      unwrapped
    }
    val tiers = GpuEquivalentExpressions.getExprTiers(
      replaced,
      (original, substituted) => (original, substituted) match {
        case (gpuOriginal: GpuExpression, gpuSubstituted: GpuExpression)
            if GpuBatchUtils.isFixedWidth(gpuOriginal.dataType) &&
              astSubexpressions.contains(GpuExpressionEquals(gpuOriginal)) =>
          GpuProjectAstExpression(gpuSubstituted)
        case _ => substituted
      })
    val finalTier = tiers.last.zip(astOutputs).map {
      case (expression, Some(_)) => rewrap(expression)
      case (expression, None) => expression
    }
    tiers.dropRight(1) :+ finalTier
  }

  private[rapids] def tableFromBatch(batch: ColumnarBatch): Table = {
    if (batch.numCols() != 0) {
      GpuColumnVector.from(batch)
    } else {
      withResource(Scalar.fromBool(false)) { falseScalar =>
        withResource(ai.rapids.cudf.ColumnVector.fromScalar(falseScalar, batch.numRows())) {
          falseColumn => new Table(falseColumn)
        }
      }
    }
  }
}

case class GpuProjectAstExpression(child: GpuExpression)
    extends ShimUnaryExpression with GpuExpression with GpuMetricsInjectable with AutoCloseable {
  @transient private[this] var compiledExpression: CompiledExpression = _
  private[this] var opTime: GpuMetric = NoopMetric

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"AST($child)"

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    opTime = metrics.getOrElse(OP_TIME_LEGACY, NoopMetric)
  }

  override def close(): Unit = synchronized {
    Option(compiledExpression).foreach(_.safeClose())
    compiledExpression = null
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectAstExpression.tableFromBatch(batch)) { table =>
      computeColumn(table)
    }
  }

  def computeColumn(table: Table): GpuColumnVector = {
    NvtxIdWithMetrics(NvtxRegistry.PROJECT_AST, opTime) {
      closeOnExcept(getCompiledExpression.computeColumn(table)) { result =>
        GpuColumnVector.from(result, dataType)
      }
    }
  }

  private def getCompiledExpression: CompiledExpression = synchronized {
    if (compiledExpression == null) {
      val compiled = NvtxIdWithMetrics(NvtxRegistry.COMPILE_ASTS, opTime) {
        // Project AST has a single input table.
        child.convertToAst(Int.MaxValue).compile()
      }
      closeOnExcept(compiled) { _ =>
        Option(TaskContext.get()).foreach { taskContext =>
          onTaskCompletion(taskContext) {
            close()
          }
        }
        compiledExpression = compiled
      }
    }
    compiledExpression
  }
}
