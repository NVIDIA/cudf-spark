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
import ai.rapids.cudf.ast.{AstExpression, CompiledExpression}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.OP_TIME_LEGACY
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuProjectAstExpressionBase
    extends ShimUnaryExpression with GpuExpression with GpuMetricsInjectable with AutoCloseable {
  override def child: GpuExpression

  protected def compileNvtxId: NvtxId
  protected def computeNvtxId: NvtxId
  protected def compileAst(ast: AstExpression): CompiledExpression = ast.compile()

  @transient private[this] var compiledExpression: CompiledExpression = _
  private[this] var opTime: GpuMetric = NoopMetric

  protected def releaseAdditionalResources(): Seq[AutoCloseable] = Seq.empty

  override final def dataType: DataType = child.dataType

  override final def nullable: Boolean = child.nullable

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    // OP_TIME_LEGACY is the owning operator's non-RDD timing metric, not a legacy AST metric.
    opTime = metrics.getOrElse(OP_TIME_LEGACY, NoopMetric)
  }

  override final def close(): Unit = {
    val toClose = synchronized {
      val current = compiledExpression
      compiledExpression = null
      releaseAdditionalResources() ++ Option(current)
    }
    toClose.safeClose()
  }

  override final def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectAstExpressionBase.tableFromBatch(batch)) { table =>
      computeColumn(table)
    }
  }

  private[rapids] final def computeColumn(table: Table): GpuColumnVector = {
    val compiled = getCompiledExpression
    withComputeMetrics(table.getRowCount) {
      closeOnExcept(compiled.computeColumn(table)) { result =>
        GpuColumnVector.from(result, dataType)
      }
    }
  }

  private[rapids] def withComputeMetrics[T](rows: Long)(body: => T): T =
    NvtxIdWithMetrics(computeNvtxId, opTime)(body)

  private[rapids] final def withCompileMetrics[T](body: => T): T =
    NvtxIdWithMetrics(compileNvtxId, opTime)(body)

  private[rapids] final def getCompiledExpression: CompiledExpression = synchronized {
    if (compiledExpression == null) {
      val compiled = withCompileMetrics {
        // Force every bound reference to the left table; Project AST has one input table.
        compileAst(child.convertToAst(Int.MaxValue))
      }
      closeOnExcept(compiled) { _ =>
        var completed = false
        Option(TaskContext.get()).foreach { taskContext =>
          onTaskCompletion(taskContext) {
            completed = true
            close()
          }
        }
        if (completed) {
          throw new IllegalStateException(
            "Task completed while registering compiled expression cleanup callback")
        }
        compiledExpression = compiled
      }
    }
    compiledExpression
  }
}

object GpuProjectAstExpressionBase {
  @tailrec
  private[rapids] def extractTopLevel(
      expression: Expression): Option[GpuProjectAstExpressionBase] = {
    expression match {
      case alias: GpuAlias => extractTopLevel(alias.child)
      case astExpression: GpuProjectAstExpressionBase => Some(astExpression)
      case _ => None
    }
  }

  private[rapids] def replaceChild(alias: GpuAlias, child: Expression): GpuAlias = {
    if (child eq alias.child) {
      alias
    } else {
      GpuAlias(child, alias.name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
    }
  }

  private[rapids] def buildExprTiers(
      expressions: Seq[Expression],
      conf: SQLConf,
      enableAstJit: Boolean = false,
      enableAst: Boolean = false): Seq[Seq[Expression]] = {
    val legacy = enableAst || expressions.exists(
      GpuProjectAstExpression.extractTopLevel(_).isDefined)
    GpuProjectAstPlanner.buildExprTiers(expressions, conf, legacy, enableAstJit)
  }

  private[rapids] def tableFromBatch(batch: ColumnarBatch): Table = {
    if (batch.numCols() != 0) {
      GpuColumnVector.from(batch)
    } else {
      // cuDF cannot represent a row-count-only table, so use a dummy fixed-width column.
      withResource(Scalar.fromBool(false)) { falseScalar =>
        withResource(ai.rapids.cudf.ColumnVector.fromScalar(falseScalar, batch.numRows())) {
          falseColumn => new Table(falseColumn)
        }
      }
    }
  }
}

object GpuProjectAstExpression {
  /** Extracts a legacy Project AST wrapper after unwrapping any top-level aliases. */
  private[rapids] def extractTopLevel(expression: Expression): Option[GpuProjectAstExpression] =
    GpuProjectAstExpressionBase.extractTopLevel(expression).collect {
      case astExpression: GpuProjectAstExpression => astExpression
    }

  private def asAst(child: GpuExpression): GpuProjectAstExpression = child match {
    case astExpression: GpuProjectAstExpression => astExpression
    case other => GpuProjectAstExpression(other)
  }

  private[rapids] def wrap(expression: NamedExpression): NamedExpression = expression match {
    case alias @ GpuAlias(child: GpuExpression, _) =>
      GpuProjectAstExpressionBase.replaceChild(alias, asAst(child))
    case other => other
  }
}

case class GpuProjectAstExpression(child: GpuExpression)
    extends GpuProjectAstExpressionBase {

  override protected def compileNvtxId: NvtxId = NvtxRegistry.COMPILE_ASTS

  override protected def computeNvtxId: NvtxId = NvtxRegistry.PROJECT_AST

  override def toString: String = s"AST($child)"
}
