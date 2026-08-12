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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.catalyst.expressions.GpuEquivalentExpressions
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuProjectAstExpressionBase extends GpuExpression {
  private[rapids] def computeColumn(table: Table): GpuColumnVector
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
}

object GpuProjectAstExpression {
  private[rapids] def replaceChild(alias: GpuAlias, child: Expression): GpuAlias = {
    if (child eq alias.child) {
      alias
    } else {
      GpuAlias(child, alias.name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
    }
  }

  private def asAst(child: GpuExpression): GpuProjectAstExpression = child match {
    case astExpression: GpuProjectAstExpression => astExpression
    case other => GpuProjectAstExpression(other)
  }

  private[rapids] def wrap(expression: NamedExpression): NamedExpression = expression match {
    case alias @ GpuAlias(child: GpuExpression, _) =>
      replaceChild(alias, asAst(child))
    case other => other
  }

  /** Extracts a legacy Project AST wrapper after unwrapping any top-level aliases. */
  @tailrec
  private[rapids] def extractTopLevel(expression: Expression): Option[GpuProjectAstExpression] = {
    expression match {
      case alias: GpuAlias => extractTopLevel(alias.child)
      case astExpression: GpuProjectAstExpression => Some(astExpression)
      case _ => None
    }
  }

  private def unwrap(expression: Expression): Expression = expression match {
    case alias: GpuAlias => replaceChild(alias, unwrap(alias.child))
    case astExpression: GpuProjectAstExpression => astExpression.child
    case jitExpression: GpuAstJitExpression => jitExpression.child
    case other => other
  }

  private def rewrap(expression: Expression): Expression = expression match {
    case namedExpression: NamedExpression => wrap(namedExpression)
    case other => other
  }

  private def rewrapAstTiers(
      tiers: Seq[Seq[Expression]],
      astOutputs: Seq[Boolean]): Seq[Seq[Expression]] = {
    val finalTier = tiers.last
    require(finalTier.size == astOutputs.size,
      "The final expression tier must preserve the project output count")
    def referenceSet(taggedTier: Iterable[(Expression, Boolean)]): Set[ExprId] = {
      taggedTier.collect { case (expression, true) => expression }
          .flatMap(_.references.iterator)
          .map(_.exprId)
          .toSet
    }
    val astReferences = referenceSet(finalTier.zip(astOutputs))

    // Tier aliases are the dataflow graph after CSE, so follow them backwards from AST outputs.
    val (commonTiers, _) = tiers.dropRight(1).foldRight(
      (List.empty[Seq[Expression]], astReferences)) {
      case (tier, (rewrittenTiers, requiredExprIds)) =>
        val taggedTier = tier.map {
          case alias: GpuAlias if requiredExprIds.contains(alias.exprId) => (alias, true)
          case expression => (expression, false)
        }
        val dependencies = referenceSet(taggedTier)
        val rewrittenTier = taggedTier.map {
          case (alias, true) if GpuBatchUtils.isFixedWidth(alias.dataType) => rewrap(alias)
          case (expression, _) => expression
        }
        (rewrittenTier :: rewrittenTiers, requiredExprIds ++ dependencies)
    }

    commonTiers :+ finalTier.zip(astOutputs).map {
      case (expression, true) => rewrap(expression)
      case (expression, false) => expression
    }
  }

  private[rapids] def buildExprTiers(
      expressions: Seq[Expression],
      conf: SQLConf,
      enableProjectAstJit: Boolean = false): Seq[Seq[Expression]] = {
    val astOutputs = expressions.map(extractTopLevel(_).isDefined)
    val hasAstOutputs = astOutputs.contains(true)
    val hasJitOutputs = expressions.exists(GpuAstJitExpression.extractTopLevel(_).isDefined)
    // CSE must see through backend markers so all outputs can share the same tiers.
    val unwrapped = if (hasAstOutputs || hasJitOutputs) {
      expressions.map(unwrap)
    } else {
      expressions
    }
    val replaced = if (RapidsConf.ENABLE_COMBINED_EXPRESSIONS.get(conf)) {
      GpuEquivalentExpressions.replaceMultiExpressions(unwrapped, conf)
    } else {
      unwrapped
    }
    val tiers = GpuEquivalentExpressions.getExprTiers(replaced)
    val astTiers = if (hasAstOutputs) {
      rewrapAstTiers(tiers, astOutputs)
    } else {
      tiers
    }
    if (enableProjectAstJit) {
      // Project binding selects JIT after CSE so newly exposed tiers are eligible.
      astTiers.map(_.map(GpuAstJitExpression.wrapTierExpression))
    } else {
      // Only the Project-specific binder selects JIT after CSE.
      astTiers
    }
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

case class GpuProjectAstExpression(child: GpuExpression)
    extends ShimUnaryExpression with GpuProjectAstExpressionBase
        with GpuMetricsInjectable with AutoCloseable {
  @transient private[this] var compiledExpression: CompiledExpression = _
  private[this] var opTime: GpuMetric = NoopMetric

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"AST($child)"

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    opTime = metrics.getOrElse(OP_TIME_LEGACY, NoopMetric)
  }

  override def close(): Unit = {
    val toClose = synchronized {
      val current = compiledExpression
      compiledExpression = null
      current
    }
    Option(toClose).foreach(_.safeClose())
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectAstExpression.tableFromBatch(batch)) { table =>
      computeColumn(table)
    }
  }

  private[rapids] override def computeColumn(table: Table): GpuColumnVector = {
    val compiled = getCompiledExpression
    NvtxIdWithMetrics(NvtxRegistry.PROJECT_AST, opTime) {
      closeOnExcept(compiled.computeColumn(table)) { result =>
        GpuColumnVector.from(result, dataType)
      }
    }
  }

  private def getCompiledExpression: CompiledExpression = synchronized {
    if (compiledExpression == null) {
      val compiled = NvtxIdWithMetrics(NvtxRegistry.COMPILE_ASTS, opTime) {
        // Force every bound reference to the left table; Project AST has one input table.
        child.convertToAst(Int.MaxValue).compile()
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
            "Task completed while registering the Project AST cleanup callback")
        }
        compiledExpression = compiled
      }
    }
    compiledExpression
  }
}
