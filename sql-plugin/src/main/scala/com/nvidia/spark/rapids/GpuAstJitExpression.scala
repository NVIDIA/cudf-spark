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

import scala.collection.mutable

import ai.rapids.cudf.{DType, Table}
import ai.rapids.cudf.ast.{AstExpression, AstJitProgram, CompiledExpression}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionEquals
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuAstJitExpression {
  private final case class JitInputColumn(
      ordinal: Int,
      dataType: DType,
      nullable: Boolean)

  private final class JitProgramGroup(
      val expressions: Vector[GpuAstJitExpression],
      val referencedOrdinals: Vector[Int]) {
    def matches(other: Seq[GpuAstJitExpression]): Boolean = {
      expressions.size == other.size &&
        expressions.iterator.zip(other.iterator).forall { case (left, right) => left eq right }
    }
  }

  private object JitProgramGroup {
    def apply(expressions: Seq[GpuAstJitExpression]): JitProgramGroup = {
      val roots = expressions.toVector
      val referencedOrdinals = roots.iterator.flatMap { expression =>
        expression.child.collect {
          case reference: GpuBoundReference => reference.ordinal
        }
      }.toSet.toVector.sorted
      new JitProgramGroup(roots, referencedOrdinals)
    }
  }

  private def inputSchema(
      group: JitProgramGroup,
      table: Table): Vector[JitInputColumn] = {
    group.referencedOrdinals.map { ordinal =>
      val column = table.getColumn(ordinal)
      JitInputColumn(ordinal, column.getType, column.hasValidityVector)
    }
  }

  private final case class JitRoot(
      index: Int,
      child: GpuExpression,
      operations: Set[GpuExpressionEquals])

  private final class JitGroup(val id: Int) {
    val roots: mutable.ArrayBuffer[JitRoot] = mutable.ArrayBuffer.empty
    val operations: mutable.HashSet[GpuExpressionEquals] = mutable.HashSet.empty

    def canAdd(
        newRoots: Seq[JitRoot],
        maxOps: Int,
        maxOutputs: Int): Boolean = {
      val addedOps = newRoots.iterator.flatMap(_.operations).count(!operations.contains(_))
      roots.size <= maxOutputs - newRoots.size && operations.size <= maxOps - addedOps
    }

    def add(newRoots: Seq[JitRoot]): Unit = {
      roots ++= newRoots
      newRoots.foreach(root => operations ++= root.operations)
    }
  }

  /** Extracts an AST JIT wrapper after unwrapping any top-level aliases. */
  private[rapids] def extractTopLevel(expression: Expression): Option[GpuAstJitExpression] =
    GpuProjectAstExpressionBase.extractTopLevel(expression).collect {
      case jitExpression: GpuAstJitExpression => jitExpression
    }

  private[rapids] def canUseAstJit(expression: Expression): Boolean = expression match {
    case gpuExpression: GpuExpression =>
      GpuBatchUtils.isFixedWidth(expression.dataType) &&
        gpuExpression.supportsAstJit && gpuExpression.containsAstJitOperator
    case _ => false
  }

  private def astJitChild(child: GpuExpression): Option[GpuExpression] = child match {
    case jitExpression: GpuAstJitExpression => Some(jitExpression.child)
    case astExpression: GpuProjectAstExpression => astJitChild(astExpression.child)
    case other if canUseAstJit(other) => Some(other)
    case _ => None
  }

  private def asAstJit(child: GpuExpression): Option[GpuAstJitExpression] = child match {
    case jitExpression: GpuAstJitExpression => Some(jitExpression)
    case other => astJitChild(other).map(GpuAstJitExpression(_))
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

  private def operationSet(expression: GpuExpression): Set[GpuExpressionEquals] = {
    expression.collect {
      case gpuExpression: GpuExpression if gpuExpression.selfIsAstJitOperator =>
        GpuExpressionEquals(gpuExpression)
    }.toSet
  }

  private def connectedComponents(roots: Seq[JitRoot]): Seq[Seq[JitRoot]] = {
    val parents = roots.indices.toArray

    def find(index: Int): Int = {
      var root = index
      while (parents(root) != root) {
        root = parents(root)
      }
      var current = index
      while (parents(current) != current) {
        val next = parents(current)
        parents(current) = root
        current = next
      }
      root
    }

    def union(left: Int, right: Int): Unit = {
      val leftRoot = find(left)
      val rightRoot = find(right)
      if (leftRoot != rightRoot) {
        parents(rightRoot) = leftRoot
      }
    }

    val operationOwner = mutable.HashMap.empty[GpuExpressionEquals, Int]
    roots.zipWithIndex.foreach { case (root, index) =>
      root.operations.foreach { operation =>
        operationOwner.get(operation) match {
          case Some(previous) => union(previous, index)
          case None => operationOwner.put(operation, index)
        }
      }
    }

    val components = mutable.LinkedHashMap.empty[Int, mutable.ArrayBuffer[JitRoot]]
    roots.indices.foreach { index =>
      components.getOrElseUpdate(find(index), mutable.ArrayBuffer.empty) += roots(index)
    }
    components.values.map(_.toSeq).toSeq
  }

  private def groupRoots(
      roots: Seq[JitRoot],
      maxOps: Int,
      maxOutputs: Int): Map[Int, Int] = {
    val groups = mutable.ArrayBuffer.empty[JitGroup]

    def newGroup(): JitGroup = {
      val group = new JitGroup(groups.size)
      groups += group
      group
    }

    connectedComponents(roots).foreach { component =>
      val componentOps = component.iterator.flatMap(_.operations).toSet
      if (component.size <= maxOutputs && componentOps.size <= maxOps) {
        val group = groups.find(_.canAdd(component, maxOps, maxOutputs)).getOrElse(newGroup())
        group.add(component)
      } else {
        val componentGroups = mutable.ArrayBuffer.empty[JitGroup]
        component.foreach { root =>
          val candidates = componentGroups.filter(_.canAdd(Seq(root), maxOps, maxOutputs))
          val group = if (candidates.nonEmpty) {
            candidates.maxBy(group => root.operations.count(group.operations.contains))
          } else {
            val created = newGroup()
            componentGroups += created
            created
          }
          group.add(Seq(root))
        }
      }
    }

    groups.iterator.flatMap { group =>
      group.roots.iterator.map(root => root.index -> group.id)
    }.toMap
  }

  private[rapids] def wrapTierExpressions(
      expressions: Seq[Expression],
      conf: SQLConf): Seq[Expression] = {
    val roots = expressions.zipWithIndex.flatMap { case (expression, index) =>
      expression match {
        case GpuAlias(child: GpuExpression, _) =>
          astJitChild(child).map { jitChild =>
            JitRoot(index, jitChild, operationSet(jitChild))
          }
        case _ => None
      }
    }
    val groupIds = groupRoots(
      roots,
      RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OPS.get(conf),
      RapidsConf.PROJECT_AST_JIT_MAX_GROUP_OUTPUTS.get(conf))
    val rootsByIndex = roots.iterator.map(root => root.index -> root).toMap
    expressions.zipWithIndex.map { case (expression, index) =>
      (expression, rootsByIndex.get(index), groupIds.get(index)) match {
        case (alias: GpuAlias, Some(root), Some(groupId)) =>
          GpuProjectAstExpressionBase.replaceChild(
            alias, GpuAstJitExpression(root.child, groupId))
        case _ => expression
      }
    }
  }

  private[rapids] def outputGroups(
      expressions: Seq[GpuAstJitExpression]): Seq[Seq[GpuAstJitExpression]] = {
    val groups = mutable.LinkedHashMap.empty[Int, mutable.ArrayBuffer[GpuAstJitExpression]]
    expressions.foreach { expression =>
      groups.getOrElseUpdate(expression.groupId, mutable.ArrayBuffer.empty) += expression
    }
    groups.values.map(_.toSeq).toSeq
  }

  private[rapids] def executionGroups(
      expressions: Seq[GpuAstJitExpression],
      multiOutputEnabled: Boolean): Seq[Seq[GpuAstJitExpression]] = {
    if (multiOutputEnabled) {
      outputGroups(expressions)
    } else {
      expressions.map(Seq(_))
    }
  }

  private[rapids] def computeColumns(
      expressions: Seq[GpuAstJitExpression],
      table: Table): ColumnarBatch = {
    require(expressions.nonEmpty, "AST JIT requires at least one expression")
    val owner = expressions.head
    val program = owner.getJitProgram(expressions, table)
    owner.withComputeMetrics {
      withResource(program.computeTable(table)) { result =>
        GpuColumnVector.from(result, expressions.map(_.dataType).toArray)
      }
    }
  }

  private def hasJitCandidate(expression: Expression): Boolean = expression.find {
    case gpuExpression: GpuExpression =>
      gpuExpression.supportsAstJit && gpuExpression.containsAstJitOperator
    case _ => false
  }.isDefined

  private def finalBackend(expression: Expression): String = {
    GpuProjectAstExpressionBase.extractTopLevel(expression) match {
      case Some(_: GpuAstJitExpression) => "AST JIT"
      case Some(_: GpuProjectAstExpression) => "AST"
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

case class GpuAstJitExpression(child: GpuExpression, groupId: Int = 0)
    extends GpuProjectAstExpressionBase with Retryable {

  @transient private[this] var jitProgram: AstJitProgram = _
  @transient private[this] var jitProgramGroup: GpuAstJitExpression.JitProgramGroup = _
  @transient private[this] var jitProgramSchema: Vector[GpuAstJitExpression.JitInputColumn] = _

  override def disableTieredProjectCombine: Boolean = true

  override protected def compileNvtxId: NvtxId = NvtxRegistry.COMPILE_AST_JIT

  override protected def computeNvtxId: NvtxId = NvtxRegistry.PROJECT_AST_JIT

  override protected def compileAst(ast: AstExpression): CompiledExpression = ast.compileJit()

  protected def compileJitProgram(
      table: Table,
      expressions: Array[CompiledExpression]): AstJitProgram = {
    AstJitProgram.compile(table, expressions: _*)
  }

  private[rapids] def getJitProgram(
      expressions: Seq[GpuAstJitExpression],
      table: Table): AstJitProgram = synchronized {
    require(expressions.head eq this, "The first AST JIT expression must own the group program")
    val groupMatches = jitProgramGroup != null && jitProgramGroup.matches(expressions)
    val group = if (groupMatches) {
      jitProgramGroup
    } else {
      GpuAstJitExpression.JitProgramGroup(expressions)
    }
    val schema = GpuAstJitExpression.inputSchema(group, table)
    if (jitProgram == null || !groupMatches || jitProgramSchema != schema) {
      val compiledExpressions = group.expressions.map(_.getCompiledExpression).toArray
      val replacement = withCompileMetrics {
        compileJitProgram(table, compiledExpressions)
      }
      val previous = jitProgram
      jitProgram = replacement
      jitProgramGroup = group
      jitProgramSchema = schema
      Option(previous).foreach(_.safeClose())
    }
    jitProgram
  }

  override protected def releaseAdditionalResources(): Seq[AutoCloseable] = {
    val current = jitProgram
    jitProgram = null
    jitProgramGroup = null
    jitProgramSchema = null
    Option(current).toSeq
  }

  override def toString: String = s"AST_JIT($child)"

  override def checkpoint(): Unit = {
    getCompiledExpression
  }

  // Compiled ASTs and schema-matched programs remain valid across retry attempts.
  override def restore(): Unit = ()
}
