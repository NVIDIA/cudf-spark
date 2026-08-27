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

package org.apache.spark.sql.rapids.execution

import scala.annotation.tailrec

import com.nvidia.spark.rapids.{
  GpuCoalesceBatches, GpuExec, GpuExpression, GpuFilterExec, GpuNondeterministic, GpuProjectExec}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute, ExprId, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An auxiliary physical plan used to collect range-partition boundaries.
 *
 * The range exchange exposes this node as a subquery so Spark includes the narrow scan and its
 * normal GPU metrics in the SQL physical plan. The exchange executes it once while constructing
 * its range partitioner; the full-width exchange child remains the source of shuffled rows.
 */
private[rapids] case class GpuRangeBoundaryExec(child: SparkPlan)
    extends ShimUnaryExecNode with GpuExec {
  override def output: Seq[Attribute] = child.output
  override def nodeName: String = "GpuRangeBoundaryCollect"

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    child.executeColumnar()
}

/**
 * Builds a key-only physical plan for range-boundary collection when it is safe to do so.
 * Unsupported plans fall back to sampling the full exchange input.
 */
private[rapids] object GpuRangeBoundaryPlan {
  private def referencedExprIds(expressions: Seq[Expression]): Set[ExprId] =
    expressions.flatMap(_.references).map(_.exprId).toSet

  def build(plan: SparkPlan, ordering: Seq[SortOrder]): Option[GpuRangeBoundaryExec] = {
    val required = referencedExprIds(ordering)
    if (required.isEmpty) {
      None
    } else {
      prune(plan, required).flatMap { pruned =>
        val selected = pruned.output.filter(attr => required.contains(attr.exprId))
        if (selected.map(_.exprId).toSet != required) {
          None
        } else {
          val keyOnly = if (selected.length == pruned.output.length) {
            pruned
          } else {
            GpuProjectExec(selected.toList, pruned)
          }
          Some(GpuRangeBoundaryExec(keyOnly))
        }
      }
    }
  }

  private def isBoundaryRepeatable(expression: Expression): Boolean = {
    if (expression.deterministic) {
      true
    } else {
      expression match {
        case gpu: GpuExpression if gpu.selfNonDeterministic =>
          gpu.isInstanceOf[GpuNondeterministic] && gpu.children.forall(isBoundaryRepeatable)
        case _ if expression.children.forall(_.deterministic) =>
          false
        case _ =>
          expression.children.forall(isBoundaryRepeatable)
      }
    }
  }

  private def selectProjectExpressions(
      projectList: List[NamedExpression],
      localOutputIds: Set[ExprId],
      required: Set[ExprId]): Option[List[NamedExpression]] = {
    val projectOutputIds = projectList.map(_.exprId).toSet
    if (!required.subsetOf(projectOutputIds)) {
      None
    } else {
      @tailrec
      def dependencyClosure(needed: Set[ExprId]): List[NamedExpression] = {
        val selected = projectList.filter(ne => needed.contains(ne.exprId))
        val localDependencies = referencedExprIds(selected).intersect(localOutputIds)
        val expanded = needed ++ localDependencies
        if (expanded == needed) selected else dependencyClosure(expanded)
      }

      val selected = dependencyClosure(required)
      if (selected.forall(isBoundaryRepeatable)) Some(selected) else None
    }
  }

  private def prune(plan: SparkPlan, required: Set[ExprId]): Option[SparkPlan] = plan match {
    case project: GpuProjectExec =>
      val childOutputIds = project.child.output.map(_.exprId).toSet
      val projectOutputIds = project.projectList.map(_.exprId).toSet
      val localOutputIds = projectOutputIds -- childOutputIds
      selectProjectExpressions(project.projectList, localOutputIds, required).flatMap { selected =>
        // Keep project-local dependencies here instead of requesting aliases from the child scan.
        val childRequired = referencedExprIds(selected) -- localOutputIds
        prune(project.child, childRequired).map { child =>
          project.copy(projectList = selected.toList, child = child)
        }
      }

    case filter: GpuFilterExec if filter.condition.deterministic =>
      val childRequired = required ++ referencedExprIds(Seq(filter.condition))
      prune(filter.child, childRequired).map { child =>
        filter.withNewChildren(Seq(child))
      }

    case coalesce: GpuCoalesceBatches =>
      prune(coalesce.child, required).map { child =>
        coalesce.withNewChildren(Seq(child))
      }

    case scan: GpuFileSourceScanExec =>
      pruneFileScan(scan, required)

    case _ =>
      None
  }

  private def pruneFileScan(
      scan: GpuFileSourceScanExec,
      required: Set[ExprId]): Option[GpuFileSourceScanExec] = {
    if (!required.subsetOf(scan.output.map(_.exprId).toSet)) {
      return None
    }

    val dataColumnCount = scan.requiredSchema.length
    val dataPairs = scan.output.take(dataColumnCount).zip(scan.requiredSchema.fields)
    val partitionPairs = scan.output.drop(dataColumnCount).zip(scan.readPartitionSchema.fields)
    val selectedData = dataPairs.filter { case (attr, _) => required.contains(attr.exprId) }
    val selectedPartitions =
      partitionPairs.filter { case (attr, _) => required.contains(attr.exprId) }

    val selectedOutput = selectedData.map(_._1) ++ selectedPartitions.map(_._1)
    if (selectedOutput.map(_.exprId).toSet != required) {
      None
    } else {
      val originalPartitionOutput = scan.originalOutput.drop(dataColumnCount)
      Some(scan.copy(
        originalOutput = selectedData.map(_._1) ++ originalPartitionOutput,
        requiredSchema = StructType(selectedData.map(_._2)),
        requiredPartitionSchema = Some(StructType(selectedPartitions.map(_._2))))(scan.rapidsConf))
    }
  }
}
