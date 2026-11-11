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
  CoalesceGoal, GpuCoalesceBatches, GpuColumnarToRowExec, GpuExec, GpuExpression, GpuFilterExec,
  GpuLiteral, GpuProjectExec, GpuRowToColumnarExec, GpuShuffleCoalesceExec,
  GpuShuffledAsymmetricHashJoinExec, RapidsConf, TargetSize}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute, Expression, ExprId, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.python.BatchEvalPythonExec
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, GpuShuffleEnv}
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution.python.GpuArrowEvalPythonExec
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An auxiliary physical plan used to collect range-partition boundaries.
 *
 * The range exchange exposes this node as a subquery so Spark includes the narrow scan and its
 * normal GPU metrics in the SQL physical plan. The exchange executes it once while constructing
 * its range partitioner; the full-width exchange child remains the source of shuffled rows.
 *
 * This is an internal auxiliary node, not a CPU-to-GPU replacement registered with GpuOverrides.
 * If the auxiliary plan cannot be built, the range exchange samples its original full-width GPU
 * child instead.
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
  // Mirror GpuTransitionOverrides.addPostShuffleCoalesce so the terminal leaf feeds the same
  // batches: coalesce device batches under the RAPIDS shuffle, otherwise coalesce the
  // host-serialized read as it comes back onto the GPU.
  private def wrapPostShuffleCoalesce(
      terminal: SparkPlan, rapidsConf: RapidsConf): SparkPlan = {
    if (GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
      GpuCoalesceBatches(terminal, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
    } else {
      GpuShuffleCoalesceExec(terminal, rapidsConf.gpuTargetBatchSizeBytes)
    }
  }

  // Only a GPU shuffle stage may become a terminal leaf: a reused CPU exchange's columnar
  // path is unsupported and would fail at boundary execution.
  private def isGpuShuffleStage(plan: SparkPlan): Boolean = plan match {
    case stage: ShuffleQueryStageExec =>
      stage.shuffle.isInstanceOf[GpuShuffleExchangeExecBase]
    case _ => false
  }

  private def referencedExprIds(expressions: Seq[Expression]): Set[ExprId] =
    expressions.flatMap(_.references).map(_.exprId).toSet

  def build(plan: SparkPlan, ordering: Seq[SortOrder]): Option[GpuRangeBoundaryExec] = {
    val required = referencedExprIds(ordering)
    if (required.isEmpty) {
      None
    } else {
      val rapidsConf = new RapidsConf(plan.conf)
      val pruneThroughOperators =
        RapidsConf.RANGE_PARTITIONING_PRUNE_THROUGH_OPERATORS.get(plan.conf)
      prune(plan, required, Set.empty, pruneThroughOperators, rapidsConf).flatMap { pruned =>
        val selected = pruned.output.filter(attr => required.contains(attr.exprId))
        if (selected.map(_.exprId).toSet != required) {
          None
        } else {
          val keyOnly = if (selected.length == pruned.output.length) {
            pruned
          } else {
            GpuProjectExec(selected.toList, pruned)()
          }
          Some(GpuRangeBoundaryExec(keyOnly))
        }
      }
    }
  }

  // GpuRand consumes one RNG value per row, independently of batch boundaries, and its
  // checkpoint/restore support preserves that stream through retries. Other nondeterministic
  // expressions (for example partition/batch-dependent IDs) are not safe to prune this way.
  private def isSampleable(expression: Expression): Boolean = expression match {
    case _ if expression.deterministic => true
    case random: GpuRand => random.child match {
      case GpuLiteral(_, IntegerType | LongType) => true
      case _ => false
    }
    case gpu: GpuExpression if !gpu.selfNonDeterministic &&
        gpu.children.exists(child => !child.deterministic) =>
      gpu.children.forall(isSampleable)
    case _ => false
  }

  private def selectProjectExpressions(
      projectList: List[NamedExpression],
      localOutputIds: Set[ExprId],
      required: Set[ExprId],
      deterministicRequired: Set[ExprId])
      : Option[(List[NamedExpression], List[NamedExpression])] = {
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
      val deterministicSelected = dependencyClosure(deterministicRequired)
      // Sampling produces advisory split points; shuffled rows still come from the original
      // child, and the original ordering compares those rows against the collected bounds.
      // A separately evaluated random sort key can affect balance, but cannot change row values
      // or invalidate range ordering. Randomness must not change which rows reach the sampler.
      // Track filter dependencies through aliases and require them to be deterministic.
      if (selected.forall(isSampleable) && deterministicSelected.forall(_.deterministic)) {
        Some((selected, deterministicSelected))
      } else {
        None
      }
    }
  }

  private def prune(
      plan: SparkPlan,
      required: Set[ExprId],
      deterministicRequired: Set[ExprId],
      pruneThroughOperators: Boolean,
      rapidsConf: RapidsConf): Option[SparkPlan] = plan match {
    case project: GpuProjectExec =>
      val childOutputIds = project.child.output.map(_.exprId).toSet
      val projectOutputIds = project.projectList.map(_.exprId).toSet
      val localOutputIds = projectOutputIds -- childOutputIds
      selectProjectExpressions(project.projectList, localOutputIds, required,
          deterministicRequired).flatMap { case (selected, deterministicSelected) =>
        // Keep project-local dependencies here instead of requesting aliases from the child scan.
        val childRequired = referencedExprIds(selected) -- localOutputIds
        val childDeterministicRequired = referencedExprIds(deterministicSelected) -- localOutputIds
        prune(project.child, childRequired, childDeterministicRequired, pruneThroughOperators,
          rapidsConf).map { child =>
          project.withProjectList(selected.toList, newChild = child)
        }
      }

    case filter: GpuFilterExec if filter.condition.deterministic =>
      val filterRequired = referencedExprIds(Seq(filter.condition))
      val childRequired = required ++ filterRequired
      prune(filter.child, childRequired, deterministicRequired ++ filterRequired,
        pruneThroughOperators, rapidsConf).map { child =>
        filter.withNewChildren(Seq(child))
      }

    // Reader and stage terminals come back already coalesced, so a coalesced child would
    // otherwise stack two batch boundaries on the sampling path.
    case coalesce: GpuCoalesceBatches =>
      prune(coalesce.child, required, deterministicRequired, pruneThroughOperators,
        rapidsConf).map { child =>
        child match {
          case inner: GpuCoalesceBatches =>
            GpuCoalesceBatches(inner.child,
              CoalesceGoal.maxRequirement(coalesce.goal, inner.goal))
          case _ => coalesce.withNewChildren(Seq(child))
        }
      }

    // The pruneThroughOperators-gated cases below change which plan computes the sampled keys,
    // which is why they sit behind spark.rapids.sql.rangePartitioning.pruneThroughOperators.

    // A host-serialized shuffle read wraps the exchange in this coalesce, so stepping through
    // it keeps the exchange and the map-side plan (islands included) reachable.
    case shuffleCoalesce: GpuShuffleCoalesceExec if pruneThroughOperators =>
      prune(shuffleCoalesce.child, required, deterministicRequired, pruneThroughOperators,
        rapidsConf)

    // A reader only changes how its child stage's output is partitioned and read. Sampling it
    // preserves the key set but not the key multiset: repeated partition specs (the non-split
    // side of a skew join) repeat rows, which can only reweight boundary placement. Treating
    // the reader and its materialized stage as a terminal leaf skips re-running the join or
    // the Python island that produced the stage input.
    case reader: GpuCustomShuffleReaderExec if pruneThroughOperators &&
        isGpuShuffleStage(reader.child) =>
      Some(wrapPostShuffleCoalesce(reader, rapidsConf))

    // Sampling a materialized stage conserves the key multiset and skips everything upstream
    // of it: the island, join, or map-side plan that produced the stage's input.
    case stage: ShuffleQueryStageExec if pruneThroughOperators && isGpuShuffleStage(stage) =>
      Some(wrapPostShuffleCoalesce(stage, rapidsConf))

    // Row-boundary islands (C2R → BatchEvalPython → R2C) are pass-through only when no range
    // key depends on a Python UDF output and every UDF is deterministic: a UDF-dependent key
    // would change the sampled values, and the boundary plan executes the subtree separately
    // from the shuffle input, so both executions must agree on the keys.
    case python: BatchEvalPythonExec if pruneThroughOperators &&
        python.udfs.forall(_.deterministic) &&
        required.intersect(python.resultAttrs.map(_.exprId).toSet).isEmpty =>
      prune(python.child, required, deterministicRequired, pruneThroughOperators, rapidsConf)

    // Same pass-through for the Arrow-batched Python island, so key-only collection also
    // composes with Arrow-optimized Python UDFs.
    case arrow: GpuArrowEvalPythonExec if pruneThroughOperators &&
        arrow.udfs.forall(_.deterministic) &&
        required.intersect(arrow.resultAttrs.map(_.exprId).toSet).isEmpty =>
      prune(arrow.child, required, deterministicRequired, pruneThroughOperators, rapidsConf)

    // The island nodes are dropped rather than rewrapped: keeping R2C/C2R around the pruned
    // child would add a pointless column→row→column round-trip to the sampling path, which
    // must stay columnar.
    case r2c: GpuRowToColumnarExec if pruneThroughOperators =>
      prune(r2c.child, required, deterministicRequired, pruneThroughOperators, rapidsConf)

    case c2r: GpuColumnarToRowExec if pruneThroughOperators =>
      prune(c2r.child, required, deterministicRequired, pruneThroughOperators, rapidsConf)

    // A GPU shuffle conserves rows, so the boundary plan can drop the exchange and sample its
    // map-side child directly instead of re-shuffling inside the auxiliary subquery.
    case exch: GpuShuffleExchangeExecBase if pruneThroughOperators =>
      prune(exch.child, required, deterministicRequired, pruneThroughOperators, rapidsConf)

    // LeftOuter keeps every unmatched left row and repeats each matched left row once per
    // matching right row, so the key set stays the left leg's and duplicate right-side keys
    // can only reweight boundary placement. Any other join type, or a key set that needs the
    // right side, falls back to full-width sampling.
    case join: GpuShuffledAsymmetricHashJoinExec if pruneThroughOperators &&
        join.joinType == LeftOuter &&
        required.subsetOf(join.left.output.map(_.exprId).toSet) =>
      prune(join.left, required, deterministicRequired, pruneThroughOperators, rapidsConf)

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
        requiredPartitionSchema = Some(StructType(selectedPartitions.map(_._2))))(
        scan.rapidsConf, scan.partitionPlanningScan))
    }
  }
}
