/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.mutable

import com.nvidia.spark.rapids.GpuTypedImperativeSupportedAggregateExecMeta.{preRowToColProjection, readBufferConverter}
import com.nvidia.spark.rapids.delta.DeltaProvider
import com.nvidia.spark.rapids.lore.GpuLore
import com.nvidia.spark.rapids.shims.{GpuBatchScanExec, SparkShimImpl}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, BoundReference, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExecBase, DropTableExec, ShowTablesExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ENSURE_REQUIREMENTS,
  Exchange, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.rapids.{GpuDataSourceScanExec, GpuFileSourceScanExec, GpuShuffleEnv, GpuTaskMetrics}
import org.apache.spark.sql.rapids.execution.{ExchangeMappingCache, GpuBroadcastExchangeExec, GpuBroadcastExchangeExecBase, GpuBroadcastToRowExec, GpuCustomShuffleReaderExec, GpuHashJoin, GpuShuffleExchangeExecBase, GpuSubqueryBroadcastExec}
import org.apache.spark.sql.types.StructType

/**
 * Rules that run after the row to columnar and columnar to row transitions have been inserted.
 * These rules insert transitions to and from the GPU, and then optimize various transitions.
 */
class GpuTransitionOverrides(sparkSession: SparkSession = null) extends Rule[SparkPlan] {
  // previous name of the field `conf` collides with Rule#conf as of Spark 3.1.1
  var rapidsConf: RapidsConf = null

  def optimizeGpuPlanTransitions(plan: SparkPlan): SparkPlan = plan match {
    case HostColumnarToGpu(r2c: RowToColumnarExec, goal) =>
      val optimizedChild = optimizeGpuPlanTransitions(r2c.child)
      val projectedChild = checkInjectedProject(optimizedChild, optimizedChild, isRowToCol = true)
      GpuRowToColumnarExec(projectedChild, goal)
    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      GpuColumnarToRowExec(optimizeGpuPlanTransitions(bb.child))
    case p if p.children.size == 1 =>
      // single child case, need to check over GpuColumnarToRowExec for injected projects
      val optChild = optimizeGpuPlanTransitions(p.children.head)
      if (optChild.isInstanceOf[GpuColumnarToRowExec]) {
        // inserts postColumnarToRowTransition into newly-created GpuColumnarToRowExec
        p.withNewChildren(Array(checkInjectedProject(p, optChild, isRowToCol = false)))
      } else {
        p.withNewChildren(Array(optChild))
      }
    case p => // default case
      p.withNewChildren(p.children.map(optimizeGpuPlanTransitions))
  }

  /** Adds the appropriate coalesce after a shuffle depending on the type of shuffle configured */
  private def addPostShuffleCoalesce(plan: SparkPlan): SparkPlan = {
    if (GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
      GpuCoalesceBatches(plan, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
    } else {
      GpuShuffleCoalesceExec(plan, rapidsConf.gpuTargetBatchSizeBytes)
    }
  }

  /**
   * Apply the potentially injected ProjectExec from TreeNodeTag
   */
  private def checkInjectedProject(srcPlan: SparkPlan,
      dstPlan: SparkPlan,
      isRowToCol: Boolean): SparkPlan = {
    readBufferConverter(srcPlan, isRowToCol) match {
      case Some(expressions) => ProjectExec(expressions, dstPlan)
      case None => dstPlan
    }
  }

  def optimizeAdaptiveTransitions(
      plan: SparkPlan,
      parent: Option[SparkPlan]): SparkPlan = plan match {

    case bb @ GpuBringBackToHost(child) if parent.isEmpty =>
      // This is hacky but we need to remove the GpuBringBackToHost from the final
      // query stage, if there is one. It gets inserted by
      // GpuTransitionOverrides.insertColumnarFromGpu around columnar adaptive
      // plans when we are writing to columnar formats on the GPU. It would be nice to avoid
      // inserting it in the first place but we just don't have enough context
      // at the time GpuTransitionOverrides is applying rules.
      optimizeAdaptiveTransitions(child, Some(bb))

    // HostColumnarToGpu(RowToColumnarExec(..)) => GpuRowToColumnarExec(..)
    case HostColumnarToGpu(r2c: RowToColumnarExec, goal) =>
      val child = optimizeAdaptiveTransitions(r2c.child, Some(r2c))
      child match {
        case a: AdaptiveSparkPlanExec =>
          // we hit this case when we have an adaptive plan wrapped in a write
          // to columnar file format on the GPU
          val columnarAdaptivePlan = SparkShimImpl.columnarAdaptivePlan(a, goal)
          optimizeAdaptiveTransitions(columnarAdaptivePlan, None)
        case _ =>
          val newChild = checkInjectedProject(child, child, isRowToCol = true)
          GpuRowToColumnarExec(newChild, goal)
      }

    // adaptive plan final query stage with columnar output
    case r2c @ RowToColumnarExec(child) if parent.isEmpty =>
      val optimizedChild = optimizeAdaptiveTransitions(child, Some(r2c))
      val projectedChild = checkInjectedProject(optimizedChild, optimizedChild, isRowToCol = true)
      GpuRowToColumnarExec(projectedChild, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))

    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      // We typically want the final operator in the plan (the operator that has no parent) to be
      // wrapped in `ColumnarToRowExec(GpuBringBackToHost(_))` operators to
      // bring the data back onto the host and be translated to rows so that it can be returned
      // from the Spark API. However, in the case of AQE, each exchange operator is treated as an
      // individual query with no parent and we need to remove these operators in this case
      // because we need to return an operator that implements `BroadcastExchangeLike` or
      // `ShuffleExchangeLike`.
      bb.child match {
        case GpuShuffleCoalesceExec(e: GpuShuffleExchangeExecBase, _)
            if GpuTransitionOverrides.canExposeExchangeForAqeStage(e, parent) =>
          // The coalesce step gets added back into the plan later on, in a
          // future query stage that reads the output from this query stage. This
          // is handled in the case clauses below.
          e.withNewChildren(e.children.map(c => optimizeAdaptiveTransitions(c, Some(e))))
        case GpuCoalesceBatches(e: GpuShuffleExchangeExecBase, _)
            if GpuTransitionOverrides.canExposeExchangeForAqeStage(e, parent) =>
          // The coalesce step gets added back into the plan later on, in a
          // future query stage that reads the output from this query stage. This
          // is handled in the case clauses below.
          e.withNewChildren(e.children.map(c => optimizeAdaptiveTransitions(c, Some(e))))
        case _ => optimizeAdaptiveTransitions(bb.child, Some(bb)) match {
          case e: GpuBroadcastExchangeExecBase
              if GpuTransitionOverrides.canExposeExchangeForAqeStage(e, parent) => e
          case e: GpuShuffleExchangeExecBase
              if GpuTransitionOverrides.canExposeExchangeForAqeStage(e, parent) => e
          case other => GpuColumnarToRowExec(other)
        }
      }

    case s: ShuffleQueryStageExec =>
      // When reading a materialized shuffle query stage in AQE mode, we need to insert an
      // operator to coalesce batches. We either insert it directly around the shuffle query
      // stage, or around the custom shuffle reader, if one exists.
      val plan = GpuTransitionOverrides.getNonQueryStagePlan(s)
      if (plan.supportsColumnar && plan.isInstanceOf[GpuExec]) {
        (plan, parent) match {
          case (_, Some(x)) if SparkShimImpl.isCustomReaderExec(x) =>
            // We can't insert a coalesce batches operator between a custom shuffle reader
            // and a shuffle query stage, so we instead insert it around the custom shuffle
            // reader later on, in the next top-level case clause.
            s
          case (ex: ShuffleExchangeLike, Some(x))
              if SparkShimImpl.shuffleParentReadsShuffleData(ex, x) =>
            // In some cases, the parent might have to read the shuffle data directly, so
            // we don't need the post-shuffle coalesce exec since the parent should 
            // coalesce the shuffle data as needed
            s
          case _ =>
            // Directly wrap shuffle query stage with coalesce batches operator
            addPostShuffleCoalesce(s)
        }
      } else {
        readBufferConverter(s.plan, isR2C = true).foreach { p =>
          s.setTagValue(preRowToColProjection, p -> 0)
        }
        s
      }

    case e: GpuCustomShuffleReaderExec =>
      // We wrap custom shuffle readers with a coalesce batches operator here.
      addPostShuffleCoalesce(e.copy(child = optimizeAdaptiveTransitions(e.child, Some(e))))

    // Leave TableCacheQueryStage wrapper intact and let shims decide behavior.
    case p if SparkShimImpl.getTableCacheNonQueryStagePlan(p).nonEmpty =>
      p.withNewChildren(p.children.map(c => optimizeAdaptiveTransitions(c, Some(p))))

    case c2re: ColumnarToRowExec if
        SparkShimImpl.checkCToRWithExecBroadcastAQECoalPart(c2re, parent) =>
      val shuffle = SparkShimImpl.getShuffleFromCToRWithExecBroadcastAQECoalPart(c2re)
      shuffle match {
        case Some(s) =>
            /*
             * When we find this pattern, explicitly add in the GPU columnar to row and CPU
             * exchange for executor broadcast.
             * Note that this likely adds some performance overhead because we end up doing
             * an extra exchange.
             *
             * +- Exchange (SinglePartition, EXECUTOR_BROADCAST)
             *     +- GpuColumnarToRow
             *         +- GpuShuffleCoalesce
             *             +- ShuffleQueryStage
             *                 +- GpuColumnarExchange
             */
          val gpuc2r = GpuColumnarToRowExec(optimizeAdaptiveTransitions(s, Some(plan)))
          SparkShimImpl.addExecBroadcastShuffle(gpuc2r)
        case _ => c2re
      }

    case ColumnarToRowExec(e: ShuffleQueryStageExec) =>
      val c2r = GpuColumnarToRowExec(optimizeAdaptiveTransitions(e, Some(plan)))
      SparkShimImpl.addRowShuffleToQueryStageTransitionIfNeeded(c2r, e)

    // If Spark wrapped a TableCache query stage with a CPU ColumnarToRow, replace it with
    // a GPU ColumnarToRow to avoid CPU accessing GPU vectors.
    case ColumnarToRowExec(e) if SparkShimImpl.getTableCacheNonQueryStagePlan(e).nonEmpty =>
      GpuColumnarToRowExec(optimizeAdaptiveTransitions(e, Some(plan)))

    case ColumnarToRowExec(e: BroadcastQueryStageExec) =>
      // In Spark an AdaptiveSparkPlanExec when doing a broadcast expects to only ever see
      // a BroadcastQueryStageExec as its child. The ColumnarToRowExec can be inserted by Spark
      // when it sees us doing columnar processing and the output expects rows. But we cannot
      // keep the ColumnarToRowExec there, nor can we insert in a GpuBroadcastToRowExec in its
      // place. We have to rearrange the order of operations so it is a child of the
      // BroadcastQueryStageExec (which will likely involve another broadcast)
      e.plan match {
        case ReusedExchangeExec(output, b: GpuBroadcastExchangeExec) => 
          // we can't directly re-use a GPU broadcast exchange to feed a CPU broadcast
          // join but Spark will sometimes try and do this
          val keys = output.map { a => a.asInstanceOf[Expression] }
          SparkShimImpl.newBroadcastQueryStageExec(e,
            GpuBroadcastToRowExec(keys, b.mode, e.plan))
        case b: GpuBroadcastExchangeExec =>
          // This is very rare, but it can happen
          val keys = b.output.map { a => a.asInstanceOf[Expression] }
          SparkShimImpl.newBroadcastQueryStageExec(e,
            GpuBroadcastToRowExec(keys, b.mode, e.plan))
        case other =>
          throw new IllegalStateException(s"Don't know how to handle a " +
            s"BroadcastQueryStageExec with $other")
      }

    case p if p.children.length == 1 => // single child case
      val optChild = optimizeAdaptiveTransitions(p.children.head, Some(p))
      if (optChild.isInstanceOf[GpuColumnarToRowExec]) {
        val projectedChild = checkInjectedProject(p, optChild, isRowToCol = false)
        p.withNewChildren(Array(projectedChild))
      } else {
        p.withNewChildren(Array(optChild))
      }

    case p => // default case
      p.withNewChildren(p.children.map(c => optimizeAdaptiveTransitions(c, Some(p))))
  }

  /**
   * Fixes up instances of HostColumnarToGpu that are operating on nested types.
   * There are no batch methods to access nested types in Spark's ColumnVector, and as such
   * HostColumnarToGpu does not support nested types due to the performance problem. If there's
   * nested types involved, use a CPU columnar to row transition followed by a GPU row to
   * columnar transition which is a more optimized code path for these types.
   * This is done as a fixup pass since there are earlier transition optimizations that are
   * looking for HostColumnarToGpu when optimizing transitions.
   */
  def fixupHostColumnarTransitions(plan: SparkPlan): SparkPlan = plan match {
    case HostColumnarToGpu(child, goal) if DataTypeUtils.hasNestedTypes(child.schema) =>
      GpuRowToColumnarExec(ColumnarToRowExec(fixupHostColumnarTransitions(child)), goal)
    case p => p.withNewChildren(p.children.map(fixupHostColumnarTransitions))
  }

  @tailrec
  private def isGpuShuffleLike(execNode: SparkPlan): Boolean = execNode match {
    case _: GpuShuffleExchangeExecBase | _: GpuCustomShuffleReaderExec => true
    case qs: ShuffleQueryStageExec => isGpuShuffleLike(qs.plan)
    case _ => false
  }

  /**
   * This optimizes the plan to remove [[GpuCoalesceBatches]] nodes that are unnecessary
   * or undesired in some situations.
   *
   * @note This does not examine [[GpuShuffleCoalesceExec]] nodes in the plan, as they
   *       are always required after GPU columnar exchanges during normal shuffle
   *       to place the data after shuffle on the GPU. Those nodes also do not
   *       coalesce to the same goal as used by [[GpuCoalesceBatches]], so a
   *       [[GpuShuffleCoalesceExec]] immediately followed by a [[GpuCoalesceBatches]] is
   *       not unusual.
   */
  def optimizeCoalesce(plan: SparkPlan): SparkPlan = plan match {
    case c2r @ GpuColumnarToRowExec(gpuCoalesce: GpuCoalesceBatches, _)
      if !isGpuShuffleLike(gpuCoalesce.child) =>
        // Don't build a batch if we are just going to go back to ROWS
        // and there isn't a GPU shuffle involved
        c2r.withNewChildren(gpuCoalesce.children.map(optimizeCoalesce))
    case GpuCoalesceBatches(r2c: GpuRowToColumnarExec, goal: TargetSize) =>
      // TODO in the future we should support this for all goals, but
      // GpuRowToColumnarExec preallocates all of the memory, and the builder does not
      // support growing the sizes dynamically....

      // Don't build batches and then coalesce, just build the right sized batch
      GpuRowToColumnarExec(optimizeCoalesce(r2c.child),
        CoalesceGoal.maxRequirement(goal, r2c.goal).asInstanceOf[CoalesceSizeGoal])
    case GpuCoalesceBatches(co: GpuCoalesceBatches, goal) =>
      GpuCoalesceBatches(optimizeCoalesce(co.child), CoalesceGoal.maxRequirement(goal, co.goal))
    case GpuCoalesceBatches(child: GpuExec, goal)
      if CoalesceGoal.satisfies(child.outputBatching, goal) =>
      // The goal is already satisfied so remove the batching
      child.withNewChildren(child.children.map(optimizeCoalesce))
    case p =>
      p.withNewChildren(p.children.map(optimizeCoalesce))
  }

  /**
   * Removes `GpuCoalesceBatches(GpuShuffleCoalesceExec(build side))` from either side of a
   * GpuShuffledInnerHashJoinExec since that node handles shuffled data directly. For other joins,
   * it removes it for for the build side. The coalesce logic has been moved to the
   * `GpuShuffleCoalesceExec` class, and is handled differently to prevent holding onto the
   * GPU semaphore for stream IO.
   */
  def shuffledHashJoinOptimizeShuffle(plan: SparkPlan): SparkPlan = {
    plan match {
      case j: GpuShuffledSizedHashJoinExec[_] =>
        val newChildren = Seq(j.left, j.right).map {
          case GpuCoalesceBatches(GpuShuffleCoalesceExec(c, _), _) => c
          case GpuShuffleCoalesceExec(c, _) => c
          case c => c
        }.map(shuffledHashJoinOptimizeShuffle)
        j.withNewChildren(newChildren)
      case x@GpuShuffledHashJoinExec(
          _, _, _, buildSide, _,
          left: GpuShuffleCoalesceExec,
          GpuCoalesceBatches(GpuShuffleCoalesceExec(rc, _), _), _, _)
          if buildSide == GpuBuildRight && rapidsConf.shuffledHashJoinOptimizeShuffle =>
        x.withNewChildren(
          Seq(shuffledHashJoinOptimizeShuffle(left), shuffledHashJoinOptimizeShuffle(rc)))
      case x@GpuShuffledHashJoinExec(
          _, _, _, buildSide, _,
          GpuCoalesceBatches(GpuShuffleCoalesceExec(lc, _), _),
          right: GpuShuffleCoalesceExec, _, _)
          if buildSide == GpuBuildLeft && rapidsConf.shuffledHashJoinOptimizeShuffle =>
        x.withNewChildren(
          Seq(shuffledHashJoinOptimizeShuffle(lc), shuffledHashJoinOptimizeShuffle(right)))
      case p => p.withNewChildren(p.children.map(shuffledHashJoinOptimizeShuffle))
    }
  }

  private def insertCoalesce(plans: Seq[SparkPlan], goals: Seq[CoalesceGoal],
      disableUntilInput: Boolean): Seq[SparkPlan] = {
    plans.zip(goals).map {
      case (plan, null) =>
        // No coalesce requested
        insertCoalesce(plan, disableUntilInput)
      case (plan, goal: RequireSingleBatchLike) =>
        // Even if coalesce is disabled a single batch is required to make this operator work
        // This should not cause bugs because we require a single batch in situations where
        // Spark also buffers data, so any operator that needs coalesce disabled would also
        // get an incorrect answer in regular Spark
        GpuCoalesceBatches(insertCoalesce(plan, disableUntilInput), goal)
      case (plan, _) if disableUntilInput =>
        // We wanted to coalesce the input but cannot because it could cause errors
        insertCoalesce(plan, disableUntilInput)
      case (plan, goal) =>
        GpuCoalesceBatches(insertCoalesce(plan, disableUntilInput), goal)
    }
  }

  /**
   * Essentially check if this plan is in the same task as a file input.
   */
  private def hasDirectLineToInput(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => false
    case _: DataSourceScanExec => true
    case _: GpuDataSourceScanExec => true
    case _: DataSourceV2ScanExecBase => true
    case _: RDDScanExec => true // just in case an RDD was reading in data
    case p => p.children.exists(hasDirectLineToInput)
  }

  /**
   * Essentially check if we have hit a boundary of a task.
   */
  private def shouldEnableCoalesce(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => true
    case _: DataSourceScanExec => true
    case _: GpuDataSourceScanExec => true
    case _: DataSourceV2ScanExecBase => true
    case _: RDDScanExec => true // just in case an RDD was reading in data
    case _: ExpandExec => true
    case _ => false
  }

  /**
   * Because we cannot change the executors in spark itself we need to try and account for
   * the ones that might have issues with coalesce here.
   */
  private def disableCoalesceUntilInput(plan: SparkPlan): Boolean = {
    InputFileBlockRule.hasInputFileExpression(plan)
  }

  private def disableScanUntilInput(plan: SparkPlan): Boolean = {
    InputFileBlockRule.hasInputFileExpression(plan)
  }

  // This walks from the output to the input to look for any uses of InputFileName,
  // InputFileBlockStart, or InputFileBlockLength when we use a Parquet read because
  // we can't support the coalesce file reader optimization when this is used.
  private def updateScansForInputAndOrder(plan: SparkPlan,
      disableUntilInput: Boolean = false): SparkPlan = {
    plan match {
      case batchScan: GpuBatchScanExec =>
        if (disableUntilInput || disableScanUntilInput(batchScan)) {
          val newScan = batchScan.scan.withInputFile()
          if (newScan ne batchScan.scan) {
            batchScan.copy(scan = newScan)
          } else {
            batchScan
          }
        } else {
          batchScan
        }
      case fileSourceScan: GpuFileSourceScanExec =>
        if ((disableUntilInput || disableScanUntilInput(fileSourceScan))) {
          fileSourceScan.copy(queryUsesInputFile = true)(fileSourceScan.rapidsConf)
        } else {
          fileSourceScan
        }
      case p =>
        val planDisableUntilInput = disableScanUntilInput(p) && hasDirectLineToInput(p)
        p.withNewChildren(p.children.map(c => {
          updateScansForInputAndOrder(c, planDisableUntilInput || disableUntilInput)
        }))
    }
  }

  def extractAttrReferences[A <: Expression](expression: A): Seq[AttributeReference] = {
    expression match {
      case a: AttributeReference =>
        Seq(a)
      case other: Expression =>
        other.children.flatMap(extractAttrReferences)
    }
  }

  private def withPrunedPartSchema(
      fss: GpuFileSourceScanExec,
      referenceList: Seq[Expression]): GpuFileSourceScanExec = {
    // Luckily partition columns do not support nested types. So only need to prune the
    // top level columns.
    // Prune the output of the Scan so it only includes things that the referenceList will
    // actually read
    val neededExprIds = referenceList.flatMap(extractAttrReferences).map(_.exprId).toSet
    val partOutAttrs = fss.output.drop(fss.requiredSchema.length)
    val neededPartIndexes = partOutAttrs.zipWithIndex.collect{
      case (provided, index) if neededExprIds.contains(provided.exprId) => index
    }

    val prunedPartSchema = Some(StructType(neededPartIndexes.map(fss.relation.partitionSchema)))
    fss.copy(requiredPartitionSchema = prunedPartSchema)(fss.rapidsConf)
  }

  // This tries to prune the partition schema for GpuFileSourceScanExec by leveraging
  // the project list of the first GpuProjectExec after a GpuFileSourceScanExec.
  private def prunePartitionForFileSourceScan(plan: SparkPlan): SparkPlan = plan match {
    case p @ GpuProjectExecLike(projectList, fss: GpuFileSourceScanExec) =>
      // A ProjectExec next to FileSourceScanExec, for cases like
      //   df.groupBy("b").agg(max($"a")), or
      //   df.select("b")
      p.withNewChildren(Seq(withPrunedPartSchema(fss, projectList)))

    case p @ GpuProjectExecLike(
        projectList,
        f @ GpuFilterExec(condition, fss: GpuFileSourceScanExec)) =>
      // A FilterExec is between the ProjectExec and FileSourceScanExec, for cases like
      //   df.select("a").filter("a != 1")
      p.withNewChildren(Seq(
        f.withNewChildren(Seq(withPrunedPartSchema(fss, projectList :+ condition)))))

    // Partial GPU plan cases.
    // This rule executes before rules override the ColumnarToRowExec, so the exec is the
    // CPU version here.
    case p @ ProjectExec(projectList, cr @ ColumnarToRowExec(fss: GpuFileSourceScanExec)) =>
      // cpu project + gpu file scan
      p.withNewChildren(Seq(
        cr.withNewChildren(Seq(withPrunedPartSchema(fss, projectList)))))

    case p @ ProjectExec(
        projectList,
        cr @ ColumnarToRowExec(f @ GpuFilterExec(condition, fss: GpuFileSourceScanExec))) =>
      // cpu project + gpu filter + gpu file scan
      p.withNewChildren(Seq(
        cr.withNewChildren(Seq(
          f.withNewChildren(Seq(withPrunedPartSchema(fss, projectList :+ condition)))))))

    case p @ ProjectExec(
        projectList,
        f @ FilterExec(condition, cr @ ColumnarToRowExec(fss: GpuFileSourceScanExec))) =>
      // cpu project + cpu filter + gpu file scan
      p.withNewChildren(Seq(
        f.withNewChildren(Seq(
          cr.withNewChildren(Seq(withPrunedPartSchema(fss, projectList :+ condition)))))))

    case p @ GpuProjectExecLike(
        projectList,
        rc @ RowToColumnarExec(
            f @ FilterExec(condition, cr @ ColumnarToRowExec(fss: GpuFileSourceScanExec)))) =>
      // gpu project + cpu filter + gpu file scan
      p.withNewChildren(Seq(
        rc.withNewChildren(Seq(
          f.withNewChildren(Seq(
            cr.withNewChildren(Seq(withPrunedPartSchema(fss, projectList :+ condition)))))))))

    case _ =>
      plan.withNewChildren(plan.children.map(prunePartitionForFileSourceScan))
  }

  // This walks from the output to the input so disableUntilInput can walk its way from when
  // we hit something that cannot allow for coalesce up until the input
  private def insertCoalesce(plan: SparkPlan,
      disableUntilInput: Boolean = false): SparkPlan = plan match {
    case exec: GpuExec =>
      // We will disable coalesce if it is already disabled and we cannot re-enable it
      val shouldDisable = (disableUntilInput && !shouldEnableCoalesce(exec)) ||
        //or if we should disable it and it is in a stage with a file input that would matter
        (exec.disableCoalesceUntilInput() && hasDirectLineToInput(exec))
      val tmp = exec.withNewChildren(insertCoalesce(exec.children, exec.childrenCoalesceGoal,
        shouldDisable))
      if (exec.coalesceAfter && !shouldDisable) {
        GpuCoalesceBatches(tmp, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
      } else {
        tmp
      }
    case p =>
      // We will disable coalesce if it is already disabled and we cannot re-enable it
      val shouldDisable = disableUntilInput && !shouldEnableCoalesce(p)  ||
        //or if we should disable it and it is in a stage with a file input that would matter
        (disableCoalesceUntilInput(p) && hasDirectLineToInput(p))
      p.withNewChildren(p.children.map(c => insertCoalesce(c, shouldDisable)))
  }

  /**
   * Inserts a shuffle coalesce after every shuffle to coalesce the serialized tables
   * on the host before copying the data to the GPU.
   * @note This should not be used in combination with the RAPIDS shuffle.
   */
  private def insertShuffleCoalesce(plan: SparkPlan): SparkPlan = plan match {
    case exec: GpuShuffleExchangeExecBase =>
      // always follow a GPU shuffle with a shuffle coalesce
      GpuShuffleCoalesceExec(exec.withNewChildren(exec.children.map(insertShuffleCoalesce)),
        rapidsConf.gpuTargetBatchSizeBytes)
    case exec => exec.withNewChildren(plan.children.map(insertShuffleCoalesce))
  }

  /**
   * Inserts a transition to be running on the CPU columnar
   */
  private def insertColumnarFromGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && plan.isInstanceOf[GpuExec]) {
      GpuBringBackToHost(insertColumnarToGpu(plan))
    } else if (plan.isInstanceOf[ColumnarToRowTransition] && plan.isInstanceOf[GpuExec]) {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarFromGpu))
    }
  }

  /** Returns true if the plan is a host columnar plan */
  private def isHostColumnar(plan: SparkPlan): Boolean = {
    def isGpuAdaptivePlan(plan: SparkPlan): Boolean = plan match {
      case ap: AdaptiveSparkPlanExec => GpuOverrides.probablyGpuPlan(ap, rapidsConf)
      case _ => false
    }
    plan.supportsColumnar && !plan.isInstanceOf[GpuExec] && !isGpuAdaptivePlan(plan)
  }

  /**
   * Inserts a transition to be running on the GPU from CPU columnar
   */
  private def insertColumnarToGpu(plan: SparkPlan): SparkPlan = {
    val nonQueryStagePlan = GpuTransitionOverrides.getNonQueryStagePlan(plan)
    if (isHostColumnar(nonQueryStagePlan)) {
      HostColumnarToGpu(insertColumnarFromGpu(plan), TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    }
  }

  // If a GPU hash-based operation, such as GpuHashJoin or GpuHashAggregateExec,
  // is followed eventually by a data writing command without an intermediate node
  // changing the sort order, insert a sort to optimize the output file size.
  private def insertHashOptimizeSorts(plan: SparkPlan,
      hasWriteParent: Boolean = false): SparkPlan = {
    if (rapidsConf.enableHashOptimizeSort) {
      // Insert a sort after the last hash-based op before the query result if there are no
      // intermediate nodes that have a specified sort order. This helps with the size of
      // Parquet and ORC files.
      // Note that this is using a GPU SortOrder expression as the CPU SortOrder which should
      // normally be avoided. However since we have checked that no node later in the plan
      // needs a particular sort order, it should not be a problem in practice that would
      // trigger a redundant sort in the plan.
      plan match {
        // look for any writing command, not just a GPU writing command
        case _: GpuDataWritingCommandExec | _: DataWritingCommandExec =>
          plan.withNewChildren(plan.children.map(c => insertHashOptimizeSorts(c, true)))
        case _: GpuHashJoin | _: GpuHashAggregateExec if hasWriteParent =>
          val gpuSortOrder = getOptimizedSortOrder(plan)
          GpuSortExec(gpuSortOrder, false, plan, SortEachBatch)(gpuSortOrder)
        case _: GpuHashJoin | _: GpuHashAggregateExec => plan
        case p =>
          if (p.outputOrdering.isEmpty) {
            plan.withNewChildren(plan.children.map(c => insertHashOptimizeSorts(c, hasWriteParent)))
          } else {
            plan
          }
      }
    } else {
      plan
    }
  }

  private def getOptimizedSortOrder(plan: SparkPlan): Seq[SortOrder] = {
    plan.output.map { expr =>
      val wrapped = GpuOverrides.wrapExpr(expr, rapidsConf, None)
      wrapped.tagForGpu()
      assert(wrapped.canThisBeReplaced)
      SortOrder(wrapped.convertToGpu(), Ascending)
    }
  }

  def assertIsOnTheGpu(exp: Expression, conf: RapidsConf): Unit = {
    // There are no GpuAttributeReference or GpuSortOrder
    exp match {
      case _: AttributeReference | _: SortOrder =>
        // These are always allowed
      case bridge: GpuCpuBridgeExpression =>
        // For bridge expressions, validate the CPU expressions inside
        assertBridgeExpressionsAllowed(bridge, conf)
      case _: BoundReference | _: Literal =>
          // These are always allowed, and ignored
      case _: GpuExpression =>
        // Regular GPU expressions are allowed
      case _ =>
        val classBaseName = PlanUtils.getBaseNameFromClass(exp.getClass.toString)
        if (!conf.testingAllowedNonGpu.contains(classBaseName)) {
          throw new IllegalArgumentException(s"The expression $exp is not columnar ${exp.getClass}")
        }
    }
    exp.children.foreach(subExp => assertIsOnTheGpu(subExp, conf))
  }

  /**
   * Validates that all CPU expressions within a GpuCpuBridgeExpression are allowed in test mode.
   * This function recursively traverses the CPU expression tree inside the bridge and checks
   * each CPU expression against the testingAllowedNonGpu allowlist.
   */
  def assertBridgeExpressionsAllowed(bridge: GpuCpuBridgeExpression, conf: RapidsConf): Unit = {
    val disallowedExprs = scala.collection.mutable.ListBuffer[String]()
    val allowedExprs = scala.collection.mutable.ListBuffer[String]()

    def collectCpuExpressions(expr: Expression, path: String = ""): Unit = {
      val currentPath = if (path.isEmpty) {
        expr.getClass.getSimpleName
      } else {
        s"$path.${expr.getClass.getSimpleName}"
      }

      expr match {
        case _: Literal | _: BoundReference => ()
        case _ =>
          val classBaseName = PlanUtils.getBaseNameFromClass(expr.getClass.toString)
          if (conf.testingAllowedNonGpu.contains(classBaseName)) {
            allowedExprs += s"$currentPath ($classBaseName) [ALLOWED]"
          } else {
            disallowedExprs += s"$currentPath ($classBaseName) [NOT ALLOWED]"
          }
      }

      expr.children.zipWithIndex.foreach { case (child, index) =>
        collectCpuExpressions(child, s"$currentPath.child[$index]")
      }
    }

    collectCpuExpressions(bridge.cpuExpression)

    if (disallowedExprs.nonEmpty) {
      val errorMessage = new StringBuilder()
      errorMessage.append(s"GpuCpuBridgeExpression contains disallowed CPU expressions:\n")
      errorMessage.append(s"Bridge: ${bridge.toString}\n")
      errorMessage.append(s"CPU Expression Tree Analysis:\n")

      // Show disallowed expressions first
      errorMessage.append("  DISALLOWED EXPRESSIONS:\n")
      disallowedExprs.foreach(expr => errorMessage.append(s"    - $expr\n"))

      // When everything is allowed there is nothing to report; allowed expressions are only
      // included as context when reporting a disallowed expression.
      if (allowedExprs.nonEmpty) {
        errorMessage.append("  ALLOWED EXPRESSIONS (for context):\n")
        allowedExprs.foreach(expr => errorMessage.append(s"    - $expr\n"))
      }

      throw new IllegalArgumentException(errorMessage.toString())
    }
  }

  def assertIsOnTheGpu(plan: SparkPlan, conf: RapidsConf): Unit = {
    def isTestExempted(plan: SparkPlan): Boolean = {
      conf.testingAllowedNonGpu.exists(PlanUtils.sameClass(plan, _))
    }
    val isAdaptiveEnabled = plan.conf.adaptiveExecutionEnabled
    plan match {
      case _: BroadcastExchangeLike if isAdaptiveEnabled =>
        // broadcasts are left on CPU for now when AQE is enabled
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec
          if isAdaptiveEnabled =>
        // broadcasts are left on CPU for now when AQE is enabled
      case p if SparkShimImpl.isAqePlan(p)  =>
        // we do not yet fully support GPU-acceleration when AQE is enabled, so we skip checking
        // the plan in this case - https://github.com/NVIDIA/spark-rapids/issues/5
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case imts: InMemoryTableScanExec =>
        if (!imts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to InMemoryTableScanExec ${imts.expressions.mkString(",")}")
        }
      // some metadata operations, may add more when needed
      case _: ShowTablesExec =>
      case _: DropTableExec =>
      case _: RDDScanExec => () // Ignored
      case p if SparkShimImpl.skipAssertIsOnTheGpu(p) => () // Ignored
      case p: ExecutedCommandExec if !isTestExempted(p) =>
        val meta = GpuOverrides.wrapPlan(p, conf, None)
        if (!meta.suppressWillWorkOnGpuInfo) {
          throw new IllegalArgumentException("Part of the plan is not columnar " +
              s"${p.getClass}\n$p")
        }
      case other =>
        if (!plan.isInstanceOf[GpuExec] &&
          !isTestExempted(plan) &&
          !conf.testingAllowedNonGpu.contains(
            PlanUtils.getBaseNameFromClass(other.getClass.toString))) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar " +
            s"${plan.getClass}\n${plan}")
        }
        // Check child expressions if this is a GPU node
        plan match {
          case gpuExec: GpuExec =>
            // filter out the output expressions since those are not GPU expressions
            val planOutput = gpuExec.output.toSet
            gpuExec.gpuExpressions.filter(_ match {
                case a: Attribute => !planOutput.contains(a)
                case _ => true
            }).foreach(assertIsOnTheGpu(_, conf))
          case _ =>
        }
    }
    plan.children.foreach(assertIsOnTheGpu(_, conf))
  }

  /**
   * This is intended for testing only and this only supports looking for an exec once.
   */
  private def validateExecsInGpuPlan(plan: SparkPlan, conf: RapidsConf): Unit = {
    val validateExecs = conf.validateExecsInGpuPlan.toSet
    if (validateExecs.nonEmpty) {
      def planContainsInstanceOf(plan: SparkPlan): Boolean = {
        validateExecs.contains(plan.getClass.getSimpleName)
      }
      // to set to make uniq execs
      val execsFound = PlanUtils.findOperators(plan, planContainsInstanceOf).toSet
      val execsNotFound = validateExecs.diff(execsFound.map(_.getClass.getSimpleName))
      require(execsNotFound.isEmpty,
        s"Plan ${plan.toString()} does not contain the following execs: " +
        execsNotFound.mkString(","))
    }
  }

  def detectAndTagFinalColumnarOutput(plan: SparkPlan): SparkPlan = plan match {
    case d: DeserializeToObjectExec if d.child.isInstanceOf[GpuColumnarToRowExec] =>
      val gpuColumnar = d.child.asInstanceOf[GpuColumnarToRowExec]
      plan.withNewChildren(Seq(GpuColumnarToRowExec(gpuColumnar.child, exportColumnarRdd = true)))
    case _ => plan
  }

  /** Mark nodes as GPU planning completed. */
  private def markGpuPlanningComplete(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case g: GpuBroadcastExchangeExec => g.markGpuPlanningComplete()
      case _ =>
    }
    plan
  }

  /**
   * On some Spark platforms, AQE planning ends up not reusing as many GPU exchanges as possible.
   * This searches the plan for any GPU broadcast exchanges and checks if their original CPU plans
   * match any other previously seen GPU broadcasts with the same CPU plan.
   */
  private def fixupAdaptiveExchangeReuse(p: SparkPlan): SparkPlan = {
    def doFixup(plan: SparkPlan): SparkPlan = {
      plan.transformUp {
        case g: GpuBroadcastExchangeExec =>
          ExchangeMappingCache.findGpuExchangeReplacement(g.cpuCanonical).map { other =>
            if (other eq g) {
              g
            } else {
              ReusedExchangeExec(g.output, other)
            }
          }.getOrElse(g)
      }
    }

    // If an exchange is at the top of the plan being remapped, this is likely due to AQE
    // re-planning, and we're not allowed to change an exchange to a reused exchange in that case.
    p match {
      case e: Exchange => e.mapChildren(doFixup)
      case _ => doFixup(p)
    }
  }

  /**
   * In non-AQE mode with DPP, GpuSubqueryBroadcastExec builds its underlying
   * GpuBroadcastExchangeExec directly in GpuOverrides without going through this rule.
   * The DPP-side broadcast therefore has a structurally different child (e.g. missing
   * GpuCoalesceBatches that this rule inserts on the main plan) than the join-side
   * broadcast for the same logical CPU exchange, and its cpuCanonical is also computed
   * after GPU rewriting so it does not match the join-side cpuCanonical. Spark's
   * ReuseExchangeAndSubquery rule does not merge them, so the dim side is materialized
   * twice and DPP loses its intended performance benefit.
   *
   * This pass walks subquery expressions in the final plan, identifies the DPP-side
   * GpuBroadcastExchangeExec inside a GpuSubqueryBroadcastExec, and matches it against
   * the main-plan GpuBroadcastExchangeExec instances by (mode, child canonical form with
   * GpuCoalesceBatches stripped — see stripGpuCoalesceBatches below). When a match is found,
   * the DPP-side broadcast is rewritten to ReusedExchangeExec referencing the join-side
   * instance.
   */
  private[rapids] def fixupNonAdaptiveBroadcastReuse(p: SparkPlan): SparkPlan = {
    // Normalize a plan for signature matching by removing GpuCoalesceBatches wraps. The main-plan
    // broadcast picks these up from insertCoalesce / optimizeCoalesce but the DPP-side broadcast
    // (built earlier in GpuOverrides without going through GpuTransitionOverrides) does not, so
    // we have to strip them on both sides before comparing canonical forms. This is the only
    // structural difference observed in practice; other transitions (host->device, etc.) live
    // outside the broadcast subtree and never reach this helper.
    def stripGpuCoalesceBatches(plan: SparkPlan): SparkPlan = plan match {
      case g: GpuCoalesceBatches => stripGpuCoalesceBatches(g.child)
      case other => other.withNewChildren(other.children.map(stripGpuCoalesceBatches))
    }

    def signature(g: GpuBroadcastExchangeExec): (Any, SparkPlan) =
      (g.mode.canonicalized, stripGpuCoalesceBatches(g.child).canonicalized)

    // Collect all main-plan GpuBroadcastExchangeExec instances. SparkPlan.foreach only walks
    // the plan-tree children and does NOT descend into ExecSubqueryExpression plans, so
    // DPP-side broadcasts (which live inside GpuSubqueryBroadcastExec under a subquery
    // expression) are naturally excluded from this collection — exactly what we want, because
    // those are the instances the transformAllExpressions pass below will rewrite.
    val mainPlanBroadcasts = mutable.ArrayBuffer.empty[GpuBroadcastExchangeExec]
    p.foreach {
      case g: GpuBroadcastExchangeExec => mainPlanBroadcasts += g
      case _ =>
    }
    if (mainPlanBroadcasts.isEmpty) return p

    val bySig = mainPlanBroadcasts.groupBy(signature).map {
      case (sig, instances) => sig -> instances.head
    }

    p.transformAllExpressions {
      case sub: ExecSubqueryExpression if sub.plan.isInstanceOf[GpuSubqueryBroadcastExec] =>
        val gsb = sub.plan.asInstanceOf[GpuSubqueryBroadcastExec]
        gsb.child match {
          case dpp: GpuBroadcastExchangeExec =>
            bySig.get(signature(dpp)) match {
              case Some(matched) if !(matched eq dpp) =>
                // Use dpp.output (not matched.output) so the reused exchange exposes the
                // DPP-side attributes that downstream subquery expressions reference, while
                // reading from the matched main-plan exchange. The AQE fixup
                // fixupAdaptiveExchangeReuse uses the same shape (its g.output is the
                // DPP-side attributes from the in-pass collected map).
                val reused = ReusedExchangeExec(dpp.output, matched)
                val newGsb = gsb.withNewChildren(Seq(reused))
                  .asInstanceOf[GpuSubqueryBroadcastExec]
                sub.withNewPlan(newGsb)
              case _ => sub
            }
          case _ => sub
        }
    }
  }

  private def insertStageLevelMetrics(plan: SparkPlan): Unit = {
    val sc = SparkSession.active.sparkContext
    val gen = new AtomicInteger(0)
    val allMetrics = mutable.Map[Int, GpuTaskMetrics]()
    insertStageLevelMetrics(sc, plan, gen.getAndIncrement(), gen, allMetrics)
  }

  private def insertStageLevelMetrics(sc: SparkContext,
      plan: SparkPlan,
      currentStageId: Int,
      stageIdGen: AtomicInteger,
      allMetrics: mutable.Map[Int, GpuTaskMetrics]): Unit = {
    plan match {
      case shuffle: Exchange =>
        shuffle.children.foreach { child =>
          val newStageId = stageIdGen.getAndIncrement()
          insertStageLevelMetrics(sc, child, newStageId, stageIdGen, allMetrics)
        }
      case gpu: GpuExec if gpu.supportsColumnar =>
        // We only want to insert metrics for one of the execs per stage, but that can
        // have problems because we want it to be deserialized before any of the metrics
        // are used, but depending on how the iterators work, that might not happen, so to
        // be safe for now we are going to include it everywhere
        val metrics = allMetrics.getOrElse(currentStageId, {
          val newMetrics = new GpuTaskMetrics
          newMetrics.register(sc)
          allMetrics.put(currentStageId, newMetrics)
          newMetrics
        })
        gpu.setTaskMetrics(metrics)
        gpu.children.foreach { child =>
          insertStageLevelMetrics(sc, child, currentStageId, stageIdGen, allMetrics)
        }
      case other =>
        other.children.foreach { child =>
          insertStageLevelMetrics(sc, child, currentStageId, stageIdGen, allMetrics)
        }
    }
  }

  override def apply(sparkPlan: SparkPlan): SparkPlan =
      GpuOverrideUtil.withActiveSession(sparkSession) {
    GpuOverrideUtil.tryOverride { plan =>
    this.rapidsConf = new RapidsConf(plan.conf)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU) {
      GpuOverrides.logDuration(rapidsConf.shouldExplain,
        t => f"GPU plan transition optimization took $t%.2f ms") {
        var updatedPlan = DeltaProvider().pruneFileMetadata(plan)
        if (DeltaProvider().isPushDVPredicateDownEnabled(rapidsConf)) {
          updatedPlan = DeltaProvider().tryPushDVPredicateDownToScan(updatedPlan)
        }
        updatedPlan = insertHashOptimizeSorts(updatedPlan)
        updatedPlan = updateScansForInputAndOrder(updatedPlan)
        if (rapidsConf.isFileScanPrunePartitionEnabled) {
          updatedPlan = prunePartitionForFileSourceScan(updatedPlan)
        }
        updatedPlan = insertColumnarFromGpu(updatedPlan)
        updatedPlan = insertCoalesce(updatedPlan)
        // only insert shuffle coalesces when using normal shuffle
        if (!GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
          updatedPlan = insertShuffleCoalesce(updatedPlan)
        }
        if (plan.conf.adaptiveExecutionEnabled) {
          updatedPlan = optimizeAdaptiveTransitions(updatedPlan, None)
        } else {
          updatedPlan = optimizeGpuPlanTransitions(updatedPlan)
        }
        updatedPlan = fixupHostColumnarTransitions(updatedPlan)
        updatedPlan = optimizeCoalesce(updatedPlan)
        if (rapidsConf.shuffledHashJoinOptimizeShuffle || rapidsConf.useShuffledSymmetricHashJoin) {
          updatedPlan = shuffledHashJoinOptimizeShuffle(updatedPlan)
        }
        if (rapidsConf.exportColumnarRdd) {
          updatedPlan = detectAndTagFinalColumnarOutput(updatedPlan)
        }
        if (rapidsConf.isTestEnabled) {
          assertIsOnTheGpu(updatedPlan, rapidsConf)
          // Generate the canonicalized plan to ensure no incompatibilities.
          // The plan itself is not currently checked.
          updatedPlan.canonicalized
          validateExecsInGpuPlan(updatedPlan, rapidsConf)
        }

        // Some distributions of Spark don't properly transform the plan after the
        // plugin performs its final transformations of the plan. In this case, we 
        // need to apply any remaining rules that should have been applied.
        updatedPlan = SparkShimImpl.applyPostShimPlanRules(updatedPlan)

        updatedPlan = markGpuPlanningComplete(updatedPlan)
        if (rapidsConf.isAqeExchangeReuseFixupEnabled &&
            plan.conf.adaptiveExecutionEnabled && plan.conf.exchangeReuseEnabled) {
          updatedPlan = fixupAdaptiveExchangeReuse(updatedPlan)
        }
        if (rapidsConf.isNonAqeBroadcastReuseFixupEnabled &&
            !plan.conf.adaptiveExecutionEnabled && plan.conf.exchangeReuseEnabled) {
          updatedPlan = fixupNonAdaptiveBroadcastReuse(updatedPlan)
        }

        if (rapidsConf.isTagLoreIdEnabled) {
          updatedPlan = GpuLore.tagForLore(updatedPlan, rapidsConf)
        }

        if (rapidsConf.logQueryTransformations) {
          logWarning(s"Transformed query:" +
            s"\nOriginal Plan:\n$plan\nTransformed Plan:\n$updatedPlan")
        }

        insertStageLevelMetrics(updatedPlan)
        updatedPlan
      }
    } else {
      plan
    }
    }(sparkPlan)
  }
}

object GpuTransitionOverrides {
  val aqeQueryStageExchange = TreeNodeTag[Boolean]("rapids.aqe.queryStageExchange")

  def tagAqeQueryStageExchanges(plan: SparkPlan): Unit = {
    def tagPlanAndSubqueries(plan: SparkPlan): Unit = {
      plan.foreach { node =>
        node match {
          case exchange: Exchange =>
            exchange.setTagValue(aqeQueryStageExchange, true)
          case reused: ReusedSubqueryExec =>
            tagPlanAndSubqueries(reused.child)
          case _ =>
        }
        node.expressions.foreach(_.foreach {
          case subquery: ExecSubqueryExpression =>
            tagPlanAndSubqueries(subquery.plan)
          case _ =>
        })
      }
    }
    tagPlanAndSubqueries(plan)
  }

  def copyAqeQueryStageExchangeTag(from: SparkPlan, to: SparkPlan): Unit = {
    if (from.getTagValue(aqeQueryStageExchange).contains(true)) {
      to.setTagValue(aqeQueryStageExchange, true)
    }
  }

  private[rapids] def canExposeExchangeForAqeStage(
      exchange: SparkPlan,
      parent: Option[SparkPlan]): Boolean = {
    parent.isEmpty &&
        (exchange.getTagValue(aqeQueryStageExchange).contains(true) ||
            exchangeGeneratedByEnsureRequirements(exchange))
  }

  // AQE can insert generated shuffles during re-optimization after query-stage prep has
  // already tagged the original exchanges. A parentless ENSURE_REQUIREMENTS shuffle is a
  // query-stage root, unlike user repartition exchanges with explicit repartition origins.
  private def exchangeGeneratedByEnsureRequirements(exchange: SparkPlan): Boolean = exchange match {
    case shuffle: ShuffleExchangeLike =>
      // Compare by origin name instead of `==`. Some proprietary Spark distributions define
      // their own ENSURE_REQUIREMENTS singleton to work around upstream ShuffleOrigin being
      // sealed. Those instances are not equal to the upstream case object but preserve the
      // same origin name.
      shuffle.shuffleOrigin.toString == ENSURE_REQUIREMENTS.toString
    case _ => false
  }

  /**
   * Returning the underlying plan of a query stage, or the plan itself if it is not a
   * query stage. This method is typically used when we want to determine if a plan is
   * a GpuExec or not, and this gets hidden by the query stage wrapper.
   */
  def getNonQueryStagePlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case bqse: BroadcastQueryStageExec =>
        if (bqse.plan.isInstanceOf[ReusedExchangeExec]) {
          bqse.plan.asInstanceOf[ReusedExchangeExec].child
        } else {
          bqse.plan
        }
      case sqse: ShuffleQueryStageExec =>
        if (sqse.plan.isInstanceOf[ReusedExchangeExec]) {
          sqse.plan.asInstanceOf[ReusedExchangeExec].child
        } else {
          sqse.plan
        }
      case _ =>
        SparkShimImpl.getTableCacheNonQueryStagePlan(plan).getOrElse(plan)
    }
  }

}
