/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.{GpuBroadcastJoinMeta, ShimBinaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuBroadcastHashJoinMetaBase(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuBroadcastJoinMeta[BroadcastHashJoinExec](join, conf, parent, rule) {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override protected def runChildExprBridgeOptimization(): Unit = {
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(leftKeys ++ rightKeys)
    conditionMeta.foreach { cond =>
      val leftExprIds = join.left.output.map(_.exprId)
      val rightExprIds = join.right.output.map(_.exprId)
      if (AstUtil.canExtractNonAstConditionIfNeed(cond, leftExprIds, rightExprIds)) {
        GpuCpuBridgeOptimizer.checkAndOptimizeNonAstSubtrees(cond)
      } else {
        // Inner joins can consume a bridged post-filter. Joins that require an AST condition
        // will call requireAstForGpuOn later and reject GPU execution if the bridge remains.
        GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(cond))
      }
    }
  }

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
    GpuHashJoin.tagBuildSide(this, join.joinType, buildSide)
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }

    if (!canBuildSideBeReplaced(buildSideMeta)) {
      if (conf.isSqlExplainOnlyEnabled && wrapped.conf.adaptiveExecutionEnabled) {
        willNotWorkOnGpu("explain only mode with AQE, we cannot determine " +
          "if the broadcast for this join is on the GPU too")
      } else {
        willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
      }
    }

    if (!canThisBeReplaced) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
  }

  // Called in runAfterTagRules for a special post tagging for this broadcast join.
  def checkTagForBuildSide(): Unit = {
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }
    // Check both of the conditions to avoid duplicate reason string.
    if (!canThisBeReplaced && canBuildSideBeReplaced(buildSideMeta)) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
    if (canThisBeReplaced && !canBuildSideBeReplaced(buildSideMeta)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }
  }

  def convertToGpu(): GpuExec
}

abstract class GpuBroadcastHashJoinExecBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean) extends ShimBinaryExecNode with GpuHashJoin {
  import GpuMetric._

  // Same checks as Spark
  if (isNullAwareAntiJoin) {
    require(leftKeys.length == 1, "leftKeys length should be 1")
    require(rightKeys.length == 1, "rightKeys length should be 1")
    require(joinType == LeftAnti, "joinType must be LeftAnti.")
    require(buildSide == GpuBuildRight, "buildSide must be BuildRight.")
    require(condition.isEmpty, "null aware anti join optimize condition should be empty.")
  }

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case GpuBuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case GpuBuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  private def splitBroadcastPlan(plan: SparkPlan): (SparkPlan, List[GpuProjectExec]) = plan match {
    case project: GpuProjectExec =>
      val (broadcastPlan, projects) = splitBroadcastPlan(project.child)
      (broadcastPlan, project :: projects)
    case _ =>
      (plan, List.empty)
  }

  protected def getBroadcastPlan(plan: SparkPlan): SparkPlan = splitBroadcastPlan(plan)._1

  def broadcastExchange: GpuBroadcastExchangeExec = getBroadcastPlan(buildPlan) match {
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] =>
      bqse.plan.asInstanceOf[GpuBroadcastExchangeExec]
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuBroadcastExchangeExec]
    case gpu: GpuBroadcastExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExec]
  }

  protected def buildSidePostProjection: Option[ColumnarBatch => ColumnarBatch] = {
    val projects = splitBroadcastPlan(buildPlan)._2.reverse
    if (projects.nonEmpty) {
      val boundProjects = projects.map { project =>
        // Match GpuProjectExec's tiered binding so build-side extraction has the same
        // splitting and retry behavior as a normal project.
        GpuBindReferences.bindGpuReferencesTiered(
          project.projectList, project.child.output, conf, allMetrics)
      }
      Some((batch: ColumnarBatch) => boundProjects.foldLeft(batch) {
        case (currentBatch, boundProject) =>
          val spillableBatch = SpillableColumnarBatch(
            currentBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          boundProject.projectAndCloseWithRetrySingleBatch(spillableBatch)
      })
    } else {
      None
    }
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(
      "GpuBroadcastHashJoin does not support row-based processing")

  protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val joinOptions = RapidsConf.getJoinOptions(conf, targetSize)

    val broadcastRelation = broadcastExchange.executeColumnarBroadcast[Any]()

    val rdd = streamedPlan.executeColumnar()
    val buildSchema = getBroadcastPlan(buildPlan).schema
    val postProjectionAndClose = buildSidePostProjection
    val localIsNullAwareAntiJoin = isNullAwareAntiJoin
    rdd.mapPartitions { it =>
      val (broadcastBuiltBatch, streamIter) =
        GpuBroadcastHelper.getBroadcastBuiltBatchAndStreamIter(
          broadcastRelation,
          buildSchema,
          new CollectTimeIterator(NvtxRegistry.BROADCAST_JOIN_STREAM, it, streamTime))
      val builtBatch = postProjectionAndClose.map(_(broadcastBuiltBatch))
          .getOrElse(broadcastBuiltBatch)
      if (localIsNullAwareAntiJoin) {
        // This is to support the null-aware anti join for the LeftAnti join with
        // BuildRight. See the config "spark.sql.optimizeNullAwareAntiJoin".
        // Spark already executes all the check for the requirements, e.g. join type,
        // build side, keys length == 1. So no need to do it here again.
        // This will cover mainly 3 cases as below, similar as what Spark does.
        if (builtBatch.numRows() == 0) {
          // Build side is empty, return the stream iterator directly.
          withResource(builtBatch)(_ => streamIter)
        } else if (closeOnExcept(builtBatch)(GpuHashJoin.anyNullInKey(_, boundBuildKeys))) {
          // Spark will return an empty iterator if any nulls in the right table
          withResource(builtBatch)(_ => Iterator.empty)
        } else {
          // Nulls will be filtered out
          val nullFilteredStreamIter = streamIter.map { cb =>
            GpuHashJoin.filterNullsWithRetryAndClose(
              SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
              boundStreamKeys)
          }
          doJoin(builtBatch, nullFilteredStreamIter, joinOptions, numOutputRows,
            numOutputBatches, opTime, joinTime)
        }
      } else {
        // builtBatch will be closed in doJoin
        doJoin(builtBatch, streamIter, joinOptions, numOutputRows, numOutputBatches, opTime,
          joinTime)
      }
    }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    doColumnarBroadcastJoin()
  }
}
