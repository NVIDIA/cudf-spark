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

import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, JoinTypeChecks}

class GpuSortMergeJoinMeta(
    join: SortMergeJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SortMergeJoinExec](join, conf, parent, rule) {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] = join.condition.map(
    GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = if (GpuHashJoin.canBuildRight(join.joinType)) {
    GpuBuildRight
  } else if (GpuHashJoin.canBuildLeft(join.joinType)) {
    GpuBuildLeft
  } else {
    throw new IllegalStateException(s"Cannot build either side for ${join.joinType} join")
  }

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

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
    // Use conditions from Hash Join
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
    GpuHashJoin.tagBuildSide(this, join.joinType, buildSide)

    if (!conf.enableReplaceSortMergeJoin) {
      willNotWorkOnGpu(s"Not replacing sort merge join with hash join, " +
        s"see ${RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key}")
    }

    // make sure this is the last check - if this is SortMergeJoin, the children can be Sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // ShuffleHashJoin
    if (canThisBeReplaced) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu(s"can't replace sortMergeJoin because one of the SortExec's before " +
              s"can't be replaced.")
          } else {
            plan.shouldBeRemoved("replacing sortMergeJoin with shuffleHashJoin")
          }
        }
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val extractedCondition = GpuHashJoin.extractJoinConditionIfNeeded(
      conditionMeta, join.joinType, left, right)
    val useSizedJoin = GpuShuffledSizedHashJoinExec.useSizedJoin(conf, join.joinType,
      join.leftKeys, join.rightKeys)
    val readOpt = CoalesceReadOption(conf)
    val joinExec = join.joinType match {
      case LeftOuter | RightOuter if useSizedJoin =>
        GpuShuffledAsymmetricHashJoinExec(
          join.joinType,
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          conf.isGPUShuffle,
          conf.gpuTargetBatchSizeBytes,
          conf.sizedJoinPartitionAmplification,
          readOpt,
          join.isSkewJoin)(
          join.leftKeys,
          join.rightKeys,
          conf.joinOuterMagnificationThreshold)
      case Inner | FullOuter if useSizedJoin =>
        GpuShuffledSymmetricHashJoinExec(
          join.joinType,
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          conf.isGPUShuffle,
          conf.gpuTargetBatchSizeBytes,
          conf.sizedJoinPartitionAmplification,
          readOpt,
          join.isSkewJoin)(
          join.leftKeys,
          join.rightKeys)
      case _ =>
        GpuShuffledHashJoinExec(
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          join.joinType,
          buildSide,
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          readOpt,
          join.isSkewJoin)(
          join.leftKeys,
          join.rightKeys)
    }

    // For inner joins we can apply a post-join condition for any conditions that cannot be
    // evaluated directly in a mixed join that leverages a cudf AST expression
    val filteredJoinExec = extractedCondition.filterCondition.map(c => GpuFilterExec(c,
      joinExec)()).getOrElse(joinExec)
    extractedCondition.projectIfNeeded(filteredJoinExec)
  }
}
