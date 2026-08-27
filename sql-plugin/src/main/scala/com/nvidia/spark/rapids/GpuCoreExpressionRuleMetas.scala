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

import com.nvidia.spark.rapids.shims._
import com.nvidia.spark.rapids.window._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids._

case class SignumRuleMeta(
    a: Signum,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Signum](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuSignum(child)
}

case class AliasRuleMeta(
    a: Alias,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryAstExprMeta[Alias](a, conf, p, r) {
  override def typeMeta: DataTypeMeta = childExprs.head.typeMeta

  override def convertToGpu(child: Expression): GpuExpression =
    GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
}

case class BoundReferenceRuleMeta(
    currentRow: BoundReference,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[BoundReference](currentRow, conf, p, r) {
  // BoundReference should not be directly wrapped in a bridge (unit test compatibility)
  override def isBridgeCompatible: Boolean = false

  override def convertToGpuImpl(): GpuExpression = GpuBoundReference(
    currentRow.ordinal, currentRow.dataType, currentRow.nullable)(
    NamedExpression.newExprId, "")
}

case class AttributeReferenceRuleMeta(
    att: AttributeReference,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BaseExprMeta[AttributeReference](att, conf, p, r) {
  // This is the only NOOP operator.  It goes away when things are bound
  override def convertToGpuImpl(): Expression = {
    def findParentPlan(meta: Option[RapidsMeta[_, _, _]]): Option[SparkPlanMeta[_]] = {
      meta match {
        case Some(planMeta: SparkPlanMeta[_]) => Some(planMeta)
        case Some(other) => findParentPlan(other.parent)
        case None => None
      }
    }

    val maybeResolvedAttr = for {
      planMeta <- findParentPlan(parent)
      matched <- planMeta.childPlans.iterator
        .flatMap(_.outputAttributes.iterator)
        .find(a => a.exprId == att.exprId && a.dataType != att.dataType)
    } yield AttributeReference(
      att.name,
      matched.dataType,
      matched.nullable,
      att.metadata)(att.exprId, att.qualifier)

    // matched.dataType can still be the wrapped plan's un-pruned protobuf struct; binding
    // guarantees the final runtime type from the actual input column.
    maybeResolvedAttr.getOrElse(att)
  }

  // There are so many of these that we don't need to print them out, unless it
  // will not work on the GPU
  override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
      super.print(append, depth, all)
    }
  }
}

case class ToDegreesRuleMeta(
    a: ToDegrees,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[ToDegrees](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuToDegrees = GpuToDegrees(child)
}

case class ToRadiansRuleMeta(
    a: ToRadians,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[ToRadians](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuToRadians = GpuToRadians(child)
}

case class BinRuleMeta(
    a: Bin,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Bin](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuBin = GpuBin(child)
}

case class HexRuleMeta(
    a: Hex,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Hex](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuHex(child)
}

case class CurrentRowRuleMeta(
    currentRow: CurrentRow.type,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[CurrentRow.type](currentRow, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuSpecialFrameBoundary(currentRow)
}

case class UnboundedPrecedingRuleMeta(
    unboundedPreceding: UnboundedPreceding.type,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuSpecialFrameBoundary(unboundedPreceding)
}

case class UnboundedFollowingRuleMeta(
    unboundedFollowing: UnboundedFollowing.type,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuSpecialFrameBoundary(unboundedFollowing)
}

case class RowNumberRuleMeta(
    rowNumber: RowNumber,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[RowNumber](rowNumber, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuRowNumber
}

case class RankRuleMeta(
    rank: Rank,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[Rank](rank, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuRank(childExprs.map(_.convertToGpu()))
}

case class DenseRankRuleMeta(
    denseRank: DenseRank,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[DenseRank](denseRank, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuDenseRank(childExprs.map(_.convertToGpu()))
}

case class PercentRankRuleMeta(
    percentRank: PercentRank,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[PercentRank](percentRank, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuPercentRank(childExprs.map(_.convertToGpu()))
}

case class LeadRuleMeta(
    lead: Lead,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends OffsetWindowFunctionMeta[Lead](lead, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuLead(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
}

case class LagRuleMeta(
    lag: Lag,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends OffsetWindowFunctionMeta[Lag](lag, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuLag(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
}


case class LiteralConstructorRuleMeta(
    lit: Literal,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends LiteralExprMeta(lit, conf, parent, r)

case class WindowExpressionConstructorRuleMeta(
    windowExpression: WindowExpression,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuWindowExpressionMeta(windowExpression, conf, parent, r)

case class SpecifiedWindowFrameConstructorRuleMeta(
    windowFrame: SpecifiedWindowFrame,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSpecifiedWindowFrameMeta(windowFrame, conf, parent, r)

case class WindowSpecDefinitionConstructorRuleMeta(
    windowSpec: WindowSpecDefinition,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuWindowSpecDefinitionMeta(windowSpec, conf, parent, r)
