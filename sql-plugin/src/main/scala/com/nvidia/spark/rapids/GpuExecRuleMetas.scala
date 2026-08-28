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
import com.nvidia.spark.rapids.window.GpuWindowExecMeta

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.shims.GpuMapInPandasExecMeta

case class RangeExecRuleMeta(
    range: RangeExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[RangeExec](range, conf, p, r) {
  override def convertToGpu(): GpuExec =
    GpuRangeExec(range.start, range.end, range.step, range.numSlices, range.output,
      this.conf.gpuTargetBatchSizeBytes)
}

case class CoalesceExecRuleMeta(
    coalesce: CoalesceExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[CoalesceExec](coalesce, conf, p, r) {
  override def convertToGpu(): GpuExec =
    GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
}

case class DataWritingCommandExecRuleMeta(
    dataWritingCommand: DataWritingCommandExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[DataWritingCommandExec](dataWritingCommand, conf, p, r) {
  override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
    Seq(GpuOverrides.wrapDataWriteCmds(dataWritingCommand.cmd, this.conf, Some(this)))

  override def convertToGpu(): GpuExec =
    GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
      childPlans.head.convertIfNeeded())
}

case class LocalLimitExecRuleMeta(
    localLimitExec: LocalLimitExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
  override def convertToGpu(): GpuExec =
    GpuLocalLimitExec(localLimitExec.limit, childPlans.head.convertIfNeeded())
}

case class GlobalLimitExecRuleMeta(
    globalLimitExec: GlobalLimitExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
  override def convertToGpu(): GpuExec =
    GpuGlobalLimitExec(globalLimitExec.limit, childPlans.head.convertIfNeeded(), 0)
}

case class UnionExecRuleMeta(
    union: UnionExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[UnionExec](union, conf, p, r) {
  override def convertToGpu(): GpuExec =
    GpuUnionExec(childPlans.map(_.convertIfNeeded()))
}

case class CartesianProductExecRuleMeta(
    join: CartesianProductExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[CartesianProductExec](join, conf, p, r) {
  val condition: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val joinExec = GpuCartesianProductExec(
      left,
      right,
      None,
      this.conf.gpuTargetBatchSizeBytes)
    // The GPU does not yet support conditional joins, so conditions are implemented
    // as a filter after the join when possible.
    condition.map(c => GpuFilterExec(c.convertToGpu(),
      joinExec)()).getOrElse(joinExec)
  }
}

case class ArrowEvalPythonExecRuleMeta(
    e: ArrowEvalPythonExec,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
  val udfs: Seq[BaseExprMeta[PythonUDF]] =
    e.udfs.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
  val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    e.resultAttrs.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  override def convertToGpu(): GpuExec =
    GpuArrowEvalPythonExec(udfs.map(_.convertToGpu()).asInstanceOf[Seq[GpuPythonUDF]],
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      childPlans.head.convertIfNeeded(),
      e.evalType)
}


case class GenerateExecConstructorRuleMeta(
    gen: GenerateExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuGenerateExecSparkPlanMeta(gen, conf, parent, r)

case class ProjectExecConstructorRuleMeta(
    proj: ProjectExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuProjectExecMeta(proj, conf, parent, r)

case class BatchScanExecConstructorRuleMeta(
    p: BatchScanExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BatchScanExecMeta(p, conf, parent, r)

case class ExecutedCommandExecConstructorRuleMeta(
    p: ExecutedCommandExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExecutedCommandExecMeta(p, conf, parent, r)

case class CollectLimitExecConstructorRuleMeta(
    collectLimitExec: CollectLimitExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuCollectLimitMeta(collectLimitExec, conf, parent, r)

case class ShuffleExchangeExecConstructorRuleMeta(
    shuffle: ShuffleExchangeExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuShuffleMeta(shuffle, conf, parent, r)

case class BroadcastExchangeExecConstructorRuleMeta(
    exchange: BroadcastExchangeExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuBroadcastMeta(exchange, conf, parent, r)

case class BroadcastHashJoinExecConstructorRuleMeta(
    join: BroadcastHashJoinExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuBroadcastHashJoinMeta(join, conf, parent, r)

case class BroadcastNestedLoopJoinExecConstructorRuleMeta(
    join: BroadcastNestedLoopJoinExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuBroadcastNestedLoopJoinMeta(join, conf, parent, r)

case class HashAggregateExecConstructorRuleMeta(
    override val agg: HashAggregateExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuHashAggregateMeta(agg, conf, parent, r)

case class ObjectHashAggregateExecConstructorRuleMeta(
    override val agg: ObjectHashAggregateExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuObjectHashAggregateExecMeta(agg, conf, parent, r)

case class ShuffledHashJoinExecConstructorRuleMeta(
    join: ShuffledHashJoinExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuShuffledHashJoinMeta(join, conf, parent, r)

case class SortAggregateExecConstructorRuleMeta(
    override val agg: SortAggregateExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSortAggregateExecMeta(agg, conf, parent, r)

case class SortExecConstructorRuleMeta(
    sort: SortExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSortMeta(sort, conf, parent, r)

case class SortMergeJoinExecConstructorRuleMeta(
    join: SortMergeJoinExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSortMergeJoinMeta(join, conf, parent, r)

case class ExpandExecConstructorRuleMeta(
    expand: ExpandExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuExpandExecMeta(expand, conf, parent, r)

case class WindowExecConstructorRuleMeta(
    windowOp: WindowExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuWindowExecMeta(windowOp, conf, parent, r)

case class SampleExecConstructorRuleMeta(
    sample: SampleExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSampleExecMeta(sample, conf, parent, r)

case class SubqueryBroadcastExecConstructorRuleMeta(
    s: SubqueryBroadcastExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSubqueryBroadcastMeta(s, conf, parent, r)

case class FlatMapCoGroupsInPandasExecConstructorRuleMeta(
    flatCoPy: FlatMapCoGroupsInPandasExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuFlatMapCoGroupsInPandasExecMeta(flatCoPy, conf, parent, r)

case class FlatMapGroupsInPandasExecConstructorRuleMeta(
    flatPy: FlatMapGroupsInPandasExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuFlatMapGroupsInPandasExecMeta(flatPy, conf, parent, r)

case class MapInPandasExecConstructorRuleMeta(
    mapPy: MapInPandasExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuMapInPandasExecMeta(mapPy, conf, parent, r)

case class InMemoryTableScanExecConstructorRuleMeta(
    scan: InMemoryTableScanExec,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends InMemoryTableScanMeta(scan, conf, parent, r)
