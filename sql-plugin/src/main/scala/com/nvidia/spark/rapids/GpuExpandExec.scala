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
package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.util.Random

import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuExpandExecMeta(
    expand: ExpandExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ExpandExec](expand, conf, parent, rule) {

  private val gpuProjections: Seq[Seq[BaseExprMeta[_]]] =
    expand.projections.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))

  private val outputs: Seq[BaseExprMeta[_]] =
    expand.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = gpuProjections.flatten ++ outputs

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec = {
    val projections = gpuProjections.map(_.map(_.convertToGpu()))
    GpuExpandExec(projections, expand.output, childPlans.head.convertIfNeeded())(
      preprojectEnabled = conf.isExpandPreprojectEnabled,
      coalesceAfter = conf.isCoalesceAfterExpandEnabled)
  }
}

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 *
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      Attribute references to Output
 * @param child       Child operator
 * @param preprojectEnabled Whether to enable pre-project before expanding
 * @param coalesceAfter Whether to coalesce the output batches
 */
case class GpuExpandExec(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)(
    preprojectEnabled: Boolean = false,
    override val coalesceAfter: Boolean = true
) extends ShimUnaryExecNode with GpuExec {

  override def otherCopyArgs: Seq[AnyRef] = Seq[AnyRef](
    preprojectEnabled.asInstanceOf[java.lang.Boolean],
    coalesceAfter.asInstanceOf[java.lang.Boolean]
  )

  private val PRE_PROJECT_TIME = "preprojectTime"
  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    PRE_PROJECT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, "pre-projection time"),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // cache in a local to avoid serializing the plan
    val metricsMap = allMetrics

    var projectionsForBind = projections
    var attributesForBind = child.output
    var preprojectIter = identity[Iterator[ColumnarBatch]] _
    if (preprojectEnabled) {
      // Tiered projection is enabled, check if pre-projection is needed.
      val boundPreprojections = GpuBindReferences.bindGpuReferencesTiered(
        preprojectionList, child.output, conf, metricsMap)
      if (boundPreprojections.exprTiers.size > 1) {
        logDebug("GPU expanding with pre-projection.")
        projectionsForBind = preprojectedProjections
        attributesForBind = preprojectionList.map(_.toAttribute)
        val opMetric = metricsMap(OP_TIME_LEGACY)
        val preproMetric = metricsMap(PRE_PROJECT_TIME)
        preprojectIter = (iter: Iterator[ColumnarBatch]) => iter.map(cb =>
          GpuMetric.ns(opMetric, preproMetric) {
            boundPreprojections.projectAndCloseWithRetrySingleBatch(
              SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
          }
        )
      }
    }

    val boundProjections = projectionsForBind.map { pl =>
      GpuBindReferences.bindGpuReferencesTiered(pl, attributesForBind, conf, metricsMap)
    }

    child.executeColumnar().mapPartitions { it =>
      new GpuExpandIterator(boundProjections, metricsMap, preprojectIter(it))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  /**
   * The expressions that need to be pre-projected, and the corresponding projections
   * for expanding.
   *
   * Some rules (e.g. RewriteDistinctAggregates) in Spark will put non-leaf expressions
   * into Expand projections, then it can not leverage the GPU tiered projection across
   * the projection lists.
   * So here tries to factor out these expressions for the pre-projection before
   * expanding to avoid duplicate evaluation of semantic-equal (sub) expressions.
   *
   * e.g. projections:
   *     [if((a+b)>0) 1 else 0, null], [null, if((a+b)=0 "no" else "yes")];
   * without pre-projection, "a+b" will be evaluated twice.
   * while with pre-projection, it has
   *    preprojectionList:
   *            [if((a+b)>0) 1 else 0, if((a+b)=0 "no" else "yes")]
   *    preprojectedProjections:
   *            [_pre-project-c1#0, null], [null, _pre-project-c3#1]
   * and
   *    "_pre-project-c1#0" refers to "if((a+b)>0) 1 else 0",
   *    "_pre-project-c3#1" refers to "if((a+b)=0 "no" else "yes"
   * By leveraging the tiered projection, "a+b" will be evaluated only once.
   */
  private[this] lazy val (preprojectionList, preprojectedProjections) = {
    val projectListSet = mutable.Set[NamedExpression]()
    val newProjections = projections.map { proList =>
      proList.map {
        case attr: AttributeReference if child.outputSet.contains(attr) =>
          // A ref to child output, add it to pre-projection for passthrough.
          projectListSet += attr
          attr
        case leaf if leaf.children.isEmpty =>
          // A leaf expression is simple enough, not necessary for pre-projection.
          // e.g. GpuLiteral.
          leaf
        case notLeafNamed: NamedExpression =>
          logDebug(s"Got a named non-leaf expression: $notLeafNamed for pre-projection")
          // A named non-leaf expression, e.g. GpuAlias. Add it for pre-projection and
          // replace with its attribute.
          projectListSet += notLeafNamed
          notLeafNamed.toAttribute
        case notLeaf =>
          logDebug(s"Got a non-leaf expression: $notLeaf for pre-projection")
          // Wrap it by a new "GpuAlias", and replace with the "GpuAlias"'s attribute.
          val alias = GpuAlias(notLeaf, s"_pre-project-c${Random.nextInt}")()
          projectListSet += alias
          alias.toAttribute
      }
    }
    (projectListSet.toList, newProjections)
  }
}

class GpuExpandIterator(
    boundProjections: Seq[GpuTieredProject],
    metrics: Map[String, GpuMetric],
    it: Iterator[ColumnarBatch])
  extends Iterator[ColumnarBatch] {

  private var sb: Option[SpillableColumnarBatch] = None
  private var projectionIndex = 0
  private val numInputBatches = metrics(NUM_INPUT_BATCHES)
  private val numOutputBatches = metrics(NUM_OUTPUT_BATCHES)
  private val numInputRows = metrics(NUM_INPUT_ROWS)
  private val numOutputRows = metrics(NUM_OUTPUT_ROWS)
  private val opTime = metrics(OP_TIME_LEGACY)

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      sb.foreach(_.close())
    }
  }

  override def hasNext: Boolean = sb.isDefined || it.hasNext

  override def next(): ColumnarBatch = {

    if (sb.isEmpty) {
      val cb = it.next()
      numInputBatches += 1
      numInputRows += cb.numRows()
      sb = Some(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }

    val projectedBatch = NvtxIdWithMetrics(NvtxRegistry.EXPAND_EXEC_PROJECTIONS, opTime) {
      boundProjections(projectionIndex).projectWithRetrySingleBatch(sb.get)
    }

    numOutputBatches += 1
    numOutputRows += projectedBatch.numRows()

    projectionIndex += 1
    if (projectionIndex == boundProjections.length) {
      // we have processed all projections against the current batch
      projectionIndex = 0

      sb.foreach(_.close())
      sb = None
    }
    projectedBatch
  }
}
