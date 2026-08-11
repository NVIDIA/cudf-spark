/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuAlias, GpuExec, GpuLiteral, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, CurrentRow, Expression, ExprId, NamedExpression, RowFrame, SortOrder, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.aggregate.GpuAggregateExpression

/**
 * Base class for GPU Execs that implement window functions. This abstracts the method
 * by which the window function's input expressions, partition specs, order-by specs, etc.
 * are extracted from the specific WindowExecType.
 *
 * @tparam WindowExecType The Exec class that implements window functions
 *                        (E.g. o.a.s.sql.execution.window.WindowExec.)
 */
abstract class GpuBaseWindowExecMeta[WindowExecType <: SparkPlan] (windowExec: WindowExecType,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[WindowExecType](windowExec, conf, parent, rule) {

  /**
   * Extracts window-expression from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getInputWindowExpressions: Seq[NamedExpression]

  /**
   * Extracts partition-spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getPartitionSpecs: Seq[Expression]

  /**
   * Extracts order-by spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getOrderSpecs: Seq[SortOrder]

  /**
   * Indicates the output column semantics for the WindowExecType,
   * i.e. whether to only return the window-expression result columns (as in some Spark
   * distributions) or also include the input columns (as in Apache Spark).
   */
  def getResultColumnsOnly: Boolean

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    getInputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    getPartitionSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    getOrderSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  lazy val inputFields: Seq[BaseExprMeta[Attribute]] =
    windowExec.children.head.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  /**
   * Define all dependency expressions as `childExprs`.
   * This ensures that they are tagged for GPU execution.
   */
  override val childExprs: Seq[BaseExprMeta[_]] =
    windowExpressions ++ partitionSpec ++ orderSpec ++ inputFields

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] = Map(
    "partitionSpec" -> partitionSpec
  )

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
        .filter(expr => !expr.isInstanceOf[NamedExpression])
        .foreach(_ => willNotWorkOnGpu("Unexpected query plan with Windowing functions; " +
            "cannot convert for GPU execution. " +
            "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    // resultColumnsOnly specifies that we should only return the values of result columns for
    // this WindowExec (applies to some Spark distributions that use `projectList` and combine
    // ProjectExec with WindowExec)
    val resultColumnsOnly = getResultColumnsOnly
    // Keep the converted input fields and input window expressions separate
    // to handle the resultColumnsOnly case in GpuWindowExec.splitAndDedup
    val inputFieldExpressions = inputFields.map(_.convertToGpu().asInstanceOf[NamedExpression])
    val gpuWindowExpressions = windowExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression])
    val (pre, windowOps, post) = GpuWindowExecMeta.splitAndDedup(
      inputFieldExpressions,
      gpuWindowExpressions,
      resultColumnsOnly)
    // Order is not important for pre. It is unbound and we are inserting it in.
    val isPreNeeded =
      (AttributeSet(pre.map(_.toAttribute)) -- windowExec.children.head.output).nonEmpty
    // To check if post is needed we first have to remove a layer of indirection that
    // might not be needed. Here we want to maintain order, just to match Spark as closely
    // as possible
    val remappedWindowOps = GpuWindowExecMeta.remapAttributes(windowOps, post)
    // isPostNeeded is determined when there is a difference between the output of the WindowExec
    // computation and the output ultimately desired by this WindowExec or whether an additional
    // post computation is needed. Ultimately this is used to add an additional ProjectExec
    // if that is needed to return the correct output
    val isPostNeeded = remappedWindowOps.length != post.length ||
        remappedWindowOps.zip(post).exists {
          case (w, p) => w.exprId != p.exprId
        }
    val fixedUpWindowOps = if(isPostNeeded) {
      windowOps
    } else {
      remappedWindowOps
    }

    val allBatched = fixedUpWindowOps.forall {
      case GpuAlias(GpuWindowExpression(func, spec), _) =>
        GpuWindowExecMeta.isBatchedFunc(func, spec, conf)
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference =>
        // We allow pure result columns for running windows
        true
      case other =>
        // This should only happen if we did something wrong in splitting/deduping
        // the window expressions.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    }

    val input = if (isPreNeeded) {
      GpuProjectExec(pre.toList, childPlans.head.convertIfNeeded(), false)
    } else {
      childPlans.head.convertIfNeeded()
    }

    val windowExpr = if (allBatched) {
      val batchedOps = GpuWindowExecMeta.splitBatchedOps(fixedUpWindowOps, conf)
      batchedOps.getWindowExec(
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input,
        getPartitionSpecs,
        getOrderSpecs,
        conf)
    } else {
      new GpuWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)(getPartitionSpecs, getOrderSpecs)
    }

    if (isPostNeeded) {
      GpuProjectExec(post.toList, windowExpr, false)
    } else if (windowExpr.output != windowExec.output) {
      GpuProjectExec(windowExec.output.toList, windowExpr, false)
    } else {
      windowExpr
    }
  }
}

/**
 * Specialization of GpuBaseWindowExecMeta for org.apache.spark.sql.window.WindowExec.
 * This class implements methods to extract the window-expressions, partition columns,
 * order-by columns, etc. from WindowExec.
 */
class GpuWindowExecMeta(windowExec: WindowExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseWindowExecMeta[WindowExec](windowExec, conf, parent, rule) {

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowExec.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowExec.getClass.getMethod("projectList")
        winExpr.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private lazy val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression

  override def getInputWindowExpressions: Seq[NamedExpression] = inputWindowExpressions
  override def getPartitionSpecs: Seq[Expression] = windowExec.partitionSpec
  override def getOrderSpecs: Seq[SortOrder] = windowExec.orderSpec
  override def getResultColumnsOnly: Boolean = resultColumnsOnly
}

case class BatchedOps(running: Seq[NamedExpression],
    unboundedAgg: Seq[NamedExpression],
    unboundedDoublePass: Seq[NamedExpression],
    bounded: Seq[NamedExpression],
    passThrough: Seq[NamedExpression]) {

  private def dedupeByExprId[T <: NamedExpression](exprs: Seq[T]): Seq[T] = {
    val seen = mutable.HashSet.empty[ExprId]
    exprs.filter(expr => seen.add(expr.exprId))
  }

  /**
   * Preserve any pre-projected inputs that later batched window stages still reference.
   * Without this, chaining running/unbounded/double-pass/bounded stages can drop an
   * intermediate attribute that only appears inside a downstream window expression.
   */
  private def collectPassthroughDependencies(
      futureExpressions: Seq[NamedExpression],
      availableOutputs: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    val seen = mutable.HashSet.empty[ExprId]
    seen ++= availableOutputs.map(_.exprId)
    val childOutputByExprId = childOutput.iterator.map(attr => attr.exprId -> attr).toMap
    val needed = ArrayBuffer.empty[NamedExpression]
    futureExpressions.foreach(_.foreach {
      case attr: Attribute if !seen.contains(attr.exprId) =>
        val resolvedAttr = childOutputByExprId.getOrElse(attr.exprId, {
          throw new IllegalArgumentException(
            s"Unable to preserve passthrough dependency $attr because it is not produced by " +
                s"the original child output")
        })
        seen += resolvedAttr.exprId
        needed += resolvedAttr
      case _ =>
    })
    needed.toSeq
  }

  private def getRunningPassthrough(childOutput: Seq[Attribute]): Seq[NamedExpression] =
    dedupeByExprId(passThrough ++
      collectPassthroughDependencies(
        unboundedAgg ++ unboundedDoublePass ++ bounded,
        passThrough ++ running,
        childOutput))

  private def getUnboundedAggPassthrough(childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    val runningPassthrough = getRunningPassthrough(childOutput)
    dedupeByExprId(runningPassthrough ++
      collectPassthroughDependencies(
        unboundedDoublePass ++ bounded,
        runningPassthrough ++ running ++ unboundedAgg,
        childOutput))
  }

  private def getDoublePassPassthrough(childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    val unboundedAggPassthrough = getUnboundedAggPassthrough(childOutput)
    dedupeByExprId(unboundedAggPassthrough ++
      collectPassthroughDependencies(
        bounded,
        unboundedAggPassthrough ++ running ++ unboundedAgg ++ unboundedDoublePass,
        childOutput))
  }

  def getRunningExpressionsWithPassthrough(childOutput: Seq[Attribute]): Seq[NamedExpression] =
    getRunningPassthrough(childOutput) ++ running

  def getUnboundedAggWithRunningAsPassthrough(
      childOutput: Seq[Attribute]): Seq[NamedExpression] =
    getUnboundedAggPassthrough(childOutput) ++ unboundedAgg ++ running.map(_.toAttribute)

  def getDoublePassExpressionsWithRunningAndUnboundedAggAsPassthrough(
      childOutput: Seq[Attribute]): Seq[NamedExpression] =
    getDoublePassPassthrough(childOutput) ++
      unboundedDoublePass ++
      (unboundedAgg ++ running).map(_.toAttribute)

  def getBoundedExpressionsWithTheRestAsPassthrough(
      childOutput: Seq[Attribute]): Seq[NamedExpression] =
    getDoublePassPassthrough(childOutput) ++
      bounded ++
      (unboundedDoublePass ++ unboundedAgg ++ running).map(_.toAttribute)

  def getMinPrecedingMaxFollowingForBoundedWindows: (Int, Int) = {
    // All bounded window expressions should have window bound window specs.
    val boundedWindowSpecs = bounded.map{
      case GpuAlias(GpuWindowExpression(_, spec), _) => spec
      case other => throw new IllegalArgumentException("Expected a window-expression " +
          s" found $other")
    }
    val precedingAndFollowing = boundedWindowSpecs.map(
      GpuWindowExecMeta.getBoundedWindowPrecedingAndFollowing
    )
    (precedingAndFollowing.map{ _._1 }.min, // Only non-positive (>=0) values supported.
        precedingAndFollowing.map{ _._2 }.max)
  }

  private def getRunningWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      childOutput: Seq[Attribute],
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec =
    GpuRunningWindowExec(
      getRunningExpressionsWithPassthrough(childOutput),
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec)

  private def getUnboundedAggWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      childOutput: Seq[Attribute],
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder],
      conf: RapidsConf): GpuExec =
    GpuUnboundedToUnboundedAggWindowExec(
      getUnboundedAggWithRunningAsPassthrough(childOutput),
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec, conf.gpuTargetBatchSizeBytes)

  private def getDoublePassWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      childOutput: Seq[Attribute],
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec =
    GpuCachedDoublePassWindowExec(
      getDoublePassExpressionsWithRunningAndUnboundedAggAsPassthrough(childOutput),
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec)

  private def getBatchedBoundedWindowExec(gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      childOutput: Seq[Attribute],
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec = {
    val (prec@_, foll@_) = getMinPrecedingMaxFollowingForBoundedWindows
    new GpuBatchedBoundedWindowExec(getBoundedExpressionsWithTheRestAsPassthrough(childOutput),
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec, prec, foll)
  }

  def getWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder],
      conf: RapidsConf): GpuExec = {
    // The order of these matter so we can match the order of the parameters used to
    //  create the various aggregation functions
    val childOutput = child.output
    var currentPlan = child
    if (hasRunning) {
      currentPlan = getRunningWindowExec(gpuPartitionSpec, gpuOrderSpec, currentPlan, childOutput,
        cpuPartitionSpec, cpuOrderSpec)
    }

    if (hasUnboundedAgg) {
      currentPlan = getUnboundedAggWindowExec(gpuPartitionSpec, gpuOrderSpec, currentPlan,
        childOutput,
        cpuPartitionSpec, cpuOrderSpec, conf)
    }

    if (hasDoublePass) {
      currentPlan = getDoublePassWindowExec(gpuPartitionSpec, gpuOrderSpec, currentPlan,
        childOutput,
        cpuPartitionSpec, cpuOrderSpec)
    }

    if (hasBounded) {
      currentPlan = getBatchedBoundedWindowExec(gpuPartitionSpec, gpuOrderSpec, currentPlan,
        childOutput,
        cpuPartitionSpec, cpuOrderSpec)
    }
    currentPlan.asInstanceOf[GpuExec]
  }

  def hasRunning: Boolean = running.nonEmpty
  def hasUnboundedAgg: Boolean = unboundedAgg.nonEmpty
  def hasDoublePass: Boolean = unboundedDoublePass.nonEmpty
  def hasBounded: Boolean = bounded.nonEmpty
}

object GpuWindowExecMeta {
  /**
   * As a part of `splitAndDedup` the dedup part adds a layer of indirection. This attempts to
   * remove that layer of indirection.
   *
   * @param windowOps the windowOps output of splitAndDedup
   * @param post      the post output of splitAndDedup
   * @return a version of windowOps that has removed as many un-needed temp aliases as possible.
   */
  def remapAttributes(windowOps: Seq[NamedExpression],
      post: Seq[NamedExpression]): Seq[NamedExpression] = {
    val postRemapping = post.flatMap {
      case a@GpuAlias(attr: AttributeReference, _) => Some((attr.exprId, a))
      case _ => None
    }.groupBy(_._1)
    windowOps.map {
      case a@GpuAlias(child, _)
        // We can only replace the mapping if there is one thing to map it to.
        if postRemapping.get(a.exprId).exists(_.length == 1) =>
        val attr = postRemapping(a.exprId).head._2
        GpuAlias(child, attr.name)(attr.exprId, attr.qualifier)
      case other => other
    }
  }

  private def hasGpuWindowFunction(expr: Expression): Boolean =
    expr.find(_.isInstanceOf[GpuWindowExpression]).isDefined

  private def extractAndSave(expr: Expression,
      saved: ArrayBuffer[NamedExpression],
      deduped: mutable.HashMap[Expression, Attribute]): Expression =
    expr match {
      // Don't rename an already named expression
      case ne: NamedExpression =>
        if (!saved.exists(_.exprId == ne.exprId)) {
          saved += ne
        }
        ne.toAttribute
      case e: Expression if e.foldable =>
        e // No need to create an attribute reference if it will be evaluated as a Literal.
      case e: Expression =>
        // For other expressions, we extract it and replace it with an AttributeReference (with
        // an internal column name, e.g. "_gpu_w0"). Deduping it as we go.
        deduped.getOrElseUpdate(e, {
          val withName = GpuAlias(e, s"_gpu_w${saved.length}")()
          saved += withName
          withName.toAttribute
        })
    }

  /**
   * Checks whether the window spec is both ROWS-based and bounded.
   * Window functions of this spec can possibly still be batched.
   */
  private def isBoundedRowsWindowAndBatchable(spec: GpuWindowSpecDefinition,
      conf: RapidsConf): Boolean = {

    def inPermissibleRange(bounds: Int) =
      Math.abs(bounds) <= conf.batchedBoundedRowsWindowMax

    spec match {

      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral(prec: Int, _),
      GpuLiteral(foll: Int, _))) =>
        inPermissibleRange(prec) && inPermissibleRange(foll)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(CurrentRow),
      GpuLiteral(foll: Int, _))) =>
        inPermissibleRange(foll)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral(prec: Int, _),
      GpuSpecialFrameBoundary(CurrentRow))) =>
        inPermissibleRange(prec)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(CurrentRow),
      GpuSpecialFrameBoundary(CurrentRow))) => true
      case _ => false
    }
  }

  def getBoundedWindowPrecedingAndFollowing(spec: GpuWindowSpecDefinition): (Int, Int) =
    spec match {
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral(prec: Int, _),
      GpuLiteral(foll: Int, _))) => (prec, foll)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(CurrentRow),
      GpuLiteral(foll: Int, _))) => (0, foll)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral(prec: Int, _),
      GpuSpecialFrameBoundary(CurrentRow))) => (prec, 0)
      case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(CurrentRow),
      GpuSpecialFrameBoundary(CurrentRow))) => (0, 0)
      case _ => throw new IllegalArgumentException("Expected bounded ROWS spec, " +
          s"found $spec") // Can't reach here.
    }

  def isUnboundedToUnboundedWindow(spec: GpuWindowSpecDefinition): Boolean = spec match {
    case GpuWindowSpecDefinition(
           _, _, GpuSpecifiedWindowFrame(_,
           GpuSpecialFrameBoundary(UnboundedPreceding),
           GpuSpecialFrameBoundary(UnboundedFollowing))) => true
    case _ => false
  }

  def isBatchedRunningFunc(func: Expression, spec: GpuWindowSpecDefinition): Boolean = {
    val isSpecOkay = GpuWindowExec.isRunningWindow(spec)
    val isFuncOkay = func match {
      case f: GpuBatchedRunningWindowWithFixer => f.canFixUp
      case GpuAggregateExpression(f: GpuBatchedRunningWindowWithFixer, _, _, _, _) => f.canFixUp
      case _ => false
    }
    isSpecOkay && isFuncOkay
  }

  /**
   * Checks whether the window aggregation qualifies to be accelerated via group-by aggregation.
   * Currently, aggregations without group-by (i.e. whole-table aggregations) are not supported.
   */
  def isUnboundedToUnboundedAggFunc(func: Expression, spec: GpuWindowSpecDefinition,
      conf: RapidsConf): Boolean = {

    def noGroupByColumns(spec:GpuWindowSpecDefinition): Boolean = spec match {
      case GpuWindowSpecDefinition(partSpec, _, _) => partSpec.isEmpty
      case _ => false
    }

    if (!conf.isWindowUnboundedAggEnabled) {
      false
    } else {
      if (!GpuWindowExecMeta.isUnboundedToUnboundedWindow(spec)
            || noGroupByColumns(spec)) {
        false // Must be both unbounded, and have a group by specification.
      } else {
        func match {
          case _: GpuUnboundedToUnboundedWindowAgg => true
          case GpuAggregateExpression(_: GpuUnboundedToUnboundedWindowAgg, _, _, _, _) => true
          case _ => false
        }
      }
    }
  }

  // TODO delete this if we can support min, max, and count with
  //  isUnboundedToUnboundedAggFunc instead.
  def isBatchedDoublePassFunc(func: Expression, spec: GpuWindowSpecDefinition): Boolean =
    func match {
      case _: GpuUnboundToUnboundWindowWithFixer
        if GpuWindowExecMeta.isUnboundedToUnboundedWindow(spec) => true
      case GpuAggregateExpression(_: GpuUnboundToUnboundWindowWithFixer, _, _, _, _)
        if GpuWindowExecMeta.isUnboundedToUnboundedWindow(spec) => true
      case _ => false
    }

  def isBatchedFunc(func: Expression,
      spec: GpuWindowSpecDefinition,
      conf: RapidsConf): Boolean = {
    isBatchedRunningFunc(func, spec) ||
        isUnboundedToUnboundedAggFunc(func, spec, conf) ||
        isBatchedDoublePassFunc(func, spec) ||
        isBoundedRowsWindowAndBatchable(spec, conf)
  }

  def splitBatchedOps(windowOps: Seq[NamedExpression],
      conf: RapidsConf): BatchedOps = {
    val running = ArrayBuffer[NamedExpression]()
    val doublePass = ArrayBuffer[NamedExpression]()
    val unboundedToUnboundedAgg = ArrayBuffer[NamedExpression]()
    val batchedBounded = ArrayBuffer[NamedExpression]()
    val passThrough = ArrayBuffer[NamedExpression]()
    windowOps.foreach {
      case expr@GpuAlias(GpuWindowExpression(func, spec), _) =>
        if (isBatchedRunningFunc(func, spec)) {
          running.append(expr)
        } else if (isUnboundedToUnboundedAggFunc(func, spec, conf)) {
          unboundedToUnboundedAgg.append(expr)
        } else if (isBatchedDoublePassFunc(func, spec)) {
          doublePass.append(expr)
        } else if (isBoundedRowsWindowAndBatchable(spec, conf)) {
          batchedBounded.append(expr)
        } else {
          throw new IllegalArgumentException(
            s"Found unexpected expression $expr in window exec ${expr.getClass}")
        }
      case expr@(GpuAlias(_: AttributeReference, _) | _: AttributeReference) =>
        passThrough.append(expr)
      case other =>
        // This should only happen if we did something wrong in splitting/deduping
        // the window expressions.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    }
    BatchedOps(running.toSeq,
               unboundedToUnboundedAgg.toSeq,
               doublePass.toSeq,
               batchedBounded.toSeq,
               passThrough.toSeq)
  }

  /**
   * In some distributions expressions passed into WindowExec can have more operations
   * in them than just a WindowExpression wrapped in an GpuAlias. This is a problem if we
   * want to try and do multiple window operations in a single pass to speed things up
   * or if we need to add new transitive window functions when we are doing some memory
   * optimizations, like running window. This will split the input expressions
   * into three sets of expressions. The first set is a project with no window expressions in it at
   * all. The second takes the first as input and will only have aliases to columns in the first or
   * named expressions wrapping a single window function in it. The third uses the second as
   * input and will do any final steps to combine window functions together.
   *
   * For example `SUM(a) - SUM(b + c) over (PARTITION BY d ORDER BY e) as result` would be
   * transformed into
   * <pre>
   * Phase 1 (Pre project):
   * a, b + c as _tmp0, d, e
   *
   * Phase 2 (Window Operations):
   * SUM(a) over (PARTITION BY d ORDER BY e) as _tmp1,
   * SUM(_tmp0) over (PARTITION BY d ORDER BY e) as _tmp2
   *
   * Phase 3 (Post Project):
   * (_tmp1 - _tmp2) as result
   * </pre>
   *
   * To handle special cases (like window function of another window function eg `LAG(SUM(a), 2)`,
   * distros should split apart those into separate window operations. However, we will not
   * see all of these in just the input window expressions of the WindowExec, so we process
   * both the input fields and input window expressions, and handle whether we *only* want result
   * columns using the Post Project stage.
   *
   * @param inputFieldExprs the input fields converted to input expressions
   * @param windowExprs the input window expressions to a GpuWindowExec
   * @param resultColumnsOnly whether the output of the window operation only desires result
   * columns or the output of all input expressions
   */
  def splitAndDedup(inputFieldExprs: Seq[NamedExpression],
      windowExprs: Seq[NamedExpression],
      resultColumnsOnly: Boolean):
  (Seq[NamedExpression], Seq[NamedExpression], Seq[NamedExpression]) = {
    // This is based off of similar code in Apache Spark's `ExtractWindowExpressions.extract` but
    // has been highly modified
    val preProject = ArrayBuffer[NamedExpression]()
    val preDedupe = mutable.HashMap[Expression, Attribute]()
    val windowOps = ArrayBuffer[NamedExpression]()
    val windowDedupe = mutable.HashMap[Expression, Attribute]()
    val postProject = ArrayBuffer[NamedExpression]()

    // Process input field expressions first. There are no window functions here, so
    // all of these should pass at least to pre and window stages
    inputFieldExprs.foreach { expr =>
        // If the Spark distribution only wants to output result columns (ie, ones that
        // use projectList), then pass the input field to pre and window stages, but
        // do not pass to the post project stage (as those are specifically given in
        // the projectList set)
        if (resultColumnsOnly) {
          extractAndSave(
            extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
              .asInstanceOf[NamedExpression]
        } else {
          // If the WindowExec returns everything, then pass the input fields through all the
          // phases (with deduping)
          postProject += extractAndSave(
            extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
              .asInstanceOf[NamedExpression]
        }
    }

    // Now split and dedup the input window expressions
    windowExprs.foreach { expr =>
      if (hasGpuWindowFunction(expr)) {
        // First pass replace any operations that should be totally replaced.
        val replacePass = expr.transformDown {
          case GpuWindowExpression(
          GpuAggregateExpression(rep: GpuReplaceWindowFunction, _, _, _, _), spec)
            if rep.shouldReplaceWindow(spec) =>
            // We don't actually care about the GpuAggregateExpression because it is ignored
            // by our GPU window operations anyways.
            rep.windowReplacement(spec)
          case GpuWindowExpression(rep: GpuReplaceWindowFunction, spec)
            if rep.shouldReplaceWindow(spec) =>
            rep.windowReplacement(spec)
        }
        // Second pass looks for GpuWindowFunctions and GpuWindowSpecDefinitions to build up
        // the preProject phase
        val secondPass = replacePass.transformDown {
          case wf: GpuWindowFunction =>
            // All window functions, including those that are also aggregation functions, are
            // wrapped in a GpuWindowExpression, so dedup and save their children into the pre
            // stage, replacing them with aliases.
            val newChildren = wf.children.map(extractAndSave(_, preProject, preDedupe))
            wf.withNewChildren(newChildren)
          case wsc @ GpuWindowSpecDefinition(partitionSpec, orderSpec, _) =>
            // Extracts expressions from the partition spec and order spec to be sure that they
            // show up in the pre stage.  Because map is lazy we are going to force it to be
            // materialized, by forcing it to go through an array that cannot be lazily created
            val newPartitionSpec = partitionSpec.map(
              extractAndSave(_, preProject, preDedupe)).toArray.toSeq
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractAndSave(so.child, preProject, preDedupe)
              SortOrder(newChild, so.direction, so.nullOrdering, Seq.empty)
            }.toArray.toSeq
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)
        }
        // Final pass is to extract, dedup, and save the results.
        val finalPass = secondPass.transformDown {
          case we: GpuWindowExpression =>
            // A window Expression holds a window function or an aggregate function, so put it into
            // the windowOps phase, and create a new alias for it for the post phase
            extractAndSave(we, windowOps, windowDedupe)
        }.asInstanceOf[NamedExpression]

        postProject += finalPass
      } else {
        // There is no window function so pass the result through all of the phases (with deduping)
        postProject += extractAndSave(
          extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
            .asInstanceOf[NamedExpression]
      }
    }
    (preProject.toSeq, windowOps.toSeq, postProject.toSeq)
  }
}