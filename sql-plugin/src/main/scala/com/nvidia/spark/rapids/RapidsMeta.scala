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

import java.time.ZoneId

import scala.collection.mutable

import com.nvidia.spark.rapids.GpuTypedImperativeSupportedAggregateExecMeta.{preRowToColProjection, readBufferConverter}
import com.nvidia.spark.rapids.RapidsMeta.noNeedToReplaceReason
import com.nvidia.spark.rapids.shims.{AggregateInPandasExecShims, DistributionUtil, SparkShimImpl}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryExpression, BoundReference, Cast, ComplexTypeMergingExpression, Expression, Literal, QuaternaryExpression, RuntimeReplaceable, String2TrimExpression, TernaryExpression, TimeZoneAwareExpression, UnaryExpression, UTCTimestamp, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{ScalarSubquery, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.TimeZoneDB
import org.apache.spark.sql.rapids.aggregate.{CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinMetaBase, GpuBroadcastNestedLoopJoinMetaBase}
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, MapType, StringType, StructType}

trait DataFromReplacementRule {
  val operationName: String

  def noteDoc: Option[String] = None
  def incompatDoc: Option[String] = None
  def disabledMsg: Option[String] = None

  def confKey: String

  def getChecks: Option[TypeChecks[_]]
}

/**
 * A version of DataFromReplacementRule that is used when no replacement rule can be found.
 */
final class NoRuleDataFromReplacementRule extends DataFromReplacementRule {
  override val operationName: String = ""

  override def confKey = "NOT_FOUND"

  override def getChecks: Option[TypeChecks[_]] = None
}

object RapidsMeta {
  val gpuSupportedTag = TreeNodeTag[Set[String]]("rapids.gpu.supported")

  def noNeedToReplaceReason(klass: Class[_]) = s"there is no need to replace $klass"
}

/**
 * Holds metadata about a stage in the physical plan that is separate from the plan itself.
 * This is helpful in deciding when to replace part of the plan with a GPU enabled version.
 *
 * @param wrapped what we are wrapping
 * @param conf the config
 * @param parent the parent of this node, if there is one.
 * @param rule holds information related to the config for this object, typically this is the rule
 *          used to wrap the stage.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam OUTPUT when converting to a GPU enabled version of the plan, the generic base
 *                    type for all GPU enabled versions.
 */
abstract class RapidsMeta[INPUT <: BASE, BASE, OUTPUT <: BASE](
    val wrapped: INPUT,
    val conf: RapidsConf,
    val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) {

  /**
   * The wrapped plans that should be examined
   */
  val childPlans: Seq[SparkPlanMeta[_]]

  /**
   * The wrapped expressions that should be examined
   */
  val childExprs: Seq[BaseExprMeta[_]]

  /**
   * The wrapped scans that should be examined
   */
  val childScans: Seq[ScanMeta[_]]

  /**
   * The wrapped partitioning that should be examined
   */
  val childParts: Seq[PartMeta[_]]

  /**
   * The wrapped data writing commands that should be examined
   */
  val childDataWriteCmds: Seq[DataWritingCommandMeta[_]]

  /** The wrapped runnable commands that should be examined. */
  val childRunnableCmds: Seq[RunnableCommandMeta[_]] = Seq.empty

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = wrapped

  protected var cannotBeReplacedReasons: Option[mutable.Set[String]] = None
  private var mustBeReplacedReasons: Option[mutable.Set[String]] = None
  private var cannotReplaceAnyOfPlanReasons: Option[mutable.Set[String]] = None
  private var shouldBeRemovedReasons: Option[mutable.Set[String]] = None
  private var typeConversionReasons: Option[mutable.Set[String]] = None
  protected var cannotRunOnGpuBecauseOfSparkPlan: Boolean = false
  protected var cannotRunOnGpuBecauseOfCost: Boolean = false
  protected var willRunViaCpuBridgeReasons: Option[mutable.Set[String]] = None

  import RapidsMeta.gpuSupportedTag

  /**
   * Recursively force a section of the plan back onto CPU, stopping once a plan
   * is reached that is already on CPU.
   */
  final def recursiveCostPreventsRunningOnGpu(): Unit = {
    if (canThisBeReplaced && !mustThisBeReplaced) {
      costPreventsRunningOnGpu()
      childDataWriteCmds.foreach(_.recursiveCostPreventsRunningOnGpu())
      childRunnableCmds.foreach(_.recursiveCostPreventsRunningOnGpu())
    }
  }

  final def costPreventsRunningOnGpu(): Unit = {
    cannotRunOnGpuBecauseOfCost = true
    willNotWorkOnGpu("Removed by cost-based optimizer")
    childExprs.foreach(_.recursiveCostPreventsRunningOnGpu())
    childParts.foreach(_.recursiveCostPreventsRunningOnGpu())
    childScans.foreach(_.recursiveCostPreventsRunningOnGpu())
  }

  final def recursiveSparkPlanPreventsRunningOnGpu(): Unit = {
    cannotRunOnGpuBecauseOfSparkPlan = true
    childExprs.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childParts.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childScans.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childDataWriteCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childRunnableCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
  }

  final def recursiveSparkPlanRemoved(): Unit = {
    shouldBeRemoved("parent plan is removed")
    childExprs.foreach(_.recursiveSparkPlanRemoved())
    childParts.foreach(_.recursiveSparkPlanRemoved())
    childScans.foreach(_.recursiveSparkPlanRemoved())
    childDataWriteCmds.foreach(_.recursiveSparkPlanRemoved())
    childRunnableCmds.foreach(_.recursiveSparkPlanRemoved())
  }

  /**
   * Call this to indicate that this should not be replaced with a GPU enabled version
   * @param because why it should not be replaced.
   */
  final def willNotWorkOnGpu(because: String): Unit = {
    cannotBeReplacedReasons.get.add(because)
    // annotate the real spark plan with the reason as well so that the information is available
    // during query stage planning when AQE is on
    wrapped match {
      case p: SparkPlan =>
        p.setTagValue(gpuSupportedTag,
          p.getTagValue(gpuSupportedTag).getOrElse(Set.empty) + because)
      case _ =>
    }
  }

  final def mustBeReplaced(because: String): Unit = {
    mustBeReplacedReasons.get.add(because)
  }

  /**
   * Call this if there is a condition found that the entire plan is not allowed
   * to run on the GPU.
   */
  final def entirePlanWillNotWork(because: String): Unit = {
    cannotReplaceAnyOfPlanReasons.get.add(because)
    // recursively tag the plan so that AQE does not attempt
    // to run any of the child query stages on the GPU
    willNotWorkOnGpu(because)
    childPlans.foreach(_.entirePlanWillNotWork(because))
  }

  final def shouldBeRemoved(because: String): Unit =
    shouldBeRemovedReasons.get.add(because)

  /**
   * Call this method to record information about type conversions via DataTypeMeta.
   */
  final def addConvertedDataType(expression: Expression, typeMeta: DataTypeMeta): Unit = {
    typeConversionReasons.get.add(
      s"$expression: ${typeMeta.reasonForConversion}")
  }

  /**
   * Returns true if this node should be removed.
   */
  final def shouldThisBeRemoved: Boolean = shouldBeRemovedReasons.exists(_.nonEmpty)

  /**
   * Returns true iff this could be replaced.
   */
  final def canThisBeReplaced: Boolean = cannotBeReplacedReasons.exists(_.isEmpty)

  /**
   * Check if this will run via CPU bridge
   */
  final def willUseGpuCpuBridge: Boolean = willRunViaCpuBridgeReasons.exists(_.nonEmpty)

  /**
   * Check if this node itself has CPU bridge expressions anywhere in its expression tree.
   * This recursively checks all expressions but does NOT check descendant SparkPlan nodes.
   */
  final def hasDirectCpuBridgeExpressions: Boolean = {
    hasAnyCpuBridgeInExpressionTree
  }

  /**
   * Recursively check if any expression in this node's expression tree uses CPU bridge.
   * This checks the current node and recursively checks all child expressions.
   * For non-expression types (Parts, Scans, etc.), only checks direct usage.
   */
  protected def hasAnyCpuBridgeInExpressionTree: Boolean = {
    willUseGpuCpuBridge ||
    childExprs.exists(_.hasAnyCpuBridgeInExpressionTree) ||
    childParts.exists(_.willUseGpuCpuBridge) ||
    childScans.exists(_.willUseGpuCpuBridge) ||
    childDataWriteCmds.exists(_.willUseGpuCpuBridge) ||
    childRunnableCmds.exists(_.willUseGpuCpuBridge)
  }

  /**
   * Returns true iff this must be replaced because its children have already been
   * replaced and this needs to also be replaced for compatibility.
   */
  final def mustThisBeReplaced: Boolean = mustBeReplacedReasons.exists(_.nonEmpty)

  /**
   * Returns the list of reasons the entire plan can't be replaced. An empty
   * set means the entire plan is ok to be replaced, do the normal checking
   * per exec and children.
   */
  final def entirePlanExcludedReasons: Set[String] = {
    cannotReplaceAnyOfPlanReasons.getOrElse(mutable.Set.empty).toSet
  }

  /**
   * Returns true iff all of the expressions and their children could be replaced.
   */
  def canExprTreeBeReplaced: Boolean = childExprs.forall(_.canExprTreeBeReplaced)

  /**
   * Returns true iff all of the scans can be replaced.
   */
  def canScansBeReplaced: Boolean = childScans.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the partitioning can be replaced.
   */
  def canPartsBeReplaced: Boolean = childParts.forall(_.canThisBeReplaced)

  /**
   * Return true if the resulting node in the plan will support columnar execution
   */
  def supportsColumnar: Boolean = canThisBeReplaced

  /**
   * Returns true iff all of the data writing commands can be replaced.
   */
  def canDataWriteCmdsBeReplaced: Boolean = childDataWriteCmds.forall(_.canThisBeReplaced)

  def confKey: String = rule.confKey
  final val operationName: String = rule.operationName
  final val incompatDoc: Option[String] = rule.incompatDoc
  def isIncompat: Boolean = incompatDoc.isDefined
  final val disabledMsg: Option[String] = rule.disabledMsg
  def isDisabledByDefault: Boolean = disabledMsg.isDefined

  def initReasons(): Unit = {
    cannotBeReplacedReasons = Some(mutable.Set[String]())
    mustBeReplacedReasons = Some(mutable.Set[String]())
    shouldBeRemovedReasons = Some(mutable.Set[String]())
    cannotReplaceAnyOfPlanReasons = Some(mutable.Set[String]())
    typeConversionReasons = Some(mutable.Set[String]())
    willRunViaCpuBridgeReasons = Some(mutable.Set[String]())
  }

  /**
   * Tag all of the children to see if they are GPU compatible first.
   * Do basic common verification for the operators, and then call
   * [[tagSelfForGpu]]
   */
  final def tagForGpu(): Unit = {
    childScans.foreach(_.tagForGpu())
    childParts.foreach(_.tagForGpu())
    childExprs.foreach(_.tagForGpu())
    // Allow subclasses (e.g., SparkPlanMeta) to run expression-tree optimizations once
    runChildExprBridgeOptimization()
    childDataWriteCmds.foreach(_.tagForGpu())
    childRunnableCmds.foreach(_.tagForGpu())
    childPlans.foreach(_.tagForGpu())

    initReasons()

    if (!conf.isOperatorEnabled(confKey, isIncompat, isDisabledByDefault)) {
      if (isIncompat && !conf.isIncompatEnabled) {
        willNotWorkOnGpu(s"the GPU version of ${wrapped.getClass.getSimpleName}" +
            s" is not 100% compatible with the Spark version. ${incompatDoc.get}. To enable this" +
            s" $operationName despite the incompatibilities please set the config" +
            s" $confKey to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
            s" to enable all incompatible ops")
      } else if (isDisabledByDefault) {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled, and is disabled by default because ${disabledMsg.get}. Set $confKey" +
            s" to true if you wish to enable it")
      } else {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled. Set $confKey to true if you wish to enable it")
      }
    }

    tagSelfForGpu()
  }

  // Hook for subclasses to run expression-level optimizations at the appropriate layer.
  // Default no-op so non-plan metas do not run bridge optimization.
  protected def runChildExprBridgeOptimization(): Unit = {}

  /**
   * Do any extra checks and tag yourself if you are compatible or not.  Be aware that this may
   * already have been marked as incompatible for a number of reasons.
   *
   * All of your children should have already been tagged so if there are situations where you
   * may need to disqualify your children for various reasons you may do it here too.
   */
  def tagSelfForGpu(): Unit

  protected def indent(append: StringBuilder, depth: Int): Unit =
    append.append("  " * depth)

  def replaceMessage: String = "run on GPU"
  def noReplacementPossibleMessage(reasons: String): String = s"cannot run on GPU because $reasons"
  def suppressWillWorkOnGpuInfo: Boolean = false

  private def willWorkOnGpuInfo: String = cannotBeReplacedReasons match {
    case None => "NOT EVALUATED FOR GPU YET"
    case Some(v) if v.isEmpty && willUseGpuCpuBridge =>
      val bridgeReasons = willRunViaCpuBridgeReasons.get.mkString("; ")
      s"will run on GPU via CPU bridge because $bridgeReasons"
    case Some(v) if v.isEmpty &&
        (cannotRunOnGpuBecauseOfSparkPlan || shouldThisBeRemoved) => "could " + replaceMessage
    case Some(v) if v.isEmpty => "will " + replaceMessage
    case Some(v) =>
      noReplacementPossibleMessage(v.mkString("; "))
  }

  private def willBeRemovedInfo: String = shouldBeRemovedReasons match {
    case None => ""
    case Some(v) if v.isEmpty => ""
    case Some(v) =>
      val reasons = v.mkString("; ")
      s" but is going to be removed because $reasons"
  }

  private def typeConversionInfo: String = typeConversionReasons match {
    case None => ""
    case Some(v) if v.isEmpty => ""
    case Some(v) =>
      "The data type of following expressions will be converted in GPU runtime: " +
          v.mkString("; ")
  }

  /**
   * When converting this to a string should we include the string representation of what this
   * wraps too?  This is off by default.
   */
  protected val printWrapped = false

  final private def getIndicatorChar: String = {
    if (shouldThisBeRemoved) {
      "#"
    } else if (cannotRunOnGpuBecauseOfCost) {
      "$"
    } else if (canThisBeReplaced) {
      if (willUseGpuCpuBridge) {
        "^"  // Special indicator for CPU bridge expressions
      } else if (cannotRunOnGpuBecauseOfSparkPlan) {
        "@"
      } else if (cannotRunOnGpuBecauseOfCost) {
        "$"
      } else {
        "*"
      }
    } else {
      "!"
    }
  }

  def checkTimeZoneId(sessionZoneId: ZoneId): Unit = {
    // Both of the Spark session time zone and JVM's default time zone should be UTC.
    if (!TimeZoneDB.isSupportedTimezone(sessionZoneId)) {
      willNotWorkOnGpu("Not supported zone id. " +
        s"Actual session local zone id: $sessionZoneId")
    }

    val defaultZoneId = ZoneId.systemDefault()
    if (!TimeZoneDB.isSupportedTimezone(defaultZoneId)) {
      willNotWorkOnGpu(s"Not supported zone id. Actual default zone id: $defaultZoneId")
    }
  }

  /**
   * Create a string representation of this in append.
   * @param strBuilder where to place the string representation.
   * @param depth how far down the tree this is.
   * @param all should all the data be printed or just what does not work on the GPU?
   */
  def print(strBuilder: StringBuilder, depth: Int, all: Boolean): Unit = {
    if ((all || !canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan || 
         (!all && hasDirectCpuBridgeExpressions)) &&
        !suppressWillWorkOnGpuInfo) {
      indent(strBuilder, depth)
      strBuilder.append(getIndicatorChar)

      strBuilder.append(operationName)
        .append(" <")
        .append(wrapped.getClass.getSimpleName)
        .append("> ")

      if (printWrapped) {
        strBuilder.append(wrapped)
          .append(" ")
      }

      strBuilder.append(willWorkOnGpuInfo).
        append(willBeRemovedInfo)

      typeConversionInfo match {
        case info if info.isEmpty =>
        case info => strBuilder.append(". ").append(info)
      }

      strBuilder.append("\n")
    }
    printChildren(strBuilder, depth, all)
  }

  private final def printChildren(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    childScans.foreach(_.print(append, depth + 1, all))
    childParts.foreach(_.print(append, depth + 1, all))
    childExprs.foreach(_.print(append, depth + 1, all))
    childDataWriteCmds.foreach(_.print(append, depth + 1, all))
    childRunnableCmds.foreach(_.print(append, depth + 1, all))
    childPlans.foreach(_.print(append, depth + 1, all))
  }

  def explain(all: Boolean): String = {
    val appender = new StringBuilder()
    print(appender, 0, all)
    appender.toString()
  }

  override def toString: String = {
    explain(true)
  }
}

/**
 * Base class for metadata around `Partitioning`.
 */
abstract class PartMeta[INPUT <: Partitioning](part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Partitioning, GpuPartitioning](part, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override final def tagSelfForGpu(): Unit = {
    rule.getChecks.foreach(_.tag(this))
    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
    tagPartForGpu()
  }

  def tagPartForGpu(): Unit = {}

  override protected def runChildExprBridgeOptimization(): Unit = {
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(childExprs)
  }
}

/**
 * Metadata for Partitioning with no rule found
 */
final class RuleNotFoundPartMeta[INPUT <: Partitioning](
    part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends PartMeta[INPUT](part, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagPartForGpu(): Unit = {
    willNotWorkOnGpu(s"GPU does not currently support the operator ${part.getClass}")
  }

  override def convertToGpu(): GpuPartitioning =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanMeta[INPUT <: Scan](scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Scan, GpuScan](scan, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}

  def supportsRuntimeFilters: Boolean = false
}

/**
 * Metadata for `Scan` with no rule found
 */
final class RuleNotFoundScanMeta[INPUT <: Scan](
    scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ScanMeta[INPUT](scan, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"GPU does not currently support the operator ${scan.getClass}")
  }

  override def convertToGpu(): GpuScan =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `DataWritingCommand`.
 */
abstract class DataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RapidsMeta[INPUT, DataWritingCommand, GpuDataWritingCommand](cmd, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  def checkTimeZone(): Boolean = true

  final override def tagSelfForGpu(): Unit = {
    if (checkTimeZone()) {
      timezoneCheck()
    }
    // Update our expressions to allow them to run with the bridge if possible
    tagSelfForGpuInternal()
  }

  protected def tagSelfForGpuInternal(): Unit = {}

  // Check whether data type of intput/output contains timestamp type, which
  // is related to time zone.
  // Only UTC time zone is allowed to be consistent with previous behavior
  // for [[DataWritingCommand]]. Needs to override [[checkTimeZone]] to skip
  // UTC time zone check in sub class of [[DataWritingCommand]].
  private def timezoneCheck(): Unit = {
    val types = (wrapped.inputSet.map(_.dataType) ++ wrapped.outputSet.map(_.dataType)).toSet
    if (types.exists(GpuOverrides.isOrContainsTimestamp(_))) {
      if (!GpuOverrides.isUTCTimezone()) {
        willNotWorkOnGpu("Only UTC timezone is supported. " +
          s"Current timezone settings: (JVM : ${ZoneId.systemDefault()}, " +
          s"session: ${SQLConf.get.sessionLocalTimeZone}). ")
      }
    }
  }
}

/**
 * Metadata for `DataWritingCommand` with no rule found
 */
final class RuleNotFoundDataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends DataWritingCommandMeta[INPUT](cmd, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpuInternal(): Unit = {
    willNotWorkOnGpu(s"GPU does not currently support the operator ${cmd.getClass}")
  }

  override def convertToGpu(): GpuDataWritingCommand =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `SparkPlan`.
 */
abstract class SparkPlanMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, SparkPlan, GpuExec](plan, conf, parent, rule) {

  def tagForExplain(): Unit = {
    if (!canThisBeReplaced) {
      childExprs.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childParts.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childScans.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childDataWriteCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childRunnableCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    }
    if (shouldThisBeRemoved) {
      childExprs.foreach(_.recursiveSparkPlanRemoved())
      childParts.foreach(_.recursiveSparkPlanRemoved())
      childScans.foreach(_.recursiveSparkPlanRemoved())
      childDataWriteCmds.foreach(_.recursiveSparkPlanRemoved())
      childRunnableCmds.foreach(_.recursiveSparkPlanRemoved())
    }
    childPlans.foreach(_.tagForExplain())
  }

  override protected def runChildExprBridgeOptimization(): Unit = {
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(childExprs)
  }

  def requireAstForGpuOn(exprMeta: BaseExprMeta[_]): Unit = {
    // willNotWorkOnGpu does not deduplicate reasons. Most of the time that is fine
    // but here we want to avoid adding the reason twice, because this method can be
    // called multiple times, and also the reason can automatically be added in if
    // a child expression would not work in the non-AST case either.
    // So only add it if canExprTreeBeReplaced changed after requiring that the
    // given expression is AST-able.
    val previousExprReplaceVal = canExprTreeBeReplaced
    exprMeta.requireAstForGpu()
    val newExprReplaceVal = canExprTreeBeReplaced
    if (previousExprReplaceVal != newExprReplaceVal &&
        !newExprReplaceVal) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
  }

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] =
    plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] = Map.empty

  var cpuCost: Double = 0
  var gpuCost: Double = 0
  var estimatedOutputRows: Option[BigInt] = None

  override def convertToCpu(): SparkPlan = {
    wrapped.withNewChildren(childPlans.map(_.convertIfNeeded()))
  }

  def getReasonsNotToReplaceEntirePlan: Set[String] = {
    val childReasons = childPlans.flatMap(_.getReasonsNotToReplaceEntirePlan)
    entirePlanExcludedReasons ++ childReasons
  }

  // For adaptive execution we have to ensure we mark everything properly
  // the first time through and that has to match what happens when AQE
  // splits things up and does the subquery analysis at the shuffle boundaries.
  // If the AQE subquery analysis changes the plan from what is originally
  // marked we can end up with mismatches like happened in:
  // https://github.com/NVIDIA/spark-rapids/issues/1423
  // AQE splits subqueries at shuffle boundaries which means that it only
  // sees the children at that point. So in our fix up exchange we only
  // look at the children and mark is at will not work on GPU if the
  // child can't be replaced.
  private def fixUpExchangeOverhead(): Unit = {
    childPlans.foreach(_.fixUpExchangeOverhead())
    if (wrapped.isInstanceOf[ShuffleExchangeExec] &&
        !SparkShimImpl.isExecutorBroadcastShuffle(wrapped.asInstanceOf[ShuffleExchangeExec]) &&
        !childPlans.exists(_.supportsColumnar) &&
        (plan.conf.adaptiveExecutionEnabled ||
        !parent.exists(_.supportsColumnar))) {
      // Some platforms can present a plan where the root of the plan is a shuffle followed by
      // an AdaptiveSparkPlanExec. If it looks like the child AdaptiveSparkPlanExec will end up
      // on the GPU than this shuffle should be GPU as well.
      val shuffle = wrapped.asInstanceOf[ShuffleExchangeExec]
      val isChildOnGpu = shuffle.child match {
        case ap: AdaptiveSparkPlanExec if parent.isEmpty => GpuOverrides.probablyGpuPlan(ap, conf)
        case _ => false
      }

      if (!isChildOnGpu) {
        willNotWorkOnGpu("Columnar exchange without columnar children is inefficient")
      }

      readBufferConverter(childPlans.head.wrapped, isR2C = true).foreach { r2c =>
        wrapped.setTagValue(preRowToColProjection, r2c -> 0)
      }
    }
  }

  private def fixUpBroadcastJoins(): Unit = {
    childPlans.foreach(_.fixUpBroadcastJoins())
    wrapped match {
      case _: BroadcastHashJoinExec =>
        this.asInstanceOf[GpuBroadcastHashJoinMetaBase].checkTagForBuildSide()
      case _: BroadcastNestedLoopJoinExec =>
        this.asInstanceOf[GpuBroadcastNestedLoopJoinMetaBase].checkTagForBuildSide()
      case _ => // noop
    }
  }

  /**
   * Run rules that happen for the entire tree after it has been tagged initially.
   */
  def runAfterTagRules(): Unit = {
    // In the first pass tagSelfForGpu will deal with each operator individually.
    // Children will be tagged first and then their parents will be tagged.  This gives
    // flexibility when tagging yourself to look at your children and disable yourself if your
    // children are not all on the GPU.  In some cases we need to be able to disable our
    // children too, or in this case run a rule that will disable operations when looking at
    // more of the tree.  These exceptions should be documented here.  We need to take special care
    // that we take into account all side-effects of these changes, because we are **not**
    // re-triggering the rules associated with parents, grandparents, etc.  If things get too
    // complicated we may need to update this to have something with triggers, but then we would
    // have to be very careful to avoid loops in the rules.

    // RULES:
    // 1) If file scan plan runs on the CPU, and the following plans run on GPU, then
    // GpuRowToColumnar will be inserted. GpuRowToColumnar will invalid input_file_xxx operations,
    // So input_file_xxx in the following GPU operators will get empty value.
    // InputFileBlockRule is to prevent the SparkPlans
    // [SparkPlan (with first input_file_xxx expression), FileScan) to run on GPU
    InputFileBlockRule(this.asInstanceOf[SparkPlanMeta[SparkPlan]])

    // 2) For shuffles, avoid replacing the shuffle if the child is not going to be replaced.
    fixUpExchangeOverhead()

    // 3) Some child nodes can't run on GPU if parent nodes can't run on GPU.
    // WriteFilesExec is a new operator from Spark version 340,
    // Did not extract a shim code for simplicity
    tagChildAccordingToParent(this.asInstanceOf[SparkPlanMeta[SparkPlan]], "WriteFilesExec")

    // 4) InputFileBlockRule may change the meta of broadcast join and its child plans,
    //    and this change may cause mismatch between the join and its build side
    //    BroadcastExchangeExec, leading to errors. Need to fix the mismatch.
    fixUpBroadcastJoins()
  }

  /**
   * tag child node can't run on GPU if parent node can't run on GPU and child node is a `typeName`
   * From Spark 340, plan is like:
   *    InsertIntoHadoopFsRelationCommand
   *    +- WriteFiles
   *      +- sub plan
   * Instead of:
   *    InsertIntoHadoopFsRelationCommand
   *    +- sub plan
   * WriteFiles is a temporary node and does not have input and output, it acts like a tag node.
   * @param p        plan
   * @param typeName type name
   */
  private def tagChildAccordingToParent(p: SparkPlanMeta[SparkPlan], typeName: String): Unit = {
    p.childPlans.foreach(e => tagChildAccordingToParent(e, typeName))
    if (p.wrapped.getClass.getSimpleName.equals(typeName)) {
      assert(p.parent.isDefined)
      if (!p.parent.get.canThisBeReplaced) {
        // parent can't run on GPU, also tag this.
        p.willNotWorkOnGpu(
          s"$typeName can't run on GPU because parent can't run on GPU")
      }
    }
  }

  override final def tagSelfForGpu(): Unit = {
    rule.getChecks.foreach(_.tag(this))

    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }

    if (!canScansBeReplaced) {
      willNotWorkOnGpu("not all scans can be replaced")
    }

    if (!canPartsBeReplaced) {
      willNotWorkOnGpu("not all partitioning can be replaced")
    }

    if (!canDataWriteCmdsBeReplaced) {
      willNotWorkOnGpu("not all data writing commands can be replaced")
    }

    if (!childRunnableCmds.forall(_.canThisBeReplaced)) {
      willNotWorkOnGpu("not all commands can be replaced")
    }
    // All ExecMeta extend SparkMeta. We need to check if the requiredChildDistribution
    // is recognized or not. If it's unrecognized Distribution then we fall back to CPU.
    plan.requiredChildDistribution.foreach { d =>
      if (!DistributionUtil.isSupported(d)) {
        willNotWorkOnGpu(s"unsupported required distribution: $d")
      }
    }

    checkExistingTags()

    tagPlanForGpu()
  }

  /**
   * When AQE is enabled and we are planning a new query stage, we need to look at meta-data
   * previously stored on the spark plan to determine whether this operator can run on GPU
   */
  def checkExistingTags(): Unit = {
    wrapped.getTagValue(RapidsMeta.gpuSupportedTag)
      .foreach(_.diff(cannotBeReplacedReasons.get)
      .foreach(willNotWorkOnGpu))
  }

  /**
   * Called to verify that this plan will work on the GPU. Generic checks will have already been
   * done. In general this method should only tag this operator as bad.  If it needs to tag
   * one of its children please take special care to update the comment inside
   * `tagSelfForGpu` so we don't end up with something that could be cyclical.
   */
  def tagPlanForGpu(): Unit = {}

  /**
   * If this is enabled to be converted to a GPU version convert it and return the result, else
   * do what is needed to possibly convert the rest of the plan.
   */
  final def convertIfNeeded(): SparkPlan = {
    if (shouldThisBeRemoved) {
      if (childPlans.isEmpty) {
        throw new IllegalStateException("can't remove when plan has no children")
      } else if (childPlans.size > 1) {
        throw new IllegalStateException("can't remove when plan has more than 1 child")
      }
      childPlans.head.convertIfNeeded()
    } else {
      if (canThisBeReplaced) {
        convertToGpu()
      } else {
        convertToCpu()
      }
    }
  }

  /**
   * Gets output attributes of current SparkPlanMeta, which is supposed to be called during
   * type checking for the current plan.
   *
   * By default, it simply returns the output of wrapped plan. For specific plans, they can
   * override outputTypeMetas to apply custom conversions on the output of wrapped plan. For plans
   * which just pass through the schema of childPlan, they can set useOutputAttributesOfChild to
   * true, in order to propagate the custom conversions of childPlan if they exist.
   */
  def outputAttributes: Seq[Attribute] = outputTypeMetas match {
    case Some(typeMetas) =>
      require(typeMetas.length == wrapped.output.length,
        "The length of outputTypeMetas doesn't match to the length of plan's output")
      wrapped.output.zip(typeMetas).map {
        case (ar, meta) if meta.typeConverted =>
          addConvertedDataType(ar, meta)
          AttributeReference(ar.name, meta.dataType.get, ar.nullable, ar.metadata)(
            ar.exprId, ar.qualifier)
        case (ar, _) =>
          ar
      }
    case None if useOutputAttributesOfChild =>
      require(wrapped.children.length == 1,
        "useOutputAttributesOfChild ONLY works on UnaryPlan")
      // We pass through the outputAttributes of the child plan only if it will be really applied
      // in the runtime. We can pass through either if child plan can be replaced by GPU overrides;
      // or if child plan is available for runtime type conversion. The later condition indicates
      // the CPU to GPU data transition will be introduced as the pre-processing of the adjacent
      // GpuRowToColumnarExec, though the child plan can't produce output attributes for GPU.
      // Otherwise, we should fetch the outputAttributes from the wrapped plan.
      //
      // We can safely call childPlan.canThisBeReplaced here, because outputAttributes is called
      // via tagSelfForGpu. At this point, tagging of the child plan has already taken place.
      if (childPlans.head.canThisBeReplaced || childPlans.head.availableRuntimeDataTransition) {
        childPlans.head.outputAttributes
      } else {
        wrapped.output
      }
    case None =>
      wrapped.output
  }

  /**
   * Returns whether the resulting SparkPlan supports columnar execution
   */
  override def supportsColumnar: Boolean = wrapped.supportsColumnar || canThisBeReplaced 

  /**
   * Overrides this method to implement custom conversions for specific plans.
   */
  protected lazy val outputTypeMetas: Option[Seq[DataTypeMeta]] = None

  /**
   * Whether to pass through the outputAttributes of childPlan's meta, only for UnaryPlan
   */
  protected val useOutputAttributesOfChild: Boolean = false

  /**
   * Whether there exists runtime data transition for the wrapped plan, if true, the overriding
   * of output attributes will always work even when the wrapped plan can't be replaced by GPU
   * overrides.
   */
  val availableRuntimeDataTransition: Boolean = false
}

/**
 * Metadata for `SparkPlan` with no rule found
 */
final class RuleNotFoundSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(s"GPU does not currently support the operator ${plan.getClass}")

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Metadata for `SparkPlan` that should not be replaced or have any kind of warning for
 */
final class DoNotReplaceOrWarnSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleDataFromReplacementRule) {

  /** We don't want to spam the user with messages about these operators */
  override def suppressWillWorkOnGpuInfo: Boolean = true

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(noNeedToReplaceReason(plan.getClass))

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

sealed abstract class ExpressionContext
object ProjectExprContext extends ExpressionContext {
  override def toString: String = "project"
}
/**
 * This is a special context. All other contexts are determined by the Spark query in a generic way.
 * AST support in many cases is an optimization and so it is tagged and checked after it is
 * determined that this operation will run on the GPU. In other cases it is required. In those cases
 * AST support is determined and used when tagging the metas to see if they will work on the GPU or
 * not. This part is not done automatically.
 */
object AstExprContext extends ExpressionContext  {
  override def toString: String = "AST"

  val notSupportedMsg = "this expression does not support AST"
}
object GroupByAggExprContext extends ExpressionContext  {
  override def toString: String = "aggregation"
}
object ReductionAggExprContext extends ExpressionContext  {
  override def toString: String = "reduction"
}
object WindowAggExprContext extends ExpressionContext  {
  override def toString: String = "window"
}

object ExpressionContext {
  private[this] def findParentPlanMeta(meta: BaseExprMeta[_]): Option[SparkPlanMeta[_]] =
    meta.parent match {
      case Some(p: BaseExprMeta[_]) => findParentPlanMeta(p)
      case Some(p: SparkPlanMeta[_]) => Some(p)
      case _ => None
    }

  def getAggregateFunctionContext(meta: BaseExprMeta[_]): ExpressionContext = {
    val parent = findParentPlanMeta(meta)
    assert(parent.isDefined, "It is expected that an aggregate function is a child of a SparkPlan")
    parent.get.wrapped match {
      case agg: SparkPlan if SparkShimImpl.isWindowFunctionExec(agg) =>
        WindowAggExprContext
      // AggregateInPandasExec renamed to ArrowAggregatePythonExec in Spark 4.1.0
      case agg: SparkPlan if AggregateInPandasExecShims.isAggregateInPandasExec(agg) =>
        if (AggregateInPandasExecShims.getGroupingExpressions(agg).isEmpty) {
          ReductionAggExprContext
        } else {
          GroupByAggExprContext
        }
      case agg: BaseAggregateExec =>
        // Since Spark 3.5, Python udfs are wrapped in AggregateInPandasExec. UDFs for earlier
        // versions of Spark should be handled by the BaseAggregateExec
        if (agg.groupingExpressions.isEmpty) {
          ReductionAggExprContext
        } else {
          GroupByAggExprContext
        }
      case _ => throw new IllegalStateException(
        s"Found an aggregation function in an unexpected context $parent")
    }
  }

  def getRegularOperatorContext(meta: RapidsMeta[_, _, _]): ExpressionContext = meta.wrapped match {
    case _: Expression if meta.parent.isDefined => getRegularOperatorContext(meta.parent.get)
    case _ => ProjectExprContext
  }
}

/**
 * The metadata around `DataType`, which records the original data type, the desired data type for
 * GPU overrides, and the reason of potential conversion. The metadata is to ensure TypeChecks
 * tagging the actual data types for GPU runtime, since data types of GPU overrides may slightly
 * differ from original CPU counterparts.
 */
class DataTypeMeta(
    val wrapped: Option[DataType],
    desired: Option[DataType] = None,
    reason: Option[String] = None) {

  lazy val dataType: Option[DataType] = desired match {
    case Some(dt) => Some(dt)
    case None => wrapped
  }

  // typeConverted will only be true if there exists DataType in wrapped expression
  lazy val typeConverted: Boolean = dataType.nonEmpty && dataType != wrapped

  /**
   * Returns the reason for conversion if exists
   */
  def reasonForConversion: String = {
    val reasonMsg = (if (typeConverted) reason else None)
        .map(r => s", because $r").getOrElse("")
    s"Converted ${wrapped.getOrElse("N/A")} to " +
      s"${dataType.getOrElse("N/A")}" + reasonMsg
  }
}

object DataTypeMeta {
  /**
   * create DataTypeMeta from Expression
   */
  def apply(expr: Expression, overrideType: Option[DataType]): DataTypeMeta = {
    val wrapped = try {
      Some(expr.dataType)
    } catch {
      case _: java.lang.UnsupportedOperationException => None
      case _: org.apache.spark.SparkException => None
    }
    new DataTypeMeta(wrapped, overrideType)
  }
}

/**
 * Base class for metadata around `Expression`.
 */
abstract class BaseExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Expression, Expression](expr, conf, parent, rule) {

  private val cannotBeAstReasons: mutable.Set[String] = mutable.Set.empty
  private var mustBeAst = false

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] =
    expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override val printWrapped: Boolean = true

  def dataType: DataType = expr.dataType

  val ignoreUnsetDataTypes = false

  override def canExprTreeBeReplaced: Boolean =
    canThisBeReplaced && super.canExprTreeBeReplaced

  /**
   * Gets the DataTypeMeta of current BaseExprMeta, which is supposed to be called in the
   * tag methods of expression-level type checks.
   *
   * By default, it simply returns the data type of wrapped expression. But for specific
   * expressions, they can easily override data type for type checking through calling the
   * method `overrideDataType`.
   */
  def typeMeta: DataTypeMeta = DataTypeMeta(wrapped.asInstanceOf[Expression], overrideType)

  /**
   * Overrides the data type of the wrapped expression during type checking.
   *
   * NOTICE: This method will NOT modify the wrapped expression itself. Therefore, the actual
   * transition on data type is still necessary when converting this expression to GPU.
   */
  def overrideDataType(dt: DataType): Unit = overrideType = Some(dt)

  private var overrideType: Option[DataType] = None

  lazy val context: ExpressionContext = expr match {
    case _: WindowExpression => WindowAggExprContext
    case _: WindowFunction => WindowAggExprContext
    case _: AggregateFunction => ExpressionContext.getAggregateFunctionContext(this)
    case _: AggregateExpression => ExpressionContext.getAggregateFunctionContext(this)
    case _ => ExpressionContext.getRegularOperatorContext(this)
  }

  val isFoldableNonLitAllowed: Boolean = conf.isFoldableNonLitAllowed

  // There are 2 levels of timezone check in GPU plan tag phase:
  //    Level 1: Check whether an expression is related to timezone. This is achieved by
  //        [[needTimeZoneCheck]] below.
  //    Level 2: Check related expression has been implemented with timezone. There is a
  //        toggle flag [[isTimeZoneSupported]] for this. If false, fallback to UTC-only check as
  //        before. If yes, move to next level check. When we add timezone support for a related
  //        function. [[isTimeZoneSupported]] should be override as true.
  def checkExprForTimezone(): Unit = {
    // Level 1 check
    if (!needTimeZoneCheck) return

    // Level 2 check
    if (!isTimeZoneSupported) return checkUTCTimezone(this, getZoneId())
  }

  protected def getZoneId(): ZoneId = {
    this.wrapped match {
      case tzExpr: TimeZoneAwareExpression => tzExpr.zoneId
      case ts: UTCTimestamp => {
        assert(false, s"Have to override getZoneId() of BaseExprMeta in ${this.getClass.toString}")
        throw new IllegalArgumentException(s"Failed to get zone id from ${ts.getClass.toString}")
      }
      case _ => throw new IllegalArgumentException(
        s"Zone check should never been happened to ${this.getClass.toString} " +
          "which is not timezone related")
    }
  }

  // Level 1 timezone checking flag
  // Both [[isTimeZoneSupported]] and [[needTimeZoneCheck]] are needed to check whether timezone
  // check needed. For cast expression, only some cases are needed pending on its data type and
  // its child's data type.
  //
  //+------------------------+-------------------+-----------------------------------------+
  //|         Value          | needTimeZoneCheck |           isTimeZoneSupported           |
  //+------------------------+-------------------+-----------------------------------------+
  //| TimezoneAwareExpression| True              | False by default, True when implemented |
  //| Others                 | False             | N/A (will not be checked)               |
  //+------------------------+-------------------+-----------------------------------------+
  def needTimeZoneCheck: Boolean = {
    wrapped match {
      // CurrentDate expression will not go through this even it's a `TimeZoneAwareExpression`.
      // It will be treated as literal in Rapids.
      case _: TimeZoneAwareExpression =>
        if (wrapped.isInstanceOf[Cast]) {
          val cast = wrapped.asInstanceOf[Cast]
          castNeedsTimeZone(cast.child.dataType, cast.dataType)
        } else if(PlanShims.isAnsiCast(wrapped)) {
          val (from, to) = PlanShims.extractAnsiCastTypes(wrapped)
          castNeedsTimeZone(from, to)
        } else{
          true
        }
      case _ => false
    }
  }

  // Mostly base on Spark existing [[Cast.needsTimeZone]] method. Two changes are made:
  //    1. Override date related based on https://github.com/apache/spark/pull/40524 merged
  //    2. Existing `needsTimezone` doesn't consider complex types to string which is timezone
  //    related. (incl. struct/map/list to string).
  def castNeedsTimeZone(from: DataType, to: DataType): Boolean = (from, to) match {
    case (StringType, DateType) => false
    case (DateType, StringType) => false
    case (ArrayType(fromType, _), StringType) => castNeedsTimeZone(fromType, to)
    case (MapType(fromKey, fromValue, _), StringType) =>
      castNeedsTimeZone(fromKey, to) || castNeedsTimeZone(fromValue, to)
    case (StructType(fromFields), StringType) =>
      fromFields.exists {
        case fromField =>
          castNeedsTimeZone(fromField.dataType, to)
      }
    // Avoid copying full implementation here. Otherwise needs to create shim for TimestampNTZ
    // since Spark 3.4.0
    case _ => Cast.needsTimeZone(from, to)
  }

  // Level 2 timezone checking flag, need to override to true when supports timezone in functions
  // Useless if it's not timezone related expression defined in [[needTimeZoneCheck]]
  def isTimeZoneSupported: Boolean = false

  /**
   * Timezone check which only allows UTC timezone. This is consistent with previous behavior.
   *
   * @param meta to check whether it's UTC
   */
  def checkUTCTimezone(meta: RapidsMeta[_, _, _], zoneId: ZoneId): Unit = {
    if (!GpuOverrides.isUTCTimezone(zoneId)) {
      meta.willNotWorkOnGpu(
        TimeZoneDB.nonUTCTimezoneNotSupportedStr(meta.wrapped.getClass.toString))
    }
  }

  final override def tagSelfForGpu(): Unit = {
    if (wrapped.foldable && !GpuOverrides.isLit(wrapped) && !isFoldableNonLitAllowed) {
      willNotWorkOnGpu(s"Cannot run on GPU. Is ConstantFolding excluded? Expression " +
        s"$wrapped is foldable and operates on non literals")
    }
    rule.getChecks.foreach(_.tag(this))
    checkExprForTimezone()
    tagExprForGpu()
  }

  /**
   * Called to verify that this expression will work on the GPU. For most expressions without
   * extra checks all of the checks should have already been done.
   */
  def tagExprForGpu(): Unit = {}

  final def willNotWorkInAst(because: String): Unit = cannotBeAstReasons.add(because)

  /**
   * Mark this expression as requiring AST conversion. This prevents the bridge optimizer
   * from converting it to a bridge expression.
   */
  final def requiresAstConversion(): Unit = {
    mustBeAst = true
    // Also mark all children as requiring AST
    childExprs.foreach(_.requiresAstConversion())
  }

  /**
   * Check if this expression has been marked as requiring AST conversion.
   */
  final def mustBeAstExpression: Boolean = mustBeAst

  final def canThisBeAst: Boolean = {
    tagForAst()
    // An expression cannot be AST if it cannot be replaced (disabled), uses CPU bridge,
    // or has AST-specific issues
    canThisBeReplaced && !willUseGpuCpuBridge &&
      childExprs.forall(_.canThisBeAst) && cannotBeAstReasons.isEmpty
  }

  /**
   * Check whether this node itself can be converted to AST. It will not recursively check its
   * children. It's used to check join condition AST-ability in top-down fashion.
   */
  // A def, not a memoized val: willUseGpuCpuBridge can be flipped by requireAstForGpu()/
  // undoBridgeOptimization() after a first read, so caching would return a stale answer.
  final def canSelfBeAst: Boolean = {
    tagForAst()
    // Not AST-able if disabled, bridged (a GpuCpuBridgeExpression has no AST form), or it has
    // AST-specific issues.
    canThisBeReplaced && !willUseGpuCpuBridge && cannotBeAstReasons.isEmpty
  }

  final def requireAstForGpu(): Unit = {
    // Undo any bridge optimization that was applied before the AST requirement
    // This handles the case where bridge optimizer ran first, then AST was required later
    undoBridgeOptimization()

    requiresAstConversion()

    tagForAst()
    cannotBeAstReasons.foreach { reason =>
      willNotWorkOnGpu(s"AST is required and $reason")
    }
    childExprs.foreach(_.requireAstForGpu())
  }

  private var taggedForAst = false
  private final def tagForAst(): Unit = {
    if (!taggedForAst) {
      if (wrapped.foldable && !GpuOverrides.isLit(wrapped)) {
        willNotWorkInAst(s"Cannot convert to AST. Is ConstantFolding excluded? Expression " +
            s"$wrapped is foldable and operates on non literals")
      }

      rule.getChecks.foreach {
        case exprCheck: ExprChecks => exprCheck.tagAst(this)
        case other => throw new IllegalArgumentException(s"Unexpected check found $other")
      }

      tagSelfForAst()
      taggedForAst = true
    }
  }

  /** Called to verify that this expression will work as a GPU AST expression. */
  protected def tagSelfForAst(): Unit = {
    // NOOP
  }

  protected def willWorkInAstInfo: String = {
    if (cannotBeAstReasons.isEmpty) {
      "will run in AST"
    } else {
      s"cannot be converted to GPU AST because ${cannotBeAstReasons.mkString(";")}"
    }
  }

  /**
   * Create a string explanation for whether this expression tree can be converted to an AST
   * @param strBuilder where to place the string representation.
   * @param depth how far down the tree this is.
   * @param all should all the data be printed or just what does not work in the AST?
   */
  protected def printAst(strBuilder: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (all || !canThisBeAst) {
      indent(strBuilder, depth)
      strBuilder.append(operationName)
          .append(" <")
          .append(wrapped.getClass.getSimpleName)
          .append("> ")

      if (printWrapped) {
        strBuilder.append(wrapped)
            .append(" ")
      }

      strBuilder.append(willWorkInAstInfo).append("\n")
    }
    childExprs.foreach(_.printAst(strBuilder, depth + 1, all))
  }

  def explainAst(all: Boolean): String = {
    tagForAst()
    val appender = new StringBuilder()
    printAst(appender, 0, all)
    appender.toString()
  }

  // Override the replacement message for bridge expressions
  override def replaceMessage: String = if (willUseGpuCpuBridge) {
    "run on GPU via CPU bridge"
  } else {
    super.replaceMessage
  }

  /**
   * Converts a CPU expression to a GPU expression. Subclasses should
   * implement convertToGpuImpl() to provide custom logic for the conversion.
   * Anyone who wants to get the converted expression should call `convertToGpu`
   * directly, as it provides a layer of indirection for expression level
   * optimizations.
   */
  def convertToGpuImpl(): Expression

  /**
   * Converts a CPU expression to a GPU expression. It may also do other things,
   * like use the GpuCpuBridge to run some GPU expressions on the CPU and in
   * the future it may provide other types of optimizations.
   * @return a GPU expression
   */
  final override def convertToGpu(): Expression = {
    if (willUseGpuCpuBridge) {
      convertForGpuCpuBridge()
    } else {
      // Propagate the origin from the CPU expression so that GPU expressions
      // inherit the SQL query context for error messages (e.g. ANSI overflow).
      CurrentOrigin.withOrigin(wrapped.origin) {
        convertToGpuImpl()
      }
    }
  }

  /**
   * Deduplicate GPU inputs using semantic equality to reduce memory transfers and code size.
   * @param gpuInputsWithIndex GPU expressions paired with their original child indices
   * @return (deduplicated GPU inputs, mapping from original indices to deduplicated indices)
   */
  private def deduplicateGpuInputs(
      gpuInputsWithIndex: Seq[(Expression, Int)]): (Seq[Expression], Map[Int, Int]) = {

    import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionEquals

    val deduplicatedInputs = scala.collection.mutable.ListBuffer[Expression]()
    val seenExpressions = scala.collection.mutable.Map[GpuExpressionEquals, Int]()
    val inputMapping = scala.collection.mutable.Map[Int, Int]()

    gpuInputsWithIndex.foreach { case (gpuExpr, originalIndex) =>
      val exprWrapper = GpuExpressionEquals(gpuExpr)
      seenExpressions.get(exprWrapper) match {
        case Some(existingIndex) =>
          // This expression is a duplicate - map to existing index
          inputMapping(originalIndex) = existingIndex
        case None =>
          // This is a new unique expression - add it
          val newIndex = deduplicatedInputs.length
          deduplicatedInputs += gpuExpr
          seenExpressions(exprWrapper) = newIndex
          inputMapping(originalIndex) = newIndex
      }
    }

    (deduplicatedInputs.toSeq, inputMapping.toMap)
  }

  /**
   * Checks if this expression is compatible with CPU bridge execution.
   * This determines whether THIS specific expression can be wrapped in a bridge.
   * Can be overridden in specific expression metas to provide custom compatibility rules.
   *
   * @return true if this expression can be executed via CPU bridge, false otherwise
   */
  def isBridgeCompatible: Boolean = {
    import org.apache.spark.sql.catalyst.expressions.{Generator, NamedLambdaVariable, TryEval, Unevaluable, WindowFunction}
    import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
    import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

    val exprClass = expr.getClass

    // TryEval implements try_* by evaluating the child expression on CPU, catching CPU
    // exceptions, and converting them to null. To preserve that contract, the entire child
    // tree would need to stay on CPU; bridging TryEval with GPU children would bypass the
    // CPU exception path. Let the enclosing plan node fall back instead.
    def isTryEval: Boolean = classOf[TryEval].isAssignableFrom(exprClass)

    // LambdaVariable is a scoped placeholder owned by Spark object expressions such as
    // MapObjects, CatalystToExternalMap, and ExternalMapToCatalyst. Those expressions require
    // LambdaVariable children in fixed positions and cast them back during tree rewrites.
    // Bridging LambdaVariable independently can replace the placeholder with a BoundReference
    // and corrupt the parent expression tree.
    def isLambdaVariable: Boolean = classOf[LambdaVariable].isAssignableFrom(exprClass)

    // convertForGpuCpuBridge rebuilds the CPU expression by positionally replacing the
    // GPU-convertible children with BoundReferences and calling expr.withNewChildren. That is
    // only valid when this meta's childExprs correspond one-to-one, in order, with
    // expr.children. Some metas intentionally override childExprs to drop or reorder children
    // (e.g. InvokeExprMeta hides the evaluator-literal child of the Invoke that Spark 4.0 uses
    // for parse_url/to_json). Bridging those would fail with "Incorrect number of children" or
    // silently rebuild the wrong tree, so let the enclosing node fall back instead.
    def childrenMatch: Boolean = {
      val wrappedChildren = childExprs.map(_.wrapped.asInstanceOf[AnyRef])
      val exprChildren = expr.children
      wrappedChildren.length == exprChildren.length &&
        !wrappedChildren.zip(exprChildren).exists {
          case (wrappedChild, exprChild) => !wrappedChild.eq(exprChild.asInstanceOf[AnyRef])
        }
    }

    // Filter out expression types that cannot be directly executed on CPU.
    def canExecuteOnCpu: Boolean = !classOf[Unevaluable].isAssignableFrom(exprClass) &&
      !classOf[AggregateFunction].isAssignableFrom(exprClass) &&
      !classOf[WindowFunction].isAssignableFrom(exprClass) &&
      !classOf[NamedLambdaVariable].isAssignableFrom(exprClass) &&
      !classOf[Generator].isAssignableFrom(exprClass)

    // Some expressions carry task/partition-local state whose values cannot be preserved if the
    // bridge splits a batch across worker threads. The shim predicate filters out those correctness
    // blockers while allowing expressions whose mutable state is only cloned worker-local scratch.
    def isStateful: Boolean = SparkShimImpl.isExpressionStateful(expr)

    conf.isCpuBridgeEnabled &&
      !conf.bridgeDisallowList.contains(exprClass.getName) &&
      !isTryEval &&
      !isLambdaVariable &&
      childrenMatch &&
      canExecuteOnCpu &&
      !isStateful &&
      expr.deterministic
  }

  /**
   * Checks if the presence of this expression in a tree should prevent ALL bridge
   * optimization for the entire plan node.
   *
   * @return true if this expression prevents all bridge optimization in the tree
   */
  def preventsTreeBridgeOptimization: Boolean = false

  /**
   * Checks if this expression tree contains any expression that prevents bridge optimization.
   * Used by the optimizer to determine if bridge optimization should be skipped entirely.
   *
   * @return true if this expression or any child prevents bridge optimization
   */
  final def containsExpressionThatPreventsBridge: Boolean = {
    preventsTreeBridgeOptimization || childExprs.exists(_.containsExpressionThatPreventsBridge)
  }

  /**
   * Is this particular class allowed to run under the GpuCpuBridge (according to the
   * configs)
   * @return true if it can, else false.
   */
  final def canMoveToCpuBridge: Boolean = isBridgeCompatible

  // Shared constant for bridge optimization reason
  private val BRIDGE_OPTIMIZATION_REASON = "it avoids excess CPU and GPU transfers"
  
  final def moveToCpuBridge(): Unit = {
    if (!canMoveToCpuBridge) {
      throw new IllegalStateException("trying to move to bridge when not allowed")
    }
    willRunViaCpuBridgeReasons.get ++= cannotBeReplacedReasons.get
    cannotBeReplacedReasons.get.clear()
    if (willRunViaCpuBridgeReasons.get.isEmpty) {
      // We need something so it will move to the GPU and this is the only reason
      // right now
      willRunViaCpuBridgeReasons.get.add(BRIDGE_OPTIMIZATION_REASON)
    }
  }

  final def undoBridgeOptimization(): Unit = {
    // Undo bridge optimization by moving reasons back to cannotBeReplacedReasons
    // and removing the bridge optimization reason
    if (willUseGpuCpuBridge) {
      // Move non-optimization reasons back to cannotBeReplacedReasons
      val nonOptimizationReasons =
        willRunViaCpuBridgeReasons.get.filterNot(_ == BRIDGE_OPTIMIZATION_REASON)
      cannotBeReplacedReasons.get ++= nonOptimizationReasons

      // Clear bridge reasons
      willRunViaCpuBridgeReasons.get.clear()

      // Apply to children recursively
      childExprs.foreach(_.undoBridgeOptimization())
    }
  }

  /**
   * Determines if a child expression should be converted to a GPU input for the bridge.
   * Returns true if the child can be replaced AND is not a Literal, ScalarSubquery,
   * or LambdaFunction.
   * Literals, ScalarSubqueries, and LambdaFunctions are kept on the CPU side:
   * - Literals and ScalarSubqueries to avoid unnecessary data movement
   * - LambdaFunctions because they contain unevaluable placeholders (NamedLambdaVariables)
   *   that cannot be evaluated in columnar fashion on the GPU
   */
  private def shouldConvertChildToGpuInput(childMeta: BaseExprMeta[_]): Boolean = {
    import org.apache.spark.sql.catalyst.expressions.LambdaFunction
    childMeta.canThisBeReplaced &&
      !childMeta.wrapped.isInstanceOf[Literal] &&
      !childMeta.wrapped.isInstanceOf[ScalarSubquery] &&
      !childMeta.wrapped.isInstanceOf[LambdaFunction]
  }

  def convertForGpuCpuBridge(): GpuExpression = {
    require(this.wrapped.deterministic,
      s"Bridge conversion requires deterministic expressions. " +
      s"Expression ${this.wrapped.getClass.getSimpleName} is nondeterministic. " +
      s"This should have been filtered by isBridgeCompatible/canMoveToCpuBridge.")
    // Separate literals and ScalarSubQueries from GPU-convertible expressions to optimize
    // data movement, and work around bugs in Spark where only a Scala value is supported as input.
    val gpuInputsWithIndex = scala.collection.mutable.ListBuffer[(Expression, Int)]()

    // Cache conversion decisions to ensure consistency across multiple passes
    val shouldConvertToGpuInput: Map[Int, Boolean] = childExprs.zipWithIndex.map {
      case (childMeta, originalIndex) => (originalIndex, shouldConvertChildToGpuInput(childMeta))
    }.toMap

    // Build GPU inputs and track their original positions using cached decisions
    childExprs.zipWithIndex.foreach {
      case (childMeta, originalIndex) =>
        if (shouldConvertToGpuInput(originalIndex)) {
          val gpuExpr = childMeta.convertToGpu()
          gpuInputsWithIndex += ((gpuExpr, originalIndex))
        }
    }

    // Deduplicate GPU inputs using semantic equality
    val (deduplicatedGpuInputs, inputMapping) = deduplicateGpuInputs(gpuInputsWithIndex.toSeq)

    // Create bound CPU expression with mixed children:
    // - BoundReferences for GPU expressions (will be provided as columnar data)
    // - Original literals/CPU expressions in their positions (no data movement needed)
    val boundChildren: Seq[Expression] = childExprs.zipWithIndex.map {
      case (childMeta, originalIndex) =>
        if (shouldConvertToGpuInput(originalIndex)) {
          // Replace with BoundReference pointing to deduplicated GPU input position
          // This lookup is guaranteed to succeed because we cached shouldConvertToGpuInput
          // decisions above, ensuring consistency between the build and bound passes
          val deduplicatedIndex = inputMapping(originalIndex)
          val gpuExpr = deduplicatedGpuInputs(deduplicatedIndex)
          BoundReference(deduplicatedIndex, gpuExpr.dataType, gpuExpr.nullable): Expression
        } else {
          childMeta.wrapped.asInstanceOf[Expression]
        }
    }
    val boundCpuExpression = expr.withNewChildren(boundChildren)

    val bridgeExpression = GpuCpuBridgeExpression(
      gpuInputs = deduplicatedGpuInputs,
      cpuExpression = boundCpuExpression,
      outputDataType = expr.dataType,
      outputNullable = expr.nullable)

    // Apply bridge optimization to merge adjacent bridge expressions
    GpuCpuBridgeOptimizer.optimizeByMergingBridgeExpressions(bridgeExpression)
  }
}

abstract class ExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends BaseExprMeta[INPUT](expr, conf, parent, rule) {

  def convertToGpuImpl(): GpuExpression
}

/**
 * Base class for metadata around `RuntimeReplaceableExpression`. We will never
 * get a RuntimeReplaceableExpression as it will be converted to the actual Expression
 * by the time we get it. We need to have this here as some Expressions e.g. UnaryPositive
 * don't extend UnaryExpression.
 */
abstract class RuntimeReplaceableUnaryExprMeta[INPUT <: RuntimeReplaceable](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMetaBase[INPUT](expr, conf, parent, rule)

/** Base metadata class for RuntimeReplaceable expressions that support conversion to AST as well */
abstract class RuntimeReplaceableUnaryAstExprMeta[INPUT <: RuntimeReplaceable](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RuntimeReplaceableUnaryExprMeta[INPUT](expr, conf, parent, rule)

/**
 * Base class for metadata around `UnaryExpression`.
 */
abstract class UnaryExprMeta[INPUT <: Expression with UnaryLike[Expression]](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends UnaryExprMetaBase[INPUT](expr, conf, parent, rule)

protected abstract class UnaryExprMetaBase[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression =
    convertToGpu(childExprs.head.convertToGpu())

  def convertToGpu(child: Expression): GpuExpression

  /**
   * `ConstantFolding` executes early in the logical plan process, which
   * simplifies many things before we get to the physical plan. If you enable
   * AQE, some optimizations can cause new expressions to show up that would have been
   * folded in by the logical plan optimizer (like `cast(null as bigint)` which just
   * becomes Literal(null, Long) after `ConstantFolding`), so enabling this here
   * allows us to handle these when they are generated by an AQE rule.
   */
  override val isFoldableNonLitAllowed: Boolean = true
}

/** Base metadata class for unary expressions that support conversion to AST as well */
abstract class UnaryAstExprMeta[INPUT <: UnaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends UnaryExprMeta[INPUT](expr, conf, parent, rule) {
}

/**
 * Base class for metadata around `AggregateFunction`.
 */
abstract class AggExprMeta[INPUT <: AggregateFunction](
    val expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def tagExprForGpu(): Unit = {
    tagAggForGpu()
    if (needsAnsiCheck) {
      GpuOverrides.checkAndTagAnsiAgg(ansiTypeToCheck, this)
    }
  }

  // not all aggs overwrite this
  def tagAggForGpu(): Unit = {}

  override final def convertToGpuImpl(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression

  // Set to false if the aggregate doesn't overflow and therefore
  // shouldn't error
  def needsAnsiCheck: Boolean = true

  // The type to use to determine whether the aggregate could overflow.
  // Set to None, if we should fallback for all types
  val ansiTypeToCheck: Option[DataType] = Some(expr.dataType)
}

/**
 * Base class for metadata around `ImperativeAggregate`.
 */
abstract class ImperativeAggExprMeta[INPUT <: ImperativeAggregate](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends AggExprMeta[INPUT](expr, conf, parent, rule) {

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Base class for metadata around `TypedImperativeAggregate`.
 */
abstract class TypedImperativeAggExprMeta[INPUT <: TypedImperativeAggregate[_]](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ImperativeAggExprMeta[INPUT](expr, conf, parent, rule) {

  /**
   * Returns aggregation buffer with the actual data type under GPU runtime. This method is
   * called to override the data types of typed imperative aggregation buffers during GPU
   * overriding.
   */
  def aggBufferAttribute: AttributeReference

  /**
   * Returns a buffer converter who can generate a Expression to transform the aggregation buffer
   * of wrapped function from CPU format to GPU format. The conversion occurs on the CPU, so the
   * generated expression should be a CPU Expression executed by row.
   */
  def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
    throw new NotImplementedError("The method should be implemented by specific functions")

  /**
   * Returns a buffer converter who can generate a Expression to transform the aggregation buffer
   * of wrapped function from GPU format to CPU format. The conversion occurs on the CPU, so the
   * generated expression should be a CPU Expression executed by row.
   */
  def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
    throw new NotImplementedError("The method should be implemented by specific functions")

  /**
   * Whether buffers of current Aggregate is able to be converted from CPU to GPU format and
   * reversely in runtime. If true, it assumes both createCpuToGpuBufferConverter and
   * createGpuToCpuBufferConverter are implemented.
   */
  val supportBufferConversion: Boolean = false
}

/**
 * Base class for metadata around `BinaryExpression`.
 */
abstract class BinaryExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression = {
    val Seq(lhs, rhs) = childExprs.map(_.convertToGpu())
    convertToGpu(lhs, rhs)
  }

  def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression
}

/** Base metadata class for binary expressions that support conversion to AST */
abstract class BinaryAstExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends BinaryExprMeta[INPUT](expr, conf, parent, rule) {

  override def tagSelfForAst(): Unit = {
    if (wrapped.left.dataType != wrapped.right.dataType) {
      willNotWorkInAst("AST binary expression operand types must match, found " +
          s"${wrapped.left.dataType},${wrapped.right.dataType}")
    }
  }
}

/**
 * Base class for metadata around `TernaryExpression`.
 */
abstract class TernaryExprMeta[INPUT <: TernaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression = {
    val Seq(child0, child1, child2) = childExprs.map(_.convertToGpu())
    convertToGpu(child0, child1, child2)
  }

  def convertToGpu(val0: Expression, val1: Expression,
                   val2: Expression): GpuExpression
}

/**
 * Base class for metadata around `QuaternaryExpression`.
 */
abstract class QuaternaryExprMeta[INPUT <: QuaternaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression = {
    val Seq(child0, child1, child2, child3) = childExprs.map(_.convertToGpu())
    convertToGpu(child0, child1, child2, child3)
  }

  def convertToGpu(val0: Expression, val1: Expression,
    val2: Expression, val3: Expression): GpuExpression
}

abstract class String2TrimExpressionMeta[INPUT <: String2TrimExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpuImpl(): GpuExpression = {
    val gpuCol :: gpuTrimParam = childExprs.map(_.convertToGpu())
    convertToGpu(gpuCol, gpuTrimParam.headOption)
  }

  def convertToGpu(column: Expression, target: Option[Expression] = None): GpuExpression
}

/**
 * Base class for metadata around `ComplexTypeMergingExpression`.
 */
abstract class ComplexTypeMergingExprMeta[INPUT <: ComplexTypeMergingExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {
  override final def convertToGpuImpl(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Metadata for `Expression` with no rule found
 */
final class RuleNotFoundExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ExprMeta[INPUT](expr, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagExprForGpu(): Unit =
    willNotWorkOnGpu(s"GPU does not currently support the operator ${expr.getClass}")

  override def convertToGpuImpl(): GpuExpression =
    throw new IllegalStateException(s"Cannot be converted to GPU ${expr.getClass} " +
        s"${expr.dataType} $expr")
}

/** Base class for metadata around `RunnableCommand`. */
abstract class RunnableCommandMeta[INPUT <: RunnableCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RapidsMeta[INPUT, RunnableCommand, RunnableCommand](cmd, conf, parent, rule)
{
  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty
}

/** Metadata for `RunnableCommand` with no rule found */
final class RuleNotFoundRunnableCommandMeta[INPUT <: RunnableCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends RunnableCommandMeta[INPUT](cmd, conf, parent, new NoRuleDataFromReplacementRule) {

  // Do not complain by default, as many commands are metadata-only.
  override def suppressWillWorkOnGpuInfo: Boolean = true

  override def tagSelfForGpu(): Unit =
    willNotWorkOnGpu(s"GPU does not currently support the command ${cmd.getClass}")

  override def convertToGpu(): RunnableCommand =
    throw new IllegalStateException("Cannot be converted to GPU")
}
