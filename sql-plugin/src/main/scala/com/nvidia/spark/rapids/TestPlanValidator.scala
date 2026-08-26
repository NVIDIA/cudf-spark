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

import java.util.IdentityHashMap

import scala.annotation.tailrec
import scala.collection.mutable

import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference,
  Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{DropTableExec, ShowTablesExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/** Test-only validation of the finalized physical plan. */
object TestPlanValidator {
  case class ValidationContext(
      conf: Map[String, String],
      adaptiveEnabled: Boolean,
      shouldValidate: Boolean)

  private[rapids] val validationDisabledContext =
    ValidationContext(Map.empty, adaptiveEnabled = false, shouldValidate = false)

  private val validationContextTag =
    TreeNodeTag[ValidationContext]("rapids.test.planValidationContext")

  def captureValidationContext(plan: SparkPlan): ValidationContext = {
    val rapidsConfs = plan.conf.getAllConfs.filter { case (key, _) =>
      key.startsWith("spark.rapids.")
    }.toMap
    ValidationContext(rapidsConfs, plan.conf.adaptiveExecutionEnabled, shouldValidate = true)
  }

  def tagForValidation(plan: SparkPlan, context: ValidationContext): SparkPlan = {
    plan.setTagValue(validationContextTag, context)
    plan
  }

  /** Validate a completed physical plan using the configuration that produced it. */
  def validatePlan(plan: SparkPlan): Unit = {
    resolveValidationContext(plan) match {
      case Some(context) if context.shouldValidate =>
        val conf = new RapidsConf(context.conf)
        if (conf.isSqlEnabled && conf.isSqlExecuteOnGPU && conf.isTestEnabled) {
          val finalRoot = unwrapFinalRoot(plan)
          // Preserve the existing test-mode canonicalization check.
          finalRoot.canonicalized
          val allPlanNodes = collectPlanNodes(finalRoot)
          allPlanNodes.foreach(validatePlanNode(_, conf, context.adaptiveEnabled))
          validateExecsInGpuPlan(finalRoot, allPlanNodes, conf)
        }
      case Some(_) =>
      case None =>
        if (collectPlanNodes(plan).exists(_.isInstanceOf[GpuExec])) {
          throw new IllegalStateException(
            "GPU plan is missing its test validation configuration context")
        }
    }
  }

  def assertIsOnTheGpu(exp: Expression, conf: RapidsConf): Unit = {
    // There are no GpuAttributeReference or GpuSortOrder.
    exp match {
      case _: AttributeReference | _: SortOrder =>
        // These are always allowed.
      case bridge: GpuCpuBridgeExpression =>
        // For bridge expressions, validate the CPU expressions inside
        assertBridgeExpressionsAllowed(bridge, conf)
      case _: BoundReference | _: Literal =>
        // These are always allowed and ignored.
      case _: GpuExpression =>
        // Regular GPU expressions are allowed.
      case _ =>
        val classBaseName = PlanUtils.getBaseNameFromClass(exp.getClass.toString)
        if (!conf.testingAllowedNonGpu.contains(classBaseName)) {
          throw new IllegalArgumentException(s"The expression $exp is not columnar ${exp.getClass}")
        }
    }
    exp.children.foreach(assertIsOnTheGpu(_, conf))
  }

  /**
   * Validates that all CPU expressions within a GpuCpuBridgeExpression are allowed in test mode.
   * This function recursively traverses the CPU expression tree inside the bridge and checks
   * each CPU expression against the testingAllowedNonGpu allowlist.
   */
  def assertBridgeExpressionsAllowed(bridge: GpuCpuBridgeExpression, conf: RapidsConf): Unit = {
    val disallowedExprs = mutable.ListBuffer[String]()
    val allowedExprs = mutable.ListBuffer[String]()

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
      errorMessage.append("GpuCpuBridgeExpression contains disallowed CPU expressions:\n")
      errorMessage.append(s"Bridge: $bridge\n")
      errorMessage.append("CPU Expression Tree Analysis:\n")

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

  private[rapids] def resolveValidationContext(plan: SparkPlan): Option[ValidationContext] = {
    val visited = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    var level = Seq(plan)
    while (level.nonEmpty) {
      val currentLevel = level.filter { node =>
        visited.put(node, java.lang.Boolean.TRUE) == null
      }
      val contexts = currentLevel.flatMap(_.getTagValue(validationContextTag)).distinct
      if (contexts.nonEmpty) {
        require(contexts.size == 1,
          s"Final plan has ambiguous validation contexts at the same depth: " +
            contexts.mkString(", "))
        return contexts.headOption
      }
      level = currentLevel.flatMap(planSuccessors)
    }
    None
  }

  private def validatePlanNode(
      plan: SparkPlan,
      conf: RapidsConf,
      adaptiveEnabled: Boolean): Unit = {
    def isTestExempted(plan: SparkPlan): Boolean = {
      conf.testingAllowedNonGpu.exists(PlanUtils.sameClass(plan, _))
    }

    plan match {
      case _: BroadcastExchangeLike if adaptiveEnabled =>
        // Broadcasts are left on CPU for now when AQE is enabled.
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec if adaptiveEnabled =>
        // Broadcast joins are left on CPU for now when AQE is enabled.
      case _: AdaptiveSparkPlanExec | _: QueryStageExec |
          _: ReusedExchangeExec | _: ReusedSubqueryExec | _: SubqueryExec |
          _: WholeStageCodegenExec | _: InputAdapter =>
        // Structural wrappers are validated through their underlying plans.
      case p if PlanUtils.sameClass(p, "RapidsDeltaWriteExec") =>
        // The optional Delta module uses this command stub only to keep AQE below a parent node.
        // Its child is still collected and validated.
      case p if SparkShimImpl.isAqePlan(p) =>
        // Other AQE wrappers, such as AQEShuffleReadExec, stay on CPU.
      case p if !(PlanShims.extractExecutedPlan(p) eq p) =>
        // Command/result wrappers are validated through their underlying plans.
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
      // Metadata operations
      case _: ShowTablesExec | _: DropTableExec | _: RDDScanExec =>
        // Ignored metadata and RDD operations.
      case p if SparkShimImpl.skipAssertIsOnTheGpu(p) =>
        // Ignored by the current Spark shim.
      case p: ExecutedCommandExec if !isTestExempted(p) =>
        val meta = GpuOverrides.wrapPlan(p, conf, None)
        if (!meta.suppressWillWorkOnGpuInfo) {
          throw new IllegalArgumentException(
            s"Part of the plan is not columnar ${p.getClass}\n$p")
        }
      case other =>
        if (!plan.isInstanceOf[GpuExec] &&
            !isTestExempted(plan) &&
            !conf.testingAllowedNonGpu.contains(
              PlanUtils.getBaseNameFromClass(other.getClass.toString))) {
          throw new IllegalArgumentException(
            s"Part of the plan is not columnar ${plan.getClass}\n$plan")
        }
        // Check child expressions if this is a GPU node
        plan match {
          case gpuExec: GpuExec =>
            // filter out the output expressions since those are not GPU expressions
            val planOutput = gpuExec.output.toSet
            gpuExec.gpuExpressions.filter {
              case a: Attribute => !planOutput.contains(a)
              case _ => true
            }.foreach(assertIsOnTheGpu(_, conf))
          case _ =>
        }
    }
  }

  /**
   * This is intended for testing only and this only supports looking for an exec once.
   */
  private def validateExecsInGpuPlan(
      plan: SparkPlan,
      allPlans: Seq[SparkPlan],
      conf: RapidsConf): Unit = {
    val validateExecs = conf.validateExecsInGpuPlan.toSet
    if (validateExecs.nonEmpty) {
      val execsFound = allPlans.map(_.getClass.getSimpleName).toSet
      val execsNotFound = validateExecs.diff(execsFound)
      require(execsNotFound.isEmpty,
        s"Plan ${plan.toString()} does not contain the following execs: " +
          execsNotFound.mkString(","))
    }
  }

  private[rapids] def collectPlanNodes(plan: SparkPlan): Seq[SparkPlan] = {
    val visited = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val queue = mutable.Queue[SparkPlan](plan)
    val plans = mutable.ArrayBuffer[SparkPlan]()
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (visited.put(current, java.lang.Boolean.TRUE) == null) {
        plans += current
        queue ++= planSuccessors(current)
      }
    }
    plans.toSeq
  }

  private def planSuccessors(plan: SparkPlan): Seq[SparkPlan] = {
    val extraPlans = mutable.ArrayBuffer[SparkPlan]()
    plan match {
      case adaptive: AdaptiveSparkPlanExec => extraPlans += adaptive.executedPlan
      case stage: QueryStageExec => extraPlans += stage.plan
      case reused: ReusedExchangeExec => extraPlans += reused.child
      case reused: ReusedSubqueryExec => extraPlans += reused.child
      case _ =>
    }
    val extracted = PlanShims.extractExecutedPlan(plan)
    if (!(extracted eq plan)) {
      extraPlans += extracted
    }
    plan.children ++ extraPlans ++ expressionSubqueryPlans(plan)
  }

  private def expressionSubqueryPlans(plan: SparkPlan): Seq[SparkPlan] = {
    val visited = new IdentityHashMap[Expression, java.lang.Boolean]()
    val subqueries = mutable.ArrayBuffer[SparkPlan]()

    def visit(expression: Expression): Unit = {
      if (visited.put(expression, java.lang.Boolean.TRUE) == null) {
        expression match {
          case subquery: ExecSubqueryExpression => subqueries += subquery.plan
          case _ =>
        }
        expression.children.foreach(visit)
      }
    }

    plan.expressions.foreach(visit)
    subqueries.toSeq
  }

  @tailrec
  private def unwrapFinalRoot(plan: SparkPlan): SparkPlan = plan match {
    case adaptive: AdaptiveSparkPlanExec => unwrapFinalRoot(adaptive.executedPlan)
    case stage: QueryStageExec => unwrapFinalRoot(stage.plan)
    case other =>
      val extracted = PlanShims.extractExecutedPlan(other)
      if (extracted eq other) other else unwrapFinalRoot(extracted)
  }
}
