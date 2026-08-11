/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, DataWritingCommandMeta, GpuExec, GpuMetric, GpuOverrides, PartMeta, RapidsConf, RapidsMeta, ScanMeta, SparkPlanMeta}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Strategy rule to translate a logical RapidsDeltaWrite into a physical RapidsDeltaWriteExec */
object RapidsDeltaWriteStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p: RapidsDeltaWrite =>
      Seq(RapidsDeltaWriteExec(planLater(p.child)))
    case _ => Nil
  }
}

/**
 * This logical node is a placeholder in the plan for where, semantically, a Delta Lake
 * write occurs. Normally Delta Lake writes are *not* visible in the SQL plan, but we
 * use this as a workaround for undesired row transitions in the plan when adaptive
 * execution is enabled. Without it, AdaptiveSparkPlanExec can become the root node
 * of the physical plan to write into Delta. Without a parent node above
 * AdaptiveSparkPlanExec, the GPU transition planning does not know whether to make
 * the result of the adaptive plan GPU columnar or CPU row, so it ends up assuming CPU
 * row and injecting a columnar-to-row transition at the end of the plan. The Delta
 * write code cannot directly replace the AdaptiveSparkPlanExec to force it to be
 * columnar, because it's tied to the QueryExecution which can only be created with
 * logical plans. Thus our workaround is to inject a node in the logical plan for
 * the write and ensure the physical node appears to Spark as a command node so the
 * AdaptiveSparkPlanExec node is a child of it rather than the parent. The
 * InsertAdaptiveSparkPlan Rule in Spark will avoid placing the AdpativeSparkPlanExec
 * node in front of command-like nodes, and the physical form of this node is one
 * of those nodes.
 */
case class RapidsDeltaWrite(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

/**
 * A stand-in physical node for a Delta write. This should *always* be replaced by
 * a GpuRapidsDeltaWriteExec during GPU planning and should never be executed.
 */
case class RapidsDeltaWriteExec(child: SparkPlan) extends V2CommandExec with UnaryExecNode {
  @transient lazy val doExecuteMethod = {
    val doExecute = classOf[SparkPlan].getDeclaredMethod("doExecute")
    doExecute.setAccessible(true)
    doExecute
  }

  override def output: Seq[Attribute] = child.output

  override def run(): Seq[InternalRow] = {
    throw new IllegalStateException("Should have been replaced with a GPU node before execution")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // During execution of Delta Lake writes, we sometimes make the entire plan
    // fall back to CPU due to the query being categorized as a Delta Lake
    // Metadata Query, such as when there are references to internal
    // metadata fields in a Parquet scan. This leaves the
    // RapidDeltaWriteExec in the plan, so we need to support row-based
    // execution in this case.
    logWarning("RapidsDeltaWriteExec performing row-based execution")
    doExecuteMethod.invoke(child).asInstanceOf[RDD[InternalRow]]
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * A stand-in physical node for a GPU Delta write. It needs to appear as a writing
 * command or data source command so the AdaptiveSparkPlanExec does not appear at the
 * root of the plan to write. See the description for RapidsDeltaWrite and the logic
 * of the InsertAdaptiveSparkPlan Rule in Spark for details.
 */
case class GpuRapidsDeltaWriteExec(child: SparkPlan) extends V2CommandExec
    with UnaryExecNode with GpuExec {
  override def output: Seq[Attribute] = child.output

  lazy val basicMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.basicMetrics
  lazy val taskMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.taskMetrics

  override lazy val allMetrics: Map[String, GpuMetric] =
    basicMetrics ++ taskMetrics

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // This is just a stub node for planning purposes and does not actually perform
    // the write. See the documentation for RapidsDeltaWrite and its use in
    // GpuOptimisticTransaction for details on how the write is handled.
    // In the future it may make sense to move the write code here, although this will
    // cause the writing code to deviate heavily from the Delta Lake source.
    child.executeColumnar()
  }

  override protected def run(): Seq[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/** Metadata tagging and converting for the custom RapidsDeltaWriteExec node. */
class RapidsDeltaWriteExecMeta(
    plan: RapidsDeltaWriteExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[RapidsDeltaWriteExec](plan, conf, parent, rule) {
  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] =
    Seq(GpuOverrides.wrapPlan(plan.child, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] = Nil
  override val childScans: Seq[ScanMeta[_]] = Nil
  override val childParts: Seq[PartMeta[_]] = Nil
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Nil

  override def convertToGpu(): GpuExec = {
    GpuRapidsDeltaWriteExec(childPlans.head.convertIfNeeded())
  }
}
