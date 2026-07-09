/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from OptimizeTableCommand.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.io.skipping.liquid.ClusteredTableUtils
import com.databricks.sql.transaction.tahoe.Snapshot
import com.databricks.sql.transaction.tahoe.commands.{DeltaCommand, DeltaOptimizeContext}
import com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeMetrics

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

case class GpuOptimizeTableCommand(
    override val child: LogicalPlan,
    userPartitionPredicates: Seq[String],
    optimizeContext: DeltaOptimizeContext
  )(val zOrderBy: Seq[UnresolvedAttribute])
  extends RunnableCommand with DeltaCommand with UnaryNode {

  override val otherCopyArgs: Seq[AnyRef] = zOrderBy :: Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): GpuOptimizeTableCommand =
    copy(child = newChild)(zOrderBy)

  override val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType)(),
    AttributeReference("metrics", Encoders.product[OptimizeMetrics].schema)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = getDeltaTable(child, "OPTIMIZE")
    val snapshot: Snapshot = table.update()
    validateSupportedOptimize(snapshot)

    val partitionColumns = snapshot.metadata.partitionColumns
    val partitionPredicates: Seq[Expression] = userPartitionPredicates.flatMap { predicate =>
      val predicates = parsePredicates(sparkSession, predicate)
      verifyPartitionPredicates(sparkSession, partitionColumns, predicates)
      predicates
    }

    new GpuOptimizeExecutor(
      sparkSession,
      snapshot,
      table.catalogTable,
      partitionPredicates,
      optimizeContext).optimize()
  }

  private def validateSupportedOptimize(snapshot: Snapshot): Unit = {
    if (zOrderBy.nonEmpty) {
      throw new IllegalStateException("Z-Order optimize should not run on GPU")
    }
    if (optimizeContext.reorg.nonEmpty) {
      throw new IllegalStateException("Delta OPTIMIZE REORG should not run on GPU")
    }
    if (optimizeContext.maxDeletedRowsRatio.nonEmpty) {
      throw new IllegalStateException(
        "Delta OPTIMIZE with deletion-vector cleanup should not run on GPU")
    }
    if (optimizeContext.isFull) {
      throw new IllegalStateException("Delta OPTIMIZE FULL should not run on GPU")
    }
    if (ClusteredTableUtils.isSupported(snapshot.protocol)) {
      throw new IllegalStateException(
        "Delta OPTIMIZE on liquid clustered tables should not run on GPU")
    }
  }
}

case class GpuOptimizeTableCommandEdge(
    override val child: LogicalPlan,
    isFull: Boolean,
    predicates: Seq[Expression]
  )(val zOrderBy: Seq[UnresolvedAttribute])
  extends RunnableCommand with DeltaCommand with UnaryNode {

  override val otherCopyArgs: Seq[AnyRef] = zOrderBy :: Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): GpuOptimizeTableCommandEdge =
    copy(child = newChild)(zOrderBy)

  override val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType)(),
    AttributeReference("metrics", Encoders.product[OptimizeMetrics].schema)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = getDeltaTable(child, "OPTIMIZE")
    val snapshot: Snapshot = table.update()
    validateSupportedOptimize(snapshot)

    verifyPartitionPredicates(sparkSession, snapshot.metadata.partitionColumns, predicates)

    new GpuOptimizeExecutor(
      sparkSession,
      snapshot,
      table.catalogTable,
      predicates,
      DeltaOptimizeContext(isFull = isFull)).optimize()
  }

  private def validateSupportedOptimize(snapshot: Snapshot): Unit = {
    if (zOrderBy.nonEmpty) {
      throw new IllegalStateException("Z-Order optimize should not run on GPU")
    }
    if (isFull) {
      throw new IllegalStateException("Delta OPTIMIZE FULL should not run on GPU")
    }
    if (ClusteredTableUtils.isSupported(snapshot.protocol)) {
      throw new IllegalStateException(
        "Delta OPTIMIZE on liquid clustered tables should not run on GPU")
    }
  }
}
