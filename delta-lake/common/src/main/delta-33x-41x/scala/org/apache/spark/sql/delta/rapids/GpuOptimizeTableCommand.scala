/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.delta.{DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.commands.{DeltaCommand, DeltaOptimizeContext}
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.rapids.commands.GpuOptimizeExecutor
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * GPU version of Delta Lake OptimizeTableCommand (compaction mode only).
 *
 * Mirrors the CPU command's input shape and output schema, but delegates execution
 * to the shared GpuOptimizeExecutor.
 */
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
    // Resolve the Delta table from the child plan
    val table = getDeltaTable(child, "OPTIMIZE")
    val snapshot: Snapshot = table.update()
    if (snapshot.version == -1) {
      throw DeltaErrors.notADeltaTableException(table.deltaLog.dataPath.toString)
    }

    // Sanity checks
    if (zOrderBy.nonEmpty) {
      throw new IllegalStateException("Z-Order optimize should not run on GPU")
    }

    val isClusteredTable = ClusteredTableUtils.isSupported(snapshot.protocol)
    if (isClusteredTable) {
      if (userPartitionPredicates.nonEmpty) {
        throw DeltaErrors.clusteringWithPartitionPredicatesException(userPartitionPredicates)
      }
      if (zOrderBy.nonEmpty) {
        throw DeltaErrors.clusteringWithZOrderByException(zOrderBy)
      }
    }

    lazy val clusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshot)
    if (optimizeContext.isFull && (!isClusteredTable || clusteringColumns.isEmpty)) {
      throw DeltaErrors.optimizeFullNotSupportedException()
    }

    val partitionColumns = snapshot.metadata.partitionColumns
    // Parse the predicate expression into Catalyst expression and verify only simple filters
    // on partition columns are present

    val partitionPredicates: Seq[Expression] = userPartitionPredicates.flatMap { predicate =>
      val predicates = parsePredicates(sparkSession, predicate)
      verifyPartitionPredicates(sparkSession, partitionColumns, predicates)
      predicates
    }

    val zOrderByColumns = zOrderBy.map(_.name)

    new GpuOptimizeExecutor(
      sparkSession,
      snapshot,
      table.catalogTable,
      partitionPredicates,
      zOrderByColumns,
      isAutoCompact = false,
      optimizeContext
    ).optimize()
  }
}
