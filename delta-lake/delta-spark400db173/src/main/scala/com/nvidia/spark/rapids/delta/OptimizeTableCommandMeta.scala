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

package com.nvidia.spark.rapids.delta

import com.databricks.sql.transaction.tahoe.DeltaLog
import com.databricks.sql.transaction.tahoe.catalog.DeltaTableV2
import com.databricks.sql.transaction.tahoe.commands.{DeltaCommand, OptimizeTableCommand,
  OptimizeTableCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuOptimizeTableCommand,
  GpuOptimizeTableCommandEdge}
import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta,
  RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.shims.OptimizeTableCommandMetaShim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.RunnableCommand

object OptimizeTableCommandMeta {
  private object DeltaCmdProxy extends DeltaCommand

  def getDeltaTableForOptimize(cmd: OptimizeTableCommand): DeltaTableV2 = {
    DeltaCmdProxy.getDeltaTable(cmd.child, "OPTIMIZE")
  }

  def getDeltaTableForOptimize(cmd: OptimizeTableCommandEdge): DeltaTableV2 = {
    DeltaCmdProxy.getDeltaTable(cmd.child, "OPTIMIZE")
  }

  def getDeltaLogForOptimize(cmd: OptimizeTableCommand): DeltaLog =
    getDeltaTableForOptimize(cmd).deltaLog

  def getDeltaLogForOptimize(cmd: OptimizeTableCommandEdge): DeltaLog = {
    getDeltaTableForOptimize(cmd).deltaLog
  }

  private def tagCatalogTableForGpu(
      meta: RapidsMeta[_, _, _],
      catalogTable: Option[CatalogTable]): Unit = {
    if (catalogTable.exists(_.isMaterializedView)) {
      meta.willNotWorkOnGpu("Delta OPTIMIZE on materialized views is not supported on GPU")
    }
    if (catalogTable.exists(_.isStreamingTable)) {
      meta.willNotWorkOnGpu("Delta OPTIMIZE on streaming tables is not supported on GPU")
    }
  }

  def tagOptimizeForGpu(
      meta: RapidsMeta[_, _, _],
      conf: RapidsConf,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      tagCommandForGpu: DeltaLog => Unit): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    tagCatalogTableForGpu(meta, catalogTable)
    tagCommandForGpu(deltaLog)
    RapidsDeltaUtils.tagForDeltaWrite(
      meta,
      deltaLog.unsafeVolatileSnapshot.schema,
      Some(deltaLog),
      Map.empty,
      SparkSession.active)
  }
}

class OptimizeTableCommandMeta(
    val optimizeCmd: OptimizeTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[OptimizeTableCommand](optimizeCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    val deltaTable = OptimizeTableCommandMeta.getDeltaTableForOptimize(optimizeCmd)
    OptimizeTableCommandMeta.tagOptimizeForGpu(
      this,
      conf,
      deltaTable.deltaLog,
      deltaTable.catalogTable,
      log => OptimizeTableCommandMetaShim.tagForGpu(this, log))
  }

  override def convertToGpu(): RunnableCommand = {
    GpuOptimizeTableCommand(
      optimizeCmd.child,
      optimizeCmd.userPartitionPredicates,
      optimizeCmd.optimizeContext)(optimizeCmd.zOrderBy)
  }
}

class OptimizeTableCommandEdgeMeta(
    val optimizeCmd: OptimizeTableCommandEdge,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[OptimizeTableCommandEdge](optimizeCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    val deltaTable = OptimizeTableCommandMeta.getDeltaTableForOptimize(optimizeCmd)
    OptimizeTableCommandMeta.tagOptimizeForGpu(
      this,
      conf,
      deltaTable.deltaLog,
      deltaTable.catalogTable,
      log => OptimizeTableCommandMetaShim.tagForGpu(this, log))
  }

  override def convertToGpu(): RunnableCommand = {
    GpuOptimizeTableCommandEdge(
      optimizeCmd.child,
      optimizeCmd.isFull,
      optimizeCmd.predicates)(optimizeCmd.zOrderBy)
  }
}
