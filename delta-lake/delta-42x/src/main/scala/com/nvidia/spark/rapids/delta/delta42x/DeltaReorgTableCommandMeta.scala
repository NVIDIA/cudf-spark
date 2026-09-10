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

package com.nvidia.spark.rapids.delta.delta42x

import scala.reflect.classTag

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils
import com.nvidia.spark.rapids.delta.common.DeltaReorgTableCommandMetaBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{IcebergCompat, RowTracking, UniversalFormat}
import org.apache.spark.sql.delta.commands.{DeltaCommand, DeltaReorgTableCommand,
  DeltaReorgTableMode}
import org.apache.spark.sql.delta.rapids.GpuDeltaReorgTableCommand
import org.apache.spark.sql.execution.command.RunnableCommand

object DeltaReorgTableCommandMeta {
  private val optimizeCommandConfKey = "spark.rapids.sql.command.OptimizeTableCommand"

  def rule: RunnableCommandRule[DeltaReorgTableCommand] = {
    new RunnableCommandRule[DeltaReorgTableCommand](
      (cmd, conf, parent, rule) =>
        new DeltaReorgTableCommandMeta(cmd, conf, parent, rule),
      "Reorganize a Delta Lake table",
      classTag[DeltaReorgTableCommand]) {
      override def confKey: String = optimizeCommandConfKey
    }
  }
}

class DeltaReorgTableCommandMeta(
    cmd: DeltaReorgTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends DeltaReorgTableCommandMetaBase(cmd, conf, parent, rule) {

  private object DeltaCmdProxy extends DeltaCommand

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    if (cmd.reorgTableSpec.reorgTableMode != DeltaReorgTableMode.PURGE ||
        cmd.reorgTableSpec.icebergCompatVersionOpt.nonEmpty) {
      willNotWorkOnGpu("Only Delta REORG TABLE APPLY (PURGE) is supported on GPU")
    }

    val table = DeltaCmdProxy.getDeltaTable(cmd.target, "REORG")
    val snapshot = table.deltaLog.unsafeVolatileSnapshot
    if (IcebergCompat.isAnyEnabled(snapshot.metadata) ||
        UniversalFormat.icebergEnabled(snapshot.metadata)) {
      willNotWorkOnGpu(
        "Delta REORG TABLE is not supported on GPU for Iceberg-compatible tables")
    }
    if (RowTracking.isEnabled(snapshot.protocol, snapshot.metadata)) {
      willNotWorkOnGpu(
        "Delta REORG TABLE is not supported on GPU for row-tracking tables")
    }

    FileFormatChecks.tag(this, snapshot.schema, ParquetFormatType, ReadFileOp)
    RapidsDeltaUtils.tagForDeltaWrite(
      this,
      snapshot.schema,
      Some(table.deltaLog),
      Map.empty,
      SparkSession.active)

    if (snapshot.isCatalogOwned) {
      willNotWorkOnGpu("Delta 4.2 requires catalog-managed REORG to run on CPU")
    }
  }

  override def convertToGpu(): RunnableCommand = {
    GpuDeltaReorgTableCommand(cmd.target)(cmd.predicates)
  }
}
