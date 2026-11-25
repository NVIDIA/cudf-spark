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

package org.apache.spark.sql.delta.rapids.delta43x

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.commands.{DeltaInsertReplaceOnOrUsingCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuWriteIntoDeltaLike}
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.types.StructType

/**
 * Retains upstream criteria resolution, source materialization, lost-block retries, cleanup,
 * and statistics. Only the prepared WriteIntoDelta's execution is redirected to the GPU writer.
 */
case class GpuInsertReplaceOnOrUsingCommand(
    gpuDeltaLog: GpuDeltaLog,
    cpuCommand: DeltaInsertReplaceOnOrUsingCommand) extends GpuWriteIntoDeltaLike {

  private def adaptedCommand: DeltaInsertReplaceOnOrUsingCommand = {
    cpuCommand.copy(writeCmd = new GpuPreparedReplaceWrite43x(gpuDeltaLog, cpuCommand.writeCmd))
  }

  override val deltaLog: DeltaLog = cpuCommand.deltaLog
  override val configuration: Map[String, String] = cpuCommand.configuration
  override val data: DataFrame = cpuCommand.data

  override def run(sparkSession: SparkSession): Seq[Row] = adaptedCommand.run(sparkSession)

  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec],
      isTableReplace: Boolean): TaggedCommitData[Action] = {
    adaptedCommand.writeAndReturnCommitData(txn, sparkSession, clusterBySpecOpt, isTableReplace)
  }

  override def withNewWriterConfiguration(
      updatedConfiguration: Map[String, String]): GpuWriteIntoDeltaLike = {
    copy(cpuCommand = cpuCommand.copy(
      writeCmd = cpuCommand.writeCmd.copy(configuration = updatedConfiguration)))
  }
}

/**
 * Upstream prepares a WriteIntoDelta by calling copy with the materialized DataFrame, target
 * alias, and by-name flag. Preserve GPU dispatch through that copy, including configuration
 * copies in saveAsTable. No materialization or retry state is owned by this adapter.
 */
private[delta43x] class GpuPreparedReplaceWrite43x(
    gpuDeltaLog: GpuDeltaLog,
    cpuWrite: WriteIntoDelta)
  extends WriteIntoDelta(cpuWrite.deltaLog, cpuWrite.mode, cpuWrite.options,
    cpuWrite.partitionColumns, cpuWrite.configuration, cpuWrite.data, cpuWrite.catalogTableOpt,
    cpuWrite.schemaInCatalog, cpuWrite.isInsertReplaceUsingByName) {

  override def copy(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType],
      isInsertReplaceUsingByName: Boolean): WriteIntoDelta = {
    require(deltaLog eq gpuDeltaLog.deltaLog, "Prepared replacement must retain its Delta log")
    new GpuPreparedReplaceWrite43x(gpuDeltaLog, super.copy(
      deltaLog, mode, options, partitionColumns, configuration, data, catalogTableOpt,
      schemaInCatalog, isInsertReplaceUsingByName))
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    GpuWriteIntoDelta43x(gpuDeltaLog, cpuWrite).run(sparkSession)
  }

  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec],
      isTableReplace: Boolean): TaggedCommitData[Action] = {
    GpuWriteIntoDelta43x(gpuDeltaLog, cpuWrite)
      .writeAndReturnCommitData(txn, sparkSession, clusterBySpecOpt, isTableReplace)
  }
}
