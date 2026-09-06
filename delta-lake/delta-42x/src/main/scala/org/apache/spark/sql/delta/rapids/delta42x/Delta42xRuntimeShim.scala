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

package org.apache.spark.sql.delta.rapids.delta42x

import scala.util.Try

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}
import com.nvidia.spark.rapids.delta.DeltaWriteUtils.toBooleanOption
import com.nvidia.spark.rapids.delta.delta42x.{Delta42xConfigChecker, Delta42xProvider,
  GpuDeltaCatalog}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaOperations, DeltaOptions}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.hooks.GpuAutoCompact42x
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShimBase, GpuDeltaLog, GpuOptimisticTransaction,
  GpuOptimisticTransactionBase, GpuWriteIntoDeltaLike, StartTransactionArg}

class Delta42xRuntimeShim extends DeltaRuntimeShimBase {

  override def getDeltaConfigChecker: DeltaConfigChecker = Delta42xConfigChecker

  override def getDeltaProvider: DeltaProvider = Delta42xProvider

  override def getGpuDeltaCatalog(
      cpuCatalog: DeltaCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = {
    new GpuDeltaCatalog(cpuCatalog, rapidsConf)
  }

  override protected def constructOptimisticTransaction(
      arg: StartTransactionArg): GpuOptimisticTransactionBase =
    new GpuOptimisticTransaction(
      arg.log, arg.catalogTable, arg.snapshot, arg.conf, GpuAutoCompact42x)

  override def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike = {
    GpuWriteIntoDelta42x(gpuDeltaLog, cpuWrite)
  }

  override def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    DeltaOperations.Write(
      mode,
      Option(partitionColumns),
      options.replaceWhere,
      options.userMetadata,
      dynamicPartitionOverwriteOption(options),
      toBooleanOption(options.canOverwriteSchema),
      toBooleanOption(options.canMergeSchema))
  }

  override def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    DeltaOperations.ReplaceTable(
      metadata,
      isManaged,
      orCreate,
      asSelect,
      options.flatMap(_.userMetadata),
      clusterBy,
      options.flatMap(_.replaceWhere),
      options.flatMap(dynamicPartitionOverwriteOption),
      toBooleanOption(options.exists(_.canOverwriteSchema)),
      toBooleanOption(options.exists(_.canMergeSchema)),
      isV1SaveAsTableOverwrite)
  }

  private def dynamicPartitionOverwriteOption(options: DeltaOptions): Option[Boolean] = {
    toBooleanOption(Try(options.isDynamicPartitionOverwriteMode).getOrElse(false))
  }
}
