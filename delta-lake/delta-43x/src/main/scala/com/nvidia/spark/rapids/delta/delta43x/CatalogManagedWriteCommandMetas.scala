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

package com.nvidia.spark.rapids.delta.delta43x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.delta.DeltaWriteUtils
import com.nvidia.spark.rapids.delta.common.{DeltaDynamicPartitionOverwriteCommandMeta =>
  CommonDynamicOverwriteMeta, UpdateCommandMeta => CommonUpdateCommandMeta}

import org.apache.spark.sql.delta.{DeltaDynamicPartitionOverwriteCommand, DeltaOptions, RowTracking}
import org.apache.spark.sql.delta.commands.UpdateCommand

/**
 * UPDATE on a catalog-managed table with row tracking stays on the CPU. The rewrite scan has to
 * read the row-tracking hidden metadata columns, which the GPU scan does not support, so the scan
 * runs on the CPU and every row is copied to the GPU through HostColumnarToGpu. Measured on a
 * cluster, that copy makes the GPU rewrite slower than the CPU rewrite.
 */
class CatalogManagedUpdateCommandMeta(
    updateCmd: UpdateCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CommonUpdateCommandMeta(updateCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    val snapshot = updateCmd.tahoeFileIndex.deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned && RowTracking.isEnabled(snapshot.protocol, snapshot.metadata)) {
      willNotWorkOnGpu("Delta 4.3 UPDATE on a catalog-managed table with row tracking is slower " +
        "on GPU than on CPU because the row-tracking metadata columns force the scan onto the " +
        "CPU and the rows are then copied to the GPU")
    }
  }
}

/**
 * A dynamic partition overwrite of a partitioned catalog-managed table without optimized write
 * stays on the CPU (see [[SmallFilePartitionedWrite]]).
 */
class CatalogManagedDynamicPartitionOverwriteCommandMeta(
    overwriteCommand: DeltaDynamicPartitionOverwriteCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CommonDynamicOverwriteMeta(overwriteCommand, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    val deltaTable = overwriteCommand.deltaTable
    val snapshot = deltaTable.deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned && snapshot.metadata.partitionColumns.nonEmpty) {
      val sessionConf = deltaTable.spark.sessionState.conf
      val writeOptions = new DeltaOptions(overwriteCommand.writeOptions, sessionConf)
      val optimizeWrite = DeltaWriteUtils.shouldOptimizeWrite(
        Some(writeOptions), sessionConf, snapshot.metadata)
      SmallFilePartitionedWrite.tagUnlessOptimizeWrite(
        this, "dynamic partition overwrite of a catalog-managed table", optimizeWrite)
    }
  }
}
