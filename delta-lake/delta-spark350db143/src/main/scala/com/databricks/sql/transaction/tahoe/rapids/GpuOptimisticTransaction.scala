/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
 *
 * This file was derived from OptimisticTransaction.scala and TransactionalWrite.scala
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

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaLog, Snapshot}
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.Clock

class GpuOptimisticTransaction(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)(implicit clock: Clock)
    extends GpuOptimisticTransactionWriteBase(deltaLog, catalogTable, snapshot, rapidsConf)(clock) {

  def this(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], snapshot, rapidsConf)
  }

  def this(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      snapshotOpt: Option[Snapshot],
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, catalogTable, snapshotOpt.getOrElse(deltaLog.update()), rapidsConf)
  }

  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], deltaLog.update(), rapidsConf)
  }

  override protected def handlePostWriteAutoCompact(
      spark: SparkSession,
      isOptimize: Boolean,
      fileActions: Seq[FileAction]): Unit = {
    lazy val autoCompactEnabled =
      spark.sessionState.conf
        .getConf[String](DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED)
        .getOrElse {
          DeltaConfigs.AUTO_COMPACT.fromMetaData(metadata)
            .getOrElse("false")
        }.toBoolean

    if (!isOptimize && autoCompactEnabled && fileActions.nonEmpty) {
      registerPostCommitHook(GpuDoAutoCompaction)
    }
  }
}
