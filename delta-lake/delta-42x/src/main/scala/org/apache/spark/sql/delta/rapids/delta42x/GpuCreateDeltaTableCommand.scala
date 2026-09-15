/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from CreateDeltaTableCommand.scala in the
 * Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.rapids.delta42x

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaErrors, Snapshot, UniversalFormat}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTableUtils
import org.apache.spark.sql.delta.rapids.{GpuCreateDeltaTableCommand40x42xBase, GpuDeltaLog, GpuOptimisticTransactionBase}

case class GpuCreateDeltaTableCommand(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan],
    operation: TableCreationModes.CreationMode = TableCreationModes.Create,
    tableByPath: Boolean = false,
    override val output: Seq[Attribute] = Nil,
    protocol: Option[Protocol] = None,
    allowCatalogManaged: Boolean = false,
    createTableFunc: Option[CatalogTable => Unit] = None)(@transient rapidsConf: RapidsConf)
  extends GpuCreateDeltaTableCommand40x42xBase(
    table, existingTableOpt, mode, query, operation, tableByPath, output, protocol,
    createTableFunc, rapidsConf) {

  override protected def enforceDependenciesInConfiguration(
      sparkSession: SparkSession,
      configuration: Map[String, String],
      snapshot: Snapshot): Map[String, String] = {
    enforceDependenciesInConfiguration(sparkSession, table, configuration, snapshot)
  }

  override protected def enforceDependenciesInConfiguration(
      sparkSession: SparkSession,
      tableDesc: CatalogTable,
      configuration: Map[String, String],
      snapshot: Snapshot): Map[String, String] = {
    UniversalFormat.enforceDependenciesInConfiguration(
      sparkSession, tableDesc, configuration, snapshot)
  }

  override protected def validateCatalogManagedTable(sparkSession: SparkSession): Unit = {
    val tableFeatures =
      TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(table.properties)
    if (!allowCatalogManaged &&
        (tableFeatures.contains(CatalogOwnedTableFeature) ||
          CatalogOwnedTableUtils.defaultCatalogOwnedEnabled(sparkSession))) {
      throw DeltaErrors.deltaCannotCreateCatalogManagedTable()
    }
  }

  override protected def validateCatalogManagedTableProperties(
      sparkSession: SparkSession,
      gpuDeltaLog: GpuDeltaLog,
      tableWithLocation: CatalogTable): Unit = {
    val deltaLog = gpuDeltaLog.deltaLog
    CatalogOwnedTableUtils.validatePropertiesForCreateDeltaTableCommand(
      spark = sparkSession,
      tableExists = deltaLog.tableExists,
      query = query,
      catalogTableProperties = tableWithLocation.properties,
      existingTableSnapshotOpt =
        if (deltaLog.tableExists) Some(deltaLog.unsafeVolatileSnapshot) else None)
  }

  override protected def metadataForReplace(
      txn: GpuOptimisticTransactionBase,
      metadata: Metadata): Metadata = {
    if (allowCatalogManaged && txn.snapshot.isCatalogOwned) {
      metadata.copy(id = txn.snapshot.metadata.id)
    } else {
      metadata
    }
  }

  override protected def catalogTableForTransaction: Option[CatalogTable] = existingTableOpt

  override protected def createCatalogTableForCreateOrReplace(
      sparkSession: SparkSession,
      table: CatalogTable,
      createTableFunc: Option[CatalogTable => Unit]): Unit = {
    createTableFunc match {
      case Some(createFunc) => createFunc(table)
      case None => super.createCatalogTableForCreateOrReplace(sparkSession, table, createTableFunc)
    }
  }
}
