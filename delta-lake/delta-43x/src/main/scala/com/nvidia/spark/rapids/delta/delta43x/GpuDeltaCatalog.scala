/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from DeltaDataSource.scala in the
 * Delta Lake project at https://github.com/delta-io/delta.
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

package com.nvidia.spark.rapids.delta.delta43x

import java.util

import com.nvidia.spark.rapids.RapidsConf
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier,
  StagedTable, StagingTableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaCatalogRestApiShim}
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.rapids.{
  GpuCreateDeltaTableCommand40x42xBase,
  GpuDeltaCatalog4x,
  GpuUnityCatalogWrapper,
  GpuWriteIntoDeltaLike}
import org.apache.spark.sql.delta.rapids.delta43x.GpuCreateDeltaTableCommand
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.types.StructType

class GpuDeltaCatalog(
    cpuCatalog: DeltaCatalog,
    rapidsConf: RapidsConf)
  extends GpuDeltaCatalog4x(cpuCatalog, rapidsConf) {

  override def name(): String = cpuCatalog.name()

  override protected lazy val isUnityCatalog: Boolean = {
    val delegateField = classOf[DelegatingCatalogExtension].getDeclaredField("delegate")
    delegateField.setAccessible(true)
    delegateField.get(cpuCatalog).getClass.getCanonicalName.startsWith("io.unitycatalog.")
  }

  override protected def getTableIdentifier(ident: Identifier): TableIdentifier = {
    val table = super.getTableIdentifier(ident)
    if (isUnityCatalog) {
      table.copy(catalog = Some(cpuCatalog.name()))
    } else {
      table
    }
  }

  override protected def getExistingTableIfExists(
      table: TableIdentifier,
      ident: Identifier,
      operation: TableCreationModes.CreationMode): Option[CatalogTable] = {
    cpuCatalog.getExistingTableIfExists(table, Some(ident), operation)
  }

  override protected def useCatalogCreateTable(sourceQuery: Option[DataFrame]): Boolean = {
    isUnityCatalog
  }

  override protected def getDeltaLogForWrite(
      existingTableOpt: Option[CatalogTable],
      tablePath: Path,
      fileSystemOptions: Map[String, String]): DeltaLog = {
    DeltaUtils.getDeltaLogFromTableOrPath(
      spark, existingTableOpt, tablePath, fileSystemOptions)
  }

  override protected def normalizeStagedTableProperties(
      properties: util.Map[String, String]): Unit = {
    if (isUnityCatalog) {
      Option(properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD)).foreach {
        oldTableId =>
          properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldTableId)
      }
    }
  }

  override protected def splitCarriedForwardProperties(
      properties: Map[String, String]): (Map[String, String], Map[String, String]) = {
    val prefix = DeltaCatalogRestApiShim.carryForwardPrefix
    val (carried, remaining) = properties.partition { case (key, _) => key.startsWith(prefix) }
    (remaining, carried.map { case (key, value) => key.stripPrefix(prefix) -> value })
  }

  /**
   * Publishes the committed table through the CPU catalog's Delta REST client, exactly as Delta's
   * own create callback does. Tagging only admits requests Delta routes through that client, so
   * any other table reaching this point would have bypassed the catalog's control plane.
   */
  override protected def createTableInCatalog(
      ident: Identifier,
      table: CatalogTable,
      snapshot: Snapshot): Unit = {
    if (!DeltaCatalogRestApiShim.createManagedTable(cpuCatalog, ident, table, snapshot)) {
      throw new IllegalStateException(
        s"$ident is not a catalog-managed table of a Delta REST catalog; " +
          "it must not be created by the GPU Delta catalog")
    }
  }

  /**
   * Delegates staging to the initialized CPU catalog so that its Delta REST client reserves or
   * loads the catalog entry and returns the effective properties (table ID, location, storage
   * credentials, replace carry-forward). The GPU staged table writes the data and commits with
   * those properties; aborting it aborts the CPU staged table.
   */
  override protected def createGpuStagedDeltaTableV2(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode): StagedTable = {
    if (!DeltaCatalogRestApiShim.isRestApiEnabled(cpuCatalog)) {
      super.createGpuStagedDeltaTableV2(ident, schema, partitions, properties, operation)
    } else {
      val cpuStaged = operation match {
        case TableCreationModes.Create =>
          cpuCatalog.stageCreate(ident, schema, partitions, properties)
        case TableCreationModes.Replace =>
          cpuCatalog.stageReplace(ident, schema, partitions, properties)
        case TableCreationModes.CreateOrReplace =>
          cpuCatalog.stageCreateOrReplace(ident, schema, partitions, properties)
      }
      new GpuStagedDeltaTableV2(ident, schema, partitions, cpuStaged.properties(), operation) {
        override def abortStagedChanges(): Unit = cpuStaged.abortStagedChanges()
      }
    }
  }

  override protected def buildGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDeltaLike],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[(CatalogTable, Snapshot) => Unit])
      : GpuCreateDeltaTableCommand40x42xBase = {
    GpuCreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      tableByPath = isByPath,
      allowCatalogManaged = isUnityCatalog && withDb.tableType == CatalogTableType.MANAGED,
      createTableFunc = tableCreateFunc)(rapidsConf)
  }
}

object GpuDeltaCatalog {

  def isUnityCatalog(catalog: StagingTableCatalog): Boolean =
    GpuUnityCatalogWrapper.isUnityCatalog(catalog)

  def unsupportedUnityCatalogReason(catalog: StagingTableCatalog): Option[String] =
    GpuUnityCatalogWrapper.unsupportedReason(catalog, classOf[GpuDeltaCatalog])

  def wrapUnityCatalog(
      catalog: StagingTableCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog =
    GpuUnityCatalogWrapper.wrap(
      catalog, classOf[GpuDeltaCatalog], new GpuDeltaCatalog(_, rapidsConf))
}
