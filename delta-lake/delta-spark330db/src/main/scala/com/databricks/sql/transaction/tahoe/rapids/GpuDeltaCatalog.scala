/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import java.util

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaErrors}
import com.databricks.sql.transaction.tahoe.commands.TableCreationModes
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging
import com.databricks.sql.transaction.tahoe.sources.DeltaSourceUtils
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StructType

class GpuDeltaCatalog(
    override val cpuCatalog: StagingTableCatalog,
    override val rapidsConf: RapidsConf)
  extends GpuDeltaCatalogBase with SupportsPathIdentifier with DeltaLogging {

  override protected def buildGpuCreateDeltaTableCommand(
      rapidsConf: RapidsConf,
      table: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      query: Option[LogicalPlan],
      operation: TableCreationModes.CreationMode,
      tableByPath: Boolean): LeafRunnableCommand = {
    GpuCreateDeltaTableCommand(
      table,
      existingTableOpt,
      mode,
      query,
      operation,
      tableByPath = tableByPath
    )(rapidsConf)
  }

  override protected def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathIdentifier(table)) return None
    val tableExists = catalog.tableExists(table)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(
          s"$table is a view. You may not write data into a view.")
      }
      if (!DeltaSourceUtils.isDeltaTable(oldTable.provider)) {
        throw new AnalysisException(s"$table is not a Delta table. Please drop this " +
          "table first if you would like to recreate it with Delta Lake.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  override protected def verifyTableAndSolidify(
      tableDesc: CatalogTable,
      query: Option[LogicalPlan]): CatalogTable = {

    if (tableDesc.bucketSpec.isDefined) {
      throw DeltaErrors.operationNotSupportedException("Bucketing", tableDesc.identifier)
    }

    val schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // Delta is case insensitive

    val validatedConfigurations = DeltaConfigs.validateConfigurations(tableDesc.properties)

    val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = validatedConfigurations)
  }

  override protected def createGpuStagedDeltaTableV2(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode): StagedTable = {
    new GpuStagedDeltaTableV2WithLogging(ident, schema, partitions, properties, operation)
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    cpuCatalog.loadTable(ident, timestamp)
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    cpuCatalog.loadTable(ident, version)
  }

  /**
   * Creates a Delta table using GPU for writing the data
   *
   * @param ident              The identifier of the table
   * @param schema             The schema of the table
   * @param partitions         The partition transforms for the table
   * @param allTableProperties The table properties that configure the behavior of the table or
   *                           provide information about the table
   * @param writeOptions       Options specific to the write during table creation or replacement
   * @param sourceQuery        A query if this CREATE request came from a CTAS or RTAS
   * @param operation          The specific table creation mode, whether this is a
   *                           Create/Replace/Create or Replace
   */
  override def createDeltaTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String],
      writeOptions: Map[String, String],
      sourceQuery: Option[DataFrame],
      operation: TableCreationModes.CreationMode
  ): Table = recordFrameProfile(
    "DeltaCatalog", "createDeltaTable") {
    super.createDeltaTable(
      ident,
      schema,
      partitions,
      allTableProperties,
      writeOptions,
      sourceQuery,
      operation)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    recordFrameProfile("DeltaCatalog", "createTable") {
      super.createTable(ident, schema, partitions, properties)
    }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageReplace") {
      super.stageReplace(ident, schema, partitions, properties)
    }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreateOrReplace") {
      super.stageCreateOrReplace(ident, schema, partitions, properties)
    }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreate") {
      super.stageCreate(ident, schema, partitions, properties)
    }

  /**
   * A staged Delta table, which creates a HiveMetaStore entry and appends data if this was a
   * CTAS/RTAS command. We have a ugly way of using this API right now, but it's the best way to
   * maintain old behavior compatibility between Databricks Runtime and OSS Delta Lake.
   */
  protected class GpuStagedDeltaTableV2WithLogging(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode)
    extends GpuStagedDeltaTableV2(ident, schema, partitions, properties, operation) {

    override def commitStagedChanges(): Unit = recordFrameProfile(
      "DeltaCatalog", "commitStagedChanges") {
      super.commitStagedChanges()
    }
  }
}
