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

package org.apache.spark.sql.delta.rapids.delta21x

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.rapids.{GpuDeltaCatalogBase, SupportsPathIdentifier}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.PartitioningUtils

class GpuDeltaCatalog(
    override val cpuCatalog: DeltaCatalog,
    override val rapidsConf: RapidsConf)
  extends GpuDeltaCatalogBase with SupportsPathIdentifier with Logging {

  override val spark: SparkSession = cpuCatalog.spark

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
        throw DeltaErrors.notADeltaTable(table.table)
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

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    cpuCatalog.loadTable(ident, timestamp)
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    cpuCatalog.loadTable(ident, version)
  }
}
