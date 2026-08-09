/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import java.net.URI

import com.nvidia.spark.rapids.{ColumnarFileFormat, GpuDataWritingCommand}
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuCreateDataSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    origProvider: Class[_],
    gpuFileFormat: ColumnarFileFormat,
    useStableSort: Boolean,
    concurrentWriterPartitionFlushSize: Long,
    baseDebugOutputPath: Option[String])
  extends GpuDataWritingCommand {

  override def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, child, SaveMode.Append, tableExists = true)
    } else {
      table.storage.locationUri.foreach { p =>
        GpuDataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
      }
      assert(table.schema.isEmpty)
      sparkSession.sessionState.catalog.validateTableLocation(table)
      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, child, SaveMode.Overwrite, tableExists = false)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      // Table location is already validated. No need to check it again during table creation.
      sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)

      result match {
        case _: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
          sparkSession.sessionState.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(
            SparkShimImpl.v1RepairTableCommand(table.identifier)).toRdd
        case _ =>
      }
    }

    CommandUtils.updateTableStats(sparkSession, table)

    Seq.empty[ColumnarBatch]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `query`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = GpuDataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None,
      origProvider = origProvider,
      gpuFileFormat = gpuFileFormat)
    try {
      dataSource.writeAndRead(mode, query, outputColumnNames, physicalPlan, useStableSort,
        concurrentWriterPartitionFlushSize, baseDebugOutputPath)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }

  private val isPartitioned = table.partitionColumnNames.nonEmpty

  private val isBucketed = table.bucketSpec.nonEmpty

  private val needSort = isPartitioned || isBucketed

  // use same logic as GpuInsertIntoHadoopFsRelationCommand
  override def requireSingleBatch: Boolean = needSort && useStableSort
}
