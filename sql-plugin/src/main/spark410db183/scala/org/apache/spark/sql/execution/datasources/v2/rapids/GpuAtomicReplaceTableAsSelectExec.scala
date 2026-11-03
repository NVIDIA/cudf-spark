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

/*** spark-rapids-shim-json-lines
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2.rapids

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, CatalogV2Util, Column, Identifier,
  StagingTableCatalog, Table, TableCatalog, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.scripting.SqlScriptingLocalVariableUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/** GPU wrapper for DBR 18.3 atomic RTAS. */
case class GpuAtomicReplaceTableAsSelectExec(
    override val output: Seq[Attribute],
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Identifier) => Unit)
  extends V2CreateTableAsSelectBaseExec with GpuExec {

  private val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override def supportsColumnar: Boolean = false

  private def tableInfo(columns: Array[Column]): TableInfo = {
    new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
  }

  private def loadForInsert(): Table = {
    catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava)
  }

  private def hasRowColumnControls: Boolean =
    tableSpec.rowFilter.isDefined || tableSpec.columnMasks.isDefined

  private def stageCreateOrReplace(columns: Array[Column]): Table = {
    if (hasRowColumnControls) {
      import CatalogV2Implicits._
      catalog.stageCreateOrReplaceWithRowColumnControls(
        ident,
        columns.asSchema,
        partitioning.toArray,
        properties.asJava,
        tableSpec.rowFilter.orNull,
        tableSpec.columnMasks.orNull,
        Array.empty[Constraint])
    } else {
      catalog.stageCreateOrReplace(ident, tableInfo(columns))
    }
  }

  private def stageReplace(columns: Array[Column]): Table = {
    if (hasRowColumnControls) {
      import CatalogV2Implicits._
      catalog.stageReplaceWithRowColumnControls(
        ident,
        columns.asSchema,
        partitioning.toArray,
        properties.asJava,
        tableSpec.rowFilter.orNull,
        tableSpec.columnMasks.orNull,
        Array.empty[Constraint])
    } else {
      catalog.stageReplace(ident, tableInfo(columns))
    }
  }

  override protected def run(): Seq[InternalRow] = {
    if (SQLConf.get.sqlScriptingForbidLocalVarsInTempObjects && tableSpec.isSessionTemp) {
      SqlScriptingLocalVariableUtils.verifyNoLocalVariableReferences(
        query, "table", ident.name)
    }

    val columns = getV2Columns(query.schema, catalog.useNullableQuerySchema)
    lazy val searchPath = CatalogV2Util.searchPathForTableIdentifier(catalog, ident)
    if (catalog.tableExists(ident)) {
      invalidateCache(catalog, ident)
    }

    val staged = if (orCreate) {
      stageCreateOrReplace(columns)
    } else if (catalog.tableExists(ident)) {
      try {
        stageReplace(columns)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(ident, searchPath, Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(ident, searchPath)
    }
    val table = Option(staged).getOrElse(loadForInsert())
    GpuAtomicDeltaWriteContext.withAtomicWrite {
      writeToTable(catalog, table, writeOptions, ident, query, true, false, true)
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    throw new IllegalStateException("Columnar execution not supported")
}
