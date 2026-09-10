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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2.rapids

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, CatalogV2Util, Identifier,
  StagingTableCatalog, Table, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{V2CreateTableAsSelectBaseExec,
  WriteToDataSourceV2Exec}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU wrapper for DBR 17.3 atomic CTAS.
 *
 * The wrapper deliberately keeps DBR's native catalog and staged table. Only the physical command
 * node is replaced; staging, nested AppendData execution, commit and abort continue through the
 * DBR 17.3 V2 APIs inherited from [[V2CreateTableAsSelectBaseExec]].
 */
case class GpuAtomicCreateTableAsSelectExec(
    override val output: Seq[Attribute],
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean)
  extends V2CreateTableAsSelectBaseExec with GpuExec {

  private val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override def supportsColumnar: Boolean = false

  private def loadForInsert(): Table = {
    catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava)
  }

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        Nil
      } else {
        throw QueryCompilationErrors.tableAlreadyExistsError(ident)
      }
    } else {
      val columns = getV2Columns(query.schema, catalog.useNullableQuerySchema)
      val table = WriteToDataSourceV2Exec.handleConcurrentCreateExceptions(ifNotExists) {
        val staged = if (tableSpec.rowFilter.isDefined || tableSpec.columnMasks.isDefined) {
          import CatalogV2Implicits._
          catalog.stageCreateWithRowColumnControls(
            ident,
            columns.asSchema,
            partitioning.toArray,
            properties.asJava,
            tableSpec.rowFilter.orNull,
            tableSpec.columnMasks.orNull)
        } else {
          val tableInfo = new TableInfo.Builder()
            .withColumns(columns)
            .withPartitions(partitioning.toArray)
            .withProperties(properties.asJava)
            .build()
          catalog.stageCreate(ident, tableInfo)
        }
        Option(staged).getOrElse(loadForInsert())
      }

      table match {
        case Some(stagedTable) =>
          GpuAtomicDeltaWriteContext.withAtomicWrite {
            writeToTable(catalog, stagedTable, writeOptions, ident, query, ifNotExists)
          }
        case None => Nil
      }
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    throw new IllegalStateException("Columnar execution not supported")
}
