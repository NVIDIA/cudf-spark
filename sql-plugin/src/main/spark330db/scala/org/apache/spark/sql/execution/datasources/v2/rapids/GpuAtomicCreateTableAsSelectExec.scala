/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2.rapids

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TableSpec}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, StagingTableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.TableWriteExecHelper
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of AtomicCreateTableAsSelectExec.
 *
 * Physical plan node for v2 create table as select, when the catalog is determined to support
 * staging table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * The CTAS operation is atomic. The creation of the table is staged and the commit of the write
 * should bundle the commitment of the metadata and the table contents in a single unit. If the
 * write fails, the table is instructed to roll back all staged changes.
 */
case class GpuAtomicCreateTableAsSelectExec(
    override val output: Seq[Attribute],
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    query: SparkPlan,
    tableSpec: TableSpec,
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends TableWriteExecHelper with GpuExec with ColumnarToRowTransition {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override def supportsColumnar: Boolean = false

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }

      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val schema = CharVarcharUtils.getRawSchema(query.schema, conf).asNullable
    val stagedTable = catalog.stageCreate(
      ident, schema, partitioning.toArray, properties.asJava)
    writeToTable(catalog, stagedTable, writeOptions, ident)
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): GpuAtomicCreateTableAsSelectExec = copy(query = newChild)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    throw new IllegalStateException("Columnar execution not supported")
}
