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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.commands.{TableCreationModes, WriteIntoDeltaEdge}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class GpuCreateDeltaTableCommand(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan],
    operation: TableCreationModes.CreationMode = TableCreationModes.Create,
    tableByPath: Boolean = false,
    override val output: Seq[Attribute] = Nil)(@transient rapidsConf: RapidsConf)
  extends GpuCreateDeltaTableCommandBase(
    table,
    existingTableOpt,
    mode,
    query,
    operation,
    tableByPath,
    output,
    rapidsConf) {

  override protected def validateQueryPlan(plan: LogicalPlan): Unit = plan match {
    case _: WriteIntoDeltaEdge =>
      throw new UnsupportedOperationException(
        "Delta CTAS/RTAS using WriteIntoDeltaEdge is not supported on GPU for DB-17.3")
    case _ =>
  }
}
