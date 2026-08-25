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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims

/**
 * Shared Delta 4.0/4.1 scaffolding for versioned GpuCreateDeltaTableCommand implementations.
 */
abstract class GpuCreateDeltaTableCommand40x41xBase(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan],
    operation: TableCreationModes.CreationMode = TableCreationModes.Create,
    tableByPath: Boolean = false,
    override val output: Seq[Attribute] = Nil,
    protocol: Option[Protocol] = None,
    createTableFunc: Option[CatalogTable => Unit] = None,
    rapidsConf: RapidsConf)
  extends GpuCreateDeltaTableCommandBase(
    table, existingTableOpt, mode, query, operation, tableByPath, output, protocol,
    createTableFunc, rapidsConf) {

  override protected def createDataFrameFromQuery(
      sparkSession: SparkSession,
      query: LogicalPlan): DataFrame = {
    TrampolineConnectShims.createDataFrame(TrampolineConnectShims.getActiveSession, query)
  }
}
