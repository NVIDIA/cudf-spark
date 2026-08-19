/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids.delta40x

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{Snapshot, UniversalFormat}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.rapids.GpuCreateDeltaTableCommand40x41xBase

/**
 * GPU version of Delta 4.0.x CreateDeltaTableCommand.
 */
case class GpuCreateDeltaTableCommand(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan],
    operation: TableCreationModes.CreationMode = TableCreationModes.Create,
    tableByPath: Boolean = false,
    override val output: Seq[Attribute] = Nil,
    protocol: Option[Protocol] = None,
    createTableFunc: Option[CatalogTable => Unit] = None)(@transient rapidsConf: RapidsConf)
  extends GpuCreateDeltaTableCommand40x41xBase(
    table, existingTableOpt, mode, query, operation, tableByPath, output, protocol,
    createTableFunc, rapidsConf) {

  override protected def enforceDependenciesInConfiguration(
      sparkSession: SparkSession,
      configuration: Map[String, String],
      snapshot: Snapshot): Map[String, String] = {
    UniversalFormat.enforceDependenciesInConfiguration(sparkSession, configuration, snapshot)
  }
}
