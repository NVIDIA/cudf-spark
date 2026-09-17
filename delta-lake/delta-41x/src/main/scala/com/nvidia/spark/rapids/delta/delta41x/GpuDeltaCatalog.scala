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

package com.nvidia.spark.rapids.delta.delta41x

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.rapids.{
  GpuCreateDeltaTableCommand40x42xBase,
  GpuDeltaCatalog4x,
  GpuWriteIntoDeltaLike}
import org.apache.spark.sql.delta.rapids.delta41x.GpuCreateDeltaTableCommand

class GpuDeltaCatalog(
    cpuCatalog: DeltaCatalog,
    rapidsConf: RapidsConf)
  extends GpuDeltaCatalog4x(cpuCatalog, rapidsConf) {

  override protected def getExistingTableIfExists(
      table: TableIdentifier,
      ident: Identifier,
      operation: TableCreationModes.CreationMode): Option[CatalogTable] = {
    cpuCatalog.getExistingTableIfExists(table)
  }

  // Delta 4.1 added catalog plugin API support for CTAS.
  override protected def useCatalogCreateTable(sourceQuery: Option[DataFrame]): Boolean = {
    isUnityCatalog
  }

  override protected def buildGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDeltaLike],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[CatalogTable => Unit]): GpuCreateDeltaTableCommand40x42xBase = {
    GpuCreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      tableByPath = isByPath,
      createTableFunc = tableCreateFunc)(rapidsConf)
  }
}
