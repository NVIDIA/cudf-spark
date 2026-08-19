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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.GpuDeltaCatalogBase

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.TableCreationModes

/**
 * Shared GPU Delta catalog base for the Delta 4.0 and 4.1 shims.
 *
 * The shared catalog logic is identical across these runtimes; version-specific
 * subclasses still own their `GpuCreateDeltaTableCommand` wiring and any Delta
 * runtime field lookup that needs to stay shim-specific.
 */
abstract class GpuDeltaCatalog4x(
    cpuCatalog: DeltaCatalog,
    rapidsConf: RapidsConf)
  extends GpuDeltaCatalogBase(cpuCatalog, rapidsConf) {

  protected def buildGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDelta],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[CatalogTable => Unit]): GpuCreateDeltaTableCommand40x41xBase

  override protected def createGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDelta],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[CatalogTable => Unit]): Unit = {
    buildGpuCreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      mode,
      writer,
      operation,
      isByPath,
      tableCreateFunc).run(spark)
  }
}
