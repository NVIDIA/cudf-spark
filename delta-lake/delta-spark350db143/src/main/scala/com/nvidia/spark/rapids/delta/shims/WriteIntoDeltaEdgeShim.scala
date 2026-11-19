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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOptions}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaEdge

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType

object WriteIntoDeltaEdgeShim {
  def create(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      schemaInCatalogOverride: Option[Option[StructType]] = None): WriteIntoDeltaEdge = {
    schemaInCatalogOverride match {
      case Some(schema) =>
        WriteIntoDeltaEdge(deltaLog, mode, options, partitionColumns, configuration, data,
          schemaInCatalog = schema)
      case None =>
        WriteIntoDeltaEdge(deltaLog, mode, options, partitionColumns, configuration, data)
    }
  }
}
