/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOptions}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaEdge
import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.types.StructType

class GpuDeltaCatalog(
    cpuCatalog: StagingTableCatalog,
    rapidsConf: RapidsConf)
  extends GpuDeltaCatalogCommon(cpuCatalog, rapidsConf) {

  override protected def getWriter(
      sourceQuery: Option[DataFrame],
      path: Path,
      comment: Option[String],
      schema: Option[StructType],
      saveMode: SaveMode,
      catalogTable: CatalogTable): Option[LogicalPlan] = {
    sourceQuery.map { df =>
      WriteIntoDeltaEdge(
        DeltaLog.forTable(spark, path),
        saveMode,
        new DeltaOptions(catalogTable.storage.properties, spark.sessionState.conf),
        catalogTable.partitionColumnNames,
        catalogTable.properties ++ comment.map("comment" -> _),
        df,
        schemaInCatalog = schema)
    }
  }
}
