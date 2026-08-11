/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaErrors, DeltaOptions}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDelta
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaLog, GpuWriteIntoDelta}
import com.databricks.sql.transaction.tahoe.sources.{DeltaDataSource, DeltaSourceUtils}
import com.nvidia.spark.rapids.{GpuCreatableRelationProvider, RapidsConf}

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.BaseRelation

/** GPU version of DeltaDataSource from Delta Lake. */
class GpuDeltaDataSource(rapidsConf: RapidsConf) extends GpuCreatableRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val partitionColumns = parameters.get(DeltaSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(DeltaDataSource.decodePartitioningColumns)
        .getOrElse(Nil)

    val gpuDeltaLog = GpuDeltaLog.forTable(sqlContext.sparkSession, path, parameters, rapidsConf)
    GpuWriteIntoDelta(
      gpuDeltaLog,
      WriteIntoDelta(
        deltaLog = gpuDeltaLog.deltaLog,
        mode = mode,
        new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
        partitionColumns = partitionColumns,
        configuration = DeltaConfigs.validateConfigurations(
          parameters.filterKeys(_.startsWith("delta.")).toMap),
        data = data)).run(sqlContext.sparkSession)

    gpuDeltaLog.deltaLog.createRelation()
  }
}
