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

package com.nvidia.spark.rapids.delta.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.BatchCDFSchemaEndVersion
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation

private[common] object DeltaCDFRelationShim {

  def changesToBatchDF(cdf: DeltaCDFRelation): DataFrame = {
    val spark = cdf.sqlContext.sparkSession
    val snapshot = cdf.snapshotWithSchemaMode.snapshot
    val readSchemaSnapshot = cdf.snapshotWithSchemaMode.schemaMode match {
      case BatchCDFSchemaEndVersion =>
        val version = cdf.endingVersion.map(snapshot.version min _).getOrElse(snapshot.version)
        snapshot.deltaLog.getSnapshotAt(version)
      case _ => snapshot
    }
    CDCReader.changesToBatchDF(
      snapshot.deltaLog,
      cdf.startingVersion.get,
      cdf.endingVersion.getOrElse(snapshot.deltaLog.update().version),
      spark,
      readSchemaSnapshot = Some(readSchemaSnapshot))
  }
}
