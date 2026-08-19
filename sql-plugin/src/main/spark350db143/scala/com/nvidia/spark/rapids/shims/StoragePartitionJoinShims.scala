/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

// Spark 3.5.0-db143, 4.0.x: StoragePartitionJoinParams is in datasources.v2 package
import org.apache.spark.sql.execution.datasources.v2.StoragePartitionJoinParams

/**
 * Shim for StoragePartitionJoinParams to handle package location change.
 * In Spark 3.5.0-db143 and 4.0.x, it's in org.apache.spark.sql.execution.datasources.v2
 * In Spark 4.1.0+ and 400db173, it moved to org.apache.spark.sql.execution.joins
 */
object StoragePartitionJoinShims {
  type SpjParams = StoragePartitionJoinParams
  
  def default(): SpjParams = StoragePartitionJoinParams()

  def fromBatchScan(spjParams: StoragePartitionJoinParams): SpjParams = spjParams
}
