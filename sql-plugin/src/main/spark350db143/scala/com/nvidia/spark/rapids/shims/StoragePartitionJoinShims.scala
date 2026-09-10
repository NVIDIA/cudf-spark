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
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
// Spark 3.5.0-db143: StoragePartitionJoinParams is in the datasources.v2 package
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

  /**
   * Reducible partition transforms (SPARK-47094) landed in Spark 4.0, so this build carries no
   * reducers and scan partition keys are already final.
   */
  def partitionValueReducer(
      spjParams: SpjParams,
      partExpressions: Seq[Expression]): Option[InternalRow => InternalRowComparableWrapper] = None
}
