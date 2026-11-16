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
{"spark":["410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution.joins.StoragePartitionJoinParams

/**
 * DBR 18.3 removes KeyGroupedShuffleSpec.reducePartitionValue, so reduce keys here.
 * Other runtime shims retain their native Spark implementation.
 */
object StoragePartitionJoinShims {
  type SpjParams = StoragePartitionJoinParams

  def default(): SpjParams = StoragePartitionJoinParams()

  /**
   * Maps a scan partition key into the reduced key space that `outputPartitioning` reports when
   * the join sides use compatible but unequal partition transforms (SPARK-47094). Absent when no
   * reduction applies, in which case partition keys are already final.
   */
  def partitionValueReducer(
      spjParams: SpjParams,
      partExpressions: Seq[Expression]): Option[InternalRow => InternalRowComparableWrapper] =
    spjParams.reducers.map { reducers =>
      (row: InternalRow) => {
        // DBR 18 removes KeyGroupedShuffleSpec.reducePartitionValue. Keep the implementation
        // equivalent to Spark so partition keys are reduced into the same comparison space.
        val partitionValues = row.toSeq(partExpressions.map(_.dataType))
        val reducedValues = partitionValues.zip(reducers).map {
          case (value, Some(reducer: Reducer[Any, Any])) => reducer.reduce(value)
          case (value, _) => value
        }.toArray
        InternalRowComparableWrapper(new GenericInternalRow(reducedValues), partExpressions)
      }
    }
}
