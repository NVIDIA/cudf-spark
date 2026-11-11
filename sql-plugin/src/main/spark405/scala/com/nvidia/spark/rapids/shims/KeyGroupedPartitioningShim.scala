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

/*** spark-rapids-shim-json-lines
{"spark":["405"],"carryForward":"sameMinor"}
{"spark":["414"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning

object KeyGroupedPartitioningShim {
  def copyWithNewPartitionValues(
      p: KeyGroupedPartitioning,
      partitionValues: Seq[InternalRow],
      isPartiallyClustered: Boolean): KeyGroupedPartitioning = {
    p.copy(
      numPartitions = partitionValues.length,
      partitionValues = partitionValues,
      isPartiallyClustered = isPartiallyClustered)
  }

  def copyForOutputPartitioning(
      p: KeyGroupedPartitioning,
      joinKeyPositions: Option[Seq[Int]],
      partitionValues: Seq[InternalRow],
      isPartiallyClustered: Boolean): KeyGroupedPartitioning = {
    val expressions = joinKeyPositions
      .map(_.map(p.expressions(_)))
      .getOrElse(p.expressions)
    val originalPartitionValues = joinKeyPositions match {
      case Some(projectionPositions) =>
        p.originalPartitionValues.map(
          KeyGroupedPartitioning.project(p.expressions, projectionPositions, _))
      case None => p.originalPartitionValues
    }
    p.copy(
      expressions = expressions,
      numPartitions = partitionValues.length,
      partitionValues = partitionValues,
      originalPartitionValues = originalPartitionValues,
      isPartiallyClustered = isPartiallyClustered)
  }

  def getUniquePartitions(p: KeyGroupedPartitioning): Seq[InternalRow] = {
    p.uniquePartitionValues
  }
}
