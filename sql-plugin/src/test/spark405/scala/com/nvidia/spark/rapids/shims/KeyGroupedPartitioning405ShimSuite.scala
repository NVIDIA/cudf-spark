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
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.FQSuiteName
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning

class KeyGroupedPartitioning405ShimSuite extends AnyFunSuite with FQSuiteName {
  test("project original partition values with join key positions") {
    val expressions = Seq(Literal(1), Literal(2))
    val originalPartitionValues = Seq(InternalRow(10, 20), InternalRow(30, 40))
    val partitioning = KeyGroupedPartitioning(
      expressions = expressions,
      numPartitions = 2,
      partitionValues = originalPartitionValues,
      originalPartitionValues = originalPartitionValues,
      mayContainUnknownPartitionKeys = true)
    val newPartitionValues = Seq(InternalRow(20), InternalRow(40))

    val copied = KeyGroupedPartitioningShim.copyForOutputPartitioning(
      partitioning,
      joinKeyPositions = Some(Seq(1)),
      partitionValues = newPartitionValues,
      isPartiallyClustered = false)

    assert(copied.expressions === Seq(expressions(1)))
    assert(copied.partitionValues === newPartitionValues)
    assert(copied.originalPartitionValues.map(_.getInt(0)) === Seq(20, 40))
    assert(copied.originalPartitionValues.forall(_.numFields == 1))
    assert(copied.mayContainUnknownPartitionKeys)
  }
}
