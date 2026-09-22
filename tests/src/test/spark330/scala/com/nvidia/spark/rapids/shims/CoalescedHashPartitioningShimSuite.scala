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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.types.IntegerType

class CoalescedHashPartitioningShimSuite extends AnyFunSuite {
  test("legacy Spark versions report the coalesced partition count") {
    val hashPartitioning = HashPartitioning(
      Seq(AttributeReference("key", IntegerType)()), 8)
    val specs = Seq(
      CoalescedPartitionSpec(0, 3, None),
      CoalescedPartitionSpec(3, 8, None))

    val result = CoalescedHashPartitioningShim(hashPartitioning, specs)

    assert(result == hashPartitioning.copy(numPartitions = 2))
  }
}
