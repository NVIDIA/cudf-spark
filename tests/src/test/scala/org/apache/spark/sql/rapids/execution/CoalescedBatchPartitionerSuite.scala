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

package org.apache.spark.sql.rapids.execution

import org.scalatest.funsuite.AnyFunSuite

class CoalescedBatchPartitionerSuite extends AnyFunSuite {

  test("CoalescedBatchPartitioner groups parent partitions by start indices") {
    // parent with 5 partitions, coalesced into [0,1] [2,3] [4]
    val parent = new BatchPartitionIdPassthrough(5)
    val part = new CoalescedBatchPartitioner(parent, Array(0, 2, 4))
    assertResult(3)(part.numPartitions)
    // BatchPartitionIdPassthrough.getPartition(k) == k, then mapped into the coalesced group
    assertResult(0)(part.getPartition(0))
    assertResult(0)(part.getPartition(1))
    assertResult(1)(part.getPartition(2))
    assertResult(1)(part.getPartition(3))
    assertResult(2)(part.getPartition(4))
  }

  test("CoalescedBatchPartitioner equals and hashCode honor the partition layout") {
    val parent = new BatchPartitionIdPassthrough(5)
    val a = new CoalescedBatchPartitioner(parent, Array(0, 2, 4))
    val b = new CoalescedBatchPartitioner(parent, Array(0, 2, 4))
    val c = new CoalescedBatchPartitioner(parent, Array(0, 3))
    assert(a == b)
    assertResult(a.hashCode)(b.hashCode)
    assert(a != c)
    // a non-partitioner falls through the `case _ => false` arm
    assert(!a.equals("not a partitioner"))
  }
}
