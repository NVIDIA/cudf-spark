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
{"spark":["500"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.mockito.Mockito.{doReturn, spy}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class GpuUnionExecShimSuite extends AnyFunSuite {

  private def newConf(enabled: Boolean = true): SQLConf = {
    val conf = new SQLConf()
    conf.setConf(SQLConf.UNION_OUTPUT_PARTITIONING, enabled)
    conf
  }

  private def mockPlan(
      output: Seq[Attribute],
      partitioning: Partitioning): SparkPlan = {
    val plan = spy(LocalTableScanExec(output, Nil, None))
    doReturn(partitioning).when(plan).outputPartitioning
    plan
  }

  test("SPARK-58317: PartitioningCollection child intersects to single HashPartitioning") {
    val conf = newConf(enabled = true)
    val attrA = AttributeReference("a", IntegerType)(exprId = ExprId(1))
    val attrB = AttributeReference("b", IntegerType)(exprId = ExprId(2))
    val unionAttrA = AttributeReference("a", IntegerType)(exprId = ExprId(10))
    val unionAttrB = AttributeReference("b", IntegerType)(exprId = ExprId(20))
    val unionOutput = Seq(unionAttrA, unionAttrB)

    // Child 0: inner join leg reporting PartitioningCollection(Hash(a), Hash(b))
    val child0Part = PartitioningCollection(Seq(
      HashPartitioning(Seq(attrA), 4),
      HashPartitioning(Seq(attrB), 4)
    ))
    val child0 = mockPlan(Seq(attrA, attrB), child0Part)

    // Child 1: left join leg reporting single HashPartitioning(a)
    val child1Part = HashPartitioning(Seq(attrA), 4)
    val child1 = mockPlan(Seq(attrA, attrB), child1Part)

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    // Intersects to single HashPartitioning(unionAttrA)
    val expected = HashPartitioning(Seq(unionAttrA), 4)
    assert(actual == expected)
  }

  test("SPARK-58317: all PartitioningCollection children pass through common candidates") {
    val conf = newConf(enabled = true)
    val attrA = AttributeReference("a", IntegerType)(exprId = ExprId(1))
    val attrB = AttributeReference("b", IntegerType)(exprId = ExprId(2))
    val unionAttrA = AttributeReference("a", IntegerType)(exprId = ExprId(10))
    val unionAttrB = AttributeReference("b", IntegerType)(exprId = ExprId(20))
    val unionOutput = Seq(unionAttrA, unionAttrB)

    val child0Part = PartitioningCollection(Seq(
      HashPartitioning(Seq(attrA), 4),
      HashPartitioning(Seq(attrB), 4)
    ))
    val child0 = mockPlan(Seq(attrA, attrB), child0Part)

    val child1Part = PartitioningCollection(Seq(
      HashPartitioning(Seq(attrA), 4),
      HashPartitioning(Seq(attrB), 4)
    ))
    val child1 = mockPlan(Seq(attrA, attrB), child1Part)

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    val expected = PartitioningCollection(Seq(
      HashPartitioning(Seq(unionAttrA), 4),
      HashPartitioning(Seq(unionAttrB), 4)
    ))
    assert(actual == expected)
  }

  test("SPARK-58317: disjoint partitionings reduce to UnknownPartitioning") {
    val conf = newConf(enabled = true)
    val attrA = AttributeReference("a", IntegerType)(exprId = ExprId(1))
    val attrB = AttributeReference("b", IntegerType)(exprId = ExprId(2))
    val unionAttrA = AttributeReference("a", IntegerType)(exprId = ExprId(10))
    val unionAttrB = AttributeReference("b", IntegerType)(exprId = ExprId(20))
    val unionOutput = Seq(unionAttrA, unionAttrB)

    val child0Part = PartitioningCollection(Seq(
      HashPartitioning(Seq(attrA), 4)
    ))
    val child0 = mockPlan(Seq(attrA, attrB), child0Part)

    val child1Part = HashPartitioning(Seq(attrB), 4)
    val child1 = mockPlan(Seq(attrA, attrB), child1Part)

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    assert(actual == UnknownPartitioning(0))
  }

  test("SPARK-58317: SinglePartition in PartitioningCollection intersects with SinglePartition") {
    val conf = newConf(enabled = true)
    val attrA = AttributeReference("a", IntegerType)(exprId = ExprId(1))
    val unionAttrA = AttributeReference("a", IntegerType)(exprId = ExprId(10))
    val unionOutput = Seq(unionAttrA)

    val child0Part = PartitioningCollection(Seq(
      SinglePartition,
      HashPartitioning(Seq(attrA), 1)
    ))
    val child0 = mockPlan(Seq(attrA), child0Part)

    val child1 = mockPlan(Seq(attrA), SinglePartition)

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    assert(actual == SinglePartition)
  }

  test("SPARK-58819: AttributeMap normalizes attributes differing in qualifier or nullability") {
    val conf = newConf(enabled = true)
    val unionAttrA = AttributeReference("c1", IntegerType, nullable = true)(exprId = ExprId(100))
    val unionOutput = Seq(unionAttrA)

    // Child 0 output has qualifier "t1" and nullable = false (e.g. from filter or view)
    val child0OutputAttr = AttributeReference(
      "c1",
      IntegerType,
      nullable = false
    )(exprId = ExprId(1), qualifier = Seq("t1"))

    // Child 0 partitioning references the same exprId but nullable = true and empty qualifier
    val child0PartAttr = AttributeReference(
      "c1",
      IntegerType,
      nullable = true
    )(exprId = ExprId(1), qualifier = Seq.empty)
    val child0 = mockPlan(Seq(child0OutputAttr), HashPartitioning(Seq(child0PartAttr), 4))

    // Child 1 output and partitioning agree on exprId 2
    val child1Attr = AttributeReference("c1", IntegerType, nullable = true)(exprId = ExprId(2))
    val child1 = mockPlan(Seq(child1Attr), HashPartitioning(Seq(child1Attr), 4))

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    // AttributeMap matches by exprId and maps both to unionAttrA
    val expected = HashPartitioning(Seq(unionAttrA), 4)
    assert(actual == expected)
  }

  test("Union output partitioning disabled returns UnknownPartitioning") {
    val conf = newConf(enabled = false)
    val attrA = AttributeReference("a", IntegerType)(exprId = ExprId(1))
    val unionAttrA = AttributeReference("a", IntegerType)(exprId = ExprId(10))
    val unionOutput = Seq(unionAttrA)

    val child0 = mockPlan(Seq(attrA), HashPartitioning(Seq(attrA), 4))
    val child1 = mockPlan(Seq(attrA), HashPartitioning(Seq(attrA), 4))

    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq(child0, child1),
      unionOutput,
      conf
    )

    assert(actual == UnknownPartitioning(0))
  }

  test("Empty children returns UnknownPartitioning") {
    val conf = newConf(enabled = true)
    val actual = GpuUnionExecShim.getOutputPartitioning(
      Seq.empty,
      Seq.empty,
      conf
    )
    assert(actual == UnknownPartitioning(0))
  }
}
