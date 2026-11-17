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
{"spark":["410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Collections

import com.nvidia.spark.rapids.{FQSuiteName, GpuScan}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression,
  Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition,
  PartitionReader, PartitionReaderFactory, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.execution.{BaseSubqueryExec, InSubqueryExec}
import org.apache.spark.sql.execution.datasources.rapids.DataSourceStrategyUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class GpuBatchScanExecSuite extends AnyFunSuite with BeforeAndAfterAll with FQSuiteName {
  private val testSchema = StructType(Seq(StructField("k", IntegerType, nullable = false)))
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("GpuBatchScanExecSuite")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private case class TestPartition(key: Int, id: String)
      extends InputPartition with HasPartitionKey {
    override def partitionKey(): InternalRow = InternalRow(key)
    override def preferredLocations(): Array[String] = Array(id)
  }

  private object NoopReaderFactory extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
      throw new UnsupportedOperationException("read is not used by this test")
  }

  private object TestTable extends Table {
    override def name(): String = "test table"
    override def schema(): StructType = testSchema
    override def capabilities(): java.util.Set[TableCapability] =
      Collections.singleton(TableCapability.BATCH_READ)
  }

  private class FilteringGpuScan(
      initial: Seq[InputPartition],
      afterFilter: Seq[InputPartition])
      extends GpuScan with SupportsRuntimeV2Filtering {
    private var current = initial
    var filterCalls: Int = 0

    override def readSchema(): StructType = testSchema

    override def toBatch: Batch = new Batch {
      override def planInputPartitions(): Array[InputPartition] = current.toArray
      override def createReaderFactory(): PartitionReaderFactory = NoopReaderFactory
    }

    override def filterAttributes(): Array[NamedReference] = Array.empty

    override def filter(predicates: Array[Predicate]): Unit = {
      filterCalls += 1
      current = afterFilter
    }

    override def withInputFile(): GpuScan = this
  }

  test("data filters participate in plan identity, canonicalization, and display") {
    val scan = mock[GpuScan]
    val batch = mock[Batch]
    val table = mock[Table]
    when(scan.toBatch).thenReturn(batch)
    when(scan.description()).thenReturn("mock scan")
    when(table.name()).thenReturn("mock table")

    val filterOne = Literal(1)
    val filterTwo = Literal(2)
    val withFilterOne = GpuBatchScanExec(
      output = Seq.empty,
      scan = scan,
      table = table,
      dataFilters = Seq(filterOne))
    val withFilterTwo = withFilterOne.copy(dataFilters = Seq(filterTwo))

    assert(withFilterOne !== withFilterTwo)
    assert(withFilterOne.doCanonicalize().dataFilters === Seq(filterOne))
    assert(withFilterOne.simpleString(10).contains("DataFilters: [1]"))
  }

  test("keyed partitions preserve multiplicity, ordering, and runtime-filter padding") {
    val key = AttributeReference("k", IntegerType, nullable = false)()
    val initial: Seq[InputPartition] = Seq(
      TestPartition(2, "two"),
      TestPartition(1, "one-a"),
      TestPartition(1, "one-b"),
      TestPartition(3, "three"))
    val filtered: Seq[InputPartition] = Seq(
      TestPartition(3, "three"),
      TestPartition(1, "one-b"))
    val scan = new FilteringGpuScan(initial, filtered)

    val runtimeFilter = InSubqueryExec(
      child = key,
      plan = mock[BaseSubqueryExec],
      exprId = NamedExpression.newExprId,
      result = Array[Any](1, 3))
    assert(runtimeFilter.values.isDefined)
    assert(DataSourceStrategyUtils.translateRuntimeFilter(runtimeFilter).isDefined)

    val exec = GpuBatchScanExec(
      output = Seq(key),
      scan = scan,
      runtimeFilters = Seq(DynamicPruningExpression(runtimeFilter)),
      ordering = None,
      table = TestTable,
      keyGroupedPartitioning = Some(Seq(key)))

    val output = exec.outputPartitioning.asInstanceOf[KeyedPartitioning]
    val keyCounts = output.partitionKeys.map(_.row.getInt(0))
      .groupMapReduce(identity)(_ => 1)(_ + _)
    assert(keyCounts === Map(1 -> 2, 2 -> 1, 3 -> 1))

    val rdd = exec.inputRDD
    assert(rdd.isInstanceOf[GpuDataSourceRDD])
    val actual = rdd.partitions.toSeq.map(rdd.preferredLocations)
    assert(scan.filterCalls === 1)
    assert(actual === Seq(Seq("one-b"), Seq.empty, Seq.empty, Seq("three")))
  }
}
