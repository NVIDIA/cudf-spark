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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Collections

import com.nvidia.spark.rapids.{GpuScan, SparkQueryCompareTestSuite}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition,
    PartitionReaderFactory, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class GpuBatchScanExecCanonicalizeSuite extends SparkQueryCompareTestSuite with MockitoSugar {
  private object EmptyBatch extends Batch {
    override def planInputPartitions(): Array[InputPartition] = Array.empty
    override def createReaderFactory(): PartitionReaderFactory = null
  }

  private val scan = new GpuScan {
    override def readSchema(): StructType = new StructType()
    override def toBatch: Batch = EmptyBatch
    override def withInputFile(): GpuScan = this
    override def description(): String = "canonicalize-test-scan"
  }

  private val id = AttributeReference("id", IntegerType)()
  private val data = AttributeReference("data", StringType)()
  private val extra = AttributeReference("extra", IntegerType)()
  private val storeId = AttributeReference("store_id", IntegerType)()
  private val deptId = AttributeReference("dept_id", IntegerType)()
  private val payload = AttributeReference("data", IntegerType)()

  private def exec(keys: Option[Seq[AttributeReference]]): GpuBatchScanExec = {
    GpuBatchScanExec(
      output = Seq(id, data),
      scan = scan,
      table = mock[Table],
      keyGroupedPartitioning = keys)
  }

  test("equals and hashCode ignore partition keys pruned out of output") {
    withCpuSparkSession { _ =>
      val withDangling = exec(Some(Seq(id, extra)))
      val pruned = exec(Some(Seq(id)))
      assert(withDangling.prunedKeyGroupedPartitioning == pruned.prunedKeyGroupedPartitioning)
      assert(withDangling == pruned)
      assert(withDangling.hashCode() == pruned.hashCode())
      // sameResult goes through SparkPlan.canonicalized, the AQE reuse / reuse-exchange path.
      assert(withDangling.sameResult(pruned))
    }
  }

  test("sameResult holds for equivalent scans that differ only by ExprId") {
    withCpuSparkSession { _ =>
      val idB = AttributeReference("id", IntegerType)()
      val extraB = AttributeReference("extra", IntegerType)()
      val dataB = AttributeReference("data", StringType)()
      val left = GpuBatchScanExec(
        output = Seq(id, data),
        scan = scan,
        table = mock[Table],
        keyGroupedPartitioning = Some(Seq(id, extra)))
      val right = GpuBatchScanExec(
        output = Seq(idB, dataB),
        scan = scan,
        table = mock[Table],
        keyGroupedPartitioning = Some(Seq(idB, extraB)))
      assert(left.sameResult(right),
        "Canonicalized GPU scans with different ExprIds should still sameResult")
      assert(left.doCanonicalize().sameResult(right.doCanonicalize()))
    }
  }

  test("doCanonicalize drops dangling keys and preserves remaining key order") {
    withCpuSparkSession { _ =>
      val plan = exec(Some(Seq(id, data, extra)))
      val canonical = plan.doCanonicalize()
      val keys = canonical.keyGroupedPartitioning.get
      assert(keys.map(_.dataType) == Seq(IntegerType, StringType))
    }
  }

  // SPARK-59248: join/filter key is a non-leading subset of the partition keys, and the leading
  // key has been pruned from scan output. replanWithRuntimeFilters must still see the full-width
  // reported keys; pruned or empty keys misalign HasPartitionKey rows.
  test("runtime-filter replan uses full-width keys after pruning a leading partition key") {
    val v2Conf = new SparkConf().set(SQLConf.V2_BUCKETING_ENABLED.key, "true")
    withCpuSparkSession({ _ =>
      val keyedScan = new KeyedRuntimeFilterGpuScan
      val table = mock[Table]
      when(table.name()).thenReturn("prune_lead_t")
      when(table.columns()).thenReturn(Array(
        Column.create("store_id", IntegerType),
        Column.create("dept_id", IntegerType),
        Column.create("data", IntegerType)))
      when(table.partitioning()).thenReturn(Array(
        Expressions.identity("store_id"),
        Expressions.identity("dept_id")))
      when(table.capabilities()).thenReturn(Collections.emptySet[TableCapability]())

      val plan = GpuBatchScanExec(
        // Leading store_id is absent from output, matching SPARK-59248 column pruning.
        output = Seq(deptId, payload),
        scan = keyedScan,
        runtimeFilters = Seq(EqualTo(deptId, Literal(10))),
        table = table,
        keyGroupedPartitioning = Some(Seq(storeId, deptId)))

      assert(plan.keyGroupedPartitioning.get.map(_.asInstanceOf[AttributeReference].name) ==
        Seq("store_id", "dept_id"))
      assert(!plan.output.exists(_.name == "store_id"))
      plan.outputPartitioning match {
        case k: KeyedPartitioning =>
          assert(k.expressions.size == 1,
            s"Planner view should drop the pruned leading key, found ${k.expressions}")
        case other =>
          fail(s"Expected a projected KeyedPartitioning, found $other")
      }

      val filtered = plan.filteredPartitions
      assert(filtered.exists(_.isDefined),
        "Expected at least one remaining keyed partition after the runtime filter")
      assert(filtered.exists(_.isEmpty),
        "Expected filtered-out keys to keep their original slots as None")
      val keptKeys = filtered.flatten.map(_.asInstanceOf[HasPartitionKey].partitionKey())
      assert(keptKeys.forall(_.getInt(1) == 10),
        s"Runtime filter should keep only dept_id=10 keys, found $keptKeys")
      assert(keptKeys.forall(row => row.numFields == 2),
        "HasPartitionKey rows stay full-width even after the leading output column is pruned")
    }, v2Conf)
  }

  private class KeyedInputPartition(key: InternalRow)
      extends InputPartition with HasPartitionKey {
    override def partitionKey(): InternalRow = key
  }

  private class KeyedRuntimeFilterGpuScan extends GpuScan with SupportsRuntimeV2Filtering {
    private var parts: Array[InputPartition] = Array(
      new KeyedInputPartition(InternalRow(1, 10)),
      new KeyedInputPartition(InternalRow(1, 20)),
      new KeyedInputPartition(InternalRow(2, 5)),
      new KeyedInputPartition(InternalRow(2, 10)))

    override def readSchema(): StructType =
      new StructType()
        .add("dept_id", IntegerType)
        .add("data", IntegerType)

    override def toBatch: Batch = new Batch {
      override def planInputPartitions(): Array[InputPartition] = parts
      override def createReaderFactory(): PartitionReaderFactory = null
    }

    override def withInputFile(): GpuScan = this
    override def description(): String = "keyed-runtime-filter-scan"

    override def filterAttributes(): Array[NamedReference] =
      Array(Expressions.column("dept_id"))

    override def filter(predicates: Array[Predicate]): Unit = {
      parts = parts.filter { p =>
        p.asInstanceOf[HasPartitionKey].partitionKey().getInt(1) == 10
      }
    }
  }
}
