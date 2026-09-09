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

import com.nvidia.spark.rapids.{GpuScan, SparkQueryCompareTestSuite}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
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
}
