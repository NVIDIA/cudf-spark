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
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuScan, SparkQueryCompareTestSuite}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class GpuBatchScanExecHashSuite extends SparkQueryCompareTestSuite {
  private object TestCustomMetric extends CustomMetric {
    override def name(): String = "testCustomMetric"
    override def description(): String = "test custom metric"
    override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = taskMetrics.sum.toString
  }

  private object EmptyBatch extends Batch {
    override def planInputPartitions(): Array[InputPartition] = Array.empty
    override def createReaderFactory(): PartitionReaderFactory = null
  }

  private val scan = new GpuScan {
    override def readSchema(): StructType = new StructType()
    override def toBatch: Batch = EmptyBatch
    override def withInputFile(): GpuScan = this
    override def description(): String = "hash-test-scan"
    override def supportedCustomMetrics(): Array[CustomMetric] = Array(TestCustomMetric)
  }

  private def exec(spjParams: StoragePartitionJoinShims.SpjParams): GpuBatchScanExec = {
    GpuBatchScanExec(
      output = Nil,
      scan = scan,
      table = null,
      spjParams = spjParams)
  }

  test("hashCode includes spjParams") {
    val base = exec(StoragePartitionJoinShims.default())
    assert(base.hashCode() !=
      exec(StoragePartitionJoinShims.default().copy(replicatePartitions = true)).hashCode())
    assert(base.hashCode() !=
      exec(StoragePartitionJoinShims.default().copy(applyPartialClustering = true)).hashCode())
    assert(base.hashCode() != exec(StoragePartitionJoinShims.default().copy(
      commonPartitionValues = Some(Seq(
        (new GenericInternalRow(Array[Any](7)).asInstanceOf[InternalRow], 2))))).hashCode())
  }

  test("equal instances hash equally") {
    def params = StoragePartitionJoinShims.default().copy(
      commonPartitionValues = Some(Seq(
        (new GenericInternalRow(Array[Any](7)).asInstanceOf[InternalRow], 2))),
      applyPartialClustering = true,
      replicatePartitions = true)
    val a = exec(params)
    val b = exec(params)
    assert(a == b)
    assert(a.hashCode() == b.hashCode())
  }

  test("task custom metrics use the accumulators exposed by the plan") {
    withCpuSparkSession { _ =>
      val plan = exec(StoragePartitionJoinShims.default())
      assert(plan.scanCustomSQLMetrics(TestCustomMetric.name()) eq
        plan.metrics(TestCustomMetric.name()))
    }
  }
}
