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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuScan, SparkQueryCompareTestSuite}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class GpuBatchScanExecMetricsSuite extends SparkQueryCompareTestSuite with MockitoSugar {
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
    override def description(): String = "metrics-test-scan"
    override def supportedCustomMetrics(): Array[CustomMetric] = Array(TestCustomMetric)
  }

  test("task custom metrics use the accumulators exposed by the plan") {
    withCpuSparkSession { _ =>
      val plan = GpuBatchScanExec(
        output = Nil,
        scan = scan,
        table = mock[Table])
      assert(plan.scanCustomSQLMetrics(TestCustomMetric.name()) eq
        plan.metrics(TestCustomMetric.name()))
    }
  }
}
