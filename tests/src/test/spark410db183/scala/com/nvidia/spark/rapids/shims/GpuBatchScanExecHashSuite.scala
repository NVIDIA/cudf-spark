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

import com.nvidia.spark.rapids.{GpuScan, SparkQueryCompareTestSuite}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader,
  PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuBatchScanExecHashSuite extends SparkQueryCompareTestSuite {
  import GpuBatchScanExecHashSuite._

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

  private def exec(
      keyGroupedPartitioning: Option[Seq[Expression]] = None,
      dataFilters: Seq[Expression] = Nil): GpuBatchScanExec = {
    GpuBatchScanExec(
      output = Nil,
      scan = scan,
      table = null,
      keyGroupedPartitioning = keyGroupedPartitioning,
      dataFilters = dataFilters)
  }

  test("hashCode includes key-grouped partitioning and data filters") {
    val base = exec()
    assert(base.hashCode() !=
      exec(keyGroupedPartitioning = Some(Seq(Literal(1)))).hashCode())
    assert(base.hashCode() != exec(dataFilters = Seq(Literal.TrueLiteral)).hashCode())
  }

  test("equal instances hash equally") {
    def plan = exec(
      keyGroupedPartitioning = Some(Seq(Literal(1))),
      dataFilters = Seq(Literal.TrueLiteral))
    val a = plan
    val b = plan
    assert(a == b)
    assert(a.hashCode() == b.hashCode())
  }

  test("task custom metrics use the accumulators exposed by the plan") {
    withCpuSparkSession { _ =>
      val metricScan = new GpuScan {
        override def readSchema(): StructType = new StructType()
        override def toBatch: Batch = new Batch {
          override def planInputPartitions(): Array[InputPartition] = Array(MetricPartition)
          override def createReaderFactory(): PartitionReaderFactory = MetricReaderFactory
        }
        override def withInputFile(): GpuScan = this
        override def description(): String = "metric-test-scan"
        override def supportedCustomMetrics(): Array[CustomMetric] = Array(TestCustomMetric)
      }
      val plan = GpuBatchScanExec(output = Nil, scan = metricScan, table = null)
      assert(plan.scanCustomSQLMetrics(TestCustomMetric.name()) eq
        plan.metrics(TestCustomMetric.name()))
      plan.inputRDD.asInstanceOf[org.apache.spark.rdd.RDD[ColumnarBatch]]
        .foreachPartition(batches => batches.foreach(_.close()))
      assert(plan.metrics(TestCustomMetric.name()).value == 7L)
    }
  }
}

object GpuBatchScanExecHashSuite {
  private case object MetricPartition extends InputPartition

  private object MetricReaderFactory extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
      throw new UnsupportedOperationException("Only columnar reads are supported")

    override def supportColumnarReads(partition: InputPartition): Boolean = true

    override def createColumnarReader(
        partition: InputPartition): PartitionReader[ColumnarBatch] = {
      new PartitionReader[ColumnarBatch] {
        private var emitted = false
        private var metricValue = 0L

        override def next(): Boolean = {
          if (emitted) {
            false
          } else {
            emitted = true
            true
          }
        }

        override def get(): ColumnarBatch = {
          metricValue = 7L
          new ColumnarBatch(Array.empty[ColumnVector], 1)
        }

        override def currentMetricsValues(): Array[CustomTaskMetric] = {
          Array(new CustomTaskMetric {
            override def name(): String = "testCustomMetric"
            override def value(): Long = metricValue
          })
        }

        override def close(): Unit = {}
      }
    }
  }
}
