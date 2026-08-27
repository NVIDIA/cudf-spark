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

package com.nvidia.spark.rapids

import java.io.IOException

import com.nvidia.spark.rapids.shims.GpuDataSourceRDD
import org.apache.hadoop.fs.{FileSystem, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

class PartitionReaderMetricsTestFileSystem extends RawLocalFileSystem

class PartitionReaderWithBytesReadSuite extends AnyFunSuite with MockitoSugar {

  private def withTaskContext(testBody: MockTaskContext => Unit): Unit = {
    val context = new MockTaskContext(taskAttemptId = 1L, partitionId = 0) {
      private val stableTaskMetrics = super.taskMetrics()
      override def taskMetrics() = stableTaskMetrics
    }
    TrampolineUtil.setTaskContext(context)
    try {
      testBody(context)
    } finally {
      TrampolineUtil.unsetTaskContext()
    }
  }

  @scala.annotation.nowarn("msg=method getStatistics in class FileSystem is deprecated")
  private def statistics = FileSystem.getStatistics(
    "partition-reader-metrics", classOf[PartitionReaderMetricsTestFileSystem])

  test("filesystem bytes are added only once and compose with explicit metrics") {
    withTaskContext { context =>
      val tracker = new FileSystemBytesReadTracker
      statistics.incrementBytesRead(10L)

      tracker.update()
      tracker.update()
      assert(context.taskMetrics().inputMetrics.bytesRead == 10L)

      TrampolineUtil.incBytesRead(context.taskMetrics().inputMetrics, 5L)
      statistics.incrementBytesRead(7L)
      tracker.update()
      assert(context.taskMetrics().inputMetrics.bytesRead == 22L)
    }
  }

  test("partition reader flushes bytes when next or close fails") {
    withTaskContext { context =>
      val delegate = new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = {
          statistics.incrementBytesRead(9L)
          throw new IOException("injected next failure")
        }

        override def get(): ColumnarBatch = null

        override def close(): Unit = {
          statistics.incrementBytesRead(4L)
          throw new IOException("injected close failure")
        }
      }
      val reader = new PartitionReaderWithBytesRead(delegate)

      intercept[IOException](reader.next())
      assert(context.taskMetrics().inputMetrics.bytesRead == 9L)

      intercept[IOException](reader.close())
      assert(context.taskMetrics().inputMetrics.bytesRead == 13L)
    }
  }

  test("GPU datasource RDD accounts for reader construction and preserves explicit bytes") {
    withTaskContext { context =>
      val inputPartition = new InputPartition {}
      val factory = new PartitionReaderFactory {
        override def createReader(partition: InputPartition) =
          throw new UnsupportedOperationException

        override def createColumnarReader(partition: InputPartition) = {
          statistics.incrementBytesRead(3L)
          new PartitionReader[ColumnarBatch] {
            private var hasNext = true

            override def next(): Boolean = {
              if (hasNext) {
                hasNext = false
                statistics.incrementBytesRead(7L)
                TrampolineUtil.incBytesRead(context.taskMetrics().inputMetrics, 5L)
                true
              } else {
                false
              }
            }

            override def get(): ColumnarBatch = new ColumnarBatch(Array.empty, 1)
            override def close(): Unit = {}
          }
        }

        override def supportColumnarReads(partition: InputPartition): Boolean = true
      }
      val rdd = GpuDataSourceRDD(mock[SparkContext], Seq(inputPartition), factory)
      val iterator = rdd.compute(rdd.partitions.head, context)

      assert(iterator.hasNext)
      iterator.next()
      assert(!iterator.hasNext)
      assert(context.taskMetrics().inputMetrics.bytesRead == 15L)
      context.markTaskComplete()
    }
  }

  test("GPU datasource RDD flushes bytes when a task stops before consuming a batch") {
    withTaskContext { context =>
      val inputPartition = new InputPartition {}
      val factory = new PartitionReaderFactory {
        override def createReader(partition: InputPartition) =
          throw new UnsupportedOperationException

        override def createColumnarReader(partition: InputPartition) = {
          statistics.incrementBytesRead(3L)
          new PartitionReader[ColumnarBatch] {
            override def next(): Boolean = {
              statistics.incrementBytesRead(7L)
              true
            }

            override def get(): ColumnarBatch = new ColumnarBatch(Array.empty, 1)
            override def close(): Unit = {}
          }
        }

        override def supportColumnarReads(partition: InputPartition): Boolean = true
      }
      val rdd = GpuDataSourceRDD(mock[SparkContext], Seq(inputPartition), factory)
      val iterator = rdd.compute(rdd.partitions.head, context)

      assert(iterator.hasNext)
      assert(context.taskMetrics().inputMetrics.bytesRead == 0L)
      context.markTaskComplete()
      assert(context.taskMetrics().inputMetrics.bytesRead == 10L)
    }
  }
}
