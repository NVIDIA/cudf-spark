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

import java.io.FileNotFoundException
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong

import com.nvidia.spark.rapids.shims.{GpuDataSourceCustomMetrics,
  GpuDataSourceCustomMetricsFactory, GpuDataSourceRDD}
import org.apache.hadoop.fs.{FileSystem, RawLocalFileSystem}
import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

class PartitionReaderMetricsTestFileSystem extends RawLocalFileSystem

object FileSystemBytesReadTrackerSuite {
  private val nextTaskAttemptId = new AtomicLong(1L)
}

class FileSystemBytesReadTrackerSuite extends AnyFunSuite with MockitoSugar {
  import FileSystemBytesReadTrackerSuite.nextTaskAttemptId

  private class TestTaskContext(taskAttemptId: Long)
      extends MockTaskContext(taskAttemptId, partitionId = 0) {
    private var completed = false

    override def isCompleted(): Boolean = completed

    override def markTaskComplete(): Unit = {
      if (!completed) {
        completed = true
        super.markTaskComplete()
      }
    }
  }

  private def withTaskContext(testBody: MockTaskContext => Unit): Unit = {
    val context = new TestTaskContext(nextTaskAttemptId.getAndIncrement())
    TrampolineUtil.setTaskContext(context)
    try {
      testBody(context)
    } finally {
      try {
        context.markTaskComplete()
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  @scala.annotation.nowarn("msg=method getStatistics in class FileSystem is deprecated")
  private def statistics = FileSystem.getStatistics(
    "partition-reader-metrics", classOf[PartitionReaderMetricsTestFileSystem])

  test("filesystem bytes are added only once and compose with explicit metrics") {
    withTaskContext { context =>
      val tracker = FileSystemBytesReadTracker.forTask(context)
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
      val rdd = GpuDataSourceRDD(
        mock[SparkContext], Seq(inputPartition), factory, includeRefreshHint = false)
      val iterator = rdd.compute(rdd.partitions.head, context)

      assert(iterator.hasNext)
      iterator.next()
      assert(!iterator.hasNext)
      assert(context.taskMetrics().inputMetrics.bytesRead == 15L)
      context.markTaskComplete()
    }
  }

  private val missingFilePath = "file:/missing%20ORC.orc"

  Seq(
    ("direct V2", false,
      () => GpuFileNotFoundException(
        missingFilePath, new FileNotFoundException("missing ORC file"))),
    ("wrapped V1", true,
      () => new ExecutionException(GpuFileNotFoundException(
        missingFilePath, new FileNotFoundException("missing ORC file"))))
  ).foreach { case (name, includeRefreshHint, failure) =>
    test(s"GPU datasource RDD enriches next() missing-file failures - $name") {
      withTaskContext { context =>
        val inputPartition = new InputPartition {}
        val factory = new PartitionReaderFactory {
          override def createReader(partition: InputPartition) =
            throw new UnsupportedOperationException

          override def createColumnarReader(partition: InputPartition) =
            new PartitionReader[ColumnarBatch] {
              private var hasNext = true

              override def next(): Boolean = {
                if (hasNext) {
                  hasNext = false
                  true
                } else {
                  false
                }
              }

              override def get(): ColumnarBatch = {
                statistics.incrementBytesRead(7L)
                throw failure()
              }

              override def close(): Unit = {}
            }

          override def supportColumnarReads(partition: InputPartition): Boolean = true
        }
        val rdd = GpuDataSourceRDD(
          mock[SparkContext], Seq(inputPartition), factory,
          includeRefreshHint = includeRefreshHint)
        val iterator = rdd.compute(rdd.partitions.head, context)

        assert(iterator.hasNext)
        val error = intercept[Exception](iterator.next())
        if (VersionUtils.isSpark400OrLater) {
          val sparkError = error.asInstanceOf[SparkException]
          val condition = sparkError.getClass.getMethod("getCondition").invoke(sparkError)
          val parameters = sparkError.getClass.getMethod("getMessageParameters")
            .invoke(sparkError).asInstanceOf[java.util.Map[String, String]]
          assert(condition === "FAILED_READ_FILE.FILE_NOT_EXIST")
          assert(parameters.get("path") === missingFilePath)
        } else {
          val fileError = error.asInstanceOf[FileNotFoundException]
          assert(fileError.getMessage.contains("recreating the Dataset/DataFrame involved"))
          assert(fileError.getMessage.contains("REFRESH TABLE") === includeRefreshHint)
        }
        assert(error.getCause.isInstanceOf[FileNotFoundException])
        assert(context.taskMetrics().inputMetrics.bytesRead == 7L)
        context.markTaskComplete()
      }
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
      val rdd = GpuDataSourceRDD(
        mock[SparkContext], Seq(inputPartition), factory, includeRefreshHint = false)
      val iterator = rdd.compute(rdd.partitions.head, context)

      assert(iterator.hasNext)
      assert(context.taskMetrics().inputMetrics.bytesRead == 0L)
      context.markTaskComplete()
      assert(context.taskMetrics().inputMetrics.bytesRead == 10L)
    }
  }

  test("GPU datasource RDD closes a batch when custom metric updates fail") {
    withTaskContext { context =>
      val inputPartition = new InputPartition {}
      val reader = mock[PartitionReader[ColumnarBatch]]
      val batch = mock[ColumnarBatch]
      when(reader.next()).thenReturn(true)
      when(reader.get()).thenReturn(batch)
      when(batch.numRows()).thenReturn(1)

      val readerFactory = new PartitionReaderFactory {
        override def createReader(partition: InputPartition) =
          throw new UnsupportedOperationException

        override def createColumnarReader(partition: InputPartition) = reader

        override def supportColumnarReads(partition: InputPartition): Boolean = true
      }
      val customMetricsFactory = new GpuDataSourceCustomMetricsFactory {
        override def create(): GpuDataSourceCustomMetrics = new GpuDataSourceCustomMetrics {
          override def readerOpened(reader: PartitionReader[ColumnarBatch]): Unit = {}

          override def readerProgress(reader: PartitionReader[ColumnarBatch]): Unit = {
            throw new IllegalStateException("metric update failed")
          }

          override def readerFinished(reader: PartitionReader[ColumnarBatch]): Unit = {}
        }
      }
      val rdd = new GpuDataSourceRDD(
        mock[SparkContext], Seq(Seq(inputPartition)), readerFactory,
        includeRefreshHint = false, customMetricsFactory = customMetricsFactory)
      val iterator = rdd.compute(rdd.partitions.head, context)

      assert(iterator.hasNext)
      val error = intercept[IllegalStateException](iterator.next())
      assert(error.getMessage == "metric update failed")
      verify(batch).close()
      verify(reader).close()
      context.markTaskComplete()
    }
  }

  test("GPU datasource RDD shares filesystem accounting across compute calls") {
    withTaskContext { context =>
      def newRdd(bytesRead: Long) = {
        val inputPartition = new InputPartition {}
        val factory = new PartitionReaderFactory {
          override def createReader(partition: InputPartition) =
            throw new UnsupportedOperationException

          override def createColumnarReader(partition: InputPartition) =
            new PartitionReader[ColumnarBatch] {
              private var hasNext = true

              override def next(): Boolean = {
                if (hasNext) {
                  hasNext = false
                  statistics.incrementBytesRead(bytesRead)
                  true
                } else {
                  false
                }
              }

              override def get(): ColumnarBatch = new ColumnarBatch(Array.empty, 1)
              override def close(): Unit = {}
            }

          override def supportColumnarReads(partition: InputPartition): Boolean = true
        }
        GpuDataSourceRDD(
          mock[SparkContext], Seq(inputPartition), factory, includeRefreshHint = false)
      }

      val firstRdd = newRdd(10L)
      val secondRdd = newRdd(20L)
      val first = firstRdd.compute(firstRdd.partitions.head, context)
      val second = secondRdd.compute(secondRdd.partitions.head, context)

      assert(first.hasNext)
      assert(second.hasNext)
      assert(context.taskMetrics().inputMetrics.bytesRead == 0L)
      first.next()
      assert(context.taskMetrics().inputMetrics.bytesRead == 30L)
      second.next()
      assert(context.taskMetrics().inputMetrics.bytesRead == 30L)

      context.markTaskComplete()
      assert(context.taskMetrics().inputMetrics.bytesRead == 30L)
    }
  }
}
