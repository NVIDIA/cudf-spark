/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import java.util.concurrent.ConcurrentHashMap

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.FileSystemBytesReadTracker
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

private[rapids] trait GpuDataSourceCustomMetrics extends Serializable {
  def readerOpened(reader: PartitionReader[ColumnarBatch]): Unit

  def readerProgress(reader: PartitionReader[ColumnarBatch]): Unit

  def readerFinished(reader: PartitionReader[ColumnarBatch]): Unit
}

private[rapids] abstract class GpuDataSourceCustomMetricsFactory extends Serializable {
  def create(): GpuDataSourceCustomMetrics
}

private object NoopGpuDataSourceCustomMetrics extends GpuDataSourceCustomMetrics {
  override def readerOpened(reader: PartitionReader[ColumnarBatch]): Unit = {}

  override def readerProgress(reader: PartitionReader[ColumnarBatch]): Unit = {}

  override def readerFinished(reader: PartitionReader[ColumnarBatch]): Unit = {}
}

private object NoopGpuDataSourceCustomMetricsFactory
    extends GpuDataSourceCustomMetricsFactory {
  override def create(): GpuDataSourceCustomMetrics = NoopGpuDataSourceCustomMetrics
}

/**
 * A replacement for DataSourceRDD that combines task-thread filesystem bytes with explicit
 * bytes reported by multithreaded GPU readers. Unlike Spark's DataSourceRDD, task-thread bytes
 * are added as deltas so metric updates do not overwrite bytes reported by worker threads.
 */
class GpuDataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Seq[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    customMetricsFactory: GpuDataSourceCustomMetricsFactory =
      NoopGpuDataSourceCustomMetricsFactory
) extends RDD[InternalRow](sc, Nil) {
  import GpuDataSourceRDD.GpuDataSourceRDDPartition

  @transient private lazy val customMetricsByTask =
    new ConcurrentHashMap[Long, GpuDataSourceCustomMetrics]()

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map { case (parts, index) =>
      GpuDataSourceRDDPartition(index, parts)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartitions.flatMap(_.preferredLocations()).distinct
  }

  private def castPartition(split: Partition): GpuDataSourceRDDPartition = split match {
    case p: GpuDataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a GpuDataSourceRDDPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bytesReadTracker = FileSystemBytesReadTracker.forTask(context)
    val customMetrics = customMetricsForTask(context)

    val iterator = new Iterator[Object] {
      private val inputPartitions = castPartition(split).inputPartitions
      private var currentIter: Option[Iterator[Object]] = None
      private var currentIndex: Int = 0

      override def hasNext: Boolean = {
        val result = currentIter.exists(_.hasNext) || advanceToNextIter()
        if (!result) {
          bytesReadTracker.update()
        }
        result
      }

      override def next(): Object = {
        if (!hasNext) {
          throw new NoSuchElementException("No more elements")
        }
        currentIter.get.next()
      }

      private def advanceToNextIter(): Boolean = {
        if (currentIndex >= inputPartitions.length) {
          false
        } else {
          val inputPartition = inputPartitions(currentIndex)
          currentIndex += 1

          // TODO: SPARK-25083 remove the type erasure hack in data source scan
          val reader = partitionReaderFactory.createColumnarReader(inputPartition)
          currentIter = Some(new ReaderIterator(reader, context, bytesReadTracker, customMetrics))
          hasNext
        }
      }
    }

    new InterruptibleIterator(context, iterator).asInstanceOf[Iterator[InternalRow]]
  }

  private def customMetricsForTask(context: TaskContext): GpuDataSourceCustomMetrics = {
    val taskId = context.taskAttemptId()
    val existing = customMetricsByTask.get(taskId)
    if (existing != null) {
      existing
    } else {
      val created = customMetricsFactory.create()
      val raced = customMetricsByTask.putIfAbsent(taskId, created)
      if (raced != null) {
        raced
      } else {
        onTaskCompletion {
          customMetricsByTask.remove(taskId, created)
          ()
        }
        created
      }
    }
  }

  private class ReaderIterator(
      reader: PartitionReader[ColumnarBatch],
      context: TaskContext,
      bytesReadTracker: FileSystemBytesReadTracker,
      customMetrics: GpuDataSourceCustomMetrics) extends Iterator[Object] {
    private var valuePrepared = false
    private var hasMoreInput = true
    private var closed = false

    try {
      customMetrics.readerOpened(reader)
    } catch {
      case t: Throwable =>
        try {
          reader.close()
        } catch {
          case closeError: Throwable => t.addSuppressed(closeError)
        }
        throw t
    }

    onTaskCompletion {
      finish()
    }

    override def hasNext: Boolean = {
      if (!valuePrepared && hasMoreInput) {
        try {
          hasMoreInput = reader.next()
          if (!hasMoreInput) {
            finish()
          }
          valuePrepared = hasMoreInput
        } catch {
          case t: Throwable =>
            finishOnError(t)
            throw t
        }
      }
      valuePrepared
    }

    override def next(): Object = {
      if (!hasNext) {
        throw new NoSuchElementException("No more elements")
      }
      valuePrepared = false
      try {
        closeOnExcept(reader.get()) { batch =>
          TrampolineUtil.incInputRecordsRows(context.taskMetrics().inputMetrics, batch.numRows())
          customMetrics.readerProgress(reader)
          batch
        }
      } catch {
        case t: Throwable =>
          finishOnError(t)
          throw t
      } finally {
        bytesReadTracker.update()
      }
    }

    private def finishOnError(original: Throwable): Unit = {
      try {
        finish()
      } catch {
        case finishError: Throwable => original.addSuppressed(finishError)
      }
    }

    private def finish(): Unit = {
      if (!closed) {
        closed = true
        try {
          customMetrics.readerFinished(reader)
        } finally {
          try {
            reader.close()
          } finally {
            bytesReadTracker.update()
          }
        }
      }
    }
  }
}

object GpuDataSourceRDD {
  private case class GpuDataSourceRDDPartition(
      override val index: Int,
      inputPartitions: Seq[InputPartition]) extends Partition

  def apply(
      sc: SparkContext,
      inputPartitions: Seq[InputPartition],
      partitionReaderFactory: PartitionReaderFactory): GpuDataSourceRDD = {
    new GpuDataSourceRDD(sc, inputPartitions.map(Seq(_)), partitionReaderFactory)
  }
}
