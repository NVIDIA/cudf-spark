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

import com.nvidia.spark.rapids.{FileSystemBytesReadTracker, MetricsBatchIterator, PartitionIterator}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A replacement for DataSourceRDD that combines task-thread filesystem bytes with explicit
 * bytes reported by multithreaded GPU readers. Unlike Spark's DataSourceRDD, task-thread bytes
 * are added as deltas so metric updates do not overwrite bytes reported by worker threads.
 */
class GpuDataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Seq[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory
) extends RDD[InternalRow](sc, Nil) {
  import GpuDataSourceRDD.GpuDataSourceRDDPartition

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
    val bytesReadTracker = new FileSystemBytesReadTracker
    onTaskCompletion(context)(bytesReadTracker.update())

    val iterator = new Iterator[Object] {
      private val inputPartitions = castPartition(split).inputPartitions
      private var currentIter: Option[Iterator[Object]] = None
      private var currentIndex: Int = 0

      override def hasNext: Boolean = {
        try {
          val result = currentIter.exists(_.hasNext) || advanceToNextIter()
          if (!result) {
            bytesReadTracker.update()
          }
          result
        } catch {
          case t: Throwable =>
            bytesReadTracker.update()
            throw t
        }
      }

      override def next(): Object = {
        try {
          if (!hasNext) throw new NoSuchElementException("No more elements")
          currentIter.get.next()
        } finally {
          bytesReadTracker.update()
        }
      }

      private def advanceToNextIter(): Boolean = {
        if (currentIndex >= inputPartitions.length) {
          false
        } else {
          val inputPartition = inputPartitions(currentIndex)
          currentIndex += 1

          // TODO: SPARK-25083 remove the type erasure hack in data source scan
          val (iter, reader) = {
            val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
            val iter = new MetricsBatchIterator(
              new PartitionIterator[ColumnarBatch](batchReader))
            (iter, batchReader)
          }
          onTaskCompletion(reader.close())

          currentIter = Some(iter)
          hasNext
        }
      }
    }

    new InterruptibleIterator(context, iterator).asInstanceOf[Iterator[InternalRow]]
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
