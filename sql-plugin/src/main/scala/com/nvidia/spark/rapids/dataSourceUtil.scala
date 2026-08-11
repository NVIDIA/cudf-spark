/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.TaskContext
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

class PartitionIterator[T](reader: PartitionReader[T]) extends Iterator[T] {
  private[this] var valuePrepared = false

  override def hasNext: Boolean = {
    if (!valuePrepared) {
      valuePrepared = reader.next()
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    valuePrepared = false
    reader.get()
  }
}

class MetricsBatchIterator(iter: Iterator[ColumnarBatch]) extends Iterator[ColumnarBatch] {
  private[this] val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    val batch = iter.next()
    TrampolineUtil.incInputRecordsRows(inputMetrics, batch.numRows())
    batch
  }
}

/** Wraps a columnar PartitionReader to update bytes read metric based on filesystem statistics. */
class PartitionReaderWithBytesRead(reader: PartitionReader[ColumnarBatch])
    extends PartitionReader[ColumnarBatch] {
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics
  private[this] val getBytesRead = TrampolineUtil.getFSBytesReadOnThreadCallback()

  override def next(): Boolean = {
    val result = reader.next()
    TrampolineUtil.incBytesRead(inputMetrics, getBytesRead())
    result
  }

  override def get(): ColumnarBatch = reader.get()

  override def close(): Unit = reader.close()
}
