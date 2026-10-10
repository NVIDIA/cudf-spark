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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An abstract columnar batch iterator that gives options for auto closing
 * when the associated task completes. Also provides idempotent close semantics.
 *
 * This iterator follows the semantics of GPU RDD columnar batch iterators too in that
 * if a batch is returned by next it is the responsibility of the receiver to close
 * it.
 *
 * Generally it is good practice if hasNext would return false than any outstanding resources
 * should be closed so waiting for an explicit close is not needed.
 *
 * @param closeWithTask should the Iterator be closed at task completion or not.
 */
abstract class GpuColumnarBatchIterator(closeWithTask: Boolean)
    extends Iterator[ColumnarBatch] with AutoCloseable {
  private var isClosed = false
  if (closeWithTask) {
    // Don't install the callback if in a unit test
    Option(TaskContext.get()).foreach { tc =>
      onTaskCompletion(tc) {
        close()
      }
    }
  }

  final override def close(): Unit = {
    if (!isClosed) {
      doClose()
    }
    isClosed = true
  }

  def doClose(): Unit
}

object EmptyGpuColumnarBatchIterator extends GpuColumnarBatchIterator(false) {
  override def hasNext: Boolean = false
  override def next(): ColumnarBatch = throw new NoSuchElementException()
  override def doClose(): Unit = {}
}

class SingleGpuColumnarBatchIterator(private var batch: ColumnarBatch)
    extends GpuColumnarBatchIterator(true) {
  override def hasNext: Boolean = batch != null

  override def next(): ColumnarBatch = {
    if (batch == null) {
      throw new NoSuchElementException()
    }
    val ret = batch
    batch = null
    ret
  }

  override def doClose(): Unit = {
    if (batch != null) {
      batch.close()
      batch = null
    }
  }
}
