/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * For columnar code on the CPU it is the responsibility of the SparkPlan exec that creates a
 * `ColumnarBatch` to close it.  In the case of code running on the GPU that would waste too
 * much memory, so it is the responsibility of the code receiving the batch to close it, when it
 * is not longer needed.
 *
 * This class provides a simple way for CPU batch code to be sure that a batch gets closed. If your
 * code is executing on the GPU do not use this class.
 */
class AutoCloseColumnBatchIterator[U](itr: Iterator[U], nextBatch: Iterator[U] => ColumnarBatch)
    extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeCurrentBatch()
    }
  }

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = nextBatch(itr)
    cb
  }
}

object AutoCloseColumnBatchIterator {
  def map[U](rdd: RDD[U], f: (U) => ColumnarBatch) : RDD[ColumnarBatch] = {
    rdd.mapPartitions((itr) => new AutoCloseColumnBatchIterator(itr,
      (batchIter: Iterator[U]) => f(batchIter.next())))
  }
}
