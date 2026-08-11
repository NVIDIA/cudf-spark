/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.hybrid

import ai.rapids.cudf.{HostMemoryBuffer, PinnedMemoryPool}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableArray
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}

/**
 * Allocator that has the memory allocation protected by the retry framework to handle
 * OOM errors. And "tryPinned" indicates whether it will try pinned memory first.
 */
trait HostRetryAllocator {

  /** Visible for tests */
  final private[rapids] def allocWithRetry(
      bytesSizes: Seq[Long],
      tryPinned: Boolean): Seq[AllocResult] = {
    val arrBufs = new Array[AllocResult](bytesSizes.length)
    val arrRetryable = new Retryable {
      override def checkpoint(): Unit = {}
      override def restore(): Unit = {
        // Close the allocated buffers and clear the array once OOM
        arrBufs.safeClose()
        arrBufs.indices.foreach(i => arrBufs(i) = null)
      }
    }
    closeOnExcept(arrBufs) { _ =>
      withRetryNoSplit {
        withRestoreOnRetry(arrRetryable) {
          bytesSizes.zipWithIndex.foreach { case (size, idx) =>
            var buf: HostMemoryBuffer = null
            if (tryPinned) {
              buf = PinnedMemoryPool.tryAllocate(size)
            }
            arrBufs(idx) = if (buf != null) { // pinned
              new AllocResult(buf, isPinned = true)
            } else {
              new AllocResult(HostMemoryBuffer.allocate(size, false), isPinned = false)
            }
          } // end of foreach
        }
      } // end of withRetryNoSplit
    }
    arrBufs.toSeq
  }
}

private[rapids] class AllocResult(
    val buffer: HostMemoryBuffer,
    val isPinned: Boolean) extends AutoCloseable {

  override def close(): Unit = if (buffer != null) {
    buffer.close()
  }
}
