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

import ai.rapids.cudf.Table
import com.nvidia.spark.Retryable

/**
 * A GpuDataProducer produces data on the GPU. That data typically comes from other resources also
 * held on the GPU that cannot be released until the iterator is closed and cannot be made
 * spillable. This behaves like an Iterator but is not an Iterator because this cannot be used
 * in place of an Iterator, especially in the context of an RDD where it would violate the
 * semantics of the GpuSemaphore. Generally the lifetime of this should be entirely while the
 * GpuSemaphore is held. It is "generally" because there are a few cases where for performance
 * reasons if we know that no data is going to be produced, then we might not grab the semaphore
 * at all.
 *
 * @tparam T what it is that we are wrapping
 */
trait GpuDataProducer[T] extends AutoCloseable {
  /**
   * Returns true if there is more data to be read or false if there is not.
   */
  def hasNext: Boolean

  /**
   * If hasNext returned true, return the data. The reader is responsible for closing the
   * returned value if it needs to be closed. If there is no more data to be read then
   * an instance of NoSuchElementException should be thrown.
   */
  def next: T

  /**
   * Just like foreach on an Iterator
   */
  def foreach[U](func: T => U): Unit = {
    while (hasNext) {
      func(next)
    }
  }
}

/**
 * A table producer whose state can be checkpointed and restored across an RMM retry.
 * Implementations must keep their inputs alive until close and reproduce the next table after
 * restore without skipping or duplicating previously returned data.
 */
private[rapids] trait RetryableTableProducer extends GpuDataProducer[Table] with Retryable

object GpuDataProducer {
  /**
   * Essentially the same as doing a map on a regular iterator, but the resulting GpuDataProducer
   * takes ownership of the input GpuDataProducer. So the passed in GpuDataProducer should not be
   * closed. The returned GpuDataProducer should be closed instead.
   */
  def wrap[T, U](wrapped: GpuDataProducer[U])(modifyAndClose: U => T): GpuDataProducer[T] =
    new WrappedGpuDataProducer(wrapped, modifyAndClose)
}

class EmptyGpuDataProducer[T] extends GpuDataProducer[T] {
  override def hasNext: Boolean = false

  override def next: T = throw new NoSuchElementException()

  override def close(): Unit = {}
}

class SingleGpuDataProducer[T <: AnyRef](private var data: T) extends GpuDataProducer[T] {
  override def hasNext: Boolean = data != null

  override def next: T = {
    if (data == null) {
      throw new NoSuchElementException()
    }
    val ret = data
    data = null.asInstanceOf[T]
    ret
  }

  override def close(): Unit = {
    data match {
      case a: AutoCloseable => a.close()
      case _ =>
    }
  }
}

class WrappedGpuDataProducer[T, U](
    wrapped: GpuDataProducer[U],
    modifyAndClose: U => T) extends GpuDataProducer[T] {
  override def hasNext: Boolean = wrapped.hasNext

  override def next: T = {
    modifyAndClose(wrapped.next)
  }

  override def close(): Unit = {
    wrapped.close()
  }
}

object EmptyTableReader extends EmptyGpuDataProducer[Table]
