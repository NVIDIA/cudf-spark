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

/**
 * Helper iterator that wraps an Iterator of AutoCloseable subclasses.
 * This iterator also implements AutoCloseable, so it can be closed in case
 * of exceptions and when close is called on it, its buffered item will be
 * closed as well.
 *
 * @param wrapped the iterator we are wrapping for buffering
 * @tparam T an AutoCloseable subclass
 */
class CloseableBufferedIterator[T <: AutoCloseable](wrapped: Iterator[T])
  extends BufferedIterator[T] with AutoCloseable {
  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  private[this] var isClosed = false

  private var hd: Option[T] = None

  def head: T = {
    if (hd.isEmpty) {
      hd = Some(next())
    }
    hd.get
  }

  override def headOption: Option[T] = {
    if (hasNext) {
      Some(head)
    } else {
      None
    }
  }

  override def next(): T = if (hd.isDefined) {
    val res = hd.get
    hd = None
    res
  } else {
    wrapped.next
  }

  override def hasNext: Boolean = !isClosed && (hd.isDefined || wrapped.hasNext)

  override def close(): Unit = {
    if (!isClosed) {
      hd.foreach(_.close()) // close a buffered head item
      hd = None
      isClosed = true
    }
  }
}
