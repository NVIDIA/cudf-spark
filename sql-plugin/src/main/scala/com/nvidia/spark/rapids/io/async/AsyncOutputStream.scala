/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async

import java.io.{IOException, OutputStream}
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.rapids.ColumnarWriteTaskStatsTracker
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object AsyncOutputStream {

  /**
   * OutputStream that performs writes asynchronously. Writes are scheduled on a background thread
   * and executed in the order they were scheduled. This class is not thread-safe and should only be
   * used by a single thread.
   *
   * @param openFn a function that creates an underlying delegate OutputStream
   * @param trafficController a TrafficController that controls the flow of data to the underlying
   *                          delegate
   * @param statsTrackers a sequence of ColumnarWriteTaskStatsTracker
   */
  def apply(
      openFn: Callable[OutputStream],
      trafficController: TrafficController,
      statsTrackers: Seq[ColumnarWriteTaskStatsTracker]): AsyncOutputStream = {
    // Use a single writer thread that stays alive until close. Some cloud output streams, such as
    // the GCS connector, can bridge OutputStream writes to an async uploader through a
    // PipedOutputStream/PipedInputStream pair, where the reader can report a broken pipe if the
    // writer thread exits while the stream is still open.
    val executor = new ThrottlingExecutor(
      TrampolineUtil.newDaemonSingleThreadExecutor("AsyncOutputStream for "
        + Thread.currentThread().getName), trafficController,
      new StatsUpdaterForWriteFunc(statsTrackers).func)
    new AsyncOutputStream(openFn, executor)
  }
}

class AsyncOutputStream(openFn: Callable[OutputStream], executor: ThrottlingExecutor)
  extends OutputStream {

  private var closed = false

  // Open the underlying stream asynchronously as soon as the AsyncOutputStream is constructed,
  // so that the open can be done in parallel with other operations. This could help with
  // performance if the open is slow.
  private val openFuture = executor.submit(openFn, 0)
  // Let's give it enough time to open the stream. Something bad should have happened if it
  // takes more than 5 minutes to open a stream.
  private val openTimeoutMin = 5

  private lazy val delegate: OutputStream = {
    openFuture.get(openTimeoutMin, TimeUnit.MINUTES)
  }

  class Metrics {
    var numBytesScheduled: Long = 0
    // This is thread-safe as it is updated by the background thread and can be read by
    // any threads.
    val numBytesWritten: AtomicLong = new AtomicLong(0)
  }

  val metrics = new Metrics

  /**
   * The last error that occurred in the background thread, or None if no error occurred.
   * Once it is set, all subsequent writes that are already scheduled will fail and no new
   * writes will be accepted.
   *
   * This is thread-safe as it is set by the background thread and can be read by any threads.
   */
  val lastError: AtomicReference[Option[Throwable]] =
    new AtomicReference[Option[Throwable]](None)

  @throws[IOException]
  private def throwIfError(): Unit = {
    lastError.get() match {
      case Some(t: IOException) => throw t
      case Some(t) => throw new IOException(t)
      case None =>
    }
  }

  @throws[IOException]
  private def throwExecutionExceptionAsIOException(e: ExecutionException): Nothing = {
    e.getCause match {
      case t: IOException => throw t
      case t if t != null => throw new IOException(t)
      case _ => throw new IOException(e)
    }
  }

  @throws[IOException]
  private def ensureOpen(): Unit = {
    if (closed) {
      throw new IOException("Stream closed")
    }
  }

  private def scheduleWrite(fn: () => Unit, bytesToWrite: Int): Unit = {
    throwIfError()
    ensureOpen()

    metrics.numBytesScheduled += bytesToWrite
    executor.submit(() => {
      throwIfError()
      ensureOpen()

      try {
        fn()
        metrics.numBytesWritten.addAndGet(bytesToWrite)
      } catch {
        case t: Throwable =>
          // Update the error state
          lastError.set(Some(t))
      }
    }, bytesToWrite)
  }

  override def write(b: Int): Unit = {
    scheduleWrite(() => delegate.write(b), 1)
  }

  override def write(b: Array[Byte]): Unit = {
    val copy = new Array[Byte](b.length)
    System.arraycopy(b, 0, copy, 0, b.length)
    scheduleWrite(() => delegate.write(copy), copy.length)
  }

  /**
   * Schedules a write of the given bytes to the underlying stream. The write is executed
   * asynchronously on a background thread. The method returns immediately, and the write may not
   * have completed when the method returns.
   *
   * If an error has occurred in the background thread and [[lastError]] has been set, this function
   * will throw an IOException immediately.
   *
   * If an error has occurred in the background thread while executing a previous write after the
   * current write has been scheduled, the current write will fail with the same error.
   */
  @throws[IOException]
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val copy = new Array[Byte](len)
    System.arraycopy(b, off, copy, 0, len)
    scheduleWrite(() => delegate.write(copy, 0, len), len)
  }

  /**
   * Flushes all pending writes to the underlying stream. This method blocks until all pending
   * writes have been completed. If an error has occurred in the background thread, this method
   * will throw an IOException.
   *
   * If an error has occurred in the background thread and [[lastError]] has been set, this function
   * will throw an IOException immediately.
   *
   * If an error has occurred in the background thread while executing a previous task after the
   * current flush has been scheduled, the current flush will fail with the same error.
   */
  @throws[IOException]
  override def flush(): Unit = {
    throwIfError()
    ensureOpen()

    val f = executor.submit(() => {
      throwIfError()
      ensureOpen()

      delegate.flush()
    }, 0)

    try {
      f.get()
    } catch {
      case e: ExecutionException => throwExecutionExceptionAsIOException(e)
    }
  }

  /**
   * Closes the underlying stream and releases any resources associated with it. All pending writes
   * are flushed before closing the stream. This method blocks until all pending writes have been
   * completed.
   *
   * If an error has occurred while flushing, this function will throw an IOException.
   *
   * If an error has occurred while executing a previous task before this function is called,
   * this function will throw the same error. All resources and the underlying stream are still
   * guaranteed to be closed.
   */
  @throws[IOException]
  override def close(): Unit = {
    if (!closed) {
      Seq[AutoCloseable](
        () => {
          // Close on the async executor after all pending writes, before shutting the executor
          // down. Some cloud streams use a pipe tied to the writing thread and can report a
          // broken pipe if that thread exits before the stream is closed.
          try {
            executor.submit(() => {
              try {
                throwIfError()
                ensureOpen()
                delegate.flush()
              } catch {
                case t: Throwable =>
                  delegate.safeClose(t)
                  throw t
              }
              delegate.close()
            }, 0).get()
          } catch {
            case e: ExecutionException => throwExecutionExceptionAsIOException(e)
          }
        },
        () => executor.shutdownNow(10, TimeUnit.SECONDS),
        () => closed = true).safeClose()
    }
  }
}
