/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.mutable

import ai.rapids.cudf.CudfColumnSizeOverflowException
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.SplitReason.SplitReason
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, CpuSplitAndRetryOOM, GpuRetryOOM, GpuSplitAndRetryOOM, RmmSpark, RmmSparkThreadState}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

object RmmRapidsRetryIterator extends Logging {

  /**
   * withRetry for Iterator[T]. This helper calls a function `fn` as it takes
   * elements from the iterator given in `input`, and it can retry the work in `fn`,
   * and optionally split items into smaller chunks. The `splitPolicy` function must
   * close the item passed to it. The resulting iterator may or
   * may not have the same number of elements as the source iterator.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetry` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. Elements of `input` not manifested are the responsibility of the caller to
   * close!
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input       an iterator of T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: Iterator[T],
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    val attemptIter = new AutoCloseableAttemptSpliterator(input, fn, splitPolicy)
    new RmmRapidsRetryAutoCloseableIterator(attemptIter)
  }

  /**
   * withRetry for T. This helper calls a function `fn` with the single input `T`,
   * and it can retry the work in `fn` and optionally split `input` into smaller chunks.
   * The resulting iterator may be 1 element, if successful on the first attempt or retry,
   * or it could be multiple if splits were required.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetry` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception `input` is also closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input       a single item T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: T,
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(input), fn, splitPolicy)
    new RmmRapidsRetryAutoCloseableIterator(attemptIter)
  }

  /**
   * withRetryNoSplit for T. This helper calls a function `fn` with the `input`, and it will
   * retry the call to `fn` if needed. This does not split the
   * input into multiple chunks. The result is a single item of type K.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetryNoSplit` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception `input` is also closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input a single item T
   * @param fn    the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: T)
      (fn: T => K): K = {
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(input), fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
  }

  /**
   * withRetryNoSplit for Seq[T]. This helper calls a function `fn` with the whole sequence
   * given in `input`, and it will retry the call to `fn` if needed. This does not split the
   * input into multiple chunks. The result is a single item of type K.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetryNoSplit` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception, all elements of `input` are closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input a single item T
   * @param fn    the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: Seq[T])
      (fn: Seq[T] => K): K = {
    val wrapped = AutoCloseableSeqInternal(input)
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(wrapped), fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
  }

  /**
   * no-input withRetryNoSplit. This helper calls a function `fn` retrying the call if needed.
   * The result is a single item of type K.
   *
   * The expectation when code enters `withRetryNoSplit` is that all of the caller's data is
   * spillable already, allowing the thread to be blocked, and its data eventually spilled
   * because of other higher priority work.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param fn the work to perform. It is a function that takes nothing and produces K
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[K](fn: => K): K = {
    val attemptIter = new NoInputSpliterator(fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
  }

  /**
   * Execute `fn` inside a retry block where host/GPU allocations throw retryable
   * OOM exceptions instead of fatal errors. Unlike `withRetryNoSplit`, this does
   * NOT automatically retry — the caller manages its own retry logic within `fn`.
   *
   * Use this for incremental operations (e.g. row-by-row accumulation) where
   * partial progress is valuable and the standard atomic-retry model doesn't fit.
   *
   * @param fn the work to perform inside the retry block
   * @tparam T result type
   * @return the result of `fn`
   */
  def withRetryBlock[T](fn: => T): T = {
    RmmSpark.currentThreadStartRetryBlock()
    try {
      fn
    } finally {
      RmmSpark.currentThreadEndRetryBlock()
    }
  }

  /**
   * Block the current thread until memory is freed, following the standard
   * protocol of exiting and re-entering the retry block around the blocking call.
   *
   * Must be called from within an active retry block (i.e. inside `withRetryBlock`).
   * After this call returns, the retry block is re-entered and the caller can retry
   * the failed operation.
   */
  def blockUntilMemoryFreed(): Unit = {
    RmmSpark.currentThreadEndRetryBlock()
    try {
      RmmSpark.blockThreadUntilReady()
    } finally {
      RmmSpark.currentThreadStartRetryBlock()
    }
  }

  /**
   * Returns a tuple of (shouldRetry, shouldSplit, isFromGpuOom) depending the exception
   * passed
   */
  private def isRetryOrSplitAndRetry(ex: Throwable): (Boolean, Boolean, Boolean) = {
    ex match {
      case _: GpuRetryOOM => (true, false, true)
      case _: CpuRetryOOM => (true, false, false)
      case _: GpuSplitAndRetryOOM => (true, true, true)
      case _: CpuSplitAndRetryOOM => (true, true, false)
      case _ => (false, false, false)
    }
  }

  /**
   * Returns a tuple of (causedByRetry, causedBySplit, ifFromGpuoom) depending the exception
   * passed
   */
  private def causedByRetryOrSplit(ex: Throwable): (Boolean, Boolean, Boolean) = {
    var current = ex
    var causedByRetry = false
    var causedBySplit = false
    var isFromGpuOom = false
    // check if there is a hidden retry or split OOM
    while (current != null && !causedByRetry) {
      current = current.getCause()
      val (isRetry, isSplit, isGpuOom) = isRetryOrSplitAndRetry(current)
      causedByRetry = isRetry
      causedBySplit = causedBySplit || isSplit
      isFromGpuOom = isGpuOom
    }
    (causedByRetry, causedBySplit, isFromGpuOom)
  }

  private def isColumnSizeOverflow(ex: Throwable): Boolean =
    ex.isInstanceOf[CudfColumnSizeOverflowException]

  @tailrec
  private def isOrCausedByColumnSizeOverflow(ex: Throwable): Boolean = {
    ex != null && (isColumnSizeOverflow(ex) || isOrCausedByColumnSizeOverflow(ex.getCause))
  }

  /**
   * withRestoreOnRetry for Retryable. This helper function calls `fn` with no input and
   * returns the result. In the event of an OOM Retry exception, it calls the restore() method
   * of the input and then throws the oom exception.  This is intended to be used within the `fn`
   * of one of the withRetry* functions.  It provides an opportunity to reset state in the case
   * of a retry.
   *
   * @param r  a single item T
   * @param fn the work to perform. Takes no input and produces K
   * @tparam T element type that must be a `Retryable` subclass
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRestoreOnRetry[T <: Retryable, K](r: T)(fn: => K): K = {
    try {
      fn
    } catch {
      case ex: Throwable =>
        // Only restore on retry exceptions
        val (topLevelIsRetry, _, _) = isRetryOrSplitAndRetry(ex)
        if (topLevelIsRetry || causedByRetryOrSplit(ex)._1 || isOrCausedByColumnSizeOverflow(ex)) {
          r.restore()
        }
        throw ex
    }
  }

  /**
   * withRestoreOnRetry for Retryable. This helper function calls `fn` with no input and
   * returns the result. In the event of an OOM Retry exception, it calls the restore() method
   * of the input and then throws the oom exception.  This is intended to be used within the `fn`
   * of one of the withRetry* functions.  It provides an opportunity to reset state in the case
   * of a retry.
   *
   * @param r  a Seq of item T
   * @param fn the work to perform. Takes no input and produces K
   * @tparam T element type that must be a `Retryable` subclass
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRestoreOnRetry[T <: Retryable, K](r: Seq[T])(fn: => K): K = {
    try {
      fn
    } catch {
      case ex: Throwable =>
        // Only restore on retry exceptions
        val (topLevelIsRetry, _, _) = isRetryOrSplitAndRetry(ex)
        if (topLevelIsRetry || causedByRetryOrSplit(ex)._1 || isOrCausedByColumnSizeOverflow(ex)) {
          r.foreach(_.restore())
        }
        throw ex
    }
  }

  /**
   * Helper method to drain an iterator and ensuring that it was non-empty
   * and it had a single item in it.
   */
  private def drainSingleWithVerification[K](it: Iterator[K]): K = {
    require(it.hasNext,
      "Couldn't drain a single item with a closed iterator!")
    val item = it.next()
    require(!it.hasNext,
      "Multiple items found in the source iterator but one expected!")
    item
  }

  /** Co-work with AutoCloseableSeqInternal to print the total size information when OOM */
  trait SizeProvider {
    def sizeInBytes: Long
  }

  /**
   * AutoCloseable wrapper on Seq[T], returning a Seq[T] that can be closed.
   *
   * @param ts the Seq to wrap
   * @tparam T the type of the items in `ts`
   */
  private case class AutoCloseableSeqInternal[T <: AutoCloseable](ts: Seq[T])
      extends Seq[T] with AutoCloseable {
    override def close(): Unit = {
      ts.foreach(_.safeClose())
    }

    override def length: Int = ts.length

    override def iterator: Iterator[T] = ts.iterator

    override def apply(idx: Int): T = ts.apply(idx)

    override def toString(): String = {
      val totalSize = ts.map {
        case sp: SizeProvider => sp.sizeInBytes
        case _ => 0L
      }.sum
      s"AutoCloseableSeqInternal totalSize:$totalSize with ${length} elements, inner:\n" +
        s"[${ts.mkString("; ")}]"
    }
  }

  /**
   * An iterator of a single item that is able to close if .next
   * has not been called on it.
   *
   * @param ts the AutoCloseable item to close if this iterator hasn't been drained
   * @tparam T the type of `ts`, must be AutoCloseable
   */
  private case class SingleItemAutoCloseableIteratorInternal[T <: AutoCloseable](ts: T)
      extends Iterator[T] with AutoCloseable {

    private var wasCalledSuccessfully = false

    override def hasNext: Boolean = !wasCalledSuccessfully

    override def next(): T = {
      wasCalledSuccessfully = true
      ts
    }

    override def close(): Unit = {
      if (!wasCalledSuccessfully) {
        ts.close()
      }
    }
  }

  /**
   * A trait that defines an iterator of type K that supports two extra things:
   * the ability to split its input, and the ability to close itself.
   *
   * Note that the input's type is not defined and is not relevant to this trait.
   *
   * @tparam K the resulting type
   */
  trait Spliterator[K] extends Iterator[K] with AutoCloseable {
    override def hasNext: Boolean

    /**
     * Split is a function that is invoked by `RmmRapidsRetryIterator` when `GpuSplitAndRetryOOM`
     * or `CpuSplitAndRetryOOM`
     * is thrown. This function is implemented by `Spliterator` classes to attempt to handle
     * this exception by reducing the size of attempts (the thing that `.next` is
     * using as an input), usually by splitting a batch in half by number of rows, or
     * splitting a collection of batches into smaller collections to be attempted separately,
     * likely reducing GPU memory that needs to be manifested while calling `.next`.
     *
     * @param splitReason the reason for the split
     */
    def split(splitReason: SplitReason): Unit

    override def next(): K

    override def close(): Unit
  }

  /**
   * A spliterator that doesn't take any inputs, hence it is "empty", and it doesn't know
   * how to split. It allows the caller to call the function `fn` once on `next`.
   *
   * @param fn the work to perform. It is a function that takes nothing and produces K
   * @tparam K the resulting type
   */
  class NoInputSpliterator[K](fn: => K) extends Spliterator[K] {
    private var wasCalledSuccessfully: Boolean = false

    override def hasNext: Boolean = !wasCalledSuccessfully

    override def split(splitReason: SplitReason): Unit = {
      splitReason match {
        case SplitReason.CPU_OOM =>
          throw new CpuSplitAndRetryOOM("CPU OutOfMemory: could not split inputs and retry")
        case SplitReason.GPU_OOM =>
          throw new GpuSplitAndRetryOOM("GPU OutOfMemory: could not split inputs and retry")
        case SplitReason.CUDF_OVERFLOW =>
          throw new IllegalStateException("CUDF String column overflow caused split, but current "
            + "spliterator does not support splitting.")
      }
    }

    override def next(): K = {
      RmmSpark.currentThreadStartRetryBlock()
      val res = try {
        fn
      } finally {
        RmmSpark.currentThreadEndRetryBlock()
      }
      wasCalledSuccessfully = true
      res
    }

    override def close(): Unit = {}
  }

  /**
   * A spliterator that takes an input iterator of auto closeable T, and a function `fn`
   * that can map `T` to `K`, with an additional `splitPolicy` that can split `T` into a
   * `Seq[T]`
   *
   * It assumes the type T is AutoCloseable, and that if a split policy is specified, that it
   * is capable of handling splitting one T into a sequence of them.
   *
   * When an attempt to invoke function `fn` is successful, the item T in `input` will be
   * closed. In the case of a failure, all attempts will be closed. It is the responsibility
   * of the caller to close any remaining items in `input` that have not been attempted.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @tparam T element type that must be AutoCloseable
   * @tparam K `fn` result type
   * @param input       an iterator of T
   * @param fn          a function that takes T and produces K
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   */
  class AutoCloseableAttemptSpliterator[T <: AutoCloseable, K](
      input: Iterator[T],
      fn: T => K,
      splitPolicy: T => Seq[T])
      extends Spliterator[K] {
    def this(input: Iterator[T], fn: T => K) =
      this(input, fn, null)

    private def closeInternal(): Unit = {
      attemptStack.safeClose()
      attemptStack.clear()
      // Only the single-item wrapper owns input that has not been pulled.
      input match {
        case single: SingleItemAutoCloseableIteratorInternal[_] => single.close()
        case _ =>
      }
    }

    // Don't install the callback if in a unit test
    private val onClose = Option(TaskContext.get()).map { tc =>
      onTaskCompletion(tc) {
        closeInternal()
      }
    }

    protected val attemptStack = new mutable.ArrayStack[T]()

    override def hasNext: Boolean = input.hasNext || attemptStack.nonEmpty

    override def split(splitReason: SplitReason): Unit = {
      // If `split` OOMs, we are already the last thread standing
      // there is likely not much we can do, and for now we don't handle
      // this OOM
      if (splitPolicy == null) {
        val message = s"could not split inputs and retry. " +
          s"Current threadCountBlockedUntilReady: ${threadCountBlockedUntilReady}. " +
          s"The current attempt: " +
          s"{${attemptStack.head}}"
        splitReason match {
          case SplitReason.GPU_OOM => throw new GpuSplitAndRetryOOM(s"GPU OutOfMemory: $message")
          case SplitReason.CPU_OOM => throw new CpuSplitAndRetryOOM(s"CPU OutOfMemory: $message")
          case SplitReason.CUDF_OVERFLOW =>
            throw new IllegalStateException("CUDF String column overflow caused split," +
              s" but splitPolicy not set. The current attempt: ${attemptStack.head}")
        }
      }
      val curAttempt = attemptStack.pop()
      val splitted = try {
        // splitPolicy must take ownership of the argument
        splitPolicy(curAttempt)
      } catch {
          // We only care about OOM exceptions and wrap it by a new exception with the
          // same type to provide more context for the OOM.
          // This looks a little odd, because we can not change the type of root exception.
          // Otherwise, some unit tests will fail due to the wrong exception type returned.
          //
          // Stringify the attempt lazily (only on failure) to avoid O(n) cost on the
          // hot path when splits succeed. The attempt has not been closed yet at this
          // point because splitPolicy threw before taking ownership.
        case go: GpuRetryOOM =>
          val attemptAsString = curAttempt.toString
          throw new GpuRetryOOM(
            s"GPU OutOfMemory: " +
              s"Current threadCountBlockedUntilReady: ${threadCountBlockedUntilReady}. " +
              s"Could not split the current attempt: {$attemptAsString}"
          ).initCause(go)
        case go: GpuSplitAndRetryOOM =>
          val attemptAsString = curAttempt.toString
          throw new GpuSplitAndRetryOOM(
            s"GPU OutOfMemory: " +
              s"Current threadCountBlockedUntilReady: ${threadCountBlockedUntilReady}. " +
              s"Could not split the current attempt: {$attemptAsString}"
          ).initCause(go)
        case co: CpuRetryOOM =>
          val attemptAsString = curAttempt.toString
          throw new CpuRetryOOM(
            s"CPU OutOfMemory: " +
              s"Current threadCountBlockedUntilReady: ${threadCountBlockedUntilReady}. " +
              s"Could not split the current attempt: {$attemptAsString}"
          ).initCause(co)
        case co: CpuSplitAndRetryOOM =>
          val attemptAsString = curAttempt.toString
          throw new CpuSplitAndRetryOOM(
            s"CPU OutOfMemory: " +
              s"Current threadCountBlockedUntilReady: ${threadCountBlockedUntilReady}. " +
              s"Could not split the current attempt: {$attemptAsString}"
          ).initCause(co)
      }
      // the splitted sequence needs to be inserted in reverse order
      // so we try the first item first.
      splitted.reverse.foreach(attemptStack.push)
    }

    override def next(): K = {
      if (attemptStack.isEmpty && input.hasNext) {
        attemptStack.push(input.next())
      }
      val popped = attemptStack.head
      RmmSpark.currentThreadStartRetryBlock()
      val res = try {
        fn(popped)
      } finally {
        RmmSpark.currentThreadEndRetryBlock()
      }
      attemptStack.pop().close()
      if (attemptStack.isEmpty && !input.hasNext) {
        // No need to call the onClose because the attemptStack is empty
        onClose.foreach(_.removeCallback())
      }
      res
    }

    override def close(): Unit = {
      onClose.map(_.removeAndCall()).getOrElse(closeInternal())
    }
  }

  /**
   * RmmRapidsRetryAutoCloseableIterator exposes an iterator that can retry work,
   * specified by `fn`, abstracting away the retry specifics. Elements passed to this iterator
   * must be AutoCloseable.
   *
   * It assumes the type T is AutoCloseable, and that if a split policy is specified, that it
   * is capable of handling splitting one T into a sequence of them.
   *
   * @tparam T element type that must be AutoCloseable
   * @tparam K result type
   * @param attemptIter an iterator of T
   */
  class RmmRapidsRetryAutoCloseableIterator[T <: AutoCloseable, K](
      attemptIter: Spliterator[K])
      extends RmmRapidsRetryIterator[T, K](attemptIter) {

    override def hasNext: Boolean = {
      try {
        RetryStateTracker.enterRetryBlock()
        super.hasNext
      } finally {
        RetryStateTracker.exitRetryBlock()
      }
    }

    override def next(): K = {
      if (!hasNext) {
        throw new NoSuchElementException("Closed called on an empty iterator.")
      }
      try {
        RetryStateTracker.enterRetryBlock()
        super.next()
      } catch {
        case t: Throwable =>
          // exception occurred while trying to handle this retry
          // we close our attempts (which includes the item we last attempted)
          attemptIter.close()
          throw t
      } finally {
        RetryStateTracker.exitRetryBlock()
      }
    }
  }

  /**
   * RmmRapidsRetryIterator exposes an iterator that can retry work,
   * specified by `fn`, abstracting away the retry specifics.
   *
   * @tparam T element type
   * @tparam K `fn` result type
   * @param attemptIter an iterator of T
   */
  class RmmRapidsRetryIterator[T, K](attemptIter: Spliterator[K])
      extends Iterator[K] {
    // We want to be sure that retry will work in all cases
    TaskRegistryTracker.registerThreadForRetry()

    // this is true if an OOM was injected (only for tests)
    private var injectedOOM = false
    // this is true if the OOM was cleared after it was injected (only for tests)
    private var injectedOOMCleared = false

    override def hasNext: Boolean = attemptIter.hasNext

    private def clearInjectedOOMIfNeeded(): Unit = {
      if (injectedOOM && !injectedOOMCleared) {
        val threadId = RmmSpark.getCurrentThreadId
        // if for some reason we don't throw, or we throw something that isn't a GpuRetryOOM
        // or CpuRetryOOM
        // we want to remove the retry we registered before we leave the withRetry block.
        // If the thread is in an UNKNOWN state, then it is already cleared.
        if (RmmSpark.getStateOf(threadId) != RmmSparkThreadState.UNKNOWN) {
          RmmSpark.forceRetryOOM(threadId, 0)
        }
        injectedOOMCleared = true
      }
    }

    override def next(): K = try {
      // this is set on the first exception, and we add suppressed if there are others
      // during the retry attempts
      var lastException: Throwable = null
      var firstAttempt: Boolean = true
      var result: Option[K] = None
      var splitReason = SplitReason.NONE
      while (result.isEmpty && attemptIter.hasNext) {
        RetryStateTracker.setCurThreadRetrying(!firstAttempt)
        if (!firstAttempt) {
          // call thread block API
          try {
            threadCountBlockedUntilReady.incrementAndGet()
            RmmSpark.blockThreadUntilReady()
          } catch {
            case _: GpuSplitAndRetryOOM =>
              splitReason = SplitReason.GPU_OOM
            case _: CpuSplitAndRetryOOM =>
              splitReason = SplitReason.CPU_OOM
          } finally {
            threadCountBlockedUntilReady.decrementAndGet()
          }
        }
        firstAttempt = false
        if (splitReason != SplitReason.NONE) {
          preSplitLogging()
          attemptIter.split(splitReason)
        }
        splitReason = SplitReason.NONE
        try {
          // call the user's function
          RapidsConf.testRetryOOMInjectionMode() match {
            case mode if !injectedOOM && mode.numOoms > 0 =>
              injectedOOM = true
              // ensure we have associated our thread with the running task, as
              // `forceRetryOOM` requires a prior association.
              var threadAssociated = true
              if (!RmmSpark.isThreadWorkingOnTaskAsPoolThread) {
                // If RmmSpark isn't aware of this thread, we are going to
                // try to find the TaskContext and use it to register it for a taskID.
                // However, TaskContext is not going to work for a pool thread that isn't
                // managed by Spark, so we are going to skip registration, and skip the OOM.
                // This is a temporary workaround, see:
                // https://github.com/NVIDIA/spark-rapids/issues/13098
                threadAssociated = false
                Option(TaskContext.get()).foreach { tc =>
                  threadAssociated = true
                  RmmSpark.currentThreadIsDedicatedToTask(tc.taskAttemptId())
                }
              }
              if (threadAssociated) {
                if (mode.withSplit) {
                  RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId,
                    mode.numOoms,
                    mode.oomInjectionFilter.ordinal,
                    mode.skipCount)
                } else {
                  RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId,
                    mode.numOoms,
                    mode.oomInjectionFilter.ordinal,
                    mode.skipCount)
                }
              } else {
                log.warn("pool thread not registered with RmmSpark, cannot inject OOM. See " +
                  "https://github.com/NVIDIA/spark-rapids/issues/13098")
              }
            case _ => ()
          }
          result = Some(attemptIter.next())
          clearInjectedOOMIfNeeded()
        } catch {
          case ex: Throwable =>
            log.info("got a throwable in RmmRapidsRetryIterator.next():", ex)
            // handle a retry as the top-level exception
            val (topLevelIsRetry, topLevelIsSplit, isGpuOom) = isRetryOrSplitAndRetry(ex)
            if (topLevelIsSplit) {
              if (isGpuOom) {
                splitReason = SplitReason.GPU_OOM
              } else {
                splitReason = SplitReason.CPU_OOM
              }
              logInfo("splitReason is set " +
                s"to ${splitReason} after checking isRetryOrSplitAndRetry, related exception:",
                ex)
            }

            // handle any retries that are wrapped in a different top-level exception
            var causedByRetry = false
            if (!topLevelIsRetry) {
              val (cbRetry, cbSplit, isGpuOom) = causedByRetryOrSplit(ex)
              causedByRetry = cbRetry
              if (!(splitReason == SplitReason.GPU_OOM || splitReason == SplitReason.CPU_OOM)) {
                if (cbSplit) {
                  if (isGpuOom) {
                    splitReason = SplitReason.GPU_OOM
                  } else {
                    splitReason = SplitReason.CPU_OOM
                  }
                }
                if (splitReason == SplitReason.GPU_OOM || splitReason == SplitReason.CPU_OOM) {
                  logInfo(s"splitReason is set to ${splitReason} after checking " +
                    s"causedByRetryOrSplit, related exception:", ex)
                }
              }
            }

            clearInjectedOOMIfNeeded()

            // make sure we add any prior exceptions to this one as causes
            if (lastException != null) {
              ex.addSuppressed(lastException)
            }
            lastException = ex

            if (!topLevelIsRetry && !causedByRetry) {
              if (isOrCausedByColumnSizeOverflow(ex)) {
                // CUDF column size overflow? Attempt split-retry.
                splitReason = SplitReason.CUDF_OVERFLOW
                logInfo(s"splitReason is set to ${splitReason} after checking " +
                  s"isOrCausedByColumnSizeOverflow, related exception:", ex)
              } else {
                // we want to throw early here, since we got an exception
                // we were not prepared to handle
                throw lastException
              }
            }
          // else another exception wrapped a retry. So we are going to try again
        }
      }
      if (result.isEmpty) {
        // then lastException must be set, throw it.
        throw lastException
      }
      result.get
    } finally {
      RetryStateTracker.clearCurThreadRetrying()
    }
  }

  /**
   * Common split function from a single SpillableColumnarBatch to a sequence of them,
   * that tries to split the input into two chunks. If the input cannot be split in two,
   * because we are down to 1 row, this function throws `GpuSplitAndRetryOOM` or
   * `CpuSplitAndRetryOOM`.
   *
   * Note how this function closes the input `spillable` that is passed in.
   *
   * @return a Seq[SpillableColumnarBatch] with 2 elements.
   */
  def splitSpillableInHalfByRows: SpillableColumnarBatch => Seq[SpillableColumnarBatch] = {
    (spillable: SpillableColumnarBatch) => {
      withResource(spillable) { _ =>
        val toSplitRows = spillable.numRows()
        if (toSplitRows <= 1) {
          throw new GpuSplitAndRetryOOM(
            s"GPU OutOfMemory: a batch of $toSplitRows cannot be split!")
        }
        val (firstHalf, secondHalf) = withResource(spillable.getColumnarBatch()) { src =>
          withResource(GpuColumnVector.from(src)) { tbl =>
            val splitIx = (tbl.getRowCount / 2).toInt
            withResource(tbl.contiguousSplit(splitIx)) { cts =>
              val tables = cts.map(_.getTable)
              withResource(tables.safeMap(GpuColumnVector.from(_, spillable.dataTypes))) {
                batches =>
                  val spillables = batches.safeMap { b =>
                    SpillableColumnarBatch(
                      GpuColumnVector.incRefCounts(b),
                      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
                  }
                  closeOnExcept(spillables) { _ =>
                    require(spillables.length == 2,
                      s"Contiguous split returned ${spillables.length} tables but two were " +
                          s"expected!")
                  }
                  (spillables.head, spillables.last)
              }
            }
          }
        }
        Seq(firstHalf, secondHalf)
      }
    }
  }

  private def splitTargetSizeInHalfInternal(
      target: AutoCloseableTargetSize, isGpu: Boolean): Seq[AutoCloseableTargetSize] = {
    withResource(target) { _ =>
      // If dataSize is known and smaller than targetSize/2, halving targetSize would still
      // exceed the data, making the retry a no-op. Use dataSize/2 instead to force
      // processing roughly half the data per attempt.
      val newTarget = if (target.dataSize > 0 && target.dataSize <= target.targetSize / 2) {
        target.dataSize / 2
      } else {
        target.targetSize / 2
      }
      if (newTarget < target.minSize) {
        if (isGpu) {
          throw new GpuSplitAndRetryOOM(
            s"GPU OutOfMemory: targetSize: ${target.targetSize} cannot be split further!" +
                s" minimum: ${target.minSize}")
        } else {
          throw new CpuSplitAndRetryOOM(
            s"CPU OutOfMemory: targetSize: ${target.targetSize} cannot be split further!" +
                s" minimum: ${target.minSize}")
        }
      }
      Seq(AutoCloseableTargetSize(newTarget, target.minSize, target.dataSize))
    }
  }

  /**
   * A common split function for an AutoCloseableTargetSize, which just divides the target size
   * in half, and creates a seq with just one element representing the new target size.
   * @return a Seq[AutoCloseableTargetSize] with 1 element.
   * @throws GpuSplitAndRetryOOM if it reaches the split limit.
   */
  def splitTargetSizeInHalfGpu: AutoCloseableTargetSize => Seq[AutoCloseableTargetSize] =
    (target: AutoCloseableTargetSize) => {
      splitTargetSizeInHalfInternal(target, true)
  }

  /**
   * A common split function for an AutoCloseableTargetSize, which just divides the target size
   * in half, and creates a seq with just one element representing the new target size.
   *
   * @return a Seq[AutoCloseableTargetSize] with 1 element.
   * @throws CpuSplitAndRetryOOM if it reaches the split limit.
   */
  def splitTargetSizeInHalfCpu: AutoCloseableTargetSize => Seq[AutoCloseableTargetSize] =
    (target: AutoCloseableTargetSize) => {
      splitTargetSizeInHalfInternal(target, false)
  }

  /**
   * Log memory footprint when GPU OOM or CPU OOM happens.
   */
  val BOOKKEEP_MEMORY: Boolean =
    java.lang.Boolean.getBoolean("ai.rapids.memory.bookkeep")
  // track the callstack for each memory allocation, don't enable it unless really needed
  val BOOKKEEP_MEMORY_CALLSTACK: Boolean =
    java.lang.Boolean.getBoolean("ai.rapids.memory.bookkeep.callstack")
  // By default, only print first time to avoid too much log
  val PRE_SPLIT_PRINT_ALL: Boolean =
    java.lang.Boolean.getBoolean("ai.rapids.memory.preSplit.printAll")
  var preSplitPrinted = false

  val threadCountBlockedUntilReady: AtomicInteger = new AtomicInteger(0)

  private def preSplitLogging(): Unit = synchronized { // use synchronized to keep neat
    if (!preSplitPrinted || PRE_SPLIT_PRINT_ALL) {
      log.info(s"Current threadCountBlockedUntilReady pre split: " +
        s"${threadCountBlockedUntilReady.get()}")

      logSpillFrameworkSummary()
      if (BOOKKEEP_MEMORY) {
        logMemoryBookkeeping()
      }
      logStacktrace()
      preSplitPrinted = true
    }
  }

  private def logSpillFrameworkSummary(): Unit = {
    // print spillable status
    logInfo(SpillFramework.getHostStoreSpillableSummary)
    logInfo(SpillFramework.getDeviceStoreSpillableSummary)
  }

  // For GPU/CPU SplitAndRetryOOM, we are very interested what each task is doing when one
  // of the tasks try to split and retry (in the context of WithRetryNoSplit).
  private def logStacktrace(): Unit = {
    val sb = new StringBuilder("<<Jstack Details>>\n\n")
    Thread.getAllStackTraces.forEach((thread: Thread, stackTrace: Array[StackTraceElement])
    => {
      // Print the thread name and its state
      sb.append(s"Thread: ${thread.getName} - State: ${thread.getState} " +
        s"- Thread ID: ${thread.getId}\n")
      // Print the stack trace for this thread
      for (element <- stackTrace) {
        sb.append(s"\tat $element")
      }
      sb.append("\n\n")
    })
    logInfo(sb.toString())
  }

  private def logMemoryBookkeeping(): Unit = { // use synchronized to keep neat
    // print host memory bookkeeping
    logInfo(HostAlloc.getHostAllocBookkeepSummary())

    // print device memory bookkeeping
    // TODO: uncomment this once we have device memory bookkeeping in spark-rapids-jni
    // logInfo(BaseDeviceMemoryBuffer.getDeviceMemoryBookkeepSummary)
  }
}

/**
 * This is a wrapper that turns a target size into an autocloseable to allow it to be used
 * in withRetry blocks.  It is intended to be used to help with cases where the split calculation
 * happens inside the retry block, and depends on the target size.  On a `GpuSplitAndRetryOOM` or
 * `CpuSplitAndRetryOOM`, a split policy like `splitTargetSizeInHalfGpu` or
 * `splitTargetSizeInHalfCpu` can be used to retry the block with a smaller target size.
 */
case class AutoCloseableTargetSize(targetSize: Long, minSize: Long,
    dataSize: Long = 0) extends AutoCloseable {
  def this(targetSize: Long, minSize: Long) = this(targetSize, minSize, 0)
  override def close(): Unit = ()
}

/**
 * This leverages a ThreadLocal of boolean to track if a task thread is currently
 * executing a retry. And the boolean state will be used by all the
 * `GpuExpressionRetryable`s to determine if the context is safe to retry the evaluation.
 */
object RetryStateTracker {
  private val localIsRetrying = new ThreadLocal[java.lang.Boolean]()
  // Track if the current thread is executing inside a retry framework block (withRetry*).
  // Use a depth counter to correctly handle nested retry blocks.
  private val localRetryBlockDepth = new ThreadLocal[java.lang.Integer]()
  // Gate retry-block tracking behind the retry coverage debug flag to avoid overhead when unused.
  private val trackRetryBlock: Boolean = AllocationRetryCoverageTracker.ENABLED

  def isCurThreadRetrying: Boolean = {
    val ret = localIsRetrying.get()
    ret != null && ret
  }

  /**
   * True if the current thread is executing inside a retry framework block (e.g. withRetry).
   */
  def isInRetryBlock: Boolean = {
    if (!trackRetryBlock) {
      false
    } else {
      val depth = localRetryBlockDepth.get()
      depth != null && depth.intValue() > 0
    }
  }

  def setCurThreadRetrying(retrying: Boolean): Unit = localIsRetrying.set(retrying)

  def clearCurThreadRetrying(): Unit = localIsRetrying.remove()

  /**
   * Mark entering a retry framework block on the current thread.
   */
  def enterRetryBlock(): Unit = {
    if (trackRetryBlock) {
      val depth = localRetryBlockDepth.get()
      if (depth == null) {
        localRetryBlockDepth.set(1)
      } else {
        localRetryBlockDepth.set(depth.intValue() + 1)
      }
    }
  }

  /**
   * Mark leaving a retry framework block on the current thread.
   */
  def exitRetryBlock(): Unit = {
    if (trackRetryBlock) {
      val depth = localRetryBlockDepth.get()
      if (depth == null || depth.intValue() <= 1) {
        localRetryBlockDepth.remove()
      } else {
        localRetryBlockDepth.set(depth.intValue() - 1)
      }
    }
  }
}

object SplitReason extends Enumeration {
  type SplitReason = Value
  val GPU_OOM, CPU_OOM, CUDF_OVERFLOW, NONE = Value
}
