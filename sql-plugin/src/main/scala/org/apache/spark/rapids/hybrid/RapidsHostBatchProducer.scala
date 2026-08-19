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

package org.apache.spark.rapids.hybrid

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuMetric, NvtxIdWithMetrics, NvtxRegistry}
import com.nvidia.spark.rapids.hybrid.RapidsHostColumn
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * The iterator-like interface for RapidsHostColumns breaks down the `next` method into two
 * methods: `waitForNext` and `takeNext`. With these two methods, the interface can control the
 * internal buffer more precisely than normal Iterators, so as to achieve the more precise (host)
 * memory control.
 *
 * RapidsHostBatchProducer only works for the scenario of single producer <-> single consumer,
 * which already meets the needs of HybridParquetScan pipelines. And its all methods should be only
 * called by the consumer thread.
 */
trait RapidsHostBatchProducer {
  // Same as Iterator.hasNext
  def hasNext: Boolean

  // Wait until the producer completes the production of the element for the next consumption.
  // It means that the waiting would actually take place when the producer buffers nothing for now.
  // The method works based on the assumption there is only one consumer in current.
  def waitForNext(): Unit

  // Consume the next element which should be already cached in the internal buffer.
  def takeNext: Array[RapidsHostColumn]
}

/**
 * The dummy implementation simply bypasses the base iterator.
 */
class SyncHostBatchProducer(
    base: Iterator[Array[RapidsHostColumn]]) extends RapidsHostBatchProducer {

  private lazy val deck = mutable.Queue.empty[Array[RapidsHostColumn]]

  override def hasNext: Boolean = base.hasNext

  override def waitForNext(): Unit = {
    if (deck.isEmpty) {
      deck.enqueue(base.next())
    }
  }

  override def takeNext: Array[RapidsHostColumn] = {
    require(deck.nonEmpty, "The deck is NOT filled")
    deck.dequeue()
  }
}

/**
 * The asynchronous single-thread producer runs ParquetScanIterator in an asynchronous thread and
 * caches the result with a simple RingBuffer whose capacity is strictly limited.
 *
 * We do not use BlockingQueue as RingBuffer, because it may stack N + 1 elements for the queue of
 * capacity(N), since the blocking occurs at the end of the production (when we put the element to
 * the queue) rather than at the beginning of the production. Considering that each element is
 * usually very large, we would like to minimize the capacity of buffer for the most of time. In
 * order to support the real size=1 buffer, we implement a very simple RingBuffer.
 *
 * In addition, the async producer is not implemented as a standard Iterator because we would like
 * to split the Future.get into Future.wait and Buffer.take. By doing that, we can reduce host
 * memory overhead by putting off Buffer.take. For more details about why it is necessary,
 * please check the code of consumer: CoalesceConvertIterator.hostDevice.
 */
class PrefetchHostBatchProducer(
    taskAttId: Long,
    base: Iterator[Array[RapidsHostColumn]],
    capacity: Int,
    waitTimeMetric: GpuMetric) extends RapidsHostBatchProducer with Logging {

  @volatile private var isInit: Boolean = false
  // Mark if there is in-progress element being produced in producerThread
  @volatile private var isProducing: Boolean = false

  // The two pointers of the RingBuffer. For the simplicity, indices themselves increment
  // monotonously. And the actual index of ring buffer is `index % capacity`.
  @volatile private var readIndex: Int = 0
  @volatile private var writeIndex: Int = 0
  // Record the total batches. Also serve as the end mask of both producer and consumer.
  @volatile private var totalBatches: Int = 0

  // This lock guarantees that the status change of isProducing and the check of isProducing
  // can NOT be executed currently.
  private lazy val isInProgressLock = new ReentrantLock()
  // Asleep the consumerThread if the buffer is empty.
  // Awake the consumerThread as soon as the next element being ready.
  private lazy val emptyLock = new ReentrantLock()
  // Asleep the producerThread if the buffer is full.
  // Awake the producerThread as soon as elements being consumed.
  private lazy val fullLock = new ReentrantLock()

  private var producerThread: Thread = _

  private lazy val buffer: Array[Either[Throwable, Array[RapidsHostColumn]]] = {
    Array.ofDim[Either[Throwable, Array[RapidsHostColumn]]](capacity)
  }

  private lazy val produceFn: Runnable = new Runnable {

    // This context will be got in the main Thread during the initialization of `produceFn`
    private val taskContext: TaskContext = TaskContext.get()

    override def run(): Unit = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskAttId)
      isInProgressLock.lock()
      try {
        do {
          isProducing = true
          isInProgressLock.unlock()
          // Wait for empty slot of RingBuffer
          do {
            fullLock.synchronized {
              if (writeIndex - readIndex == capacity) {
                fullLock.wait()
              }
            }
          } while (writeIndex - readIndex == capacity)
          // Trigger the base iterator
          buffer(writeIndex % capacity) = Right(base.next())
          // Update the writeIndex and try to awake the consumer thread which calls waitForNext
          emptyLock.synchronized {
            writeIndex += 1
            emptyLock.notify()
          }

          isInProgressLock.lock()
          isProducing = false
          logInfo(s"[$taskAttId] PrefetchIterator produced $writeIndex batches, " +
            s"currently preloaded batchNum: ${writeIndex - readIndex}"
          )
        }
        while (base.hasNext)
        isInProgressLock.unlock()
      } catch {
        case ex: Throwable =>
          // transfer the exception info to the main thread as an interrupted signal
          buffer(writeIndex % capacity) = Left(ex)
          writeIndex += 1
          isProducing = false
          if (isInProgressLock.isHeldByCurrentThread) {
            isInProgressLock.unlock()
          }
          throw new RuntimeException(ex)
      } finally {
        totalBatches = writeIndex
        logInfo(s"[$taskAttId] PrefetchIterator finished all jobs($totalBatches batches)")
        RmmSpark.poolThreadFinishedForTask(taskAttId)
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def hasNext: Boolean = {
    // Lazy Init of the actual producer
    if (!isInit) {
      NvtxIdWithMetrics(NvtxRegistry.WAIT_FOR_CPU, waitTimeMetric) {
        if (!base.hasNext) {
          return false
        }
        isInit = true
        isProducing = true
        producerThread = new Thread(produceFn, s"HybridPrefetch_TID_$taskAttId")
        producerThread.start()
        return true
      }
    }
    // Firstly, check the RingBuffer. If empty, then check if there is any in-progress element.
    writeIndex > readIndex || {
      isInProgressLock.lock()
      // Need to check the RingBuffer again.
      val ret = writeIndex > readIndex || isProducing
      isInProgressLock.unlock()
      ret
    }
  }

  override def waitForNext(): Unit = {
    // Return if buffer is not empty
    if (writeIndex > readIndex) {
      return
    }
    // Waiting for "emptyLock"
    NvtxIdWithMetrics(NvtxRegistry.WAIT_FOR_CPU, waitTimeMetric) {
      do {
        emptyLock.synchronized {
          if (writeIndex == readIndex) {
            emptyLock.wait()
          }
        }
      } while (writeIndex == readIndex)
    }
  }

  override def takeNext: Array[RapidsHostColumn] = {
    require(writeIndex > readIndex, "The RingBuffer is EMPTY")

    buffer(readIndex % capacity) match {
      case Left(ex: Throwable) =>
        logError(s"[$taskAttId] PrefetchIterator: AsyncProducer failed with exceptions")
        throw new RuntimeException(s"[$taskAttId] PrefetchIterator", ex)
      case Right(ret: Array[RapidsHostColumn]) =>
        // Update the readIndex and try to awake the producer thread
        fullLock.synchronized {
          readIndex += 1
          fullLock.notify()
        }
        logInfo(s"[$taskAttId] PrefetchIterator consumed $readIndex batches, " +
          s"currently preloaded batchNum: ${writeIndex - readIndex}")
        if (totalBatches == readIndex) {
          logInfo(s"[$taskAttId] PrefetchIterator exited")
        }
        ret
    }
  }
}
