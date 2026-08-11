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

package org.apache.spark.rapids.hybrid

import com.nvidia.spark.rapids.{GpuColumnVector, GpuMetric, GpuSemaphore, NvtxIdWithMetrics, NvtxRegistry}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.hybrid.{CoalesceBatchConverter => NativeConverter, HybridHostRetryAllocator, RapidsHostColumn}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * The Iterator wrapper of the underlying NativeConverter which produces the coalesced Batch of
 * HostColumnVectors. The iterator produces RapidsHostColumn instead of HostColumnVector for
 * carrying metadata about [Pinned|Pageable]MemCpy which are displayed as Spark SQL Metrics.
 */
class CoalesceConvertIterator(cpuScanIter: Iterator[ColumnarBatch],
                              targetBatchSizeInBytes: Long,
                              schema: StructType,
                              metrics: Map[String, GpuMetric])
  extends Iterator[Array[RapidsHostColumn]] with Logging {

  private var converterImpl: NativeConverter = _

  private var srcExhausted = false

  private val converterMetrics = Map(
    "C2COutputSize" -> GpuMetric.unwrap(metrics("C2COutputSize")))

  private def upstreamHasNext(): Boolean = {
    val startTime = System.nanoTime()
    val hasNext = cpuScanIter.hasNext
    metrics("HybridScanTime") += System.nanoTime() - startTime
    hasNext
  }

  private def upstreamNext(): ColumnarBatch = {
    val startTime = System.nanoTime()
    val batch = cpuScanIter.next()
    metrics("HybridScanTime") += System.nanoTime() - startTime
    batch
  }

  override def hasNext: Boolean = {
    // isDeckFilled means if there is unconverted source data remained on the deck.
    // hasProceedingBuilders means if there exists working target vectors not being flushed yet.
    val selfHoldData = Option(converterImpl).exists { c =>
      c.isDeckFilled || c.hasProceedingBuilders
    }
    // Check the srcExhausted at first, so as to minimize the potential cost of unnecessary call of
    // prev.hasNext
    lazy val upstreamHoldData = !srcExhausted && upstreamHasNext()
    // Either converter holds data or upstreaming iterator holds data.
    if (selfHoldData || upstreamHoldData) {
      return true
    }

    if (!srcExhausted) {
      srcExhausted = true
    }
    // Close the native Converter and dump column-level metrics for performance inspection.
    Option(converterImpl).foreach { c =>
      // VeloxBatchConverter collects the eclipsedTime of C2C_Conversion by itself.
      // Here we fetch the final value before closing it.
      metrics("C2CTime") += c.eclipsedNanoSecond
      // release the native instance when upstreaming iterator has been exhausted
      val detailedMetrics = c.close()
      val tID = TaskContext.get().taskAttemptId()
      logInfo(s"task[$tID] CoalesceNativeConverter finished:\n$detailedMetrics")
      converterImpl = null
    }
    false
  }

  override def next(): Array[RapidsHostColumn] = {
    require(!srcExhausted, "Please call hasNext in previous to ensure there are remaining data")

    // Initialize the nativeConverter with the first input batch
    if (converterImpl == null) {
      converterImpl = NativeConverter(
        upstreamNext(),
        targetBatchSizeInBytes,
        schema,
        converterMetrics,
        new HybridHostRetryAllocator()
      )
    }

    // Keeps consuming input batches of cpuScanIter until targetVectors reaches `targetBatchSize`
    // or cpuScanIter being exhausted.
    while (true) {
      val needFlush = if (upstreamHasNext()) {
        metrics("CpuReaderBatches") += 1
        // The only condition leading to a nonEmpty deck is targetVectors are unset after
        // the previous flushing
        if (converterImpl.isDeckFilled) {
          converterImpl.setupTargetVectors()
        }
        // tryAppendBatch, if failed which indicates the remaining space of targetVectors is NOT
        // enough the current input batch, then the batch will be placed on the deck and trigger
        // the flush of working targetVectors
        !converterImpl.tryAppendBatch(upstreamNext())
      } else {
        // If cpuScanIter is exhausted, then flushes targetVectors as the last output item.
        srcExhausted = true
        true
      }
      if (needFlush) {
        metrics("CoalescedBatches") += 1
        val rapidsHostBatch = converterImpl.flush()
        // It is essential to check and tidy up the deck right after flushing. Because if
        // the next call of cpuScanIter.hasNext will release the batch which the deck holds
        // its reference.
        if (converterImpl.isDeckFilled) {
          converterImpl.setupTargetVectors()
        }
        return rapidsHostBatch
      }
    }

    throw new RuntimeException("should NOT reach this line")
  }
}

object CoalesceConvertIterator extends Logging {
  /**
   * Consumes the RapidsHostBatchProducer and converts the HostColumnVectors to Device ones.
   */
  def hostToDevice(hostProducer: RapidsHostBatchProducer,
                   outputAttr: Seq[Attribute],
                   metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {

      private val dataTypes = outputAttr.map(_.dataType).toArray

      override def hasNext: Boolean = hostProducer.hasNext

      /**
       * At here, for consuming AsyncProducer effectively, we would like to achieve two things:
       * 1. Does NOT acquire GpuSemaphore until AsyncProducer is ready. It excludes the scenario
       * that the task thread which holds GpuSemaphore doing nothing but waiting.
       *
       * 2. Does NOT commit the consumption of next element until getting GpuSemaphore. It means
       * that the AsyncProducerThread will not be awakened until the previous element starts to
       * be transferred to device.
       */
      override def next(): ColumnarBatch = {
        // 1. Preparing Stage
        // Before waiting for asynchronous cpu task, releases the potential GpuSemaphore being
        // held by this thread. It may happen if there exists downstreaming nodes which coalesces
        // multiple input batches (such as Aggregate).
        Option(TaskContext.get()).foreach { ctx =>
          GpuSemaphore.releaseIfNecessary(ctx)
        }
        // Firstly, waits for the next host batch being ready
        hostProducer.waitForNext()
        // Then, acquires GpuSemaphore
        Option(TaskContext.get()).foreach { ctx =>
          NvtxIdWithMetrics(NvtxRegistry.GPU_ACQUIRE_C2C, metrics("GpuAcquireTime")) {
            GpuSemaphore.acquireIfNecessary(ctx)
          }
        }
        // Finally, take the ownership of the next host batch, which might awake the asynchronous
        // producer if the producer thread was stuck by the full lock.
        val hostColumns = hostProducer.takeNext
        val rowCount = hostColumns.head.vector.getRowCount.toInt

        // 2. Transferring Stage
        val deviceVectors: Array[ColumnVector] = hostColumns.zip(dataTypes).safeMap {
          case (RapidsHostColumn(hcv, isPinned, totalBytes), dt) =>
            withResource(hcv) { _ =>
              if (isPinned) {
                metrics("PinnedH2DSize") += totalBytes
                NvtxIdWithMetrics(NvtxRegistry.PINNED_H2D, metrics("PinnedH2DTime")) {
                  GpuColumnVector.from(hcv.copyToDevice(), dt)
                }
              } else {
                metrics("PageableH2DSize") += totalBytes
                NvtxIdWithMetrics(NvtxRegistry.PAGEABLE_H2D, metrics("PageableH2DTime")) {
                  GpuColumnVector.from(hcv.copyToDevice(), dt)
                }
              }
            }
        }

        new ColumnarBatch(deviceVectors, rowCount)
      }
    }
  }
}
