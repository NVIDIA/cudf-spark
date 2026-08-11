/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet

import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.ThreadFactoryBuilder

import org.apache.spark.internal.Logging

/**
 * Executor-wide partition-file read histograms.
 *
 * Reader worker threads add one sample after a partition file finishes reading. A single
 * daemon thread copies the executor-lifetime samples into temporary buffers every 10 seconds and
 * logs throughput and filtered-size distributions. Reporting never removes original samples, so
 * every snapshot describes all partition files completed by that executor so far.
 */
private[rapids] object PartitionFileReadHistogram extends Logging {
  private val ReportIntervalSeconds = 10L
  private val BytesPerMiB = 1024.0 * 1024.0

  private case class ReadSample(filteredBytes: Long, readNanos: Long)

  private[parquet] case class HistogramSummary(
      count: Int,
      min: Double,
      p50: Double,
      p75: Double,
      p95: Double,
      max: Double)

  private val samples = new ConcurrentLinkedQueue[ReadSample]()
  private val reportLock = new Object

  @volatile private var executorId = "unknown"
  @volatile private var reporter: ScheduledExecutorService = _

  def initialize(id: String): Unit = synchronized {
    if (reporter == null) {
      executorId = id
      reporter = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
          .setNameFormat("partition-file-read-histogram-reporter-%d")
          .setDaemon(true)
          .build())
      reporter.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit = {
            try {
              report(logEmptyWindow = true)
            } catch {
              case NonFatal(error) =>
                logWarning("Unable to report partition-file read histograms", error)
            }
          }
        },
        ReportIntervalSeconds,
        ReportIntervalSeconds,
        TimeUnit.SECONDS)
    }
  }

  /**
   * Add one completed partition file to the executor's current reporting window.
   *
   * `filteredBytes` is the data remaining after footer/row-group filtering and is also the
   * throughput numerator. `readNanos` covers reading and reconstructing that filtered data.
   */
  def record(filteredBytes: Long, readNanos: Long): Unit = {
    if (filteredBytes > 0 && readNanos > 0) {
      samples.add(ReadSample(filteredBytes, readNanos))
    }
  }

  def shutdown(): Unit = synchronized {
    if (reporter != null) {
      reporter.shutdownNow()
      reporter = null
      report(logEmptyWindow = false)
    }
  }

  private def report(logEmptyWindow: Boolean): Unit = reportLock.synchronized {
    val snapshot = copySamples()
    if (snapshot.nonEmpty) {
      val throughputs = snapshot.map { sample =>
        sample.filteredBytes.toDouble * 1000000000.0 / sample.readNanos / BytesPerMiB
      }.toArray
      val sizes = snapshot.map(_.filteredBytes.toDouble / BytesPerMiB).toArray
      logHistogram("PARTITION_FILE_THROUGHPUT_HISTOGRAM", "MiB/s", summarize(throughputs))
      logHistogram(
        "PARTITION_FILE_SIZE_HISTOGRAM", "MiB", summarize(sizes), " sizeType=filtered")
    } else if (logEmptyWindow) {
      logInfo(s"PARTITION_FILE_THROUGHPUT_HISTOGRAM executorId=$executorId " +
        s"reportIntervalSeconds=$ReportIntervalSeconds scope=executorLifetime count=0")
      logInfo(s"PARTITION_FILE_SIZE_HISTOGRAM executorId=$executorId " +
        s"reportIntervalSeconds=$ReportIntervalSeconds scope=executorLifetime " +
        s"count=0 sizeType=filtered")
    }
  }

  /** Copy a weakly consistent snapshot without removing any executor-lifetime samples. */
  private def copySamples(): ArrayBuffer[ReadSample] = {
    val copied = new ArrayBuffer[ReadSample]
    val iterator = samples.iterator()
    while (iterator.hasNext) {
      copied += iterator.next()
    }
    copied
  }

  private def logHistogram(
      name: String,
      unit: String,
      summary: HistogramSummary,
      extraFields: String = ""): Unit = {
    logInfo(f"$name executorId=$executorId reportIntervalSeconds=$ReportIntervalSeconds " +
      f"scope=executorLifetime count=${summary.count} unit=$unit$extraFields " +
      f"min=${summary.min}%.2f p50=${summary.p50}%.2f p75=${summary.p75}%.2f " +
      f"p95=${summary.p95}%.2f max=${summary.max}%.2f")
  }

  private[parquet] def summarize(values: Array[Double]): HistogramSummary = {
    require(values.nonEmpty, "cannot summarize an empty histogram")
    java.util.Arrays.sort(values)
    HistogramSummary(
      values.length,
      values.head,
      percentile(values, 0.50),
      percentile(values, 0.75),
      percentile(values, 0.95),
      values.last)
  }

  private def percentile(sorted: Array[Double], fraction: Double): Double = {
    sorted(Math.ceil(sorted.length * fraction).toInt - 1)
  }
}
