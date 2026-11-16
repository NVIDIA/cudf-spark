/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}

object NvtxWithMetrics {
  def apply(name: String, color: NvtxColor, metric: Option[GpuMetric]): NvtxRange = {
    metric match {
      case Some(m) => new NvtxWithMetrics(name, color, m)
      case _ => new NvtxRange(name, color)
    }
  }
}

/**
 *  NvtxRange with option to pass one or more nano timing metric(s) that are updated upon close
 *  by the amount of time spent in the range
 */
class NvtxWithMetrics(name: String, color: NvtxColor, val metrics: Seq[GpuMetric],
    excludeMetric: Seq[GpuMetric] = Seq.empty)
  extends NvtxRange(name, color) {

  // add a convenient constructor
  def this(name: String, color: NvtxColor, metrics: GpuMetric*) =
    this(name, color, metrics.toSeq)

  private val metricRange = new MetricRange(metrics, excludeMetric)

  def needTracks: Seq[Boolean] = metricRange.needTracks

  override def close(): Unit = {
    try {
      metricRange.close()
    } finally {
      super.close()
    }
  }
}

/**
 *  NvtxId with option to pass one or more nano timing metric(s) that are updated upon close
 *  by the amount of time spent in the range
 */
object NvtxIdWithMetrics {

  def apply[V](nvtxId: NvtxId, metric: GpuMetric)(block: => V): V = {
    metric.ns {
      nvtxId.apply(block)
    }
  }

  def apply[V](nvtxId: NvtxId, metrics: GpuMetric*)(block: => V): V = {
    GpuMetric.ns(metrics, Seq.empty) {
      nvtxId.apply(block)
    }
  }
}

class MetricRange(val metrics: Seq[GpuMetric], val excludeMetric: Seq[GpuMetric] = Seq.empty)
  extends AutoCloseable {

  // add a convenient constructor
  def this(metrics: GpuMetric*) = this(metrics.toSeq)

  private val metricCount = metrics.length
  private val singleMetric = if (metricCount == 1) metrics.head else null
  private val singleActive = singleMetric != null && singleMetric.tryActivateTimer(excludeMetric)
  // Allocate tracking flags only when a multiple-metric range activates at least one timer.
  private val multipleTracks: Array[Boolean] = {
    var tracks: Array[Boolean] = null
    if (metricCount > 1) {
      val iterator = metrics.iterator
      var index = 0
      while (iterator.hasNext) {
        if (iterator.next().tryActivateTimer(excludeMetric)) {
          if (tracks == null) {
            tracks = new Array[Boolean](metricCount)
          }
          tracks(index) = true
        }
        index += 1
      }
    }
    tracks
  }
  private val hasActiveTimer = singleActive || multipleTracks != null
  private val start = if (hasActiveTimer) System.nanoTime() else 0L

  // Preserve the existing accessor without allocating a Boolean collection on the timing path.
  def needTracks: Seq[Boolean] = {
    if (metricCount == 1) {
      Seq(singleActive)
    } else if (multipleTracks == null) {
      Seq.fill(metricCount)(false)
    } else {
      multipleTracks.toSeq
    }
  }

  override def close(): Unit = {
    if (hasActiveTimer) {
      val time = System.nanoTime() - start
      if (singleActive) {
        singleMetric.deactivateTimer(time, excludeMetric)
      } else {
        val iterator = metrics.iterator
        var index = 0
        while (iterator.hasNext) {
          val metric = iterator.next()
          if (multipleTracks(index)) {
            metric.deactivateTimer(time, excludeMetric)
          }
          index += 1
        }
      }
    }
  }
}
