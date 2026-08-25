/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import scala.collection.immutable.TreeMap

import com.nvidia.spark.rapids.metrics.GpuBubbleTimerManager

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuTaskMetrics

/**
 * Trait for expressions that can have metrics injected after binding.
 * This allows execution nodes to inject their metrics into expressions
 * without coupling the expression construction to metric availability.
 * Generally this should happen when a expression is bound, and will
 * not be modified after that.
 */
trait GpuMetricsInjectable {
  /**
   * Inject metrics into this expression. Called after binding but before execution.
   * Metrics injection is not always guaranteed to happen, also the node in the
   * plan may not have added the metric that this expression expects. Generally if this
   * method is not called or the metric this expression wants is not available, then
   * the issue should be logged, but no exception should be thrown.
   * @param metrics Map of metric names to GpuMetric instances
   */
  def injectMetrics(metrics: Map[String, GpuMetric]): Unit
}

sealed class MetricsLevel(val num: Integer) extends Serializable {
  def >=(other: MetricsLevel): Boolean =
    num >= other.num
}

object MetricsLevel {
  def apply(str: String): MetricsLevel = str match {
    case "ESSENTIAL" => GpuMetric.ESSENTIAL_LEVEL
    case "MODERATE" => GpuMetric.MODERATE_LEVEL
    case _ => GpuMetric.DEBUG_LEVEL
  }
}

class GpuMetricFactory(metricsConf: MetricsLevel, context: SparkContext) {

  private [this] def createInternal(level: MetricsLevel, f: => SQLMetric): GpuMetric = {
    if (level >= metricsConf) {
      // only enable companion metrics (excluding semaphore wait time) for DEBUG_LEVEL
      WrappedGpuMetric(f, withMetricsExclSemWait = GpuMetric.DEBUG_LEVEL >= metricsConf)
    } else {
      NoopMetric
    }
  }

  def create(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createMetric(context, name))

  def createNanoTiming(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createNanoTimingMetric(context, name))

  def createSize(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createSizeMetric(context, name))

  def createAverage(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createAverageMetric(context, name))

  def createTiming(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createTimingMetric(context, name))
}

object GpuMetric extends Logging {
  // Metric names.
  val BUFFER_TIME = "bufferTime"
  val BUFFER_TIME_BUBBLE = "bufferTimeBubble"
  val SCAN_TIME = "scanTime"
  val COPY_BUFFER_TIME = "copyBufferTime"
  val GPU_DECODE_TIME = "gpuDecodeTime"
  val NUM_INPUT_ROWS = "numInputRows"
  val NUM_INPUT_BATCHES = "numInputBatches"
  val NUM_OUTPUT_ROWS = "numOutputRows"
  val NUM_OUTPUT_BATCHES = "numOutputBatches"
  val PARTITION_SIZE = "partitionSize"
  val NUM_PARTITIONS = "numPartitions"
  val OP_TIME_LEGACY = "opTimeLegacy"
  val OP_TIME_NEW = "opTimeNew"
  val OP_TIME_NEW_SHUFFLE_WRITE = "opTimeNewShuffleWrite"
  val OP_TIME_NEW_SHUFFLE_READ = "opTimeNewShuffleRead"
  val COLLECT_TIME = "collectTime"
  val CONCAT_TIME = "concatTime"
  val SORT_TIME = "sortTime"
  val REPARTITION_TIME = "repartitionTime"
  val AGG_TIME = "computeAggTime"
  val JOIN_TIME = "joinTime"
  val FILTER_TIME = "filterTime"
  val FILTER_TIME_BUBBLE = "filterTimeBubble"
  val SCHEDULE_TIME = "scheduleTime"
  val SCHEDULE_TIME_BUBBLE = "scheduleTimeBubble"
  val BUILD_DATA_SIZE = "buildDataSize"
  val BUILD_TIME = "buildTime"
  val STREAM_TIME = "streamTime"
  val NUM_TASKS_FALL_BACKED = "numTasksFallBacked"
  val NUM_TASKS_REPARTITIONED = "numTasksRepartitioned"
  val NUM_TASKS_SKIPPED_AGG = "numTasksSkippedAgg"
  val READ_FS_TIME = "readFsTime"
  val WRITE_BUFFER_TIME = "writeBufferTime"
  val FILECACHE_FOOTER_HITS = "filecacheFooterHits"
  val FILECACHE_FOOTER_HITS_SIZE = "filecacheFooterHitsSize"
  val FILECACHE_FOOTER_MISSES = "filecacheFooterMisses"
  val FILECACHE_FOOTER_MISSES_SIZE = "filecacheFooterMissesSize"
  val FILECACHE_DATA_RANGE_HITS = "filecacheDataRangeHits"
  val FILECACHE_DATA_RANGE_HITS_SIZE = "filecacheDataRangeHitsSize"
  val FILECACHE_DATA_RANGE_MISSES = "filecacheDataRangeMisses"
  val FILECACHE_DATA_RANGE_MISSES_SIZE = "filecacheDataRangeMissesSize"
  val FILECACHE_FOOTER_READ_TIME = "filecacheFooterReadTime"
  val FILECACHE_DATA_RANGE_READ_TIME = "filecacheDataRangeReadTime"
  val DELETION_VECTOR_SCATTER_TIME = "deletionVectorScatterTime"
  val DELETION_VECTOR_SIZE = "deletionVectorSize"
  val CPU_BRIDGE_PROCESSING_TIME = "cpuBridgeProcessingTime"
  val CPU_BRIDGE_WAIT_TIME = "cpuBridgeWaitTime"
  val COPY_TO_HOST_TIME = "d2hMemCopyTime"
  val READ_THROTTLING_TIME = "readThrottlingTime"
  val SMALL_JOIN_COUNT = "sizedSmallJoin"
  val BIG_JOIN_COUNT = "sizedBigJoin"
  val SYNC_READ_TIME = "shuffleSyncReadTime"
  val ASYNC_READ_TIME = "shuffleAsyncReadTime"
  val ICEBERG_BUILD_ACTION_TIME = "icebergBuildActionTime"
  val ICEBERG_POST_PROCESS_TIME = "icebergPostProcessTime"

  // Metric Descriptions.
  val DESCRIPTION_BUFFER_TIME = "buffer time"
  val DESCRIPTION_BUFFER_TIME_BUBBLE = "buffer time (GPU underloaded)"
  val DESCRIPTION_COPY_BUFFER_TIME = "copy buffer time"
  val DESCRIPTION_GPU_DECODE_TIME = "GPU decode time"
  val DESCRIPTION_NUM_INPUT_ROWS = "input rows"
  val DESCRIPTION_NUM_INPUT_BATCHES = "input columnar batches"
  val DESCRIPTION_NUM_OUTPUT_ROWS = "output rows"
  val DESCRIPTION_NUM_OUTPUT_BATCHES = "output columnar batches"
  val DESCRIPTION_PARTITION_SIZE = "partition data size"
  val DESCRIPTION_NUM_PARTITIONS = "partitions"
  val DESCRIPTION_OP_TIME_LEGACY = "op time (legacy)"
  val DESCRIPTION_OP_TIME_NEW = "op time"
  val DESCRIPTION_OP_TIME_NEW_SHUFFLE_WRITE = "op time (shuffle write partitioning)"
  val DESCRIPTION_OP_TIME_NEW_SHUFFLE_READ = "op time (shuffle read)"
  val DESCRIPTION_COLLECT_TIME = "collect batch time"
  val DESCRIPTION_CONCAT_TIME = "concat batch time"
  val DESCRIPTION_SORT_TIME = "sort time"
  val DESCRIPTION_REPARTITION_TIME = "repartition time"
  val DESCRIPTION_AGG_TIME = "aggregation time"
  val DESCRIPTION_JOIN_TIME = "join time"
  val DESCRIPTION_FILTER_TIME = "filter time"
  val DESCRIPTION_FILTER_TIME_BUBBLE = "filter time (GPU underloaded)"
  val DESCRIPTION_SCHEDULE_TIME = "I/O schedule time"
  val DESCRIPTION_SCHEDULE_TIME_BUBBLE = "I/O schedule time (GPU underloaded)"
  val DESCRIPTION_SCAN_TIME = "scan time"
  val DESCRIPTION_BUILD_DATA_SIZE = "build side size"
  val DESCRIPTION_BUILD_TIME = "build time"
  val DESCRIPTION_STREAM_TIME = "stream time"
  val DESCRIPTION_NUM_TASKS_FALL_BACKED = "number of sort fallback tasks"
  val DESCRIPTION_NUM_TASKS_REPARTITIONED = "number of tasks repartitioned for agg"
  val DESCRIPTION_NUM_TASKS_SKIPPED_AGG = "number of tasks skipped aggregation"
  val DESCRIPTION_READ_FS_TIME = "time to read fs data"
  val DESCRIPTION_WRITE_BUFFER_TIME = "time to write data to buffer"
  val DESCRIPTION_FILECACHE_FOOTER_HITS = "cached footer hits"
  val DESCRIPTION_FILECACHE_FOOTER_HITS_SIZE = "cached footer hits size"
  val DESCRIPTION_FILECACHE_FOOTER_MISSES = "cached footer misses"
  val DESCRIPTION_FILECACHE_FOOTER_MISSES_SIZE = "cached footer misses size"
  val DESCRIPTION_FILECACHE_DATA_RANGE_HITS = "cached data hits"
  val DESCRIPTION_FILECACHE_DATA_RANGE_HITS_SIZE = "cached data hits size"
  val DESCRIPTION_FILECACHE_DATA_RANGE_MISSES = "cached data misses"
  val DESCRIPTION_FILECACHE_DATA_RANGE_MISSES_SIZE = "cached data misses size"
  val DESCRIPTION_FILECACHE_FOOTER_READ_TIME = "cached footer read time"
  val DESCRIPTION_FILECACHE_DATA_RANGE_READ_TIME = "cached data read time"
  val DESCRIPTION_DELETION_VECTOR_SCATTER_TIME = "deletion vector scatter time"
  val DESCRIPTION_DELETION_VECTOR_SIZE = "deletion vector size"
  val DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME = "CPU bridge processing time"
  val DESCRIPTION_CPU_BRIDGE_WAIT_TIME = "CPU bridge elapsed time"
  val DESCRIPTION_COPY_TO_HOST_TIME = "deviceToHost memory copy time"
  val DESCRIPTION_READ_THROTTLING_TIME = "read throttling time"

  val DESCRIPTION_SMALL_JOIN_COUNT = "small joins"
  val DESCRIPTION_BIG_JOIN_COUNT = "big joins"
  val DESCRIPTION_SYNC_READ_TIME = "sync read time"
  val DESCRIPTION_ASYNC_READ_TIME = "async read time"
  val DESCRIPTION_ICEBERG_BUILD_ACTION_TIME = "iceberg build action tree time"
  val DESCRIPTION_ICEBERG_POST_PROCESS_TIME = "iceberg post process time"

  /**
   * Determine if a GpuMetric wraps a TimingMetric or NanoTimingMetric.
   *
   * NOTE: SQLMetrics.NS_TIMING_METRIC and SQLMetrics.TIMING_METRIC is private, so we have to use
   * the string directly
   */
  def isTimeMetric(input: GpuMetric): Boolean = input match {
    case w: WrappedGpuMetric =>
      w.sqlMetric.metricType.toLowerCase.contains("timing")
    case _ =>
      false
  }

  def unwrap(input: GpuMetric): SQLMetric = input match {
    case w :WrappedGpuMetric => w.sqlMetric
    case i => throw new IllegalArgumentException(s"found unsupported GpuMetric ${i.getClass}")
  }

  def unwrap(input: Map[String, GpuMetric]): Map[String, SQLMetric] = {
    val ret = input.collect {
      // remove the metrics that are not registered
      case (k, w) if w != NoopMetric => (k, unwrap(w))
    }
    val companions = input.collect {
      // add the companions
      case (k, w) if w.companionGpuMetric.isDefined =>
        (k + "_exSemWait", unwrap(w.companionGpuMetric.get))
    }

    TreeMap.apply((ret ++ companions).toSeq: _*)
  }

  def wrap(input: SQLMetric): GpuMetric = WrappedGpuMetric(input)

  def wrap(input: Map[String, SQLMetric]): Map[String, GpuMetric] = input.map {
    case (k, v) => (k, wrap(v))
  }

  def ns[T](metrics: GpuMetric*)(f: => T): T = {
    ns(metrics, Seq.empty)(f)
  }

  def ns[T](metrics: Seq[GpuMetric], excludeMetrics: Seq[GpuMetric])(f: => T): T = {
    val initedMetrics = metrics.map(m => (m, m.tryActivateTimer(excludeMetrics)))
    val start = System.nanoTime()
    try {
      f
    } finally {
      val taken = System.nanoTime() - start
      initedMetrics.foreach { case (m, isTrack) =>
        if (isTrack) m.deactivateTimer(taken, excludeMetrics)
      }
    }
  }

  /**
   * Time a block of code with an optional metric. If the metric is None, the code
   * is executed without timing.
   * 
   * @param metric Optional metric to record timing to
   * @param f The code block to execute and time
   * @return The result of executing f
   */
  def nsOption[T](metric: Option[GpuMetric])(f: => T): T = {
    metric match {
      case Some(m) => m.ns(f)
      case None => f
    }
  }

  /**
   * Time a block of code with an optional metric, excluding time from other metrics.
   * If the metric is None, the code is executed without timing.
   * 
   * @param metric Optional metric to record timing to
   * @param excludeMetrics Metrics whose time should be excluded from the timing
   * @param f The code block to execute and time
   * @return The result of executing f
   */
  def nsOption[T](metric: Option[GpuMetric], excludeMetrics: Seq[GpuMetric])(f: => T): T = {
    metric match {
      case Some(m) => m.ns(excludeMetrics)(f)
      case None => f
    }
  }

  /**
   * Give compute block, track its GpuSemaphore withholding time along with the wall time.
   *
   * The computation of semaphoreHoldingTime is a typical interval problem. There are 3 kinds of
   * semaphore holding intervals:
   *  1. The partial lead one, only the tail part is overlapped with the compute block.
   *
   *  2. The complete ones, which are fully overlapped with the compute block. The complete
   *     intervals can be got from the difference of `getSemaphoreHoldingTime`, since
   *     semaphoreHoldingTime will only be updated when the semaphore is released.
   *
   *  3. The partial lag one, only the head part is overlapped with the compute block.
   *
   * Consider the timeline and the relevant durations based on the following diagram style:
   *
   * Acq.     Rel.   Acq.        Rel.             Acq.          Rel.
   *  |--------|      |-----------|                |-------------|
   *      |----------------------------------------------| (Compute Block)
   *      <---->      <----------->                <----->
   *       (1)             (2)                       (3)
   */
  def withSemaphoreTime[T](
      wallTime: GpuMetric,
      semTime: GpuMetric,
      ctx: TaskContext)(f: => T): T = {
    val start = System.nanoTime()
    val semStart = GpuTaskMetrics.get.getSemaphoreHoldingTime
    // Compute the preBlockHead if there exists a partial lead interval. The preBlockHead will
    // be explicitly subtracted in final:
    //   Sem.Acq                                 Sem.Rel
    //      |---------------------------------------| (Partial Lead Interval)
    //                     |--------------------------------------| (Compute Block)
    //        preBlockHead
    //      <-------------->
    val preBlockHead = GpuSemaphore.getLastSemAcqAndRelTime(ctx) match {
      case (acqTime, relTime) if acqTime > relTime && acqTime < start =>
        start - acqTime
      case _ =>
        0L
    }

    try {
      f
    } finally {
      val end = System.nanoTime()
      val semEnd = GpuTaskMetrics.get.getSemaphoreHoldingTime
      // Compute the partial tail if there exists a partial lag interval. The partial tail will
      // be explicitly added in final:
      //                              Sem.Acq           Sem.Rel
      //                                 |-----------------| (Partial Lag Interval)
      //      |--------------------------------------| (Compute Block)
      //                                  partialTail
      //                                 <----------->
      val partialTail = GpuSemaphore.getLastSemAcqAndRelTime(ctx) match {
        case (acqTime, relTime) if acqTime > relTime =>
          // directly minus acqTime instead of `acqTime max start` because the ahead part has
          // been subtracted ahead of time (`semHoldTimeBefore`)
          end - acqTime
        case _ =>
          0L
      }
      wallTime.add(System.nanoTime() - start)
      semTime.add(semEnd - semStart - preBlockHead + partialTail)
    }
  }

  /**
   * Given a compute block, track its GpuBubbleTime along with the wall time.
   * GpuBubbleTime is the time when the GPU is NOT fully utilized during the compute block,
   * which is useful to identify CPU/IO-bound bottlenecks preventing full GPU utilization.
   */
  def gpuBubbleTime[T](bubbleTime: GpuMetric,
      wallTime: Option[GpuMetric] = None)(f: => T): T = {
    // start a new Timer
    val timer = GpuBubbleTimerManager.newTimer()
    val start = System.nanoTime()
    try {
      f
    } finally {
      val end = System.nanoTime()
      wallTime.foreach(_.add(end - start))
      // settle the timer and accumulate the bubble time
      bubbleTime += timer.close(end)
    }
  }

  object DEBUG_LEVEL extends MetricsLevel(0)
  object MODERATE_LEVEL extends MetricsLevel(1)
  object ESSENTIAL_LEVEL extends MetricsLevel(2)

  /**
   * Inject metrics into expressions that implement GpuMetricsInjectable.
   * Walks the expression tree and injects metrics into any expressions that support it.
   * 
   * @param expressions The expressions to inject metrics into
   * @param metrics Map of metric names to GpuMetric instances
   */
  def injectMetrics(expressions: Seq[Expression], metrics: Map[String, GpuMetric]): Unit = {
    expressions.foreach(injectMetricsIntoExpression(_, metrics))
  }

  /**
   * Inject metrics into a single expression tree.
   */
  private def injectMetricsIntoExpression(expr: Expression, 
      metrics: Map[String, GpuMetric]): Unit = {
    expr match {
      case injectable: GpuMetricsInjectable =>
        injectable.injectMetrics(metrics)
      case _ => // No metrics to inject
    }
    // Recursively inject into children
    expr.children.foreach(injectMetricsIntoExpression(_, metrics))
  }
}

sealed abstract class GpuMetric extends Serializable {
  def value: Long
  def set(v: Long): Unit
  def +=(v: Long): Unit
  def add(v: Long): Unit

  private var isTimerActive = false

  // For timing GpuMetrics, we additionally create a companion GpuMetric to track elapsed time
  // excluding semaphore wait time
  var companionGpuMetric: Option[GpuMetric] = None
  private var semWaitTimeWhenActivated = 0L
  private var excludeMetricsWhenActivated: Seq[Long] = Seq.empty
  // Snapshot of each excludeMetric's companion value at activation. Needed so
  // the companion `.add` at deactivation can subtract descendants' COMPANION
  // deltas (their `op time (excl. SemWait)`) rather than their RAW deltas
  // (their `op time`, which still embed their own SemWait). Without this,
  // any SemWait that fired inside a descendant's wrap would be subtracted
  // twice from this wrap's companion -- once via this wrap's `semWaitDelta`
  // and again embedded in the descendant's raw delta -- making the companion
  // go negative on nested wraps.
  private var excludeCompanionMetricsWhenActivated: Seq[Long] = Seq.empty

  def tryActivateTimer(excludeMetrics: Seq[GpuMetric]): Boolean = {
    if (!isTimerActive) {
      isTimerActive = true
      semWaitTimeWhenActivated = GpuTaskMetrics.get.getSemWaitTime()
      excludeMetricsWhenActivated = excludeMetrics.map(_.value)
      excludeCompanionMetricsWhenActivated =
        excludeMetrics.map(_.companionGpuMetric.map(_.value).getOrElse(0L))
      true
    } else {
      false
    }
  }

  def deactivateTimer(duration: Long, excludeMetrics: Seq[GpuMetric]): Unit = {
    if (isTimerActive) {
      isTimerActive = false

      val totalExcludeTime = if (excludeMetrics.length == excludeMetricsWhenActivated.length) {
        excludeMetrics.zip(excludeMetricsWhenActivated)
          .map { case (metric, startValue) => metric.value - startValue }
          .sum
      } else {
        throw new IllegalStateException(
          s"the excludeMetrics size ${excludeMetrics.length} does not match " +
            s"excludeMetricsWhenActivated size ${excludeMetricsWhenActivated.length}")
      }

      // Sum of descendants' companion deltas. Used for the companion update so
      // that SemWait fired inside a descendant's wrap is subtracted once (via
      // the descendant's companion delta) rather than twice.
      //
      // Invariant: when THIS metric has a companion (the `foreach` below runs),
      // every excludeMetric also has a companion. excludeMetrics are descendant
      // OP_TIME_NEW metrics, created from the same `allMetrics` map as this one,
      // so the companion ("excl. SemWait") variant exists for all of them or
      // none of them at a given metrics level. The `getOrElse(0L)` is therefore
      // a defensive fallback that does not fire in that branch; it only returns
      // 0 when this metric itself has no companion, in which case the companion
      // update below is skipped entirely.
      val totalCompanionExcludeTime = excludeMetrics
        .zip(excludeCompanionMetricsWhenActivated)
        .map { case (metric, startValue) =>
          metric.companionGpuMetric.map(_.value - startValue).getOrElse(0L)
        }.sum

      // SemWait that fired anywhere inside this wrap, task-global delta over the
      // wrap. Subtracted from the companion so it reflects "excl. SemWait".
      val semWaitDelta = GpuTaskMetrics.get.getSemWaitTime() - semWaitTimeWhenActivated

      companionGpuMetric.foreach(c =>
        c.add(duration - semWaitDelta - totalCompanionExcludeTime))
      semWaitTimeWhenActivated = 0L
      excludeMetricsWhenActivated = Seq.empty
      excludeCompanionMetricsWhenActivated = Seq.empty

      add(duration - totalExcludeTime)
    }
  }

  final def ns[T](f: => T): T = {
    ns(Seq.empty)(f)
  }

  final def ns[T](excludeMetrics: Seq[GpuMetric])(f: => T): T = {
    if (tryActivateTimer(excludeMetrics)) {
      val start = System.nanoTime()
      try {
        f
      } finally {
        deactivateTimer(System.nanoTime() - start, excludeMetrics)
      }
    } else {
      f
    }
  }
}

object NoopMetric extends GpuMetric {
  override def +=(v: Long): Unit = ()
  override def add(v: Long): Unit = ()
  override def set(v: Long): Unit = ()
  override def value: Long = 0

  override def tryActivateTimer(excludeMetrics: Seq[GpuMetric]): Boolean = false
  override def deactivateTimer(duration: Long, excludeMetrics: Seq[GpuMetric]): Unit = ()
}

final case class WrappedGpuMetric(sqlMetric: SQLMetric, withMetricsExclSemWait: Boolean = false)
  extends GpuMetric {

  if (withMetricsExclSemWait) {
    if (GpuMetric.isTimeMetric(this)) {
      companionGpuMetric = Some(WrappedGpuMetric.apply(SQLMetrics.createNanoTimingMetric(
        SparkSession.getActiveSession.get.sparkContext, sqlMetric.name.get + " (excl. SemWait)")))
    }
  }

  def +=(v: Long): Unit = sqlMetric.add(v)
  def add(v: Long): Unit = sqlMetric.add(v)
  override def set(v: Long): Unit = sqlMetric.set(v)
  override def value: Long = sqlMetric.value
}

/** A GPU metric class that just accumulates into a variable without implicit publishing. */
final class LocalGpuMetric extends GpuMetric {
  private var lval = 0L
  override def value: Long = lval
  override def set(v: Long): Unit = { lval = v }
  override def +=(v: Long): Unit = { lval += v }
  override def add(v: Long): Unit = { lval += v }
}

class CollectTimeIterator[T](
    nvtxId: NvtxId,
    it: Iterator[T],
    collectTime: GpuMetric) extends Iterator[T] {
  override def hasNext: Boolean = {
    NvtxIdWithMetrics(nvtxId, collectTime) {
      it.hasNext
    }
  }

  override def next(): T = {
    NvtxIdWithMetrics(nvtxId, collectTime) {
      it.next
    }
  }
}
