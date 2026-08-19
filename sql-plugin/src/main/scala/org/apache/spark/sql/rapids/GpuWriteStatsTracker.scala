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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuDataWritingCommand, GpuMetric, GpuMetricFactory, MetricsLevel, NoopMetric, RapidsConf}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME
import org.apache.spark.util.SerializableConfiguration

/**
 * [[ColumnarWriteTaskStatsTracker]] implementation that produces `WriteTaskStats`
 * and tracks writing times per task.
 */
class GpuWriteTaskStatsTracker(
    hadoopConf: Configuration,
    taskMetrics: Map[String, GpuMetric])
    extends BasicColumnarWriteTaskStatsTracker(hadoopConf, taskMetrics.get(TASK_COMMIT_TIME)) {
  def addGpuTime(nanos: Long): Unit = {
    taskMetrics(GpuWriteJobStatsTracker.GPU_TIME_KEY) += nanos
  }

  def sortTime: GpuMetric = taskMetrics(GpuWriteJobStatsTracker.SORT_TIME_KEY)

  def sortOpTime: GpuMetric = taskMetrics(GpuWriteJobStatsTracker.SORT_OP_TIME_KEY)

  def setSortTime(nanos: Long): Unit = sortTime.set(nanos)

  def setSortOpTime(nanos: Long): Unit = sortOpTime.set(nanos)

  def addWriteTime(nanos: Long): Unit = {
    taskMetrics(GpuWriteJobStatsTracker.WRITE_TIME_KEY) += nanos
  }

  def addWriteIOTime(nanos: Long): Unit = {
    taskMetrics(GpuWriteJobStatsTracker.WRITE_IO_TIME_KEY) += nanos
  }

  def setAsyncWriteThrottleTimes(numTasks: Int, accumulatedThrottleTimeNs: Long, minNs: Long,
      maxNs: Long): Unit = {
    val avg = if (numTasks > 0) {
      accumulatedThrottleTimeNs.toDouble / numTasks
    } else {
      0
    }
    taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_TOTAL_THROTTLE_TIME_KEY).set(
      accumulatedThrottleTimeNs)
    taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_AVG_THROTTLE_TIME_KEY).set(avg.toLong)
    taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_MIN_THROTTLE_TIME_KEY).set(minNs)
    taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_MAX_THROTTLE_TIME_KEY).set(maxNs)
  }

  def opTimeNew: GpuMetric = taskMetrics(GpuWriteJobStatsTracker.OP_TIME_NEW_KEY)
}

/**
 * Simple [[ColumnarWriteJobStatsTracker]] implementation that's serializable, capable of
 * instantiating [[GpuWriteTaskStatsTracker]] on executors and processing the
 * `WriteTaskStats` they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class GpuWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient driverSideMetrics: Map[String, GpuMetric],
    taskMetrics: Map[String, GpuMetric])
    extends BasicColumnarWriteJobStatsTracker(serializableHadoopConf, driverSideMetrics) {
  override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
    new GpuWriteTaskStatsTracker(serializableHadoopConf.value, taskMetrics)
  }

  /**
   * Exposes the Insert command's op_time metric on the job-level stats
   * tracker. Needed by `GpuFileFormatWriter.executeTask` so it can activate
   * its `.ns(excludeMetrics)` wrap before constructing the dataWriter --
   * the dataWriter constructor (or its caller's empty-partition check)
   * would otherwise consume the iterator outside the wrap and leak
   * descendant op_time updates.
   */
  def opTimeNewMetric: GpuMetric =
    taskMetrics.getOrElse(GpuWriteJobStatsTracker.OP_TIME_NEW_KEY, NoopMetric)
}

object GpuWriteJobStatsTracker {
  val GPU_TIME_KEY = "gpuTime"
  val WRITE_TIME_KEY = "writeTime"
  val SORT_TIME_KEY = "writeSortTime"
  val SORT_OP_TIME_KEY = "writeSortOpTime"
  val WRITE_IO_TIME_KEY = "writeIOTime"
  val OP_TIME_NEW_KEY = "operatorTime"
  val ASYNC_WRITE_TOTAL_THROTTLE_TIME_KEY = "asyncWriteTotalThrottleTime"
  val ASYNC_WRITE_AVG_THROTTLE_TIME_KEY = "asyncWriteAvgThrottleTime"
  val ASYNC_WRITE_MIN_THROTTLE_TIME_KEY = "asyncWriteMinThrottleTime"
  val ASYNC_WRITE_MAX_THROTTLE_TIME_KEY = "asyncWriteMaxThrottleTime"

  def basicMetrics: Map[String, GpuMetric] = BasicColumnarWriteJobStatsTracker.metrics

  def taskMetrics: Map[String, GpuMetric] = {
    val sparkContext = SparkContext.getActive.get
    val metricsConf = MetricsLevel(sparkContext.conf.get(RapidsConf.METRICS_LEVEL.key,
      RapidsConf.METRICS_LEVEL.defaultValue))
    val metricFactory = new GpuMetricFactory(metricsConf, sparkContext)
    Map(
      GPU_TIME_KEY -> metricFactory.createNanoTiming(GpuMetric.ESSENTIAL_LEVEL,
        "GPU encode and buffer time"),
      WRITE_TIME_KEY -> metricFactory.createNanoTiming(GpuMetric.ESSENTIAL_LEVEL,
        "write time"),
      SORT_OP_TIME_KEY -> metricFactory.createNanoTiming(GpuMetric.MODERATE_LEVEL,
        "GPU sort op time"),
      SORT_TIME_KEY -> metricFactory.createNanoTiming(GpuMetric.DEBUG_LEVEL,
        "GPU sort time"),
      WRITE_IO_TIME_KEY -> metricFactory.createNanoTiming(GpuMetric.DEBUG_LEVEL,
        "write I/O time"),
      OP_TIME_NEW_KEY -> metricFactory.createNanoTiming(GpuMetric.MODERATE_LEVEL,
        "op time"),
      TASK_COMMIT_TIME -> basicMetrics(TASK_COMMIT_TIME),
      ASYNC_WRITE_TOTAL_THROTTLE_TIME_KEY -> metricFactory.createNanoTiming(
        GpuMetric.DEBUG_LEVEL, "total throttle time"),
      ASYNC_WRITE_AVG_THROTTLE_TIME_KEY -> metricFactory.createNanoTiming(
        GpuMetric.DEBUG_LEVEL, "avg throttle time per async write"),
      ASYNC_WRITE_MIN_THROTTLE_TIME_KEY -> metricFactory.createNanoTiming(
        GpuMetric.DEBUG_LEVEL, "min throttle time per async write"),
      ASYNC_WRITE_MAX_THROTTLE_TIME_KEY -> metricFactory.createNanoTiming(
        GpuMetric.DEBUG_LEVEL, "max throttle time per async write")
    )
  }

  def apply(serializableHadoopConf: SerializableConfiguration,
      command: GpuDataWritingCommand): GpuWriteJobStatsTracker =
    new GpuWriteJobStatsTracker(serializableHadoopConf, command.basicMetrics, command.taskMetrics)

  def apply(serializableHadoopConf: SerializableConfiguration,
      basicMetrics: Map[String, GpuMetric],
      taskMetrics: Map[String, GpuMetric]): GpuWriteJobStatsTracker =
    new GpuWriteJobStatsTracker(serializableHadoopConf, basicMetrics, taskMetrics)
}
