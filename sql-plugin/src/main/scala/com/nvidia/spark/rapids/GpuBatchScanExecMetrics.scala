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

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.metric.SQLMetrics

trait GpuBatchScanExecMetrics extends GpuExec {
  import GpuMetric._

  def scan: Scan

  override def supportsColumnar = true

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    SCHEDULE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SCHEDULE_TIME),
    BUFFER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_BUBBLE),
    FILTER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME_BUBBLE),
    SCHEDULE_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME_BUBBLE),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    JOIN_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_TIME),
    ICEBERG_BUILD_ACTION_TIME ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_ICEBERG_BUILD_ACTION_TIME),
    ICEBERG_POST_PROCESS_TIME ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_ICEBERG_POST_PROCESS_TIME),
  ) ++ fileCacheMetrics ++ scanCustomMetrics

  lazy val fileCacheMetrics: Map[String, GpuMetric] = createFileCacheMetrics()

  private lazy val scanCustomMetrics: Map[String, GpuMetric] = {
    scan.supportedCustomMetrics().map { metric =>
      metric.name() -> WrappedGpuMetric(SQLMetrics.createV2CustomMetric(sparkContext, metric))
    }.toMap
  }
}
