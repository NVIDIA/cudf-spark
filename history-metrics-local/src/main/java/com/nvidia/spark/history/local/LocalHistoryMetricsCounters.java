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
package com.nvidia.spark.history.local;

/**
 * Immutable point-in-time view over the closed local metric-counter vocabulary.
 *
 * <p>Batch counters count public calls and declaration/summary status counters count returned
 * positions. {@link LocalMetricCounter#SUMMARY_ROWS} accumulates summarized observation counts.
 * Record, backend, queue-drop, and shutdown-drop counters use observation units when cardinality is
 * knowable; an uninspectable whole record call adds one
 * {@link LocalMetricCounter#RECORD_INVALID} unit. {@link LocalMetricCounter#QUEUE_CURRENT} is a gauge
 * and {@link LocalMetricCounter#QUEUE_HIGH_WATER} is its lifetime maximum. Drain and timeout results
 * count caller outcomes, while {@link LocalMetricCounter#SHUTDOWN_COMPLETE} increments once.
 */
public interface LocalHistoryMetricsCounters {
  /**
   * Returns the snapshotted value for one non-null counter.
   *
   * @param counter closed counter to read
   * @return snapshotted counter value
   * @throws NullPointerException if {@code counter} is null
   */
  long value(LocalMetricCounter counter);
}
