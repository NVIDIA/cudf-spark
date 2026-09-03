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

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Package-private immutable implementation used by local test-handle snapshots. */
final class ImmutableLocalHistoryMetricsCounters implements LocalHistoryMetricsCounters {
  private final EnumMap<LocalMetricCounter, Long> values =
      new EnumMap<LocalMetricCounter, Long>(LocalMetricCounter.class);

  ImmutableLocalHistoryMetricsCounters(Map<LocalMetricCounter, Long> values) {
    Objects.requireNonNull(values, "values");
    for (Map.Entry<LocalMetricCounter, Long> entry : values.entrySet()) {
      LocalMetricCounter counter = Objects.requireNonNull(entry.getKey(), "counter");
      Long value = Objects.requireNonNull(entry.getValue(), "value");
      if (value < 0L) {
        throw new IllegalArgumentException("counter value must not be negative");
      }
      this.values.put(counter, value);
    }
  }

  @Override
  public long value(LocalMetricCounter counter) {
    Long value = values.get(Objects.requireNonNull(counter, "counter"));
    return value == null ? 0L : value;
  }
}
