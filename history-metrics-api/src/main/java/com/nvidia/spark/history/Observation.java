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
package com.nvidia.spark.history;

import java.util.Map;
import java.util.Objects;

/** Immutable raw observation supplied by a metric producer without framework provenance. */
public final class Observation {
  private final MetricVersionId metric;
  private final Map<String, DimValue> dimensions;
  private final double value;
  private final long timestampMs;

  /**
   * Creates a raw observation and defensively copies its dimensions.
   *
   * @param metric governed metric identity
   * @param dimensions non-null dimension names and values
   * @param value finite observed value
   * @param timestampMs producer-defined observation time in epoch milliseconds
   * @throws NullPointerException if {@code metric}, {@code dimensions}, or an entry is null
   * @throws IllegalArgumentException if a dimension name is invalid or {@code value} is non-finite
   */
  public Observation(
      MetricVersionId metric, Map<String, DimValue> dimensions, double value, long timestampMs) {
    this.metric = Objects.requireNonNull(metric, "metric");
    this.dimensions = ImmutableDimensionMap.copy(dimensions, "dimensions");
    if (!Double.isFinite(value)) {
      throw new IllegalArgumentException("observation value must be finite");
    }
    this.value = value;
    this.timestampMs = timestampMs;
  }

  /** @return the governed metric identity */
  public MetricVersionId metric() {
    return metric;
  }

  /** @return an immutable dimension snapshot */
  public Map<String, DimValue> dimensions() {
    return dimensions;
  }

  /** @return the finite observed value */
  public double value() {
    return value;
  }

  /** @return the producer-defined observation time in epoch milliseconds */
  public long timestampMs() {
    return timestampMs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Observation)) {
      return false;
    }
    Observation that = (Observation) other;
    return Double.compare(value, that.value) == 0 &&
        timestampMs == that.timestampMs &&
        metric.equals(that.metric) &&
        dimensions.equals(that.dimensions);
  }

  @Override
  public int hashCode() {
    int result = metric.hashCode();
    result = 31 * result + dimensions.hashCode();
    long valueBits = Double.doubleToLongBits(value);
    result = 31 * result + (int) (valueBits ^ (valueBits >>> 32));
    return 31 * result + (int) (timestampMs ^ (timestampMs >>> 32));
  }

  @Override
  public String toString() {
    return "Observation{" + "metric=" + metric + ", dimensions=" + dimensions +
        ", value=" + value + ", timestampMs=" + timestampMs + '}';
  }
}
