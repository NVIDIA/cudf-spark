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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable summary request with explicit observation-time window and equality-bound dimensions.
 *
 * <p>Dimensions omitted from {@link #bound()} are deliberate equality wildcards aggregated into
 * one result. A request containing a wildcard must use {@code limit == 0}; the store validates this
 * after resolving the declared schema. The observation-time interval is {@code [fromMs, toMs)}.
 */
public final class SummaryRequest {
  private final MetricVersionId metric;
  private final Map<String, DimValue> bound;
  private final long fromMs;
  private final long toMs;
  private final int limit;

  private SummaryRequest(
      MetricVersionId metric, Map<String, DimValue> bound, long fromMs, long toMs, int limit) {
    this.metric = metric;
    this.bound = ImmutableDimensionMap.copy(bound, "bound");
    this.fromMs = fromMs;
    this.toMs = toMs;
    this.limit = limit;
  }

  /**
   * Starts a request builder for the required non-null metric version.
   *
   * @param metric required metric version
   * @return a new builder
   */
  public static Builder builder(MetricVersionId metric) {
    return new Builder(metric);
  }

  /**
   * Returns the requested metric version.
   *
   * @return requested metric version
   */
  public MetricVersionId metric() {
    return metric;
  }

  /**
   * Returns immutable equality-bound dimensions; omitted declared dimensions are wildcards.
   *
   * @return immutable bindings in insertion order
   */
  public Map<String, DimValue> bound() {
    return bound;
  }

  /**
   * Returns the inclusive observation-time lower bound in epoch milliseconds.
   *
   * @return inclusive lower bound
   */
  public long fromMs() {
    return fromMs;
  }

  /**
   * Returns the exclusive observation-time upper bound in epoch milliseconds.
   *
   * @return exclusive upper bound
   */
  public long toMs() {
    return toMs;
  }

  /**
   * Returns the most-recent observation limit, or zero for unlimited.
   *
   * @return nonnegative limit
   */
  public int limit() {
    return limit;
  }

  /** Mutable builder for one immutable summary request. */
  public static final class Builder {
    private final MetricVersionId metric;
    private final Map<String, DimValue> bound = new LinkedHashMap<String, DimValue>();
    private long fromMs;
    private long toMs;
    private int limit;
    private boolean windowSet;

    private Builder(MetricVersionId metric) {
      this.metric = Objects.requireNonNull(metric, "metric");
    }

    /**
     * Equality-binds one named dimension.
     *
     * @param name exact case-sensitive dimension name
     * @param value non-null equality value
     * @return this builder
     * @throws IllegalArgumentException if the name is invalid or already bound
     * @throws NullPointerException if {@code value} is null
     */
    public Builder bind(String name, DimValue value) {
      DimensionSpec.validateName(name);
      Objects.requireNonNull(value, "value");
      if (bound.containsKey(name)) {
        throw new IllegalArgumentException("dimension is already bound: " + name);
      }
      bound.put(name, value);
      return this;
    }

    /**
     * Adds equality bindings from a defensively copied map.
     *
     * @param values bindings to copy and add
     * @return this builder
     * @throws NullPointerException if the map, a name, or a value is null
     * @throws IllegalArgumentException if a name is invalid or already bound
     */
    public Builder bound(Map<String, DimValue> values) {
      Map<String, DimValue> copied = ImmutableDimensionMap.copy(values, "bound");
      for (Map.Entry<String, DimValue> entry : copied.entrySet()) {
        bind(entry.getKey(), entry.getValue());
      }
      return this;
    }

    /**
     * Sets the explicit observation-time interval {@code [fromMs, toMs)}.
     *
     * @param fromMs inclusive observation-time lower bound
     * @param toMs exclusive observation-time upper bound
     * @return this builder
     * @throws IllegalArgumentException unless {@code fromMs < toMs}
     */
    public Builder window(long fromMs, long toMs) {
      if (fromMs >= toMs) {
        throw new IllegalArgumentException("summary window requires fromMs < toMs");
      }
      this.fromMs = fromMs;
      this.toMs = toMs;
      windowSet = true;
      return this;
    }

    /**
     * Sets the most-recent observation limit; zero means unlimited.
     *
     * <p>A nonzero limit is valid only when every declared dimension is bound. The store performs
     * that schema-dependent validation.
     *
     * @param limit nonnegative most-recent observation limit
     * @return this builder
     * @throws IllegalArgumentException if {@code limit} is negative
     */
    public Builder limit(int limit) {
      if (limit < 0) {
        throw new IllegalArgumentException("limit must not be negative");
      }
      this.limit = limit;
      return this;
    }

    /**
     * Builds an immutable request with a defensive copy of its bindings.
     *
     * @return immutable summary request
     * @throws IllegalStateException if no explicit window was set
     */
    public SummaryRequest build() {
      if (!windowSet) {
        throw new IllegalStateException("summary window must be explicit");
      }
      return new SummaryRequest(metric, bound, fromMs, toMs, limit);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SummaryRequest)) {
      return false;
    }
    SummaryRequest that = (SummaryRequest) other;
    return fromMs == that.fromMs &&
        toMs == that.toMs &&
        limit == that.limit &&
        metric.equals(that.metric) &&
        bound.equals(that.bound);
  }

  @Override
  public int hashCode() {
    int result = metric.hashCode();
    result = 31 * result + bound.hashCode();
    result = 31 * result + (int) (fromMs ^ (fromMs >>> 32));
    result = 31 * result + (int) (toMs ^ (toMs >>> 32));
    return 31 * result + limit;
  }

  @Override
  public String toString() {
    return "SummaryRequest{" + "metric=" + metric + ", bound=" + bound +
        ", fromMs=" + fromMs + ", toMs=" + toMs + ", limit=" + limit + '}';
  }
}
