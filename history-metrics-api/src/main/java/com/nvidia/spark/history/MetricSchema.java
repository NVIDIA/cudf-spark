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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable declaration identity for one metric version.
 *
 * <p>Dimension order is significant and declares the metric version's single compound access order.
 * Recommended retention is policy advice rather than declaration identity, so it is excluded from
 * equality and hash-code comparisons.
 */
public final class MetricSchema {
  /** Maximum number of ordered dimensions in one schema. */
  public static final int MAX_DIMENSIONS = 8;

  private final MetricVersionId metric;
  private final List<DimensionSpec> dimensions;
  private final Retention recommendedRetention;

  /**
   * Creates a declaration and defensively copies the ordered dimension list.
   *
   * @param metric governed metric identity
   * @param dimensions zero through {@value #MAX_DIMENSIONS} unique named dimensions
   * @param recommendedRetention producer recommendation subject to provider policy
   * @throws NullPointerException if an argument or dimension is null
   * @throws IllegalArgumentException if the dimension bound or uniqueness rule is violated
   */
  public MetricSchema(
      MetricVersionId metric, List<DimensionSpec> dimensions, Retention recommendedRetention) {
    this.metric = Objects.requireNonNull(metric, "metric");
    this.recommendedRetention =
        Objects.requireNonNull(recommendedRetention, "recommendedRetention");
    Objects.requireNonNull(dimensions, "dimensions");
    if (dimensions.size() > MAX_DIMENSIONS) {
      throw new IllegalArgumentException("a metric schema may declare at most 8 dimensions");
    }

    List<DimensionSpec> copied = new ArrayList<DimensionSpec>(dimensions.size());
    Set<String> names = new HashSet<String>();
    for (DimensionSpec dimension : dimensions) {
      Objects.requireNonNull(dimension, "dimension");
      if (!names.add(dimension.name())) {
        throw new IllegalArgumentException("duplicate dimension name: " + dimension.name());
      }
      copied.add(dimension);
    }
    this.dimensions = Collections.unmodifiableList(copied);
  }

  /** @return the governed metric identity */
  public MetricVersionId metric() {
    return metric;
  }

  /** @return the immutable, contract-significant ordered dimensions */
  public List<DimensionSpec> dimensions() {
    return dimensions;
  }

  /** @return the producer's retention recommendation */
  public Retention recommendedRetention() {
    return recommendedRetention;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MetricSchema)) {
      return false;
    }
    MetricSchema that = (MetricSchema) other;
    return metric.equals(that.metric) && dimensions.equals(that.dimensions);
  }

  @Override
  public int hashCode() {
    return 31 * metric.hashCode() + dimensions.hashCode();
  }

  @Override
  public String toString() {
    return "MetricSchema{" + "metric=" + metric + ", dimensions=" + dimensions +
        ", recommendedRetention=" + recommendedRetention + '}';
  }
}
