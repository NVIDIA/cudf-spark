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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Source-controlled metric-family associations between governed numeric IDs and stable names.
 *
 * <p>The catalog has one ID/name/allocation-tombstone entry per metric family, never one entry per
 * contract version. Every version of a family reuses its ID/name; the same name under a different ID
 * is invalid. The production catalog is intentionally empty until a metric family completes review.
 * Names are nonempty, exact and case-sensitive, and well-formed strict UTF-8 of at most 128 encoded
 * bytes.
 * Names are neither normalized nor case-folded. Test catalog injection is package-private and is
 * exposed only by the companion test-support artifact.
 * Validation applies to the complete catalog supplied for one source revision. Retaining retired
 * ID/name associations across revisions is the catalog owner's source-review responsibility; this
 * class intentionally has no runtime registration history or baseline.
 */
public final class HistoryMetricCatalog {
  private static final int MIN_ID = 1;
  private static final int MAX_ID = 0xFFFF;
  private static final int MAX_NAME_BYTES = 128;
  private static final HistoryMetricCatalog PRODUCTION =
      new HistoryMetricCatalog(Collections.<MetricDefinition>emptyList());

  private final List<MetricDefinition> entries;
  private final Map<Integer, MetricDefinition> entriesById;

  private HistoryMetricCatalog(List<MetricDefinition> definitions) {
    Objects.requireNonNull(definitions, "definitions");

    List<MetricDefinition> copied = new ArrayList<MetricDefinition>(definitions.size());
    Map<Integer, MetricDefinition> byId =
        new HashMap<Integer, MetricDefinition>(definitions.size());
    Set<String> names = new HashSet<String>();
    for (MetricDefinition definition : definitions) {
      Objects.requireNonNull(definition, "definition");
      validateId(definition.metricId());
      validateName(definition.name());
      if (byId.put(definition.metricId(), definition) != null) {
        throw new IllegalArgumentException("duplicate metric ID: " + definition.metricId());
      }
      if (!names.add(definition.name())) {
        throw new IllegalArgumentException("duplicate metric name: " + definition.name());
      }
      copied.add(definition);
    }

    entries = Collections.unmodifiableList(copied);
    entriesById = Collections.unmodifiableMap(byId);
  }

  /**
   * Returns the governed production catalog.
   *
   * <p>The catalog may be empty until a metric ID completes source review.
   *
   * @return the immutable governed catalog
   */
  public static HistoryMetricCatalog production() {
    return PRODUCTION;
  }

  /**
   * Returns immutable definitions in their source-declared order.
   *
   * @return immutable catalog definitions
   */
  public List<MetricDefinition> entries() {
    return entries;
  }

  /**
   * Finds the permanent family definition for a governed numeric metric ID.
   *
   * @param metricId numeric ID to find
   * @return the definition, including a retired tombstone, or empty when absent
   */
  public Optional<MetricDefinition> find(int metricId) {
    return Optional.ofNullable(entriesById.get(metricId));
  }

  static HistoryMetricCatalog forTesting(List<MetricDefinition> definitions) {
    return new HistoryMetricCatalog(definitions);
  }

  private static void validateId(int metricId) {
    if (metricId < MIN_ID || metricId > MAX_ID) {
      throw new IllegalArgumentException("metric ID must be in the range 1 through 65535");
    }
  }

  private static void validateName(String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("metric name must not be empty");
    }
    if (StrictUtf8.encode(name, "metric name").length > MAX_NAME_BYTES) {
      throw new IllegalArgumentException(
          "metric name exceeds " + MAX_NAME_BYTES + " UTF-8 bytes");
    }
  }

  /** One permanent metric-family ID/name association and its allocation state. */
  public static final class MetricDefinition {
    private final int metricId;
    private final String name;
    private final boolean retired;

    MetricDefinition(int metricId, String name, boolean retired) {
      this.metricId = metricId;
      this.name = name;
      this.retired = retired;
    }

    /**
     * Returns the numeric ID in the inclusive range 1 through 65,535.
     *
     * @return governed metric-family/catalog ID
     */
    public int metricId() {
      return metricId;
    }

    /**
     * Returns the exact case-sensitive diagnostic name.
     *
     * @return governed metric name
     */
    public String name() {
      return name;
    }

    /**
     * Returns whether this association is an allocation tombstone.
     *
     * <p>Retirement prevents ID reuse; it does not deactivate existing metric versions.
     *
     * @return true for a retired allocation tombstone
     */
    public boolean retired() {
      return retired;
    }
  }
}
