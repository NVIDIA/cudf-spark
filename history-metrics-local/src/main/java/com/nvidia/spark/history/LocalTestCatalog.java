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
import java.util.List;

/**
 * Builds isolated source-declared catalogs for the explicit local companion provider.
 *
 * <p>This companion-only builder cannot allocate, reserve, or reassign a production metric ID.
 * Building validates the complete supplied live and retired catalog under the portable catalog
 * governance rules.
 */
public final class LocalTestCatalog {
  private LocalTestCatalog() {
  }

  /**
   * Returns a fresh isolated source-declaration builder.
   *
   * @return new test-catalog builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Mutable source-declaration builder whose built catalogs are immutable point-in-time copies. */
  public static final class Builder {
    private final List<HistoryMetricCatalog.MetricDefinition> definitions =
        new ArrayList<HistoryMetricCatalog.MetricDefinition>();

    private Builder() {
    }

    /**
     * Adds one live source-declared ID/name association.
     *
     * <p>Validation of the complete catalog occurs at {@link #build()}.
     *
     * @param id source-declared numeric ID
     * @param name exact case-sensitive diagnostic name
     * @return this builder
     */
    public Builder addLive(int id, String name) {
      definitions.add(new HistoryMetricCatalog.MetricDefinition(id, name, false));
      return this;
    }

    /**
     * Adds one retired ID/name tombstone.
     *
     * <p>A tombstone preserves allocation identity; it does not deactivate existing versions.
     * Validation of the complete catalog occurs at {@link #build()}.
     *
     * @param id source-declared numeric ID
     * @param name exact case-sensitive diagnostic name
     * @return this builder
     */
    public Builder addRetired(int id, String name) {
      definitions.add(new HistoryMetricCatalog.MetricDefinition(id, name, true));
      return this;
    }

    /**
     * Builds an immutable isolated catalog copy under the production governance validations.
     *
     * <p>This does not allocate, reserve, or reassign a production metric ID.
     *
     * @return immutable isolated history metric catalog
     */
    public HistoryMetricCatalog build() {
      return HistoryMetricCatalog.forTesting(
          new ArrayList<HistoryMetricCatalog.MetricDefinition>(definitions));
    }
  }
}
