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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Creates isolated catalogs for TCK and local-provider tests.
 *
 * <p>This factory intentionally lives outside the production API artifact.
 */
public final class TestHistoryMetricCatalog {
  private TestHistoryMetricCatalog() {
  }

  public static TestEntry live(int metricId, String name) {
    return new TestEntry(metricId, name, false);
  }

  public static TestEntry retired(int metricId, String name) {
    return new TestEntry(metricId, name, true);
  }

  public static HistoryMetricCatalog create(TestEntry... entries) {
    Objects.requireNonNull(entries, "entries");
    List<HistoryMetricCatalog.MetricDefinition> definitions =
        new ArrayList<HistoryMetricCatalog.MetricDefinition>(entries.length);
    for (TestEntry entry : Arrays.asList(entries.clone())) {
      Objects.requireNonNull(entry, "entry");
      definitions.add(new HistoryMetricCatalog.MetricDefinition(
          entry.metricId, entry.name, entry.retired));
    }
    return HistoryMetricCatalog.forTesting(definitions);
  }

  public static final class TestEntry {
    private final int metricId;
    private final String name;
    private final boolean retired;

    private TestEntry(int metricId, String name, boolean retired) {
      this.metricId = metricId;
      this.name = name;
      this.retired = retired;
    }
  }
}
