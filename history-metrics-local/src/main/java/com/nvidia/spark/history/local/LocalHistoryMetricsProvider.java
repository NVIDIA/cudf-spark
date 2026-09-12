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

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsProvider;
import com.nvidia.spark.history.MetricStore;
import org.apache.spark.SparkContext;

/** Optional in-memory history metrics provider for testing and validation. */
public final class LocalHistoryMetricsProvider implements HistoryMetricsProvider {
  private static final Duration MAXIMUM_PLANNING_AGE = Duration.ofDays(7);

  private LocalHistoryMetrics owner;

  public LocalHistoryMetricsProvider() {
  }

  LocalHistoryMetricsProvider(LocalHistoryMetrics owner) {
    this.owner = Objects.requireNonNull(owner, "owner");
  }

  @Override
  public String name() {
    return "local";
  }

  @Override
  public synchronized MetricStore open(SparkContext sparkContext) {
    if (owner != null) {
      throw new IllegalStateException("local history metrics provider is already open");
    }
    if (sparkContext == null) {
      throw new NullPointerException("sparkContext");
    }

    String pluginVersion = implementationVersion();
    LocalHistoryMetrics opened = LocalHistoryMetricsFactory.open(
        HistoryMetricCatalog.production(),
        Clock.systemUTC(),
        sparkProvenance(sparkContext, pluginVersion),
        MAXIMUM_PLANNING_AGE);
    owner = opened;
    return opened.store();
  }

  @Override
  public synchronized boolean shutdown(Duration timeout) {
    if (timeout == null) {
      throw new NullPointerException("timeout");
    }
    LocalHistoryMetrics current = owner;
    if (current == null) {
      return true;
    }
    boolean complete = current.shutdown(timeout);
    if (complete) {
      owner = null;
    }
    return complete;
  }

  static LocalProvenanceSource sparkProvenance(
      SparkContext sparkContext,
      String pluginVersion) {
    return new LocalProvenanceSource() {
      @Override
      public LocalProvenanceIdentity current() {
        String applicationId = nonemptyOrUnknown(sparkContext.applicationId());
        return LocalProvenanceIdentity.of(applicationId, null, pluginVersion);
      }
    };
  }

  private static String implementationVersion() {
    Package providerPackage = LocalHistoryMetricsProvider.class.getPackage();
    return providerPackage == null
        ? "unknown"
        : nonemptyOrUnknown(providerPackage.getImplementationVersion());
  }

  private static String nonemptyOrUnknown(String value) {
    return value == null || value.isEmpty() ? "unknown" : value;
  }
}
