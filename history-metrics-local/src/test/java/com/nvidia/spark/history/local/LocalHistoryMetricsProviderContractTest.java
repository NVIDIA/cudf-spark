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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.tck.HistoryMetricsProviderFactory;
import com.nvidia.spark.history.tck.HistoryMetricsProviderFixture;
import com.nvidia.spark.history.tck.MetricStoreProviderContract;

/** Runs the provider-neutral TCK through the complete explicit local owner and planning adapter. */
class LocalHistoryMetricsProviderContractTest extends MetricStoreProviderContract {
  private static final HistoryMetricsProviderFactory FACTORY = new LocalFixtureFactory();

  @Override
  protected HistoryMetricsProviderFactory providerFactory() {
    return FACTORY;
  }

  private static final class LocalFixtureFactory implements HistoryMetricsProviderFactory {
    @Override
    public HistoryMetricsProviderFixture open(
        HistoryMetricCatalog catalog,
        long initialProviderTimeMs,
        Duration maximumPlanningAge) {
      MutableClock clock = new MutableClock(initialProviderTimeMs);
      LocalProvenanceSource provenanceSource = new LocalProvenanceSource() {
        @Override
        public LocalProvenanceIdentity current() {
          return LocalProvenanceIdentity.of("tck-app", "attempt-1", "tck-provider");
        }
      };
      LocalHistoryMetrics owner = LocalHistoryMetricsFactory.open(
          catalog,
          clock,
          provenanceSource,
          maximumPlanningAge,
          LocalQueuePolicy.of(1024, 128),
          LocalExecutionPolicy.of(2, 128),
          LocalCircuitBreakerPolicy.of(
              100,
              100,
              1.0,
              Duration.ofDays(1),
              1.0,
              Duration.ofSeconds(1)));
      return new LocalFixture(catalog, clock, owner);
    }
  }

  private static final class LocalFixture implements HistoryMetricsProviderFixture {
    private static final Duration LIFECYCLE_TIMEOUT = Duration.ofSeconds(5);

    private final HistoryMetricCatalog catalog;
    private final MutableClock clock;
    private final LocalHistoryMetrics owner;

    private LocalFixture(
        HistoryMetricCatalog catalog, MutableClock clock, LocalHistoryMetrics owner) {
      this.catalog = catalog;
      this.clock = clock;
      this.owner = owner;
    }

    @Override
    public HistoryMetricCatalog catalog() {
      return catalog;
    }

    @Override
    public MetricStore store() {
      return owner.store();
    }

    @Override
    public void setProviderTime(long timestampMs) {
      clock.setMillis(timestampMs);
    }

    @Override
    public boolean awaitWrites(Duration timeout) {
      return owner.drain(timeout);
    }

    @Override
    public void close() {
      if (!owner.shutdown(LIFECYCLE_TIMEOUT)) {
        throw new IllegalStateException("local TCK fixture did not shut down");
      }
    }
  }

  private static final class MutableClock extends Clock {
    private final AtomicLong currentMs;
    private final ZoneId zone;

    private MutableClock(long initialMs) {
      this(new AtomicLong(initialMs), ZoneOffset.UTC);
    }

    private MutableClock(AtomicLong currentMs, ZoneId zone) {
      this.currentMs = currentMs;
      this.zone = zone;
    }

    private void setMillis(long timestampMs) {
      currentMs.set(timestampMs);
    }

    @Override
    public ZoneId getZone() {
      return zone;
    }

    @Override
    public Clock withZone(ZoneId requestedZone) {
      if (zone.equals(requestedZone)) {
        return this;
      }
      return new MutableClock(currentMs, requestedZone);
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis());
    }

    @Override
    public long millis() {
      return currentMs.get();
    }
  }
}
