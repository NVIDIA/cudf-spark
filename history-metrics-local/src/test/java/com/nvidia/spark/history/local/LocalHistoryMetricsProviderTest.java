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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

import com.nvidia.spark.history.HistoryMetricsProvider;
import com.nvidia.spark.history.MetricStore;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

class LocalHistoryMetricsProviderTest {
  @Test
  void serviceLoaderFindsLocalProvider() {
    List<HistoryMetricsProvider> providers = new ArrayList<HistoryMetricsProvider>();
    for (HistoryMetricsProvider provider : ServiceLoader.load(HistoryMetricsProvider.class)) {
      providers.add(provider);
    }

    assertTrue(providers.stream().anyMatch(
        provider -> provider instanceof LocalHistoryMetricsProvider));
    LocalHistoryMetricsProvider provider = providers.stream()
        .filter(LocalHistoryMetricsProvider.class::isInstance)
        .map(LocalHistoryMetricsProvider.class::cast)
        .findFirst()
        .orElseThrow(AssertionError::new);
    assertEquals("local", provider.name());
  }

  @Test
  void shutdownRetainsOwnershipUntilCleanupCompletes() {
    AtomicInteger shutdownCalls = new AtomicInteger();
    LocalHistoryMetrics owner = new LocalHistoryMetrics() {
      @Override
      public MetricStore store() {
        return null;
      }

      @Override
      public LocalHistoryMetricsTestHandle testHandle() {
        return null;
      }

      @Override
      public void save(Path target, Duration timeout) {
      }

      @Override
      public boolean drain(Duration timeout) {
        return false;
      }

      @Override
      public boolean shutdown(Duration timeout) {
        int call = shutdownCalls.incrementAndGet();
        if (call == 1) {
          return false;
        }
        if (call == 2) {
          throw new IllegalStateException("cleanup failed");
        }
        return true;
      }
    };
    LocalHistoryMetricsProvider provider = new LocalHistoryMetricsProvider(owner);

    assertFalse(provider.shutdown(Duration.ZERO));
    assertThrows(IllegalStateException.class, () -> provider.shutdown(Duration.ZERO));
    assertTrue(provider.shutdown(Duration.ZERO));
    assertTrue(provider.shutdown(Duration.ZERO));
    assertEquals(3, shutdownCalls.get());
  }

  @Test
  void opensWithRealSparkContext() {
    SparkConf conf = new SparkConf(false)
        .setMaster("local[1]")
        .setAppName("history-metrics-provider-test")
        .set("spark.ui.enabled", "false");
    SparkContext sparkContext = new SparkContext(conf);
    LocalHistoryMetricsProvider provider = new LocalHistoryMetricsProvider();
    try {
      LocalProvenanceSource provenance =
          LocalHistoryMetricsProvider.sparkProvenance(sparkContext, "test-version");
      LocalProvenanceIdentity identity = provenance.current();
      assertEquals(sparkContext.applicationId(), identity.applicationId());
      assertNull(identity.attemptId());

      MetricStore store = provider.open(sparkContext);
      assertNotNull(store);
      assertNotNull(store.info());
    } finally {
      assertTrue(provider.shutdown(Duration.ofSeconds(10)));
      sparkContext.stop();
    }
  }
}
