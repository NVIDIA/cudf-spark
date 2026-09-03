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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-09, FR-12 through FR-14, FR-17, and NFR-15/NFR-21. */
class MetricStoreContractTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(1);

  @Test
  void nfr15DefaultStoreIsAlwaysNonNullNoOpWithNoServiceSpecialCase() {
    MetricStore store = MetricStores.current();

    assertNotNull(store);
    assertEquals(HistoryMetricsApi.CURRENT_API_VERSION, store.info().apiVersion());
    assertFalse(store.info().description().isEmpty());

    // FR-05 grants the built-in no-op a zero-inspection exemption. These inputs fail if the
    // implementation asks even for list size, iteration, or an observation.
    assertDoesNotThrow(() -> store.record(null));
    assertDoesNotThrow(() -> store.record(hostileUninspectableList()));
    assertDoesNotThrow(() -> store.record(hostileOversizedList()));

    List<SummaryResponse> responses =
        store.summarize(Collections.singletonList(request()), TIMEOUT);
    assertEquals(1, responses.size());
    assertEquals(Status.Code.UNAVAILABLE, responses.get(0).status().code());
  }

  @Test
  void nfr21NoOpDeclarationIsPositionalValidatingAndStateless() {
    MetricSchema first = schema(1, "table");
    MetricSchema identicalWithDifferentRetention = new MetricSchema(
        first.metric(),
        first.dimensions(),
        new Retention(Duration.ofDays(2), Duration.ofDays(20)));

    List<SchemaStatus> accepted = MetricStores.current().declare(
        Arrays.asList(first, null, identicalWithDifferentRetention), TIMEOUT);
    assertEquals(3, accepted.size());
    assertEquals(SchemaStatus.Code.ACCEPTED, accepted.get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, accepted.get(1).code());
    assertEquals(SchemaStatus.Code.ACCEPTED, accepted.get(2).code());

    MetricSchema conflict = schema(1, "other");
    List<SchemaStatus> conflicts =
        MetricStores.current().declare(Arrays.asList(first, conflict), TIMEOUT);
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, conflicts.get(0).code());
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, conflicts.get(1).code());

    assertEquals(SchemaStatus.Code.ACCEPTED,
        MetricStores.current().declare(
            Collections.singletonList(conflict), TIMEOUT).get(0).code());
  }

  @Test
  void nfr21DeclarationBatchAndTimeoutFallbacksAreBounded() {
    MetricStore store = MetricStores.current();
    assertEquals(Collections.emptyList(), store.declare(null, TIMEOUT));
    assertEquals(Collections.emptyList(), store.declare(schemas(129), TIMEOUT));
    assertEquals(Collections.emptyList(),
        store.declare(Collections.<MetricSchema>emptyList(), TIMEOUT));

    assertEquals(SchemaStatus.Code.INVALID_REQUEST,
        store.declare(Collections.singletonList(schema(1, "table")), null)
            .get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST,
        store.declare(
            Collections.singletonList(schema(1, "table")), Duration.ofMillis(-1))
            .get(0).code());

    // The no-op resolves valid declarations locally and never invokes a provider.
    assertEquals(SchemaStatus.Code.ACCEPTED,
        store.declare(Collections.singletonList(schema(1, "table")), Duration.ZERO)
            .get(0).code());
  }

  @Test
  void fr09NoOpSummaryPreservesExactBatchCardinalityAndTimeoutOutcomes() {
    MetricStore store = MetricStores.current();

    assertEquals(Collections.emptyList(), store.summarize(null, TIMEOUT));
    assertEquals(Collections.emptyList(), store.summarize(requests(129), TIMEOUT));
    assertEquals(Collections.emptyList(),
        store.summarize(Collections.<SummaryRequest>emptyList(), TIMEOUT));

    List<SummaryResponse> invalidElements =
        store.summarize(Arrays.asList(request(), null), TIMEOUT);
    assertEquals(2, invalidElements.size());
    assertEquals(Status.Code.INVALID_REQUEST, invalidElements.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, invalidElements.get(1).status().code());

    assertEquals(Status.Code.INVALID_REQUEST,
        store.summarize(Collections.singletonList(request()), null)
            .get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST,
        store.summarize(
            Collections.singletonList(request()), Duration.ofMillis(-1))
            .get(0).status().code());
    assertEquals(Status.Code.DEADLINE_EXCEEDED,
        store.summarize(Collections.singletonList(request()), Duration.ZERO)
            .get(0).status().code());

    List<SummaryResponse> maximum = store.summarize(requests(128), TIMEOUT);
    assertEquals(128, maximum.size());
    for (SummaryResponse response : maximum) {
      assertEquals(Status.Code.UNAVAILABLE, response.status().code());
    }
  }

  @Test
  void nfr15ScopedInstallIsAtomicVersionCheckedAndDoesNotOwnTheStore()
      throws Exception {
    MetricStore noOp = MetricStores.current();
    StubStore installed = new StubStore(
        new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "installed"));

    AutoCloseable firstHandle = MetricStores.install(installed);
    assertSame(installed, MetricStores.current());
    assertThrows(IllegalStateException.class, () ->
        MetricStores.install(new StubStore(
            new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "second"))));

    firstHandle.close();
    assertSame(noOp, MetricStores.current());

    StubStore later = new StubStore(
        new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "later"));
    AutoCloseable laterHandle = MetricStores.install(later);
    firstHandle.close();
    assertSame(later, MetricStores.current());
    laterHandle.close();
    laterHandle.close();
    assertSame(noOp, MetricStores.current());
  }

  @Test
  void nfr15OnlyOneConcurrentInstallCanPublish() throws Exception {
    MetricStore noOp = MetricStores.current();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<AutoCloseable> first = executor.submit(() -> {
        ready.countDown();
        start.await();
        try {
          return MetricStores.install(new StubStore(
              new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "concurrent-1")));
        } catch (IllegalStateException e) {
          return null;
        }
      });
      Future<AutoCloseable> second = executor.submit(() -> {
        ready.countDown();
        start.await();
        try {
          return MetricStores.install(new StubStore(
              new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "concurrent-2")));
        } catch (IllegalStateException e) {
          return null;
        }
      });
      ready.await();
      start.countDown();

      AutoCloseable firstHandle = first.get();
      AutoCloseable secondHandle = second.get();
      assertEquals(firstHandle == null, secondHandle != null);
      assertNotSame(noOp, MetricStores.current());

      (firstHandle == null ? secondHandle : firstHandle).close();
      assertSame(noOp, MetricStores.current());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void nfr15RejectedInstallLeavesNoOpActive() {
    MetricStore noOp = MetricStores.current();

    assertThrows(NullPointerException.class, () -> MetricStores.install(null));
    assertSame(noOp, MetricStores.current());

    assertThrows(IllegalArgumentException.class, () ->
        MetricStores.install(new StubStore(new BackendInfo(2, "too new"))));
    assertSame(noOp, MetricStores.current());

    assertThrows(IllegalArgumentException.class, () ->
        MetricStores.install(new StubStore(null)));
    assertSame(noOp, MetricStores.current());

    assertThrows(IllegalArgumentException.class, () ->
        MetricStores.install(new ThrowingInfoStore()));
    assertSame(noOp, MetricStores.current());
  }

  @Test
  void fr17PlanningStoreAndLocatorExposeOnlyTheFrozenPublicSurface() {
    PublicApiSurface.assertMethods(
        MetricStore.class,
        "declare(java.util.List,java.time.Duration):java.util.List",
        "record(java.util.List):void",
        "summarize(java.util.List,java.time.Duration):java.util.List",
        "info():com.nvidia.spark.history.BackendInfo");
    PublicApiSurface.assertMethods(
        MetricStores.class,
        "current():com.nvidia.spark.history.MetricStore",
        "install(com.nvidia.spark.history.MetricStore):java.lang.AutoCloseable");
  }

  private static List<Observation> hostileUninspectableList() {
    return new AbstractList<Observation>() {
      @Override
      public Observation get(int index) {
        throw new AssertionError("no-op inspected an observation");
      }

      @Override
      public int size() {
        throw new AssertionError("no-op inspected the list");
      }
    };
  }

  private static List<Observation> hostileOversizedList() {
    return new AbstractList<Observation>() {
      @Override
      public Observation get(int index) {
        throw new AssertionError("no-op inspected an oversized observation");
      }

      @Override
      public int size() {
        return 129;
      }
    };
  }

  private static SummaryRequest request() {
    return SummaryRequest.builder(new MetricVersionId(1, 1)).window(1L, 2L).build();
  }

  private static MetricSchema schema(int version, String dimensionName) {
    return new MetricSchema(
        new MetricVersionId(1, version),
        Collections.singletonList(new DimensionSpec(dimensionName, DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(1), Duration.ofDays(10)));
  }

  private static List<MetricSchema> schemas(int count) {
    List<MetricSchema> schemas = new ArrayList<MetricSchema>(count);
    for (int index = 0; index < count; index++) {
      schemas.add(schema(index + 1, "table"));
    }
    return schemas;
  }

  private static List<SummaryRequest> requests(int count) {
    List<SummaryRequest> requests = new ArrayList<SummaryRequest>(count);
    for (int index = 0; index < count; index++) {
      requests.add(request());
    }
    return requests;
  }

  private static class StubStore implements MetricStore {
    private final BackendInfo info;

    StubStore(BackendInfo info) {
      this.info = info;
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public void record(List<Observation> observations) {
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public BackendInfo info() {
      return info;
    }
  }

  private static final class ThrowingInfoStore extends StubStore {
    ThrowingInfoStore() {
      super(null);
    }

    @Override
    public BackendInfo info() {
      throw new IllegalStateException("info unavailable");
    }
  }
}
