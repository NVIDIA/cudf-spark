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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.TestHistoryMetricCatalog;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/** Behavioral planning-boundary tests for the package-private local MetricStore adapter. */
class LocalMetricStorePlanningAdapterTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(1);
  private static final LocalCircuitBreakerPolicy BREAKER_POLICY =
      LocalCircuitBreakerPolicy.of(
          128, 128, 1.0, Duration.ofDays(1), 1.0, Duration.ofDays(1));
  private static final MetricVersionId METRIC_V1 = new MetricVersionId(17, 1);
  private static final MetricVersionId METRIC_V2 = new MetricVersionId(17, 2);
  private static final MetricVersionId OTHER_V1 = new MetricVersionId(18, 1);

  @Test
  void constructionValidatesInfoOnceCachesItAndCleansOwnedExecutorOnFailure() {
    InlineExecutor executor = new InlineExecutor();
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, executor, false, new MutableTicker());

    assertSame(backend.info, adapter.info());
    assertSame(backend.info, adapter.info());
    assertEquals(1, backend.infoCalls);

    for (FakeBackend invalid : Arrays.asList(
        FakeBackend.nullInfo(),
        FakeBackend.throwingInfo(),
        FakeBackend.linkageInfo(),
        FakeBackend.lowInfo(),
        FakeBackend.highInfo())) {
      InlineExecutor owned = new InlineExecutor();
      assertThrows(IllegalArgumentException.class,
          () -> adapter(invalid, owned, true, new MutableTicker()));
      assertTrue(owned.isShutdown());
      assertEquals(1, invalid.infoCalls);
    }
  }

  @Test
  void declarationBatchValidationCoalescesExactDuplicatesAndPreservesRetentionOrder() {
    FakeBackend backend = new FakeBackend();
    InlineExecutor executor = new InlineExecutor();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, executor, false, new MutableTicker());
    MetricSchema first = schema(METRIC_V1, "table", Duration.ofDays(1));
    MetricSchema duplicate = schema(METRIC_V1, "table", Duration.ofDays(1));
    MetricSchema laterRecommendation = schema(METRIC_V1, "table", Duration.ofDays(2));
    backend.declareResult = Arrays.asList(
        SchemaStatus.accepted(METRIC_V1, "first"),
        SchemaStatus.accepted(METRIC_V1, "later"));

    List<SchemaStatus> results = adapter.declare(
        Arrays.asList(first, duplicate, laterRecommendation), TIMEOUT);

    assertEquals(2, backend.lastDeclared.size());
    assertEquals(Duration.ofDays(1),
        backend.lastDeclared.get(0).recommendedRetention().planningMaxAge());
    assertEquals(Duration.ofDays(2),
        backend.lastDeclared.get(1).recommendedRetention().planningMaxAge());
    assertSame(results.get(0), results.get(1));
    assertEquals("first", results.get(0).reason());
    assertEquals("later", results.get(2).reason());
  }

  @Test
  void declarationConflictsNullsAndInvalidBatchShapesNeverReachBackend() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema ordered = schema(METRIC_V1, "table", Duration.ofDays(1));
    MetricSchema conflicting = schema(METRIC_V1, "bucket", Duration.ofDays(1));

    List<SchemaStatus> results =
        adapter.declare(Arrays.asList(ordered, null, conflicting), TIMEOUT);
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, results.get(1).code());
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(2).code());
    assertEquals(0, backend.declareCalls);

    assertTrue(adapter.declare(null, TIMEOUT).isEmpty());
    assertTrue(adapter.declare(Collections.emptyList(), TIMEOUT).isEmpty());
    assertTrue(adapter.declare(repeatedSchemas(129, ordered), TIMEOUT).isEmpty());
    assertEquals(0, backend.declareCalls);
  }

  @Test
  void planningBoundariesCaptureCallerListsOnceAndBoundHostileAccessors() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    SummaryRequest request = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));

    List<MetricSchema> unknownCardinality = new AbstractList<MetricSchema>() {
      @Override
      public MetricSchema get(int index) {
        throw new AssertionError("get must not be called");
      }

      @Override
      public int size() {
        throw new IllegalStateException("hostile size");
      }
    };
    assertTrue(adapter.declare(unknownCardinality, TIMEOUT).isEmpty());

    List<MetricSchema> knownCardinality = new AbstractList<MetricSchema>() {
      @Override
      public MetricSchema get(int index) {
        throw new java.util.ConcurrentModificationException("mutated");
      }

      @Override
      public int size() {
        return 2;
      }
    };
    List<SchemaStatus> declarationFallback =
        adapter.declare(knownCardinality, TIMEOUT);
    assertEquals(2, declarationFallback.size());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, declarationFallback.get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, declarationFallback.get(1).code());

    List<SummaryRequest> incompatibleAccessor = new AbstractList<SummaryRequest>() {
      @Override
      public SummaryRequest get(int index) {
        throw new AbstractMethodError("old caller");
      }

      @Override
      public int size() {
        return 2;
      }
    };
    List<SummaryResponse> summaryFallback =
        adapter.summarize(incompatibleAccessor, TIMEOUT);
    assertEquals(2, summaryFallback.size());
    assertEquals(Status.Code.INVALID_REQUEST, summaryFallback.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, summaryFallback.get(1).status().code());

    AtomicInteger declarationReads = new AtomicInteger();
    List<MetricSchema> singleReadDeclaration = new AbstractList<MetricSchema>() {
      @Override
      public MetricSchema get(int index) {
        if (declarationReads.getAndIncrement() != 0) {
          throw new java.util.ConcurrentModificationException("read twice");
        }
        return schema;
      }

      @Override
      public int size() {
        return 1;
      }
    };
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(singleReadDeclaration, TIMEOUT).get(0).code());

    AtomicInteger summaryReads = new AtomicInteger();
    List<SummaryRequest> singleReadSummary = new AbstractList<SummaryRequest>() {
      @Override
      public SummaryRequest get(int index) {
        if (summaryReads.getAndIncrement() != 0) {
          throw new java.util.ConcurrentModificationException("read twice");
        }
        return request;
      }

      @Override
      public int size() {
        return 1;
      }
    };
    assertEquals(Status.Code.OK,
        adapter.summarize(singleReadSummary, TIMEOUT).get(0).status().code());
    assertEquals(1, declarationReads.get());
    assertEquals(1, summaryReads.get());
    assertEquals(1, backend.declareCalls);
    assertEquals(1, backend.summaryCalls);
  }

  @Test
  void startClockFailurePreservesLocalDeclarationOutcomesAndCapturesOnce() {
    ThrowingTicker ticker = new ThrowingTicker();
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, ticker);
    MetricSchema ordered = schema(METRIC_V1, "table", Duration.ofDays(1));
    MetricSchema conflicting = schema(METRIC_V1, "bucket", Duration.ofDays(1));
    MetricSchema eligible = schema(OTHER_V1, "table", Duration.ofDays(1));
    final List<MetricSchema> values = Arrays.asList(ordered, null, conflicting, eligible);
    AtomicInteger sizeReads = new AtomicInteger();
    AtomicInteger elementReads = new AtomicInteger();
    List<MetricSchema> oneShot = new AbstractList<MetricSchema>() {
      @Override
      public MetricSchema get(int index) {
        if (elementReads.getAndIncrement() >= values.size()) {
          throw new java.util.ConcurrentModificationException("captured twice");
        }
        return values.get(index);
      }

      @Override
      public int size() {
        if (sizeReads.getAndIncrement() != 0) {
          throw new java.util.ConcurrentModificationException("size read twice");
        }
        return values.size();
      }
    };
    ticker.failure = new IllegalStateException("clock failed");

    List<SchemaStatus> results = adapter.declare(oneShot, TIMEOUT);

    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, results.get(1).code());
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(2).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, results.get(3).code());
    assertEquals(1, sizeReads.get());
    assertEquals(values.size(), elementReads.get());
    assertEquals(0, backend.declareCalls);
    assertEquals(1, adapter.counters().providerFailureCount());
  }

  @Test
  void postPreprocessingClockFailurePreservesMixedLocalDeclarationOutcomes() {
    ThrowingTicker ticker = new ThrowingTicker();
    ticker.failure = new IllegalStateException("clock failed after preprocessing");
    ticker.throwOnRead = 2;
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, ticker);
    MetricSchema ordered = schema(METRIC_V1, "table", Duration.ofDays(1));
    MetricSchema conflicting = schema(METRIC_V1, "bucket", Duration.ofDays(1));

    List<SchemaStatus> results = adapter.declare(
        Arrays.asList(ordered, null, conflicting,
            schema(OTHER_V1, "table", Duration.ofDays(1))),
        TIMEOUT);

    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, results.get(1).code());
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, results.get(2).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, results.get(3).code());
    assertEquals(0, backend.declareCalls);
    assertEquals(1, adapter.counters().providerFailureCount());
  }

  @Test
  void startClockLinkageFailurePreservesKnownSummaryValidation() {
    ThrowingTicker ticker = new ThrowingTicker();
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, new InlineExecutor(), ticker);
    SummaryRequest invalidKnown = request(METRIC_V1, 0, Collections.singletonMap(
        "bucket", DimValue.of("a")));
    SummaryRequest eligible = request(OTHER_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    ticker.linkage = new AbstractMethodError("clock linkage failed");

    List<SummaryResponse> results =
        adapter.summarize(Arrays.asList(invalidKnown, eligible), TIMEOUT);

    assertEquals(Status.Code.INVALID_REQUEST, results.get(0).status().code());
    assertEquals(Status.Code.UNAVAILABLE, results.get(1).status().code());
    assertEquals(0, backend.summaryCalls);
    assertEquals(1, adapter.counters().providerFailureCount());
  }

  @Test
  void declarationValidationPrecedesInvalidAndZeroTimeoutFallback() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema unknown = schema(new MetricVersionId(999, 1), "table", Duration.ofDays(1));
    MetricSchema accepted = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(Collections.singletonList(accepted), TIMEOUT).get(0).code());
    MetricSchema incompatible = schema(METRIC_V1, "bucket", Duration.ofDays(1));

    SchemaStatus unknownNegative =
        adapter.declare(Collections.singletonList(unknown), Duration.ofNanos(-1)).get(0);
    SchemaStatus cachedConflictZero =
        adapter.declare(Collections.singletonList(incompatible), Duration.ZERO).get(0);

    assertEquals(SchemaStatus.Code.INVALID_REQUEST, unknownNegative.code());
    assertEquals("metric ID is not present in the supplied catalog", unknownNegative.reason());
    assertEquals(SchemaStatus.Code.INCOMPATIBLE, cachedConflictZero.code());
    assertEquals(1, backend.declareCalls);
  }

  @Test
  void providerSuccessSurvivesCompletionClockFailureWithoutDoubleCountingProviderFailure() {
    ThrowingTicker ticker = new ThrowingTicker();
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      ticker.failure = new IllegalStateException("completion clock failed");
      ticker.throwOnRead = 8;
      return Collections.singletonList(SchemaStatus.accepted(schemas.get(0).metric(), null));
    };
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend, new InlineExecutor(), false, ticker, breakerPolicy(1, 1, 1.0));

    SchemaStatus result = adapter.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))), TIMEOUT).get(0);

    assertEquals(SchemaStatus.Code.ACCEPTED, result.code());
    assertEquals(0, adapter.counters().providerFailureCount());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());
    assertEquals(1L, adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SLOW));
  }

  @Test
  void providerFailureAndCompletionClockFailureCountProviderOnce() {
    ThrowingTicker ticker = new ThrowingTicker();
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      ticker.failure = new IllegalStateException("completion clock failed");
      ticker.throwOnRead = 6;
      throw new IllegalStateException("provider failed");
    };
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend, new InlineExecutor(), false, ticker, breakerPolicy(1, 1, 1.0));

    SchemaStatus result = adapter.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))), TIMEOUT).get(0);

    assertEquals(SchemaStatus.Code.UNAVAILABLE, result.code());
    assertEquals(1, adapter.counters().providerFailureCount());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());
    assertEquals(1L, adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SLOW));
  }

  @Test
  void planningBoundariesDoNotSuppressFatalVmErrors() {
    LocalMetricStorePlanningAdapter adapter =
        adapter(new FakeBackend(), new InlineExecutor(), false, new MutableTicker());
    List<MetricSchema> fatal = new AbstractList<MetricSchema>() {
      @Override
      public MetricSchema get(int index) {
        throw new OutOfMemoryError("synthetic fatal error");
      }

      @Override
      public int size() {
        return 1;
      }
    };

    assertThrows(OutOfMemoryError.class, () -> adapter.declare(fatal, TIMEOUT));
  }

  @Test
  void unexpectedFailureAfterHalfOpenReservationCannotWedgeRecovery() {
    MutableTicker ticker = new MutableTicker();
    FakeBackend backend = new FakeBackend();
    ToggleErrorExecutor executor = new ToggleErrorExecutor();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, executor, false, ticker, breakerPolicy(1, 1, 1.0));
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareHandler = (schemas, timeout) -> {
      throw new IllegalStateException("trip breaker");
    };

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());

    backend.declareHandler = null;
    ticker.nanos.set(10L);
    executor.fail = true;
    assertThrows(AssertionError.class,
        () -> adapter.declare(Collections.singletonList(schema), TIMEOUT));
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());

    ticker.nanos.set(20L);
    executor.fail = false;
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
    assertEquals(LocalCircuitBreakerState.CLOSED, adapter.breakerState());
    assertEquals(3L,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(2L,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_CLOSE));
  }

  @Test
  void malformedDeclarationOutputFallsBackOnlyEligiblePositionsWithoutPublishingCache() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult = Collections.emptyList();

    List<SchemaStatus> wrongCardinality =
        adapter.declare(Arrays.asList(null, schema), TIMEOUT);
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, wrongCardinality.get(0).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, wrongCardinality.get(1).code());

    backend.declareResult = Collections.singletonList(
        SchemaStatus.accepted(METRIC_V2, null));
    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());

    SummaryRequest request = request(METRIC_V1, 0, Collections.singletonMap(
        "bucket", DimValue.of("a")));
    backend.summaryResult = Collections.singletonList(SummaryResponse.error(
        Status.of(Status.Code.NOT_DECLARED, "provider has no declaration")));
    assertEquals(Status.Code.NOT_DECLARED,
        adapter.summarize(Collections.singletonList(request), TIMEOUT).get(0).status().code());
    assertEquals(3, backend.declareCalls + backend.summaryCalls);
    assertEquals(2, adapter.counters().malformedProviderResultCount());
    assertEquals(2,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void acceptedDeclarationsPublishSchemasForPositionalSummaryValidation() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());

    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    SummaryRequest badName = request(METRIC_V1, 0, Collections.singletonMap(
        "bucket", DimValue.of("a")));
    SummaryRequest badKind = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of(1L)));
    SummaryRequest wildcardLimit = request(METRIC_V1, 1, Collections.emptyMap());
    SummaryRequest unknown = request(METRIC_V2, 0, Collections.emptyMap());
    backend.summaryResult = Arrays.asList(
        SummaryResponse.ok(null, Coverage.COMPLETE),
        SummaryResponse.error(Status.of(
            Status.Code.NOT_DECLARED, "provider has no declaration")));

    List<SummaryResponse> results = adapter.summarize(
        Arrays.asList(valid, badName, badKind, wildcardLimit, unknown), TIMEOUT);
    assertEquals(2, backend.lastSummaries.size());
    assertSame(valid, backend.lastSummaries.get(0));
    assertSame(unknown, backend.lastSummaries.get(1));
    assertEquals(Status.Code.OK, results.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, results.get(1).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, results.get(2).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, results.get(3).status().code());
    assertEquals(Status.Code.NOT_DECLARED, results.get(4).status().code());
  }

  @Test
  void summaryBatchShapeAndZeroTimeoutKeepLocalStatusesAndSkipProvider() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, new InlineExecutor(), new MutableTicker());
    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    SummaryRequest invalid = request(METRIC_V1, 1, Collections.emptyMap());
    SummaryRequest unknown = request(METRIC_V2, 0, Collections.emptyMap());

    assertTrue(adapter.summarize(null, TIMEOUT).isEmpty());
    assertTrue(adapter.summarize(Collections.emptyList(), TIMEOUT).isEmpty());
    assertTrue(adapter.summarize(repeatedRequests(129, valid), TIMEOUT).isEmpty());
    List<SummaryResponse> nullElement =
        adapter.summarize(Arrays.asList(valid, null), TIMEOUT);
    assertEquals(Status.Code.INVALID_REQUEST, nullElement.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, nullElement.get(1).status().code());

    List<SummaryResponse> zero =
        adapter.summarize(Arrays.asList(valid, invalid, unknown), Duration.ZERO);
    assertEquals(Status.Code.DEADLINE_EXCEEDED, zero.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, zero.get(1).status().code());
    assertEquals(Status.Code.DEADLINE_EXCEEDED, zero.get(2).status().code());
    assertEquals(Status.Code.INVALID_REQUEST,
        adapter.summarize(Collections.singletonList(unknown), null)
            .get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST,
        adapter.summarize(Collections.singletonList(unknown), Duration.ofNanos(-1L))
            .get(0).status().code());
    backend.summaryResult = Collections.singletonList(SummaryResponse.error(
        Status.of(Status.Code.NOT_DECLARED, "authoritative miss")));
    assertEquals(Status.Code.NOT_DECLARED,
        adapter.summarize(Collections.singletonList(unknown), TIMEOUT)
            .get(0).status().code());
    assertEquals(1, backend.summaryCalls);
  }

  @Test
  void malformedSummaryOutputDiscardsTheWholeEligibleSubbatch() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, new InlineExecutor(), new MutableTicker());
    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));

    backend.useSummaryResult = true;
    for (List<SummaryResponse> malformed : Arrays.asList(
        (List<SummaryResponse>) null,
        Collections.<SummaryResponse>emptyList(),
        Collections.<SummaryResponse>singletonList(null),
        throwingSummaryList())) {
      backend.summaryResult = malformed;
      List<SummaryResponse> results =
          adapter.summarize(Arrays.asList(valid, valid), TIMEOUT);
      assertEquals(Status.Code.UNAVAILABLE, results.get(0).status().code());
      assertEquals(Status.Code.UNAVAILABLE, results.get(1).status().code());
    }

    SummaryRequest locallyInvalid = request(METRIC_V1, 1, Collections.emptyMap());
    backend.summaryResult = Collections.emptyList();
    List<SummaryResponse> mixed =
        adapter.summarize(Arrays.asList(locallyInvalid, valid), TIMEOUT);
    assertEquals(Status.Code.INVALID_REQUEST, mixed.get(0).status().code());
    assertEquals(Status.Code.UNAVAILABLE, mixed.get(1).status().code());
    assertEquals(5, adapter.counters().malformedProviderResultCount());
    assertEquals(5,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void planningCountersAreImmutableAndCountEveryPositionalSummaryOutcome() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, new InlineExecutor(), new MutableTicker());
    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    SummaryRequest invalid = request(METRIC_V1, 1, Collections.emptyMap());
    SummaryRequest unknown = request(METRIC_V2, 0, Collections.emptyMap());

    backend.summaryResult = Arrays.asList(
        SummaryResponse.ok(null, Coverage.COMPLETE),
        SummaryResponse.error(Status.of(
            Status.Code.NOT_DECLARED, "provider has no declaration")));
    List<SummaryResponse> responses =
        adapter.summarize(Arrays.asList(valid, invalid, unknown), TIMEOUT);
    LocalMetricStorePlanningAdapter.PlanningCounterSnapshot counters = adapter.counters();

    assertEquals(1, counters.summaryCallCount());
    assertEquals(1, counters.summaryOutcomeCount(Status.Code.OK));
    assertEquals(1, counters.summaryOutcomeCount(Status.Code.INVALID_REQUEST));
    assertEquals(1, counters.summaryOutcomeCount(Status.Code.NOT_DECLARED));
    assertEquals(1, counters.declarationOutcomeCount(SchemaStatus.Code.ACCEPTED));
    assertEquals(0, counters.providerFailureCount());
    assertThrows(UnsupportedOperationException.class,
        () -> responses.add(SummaryResponse.ok(null, Coverage.COMPLETE)));

    adapter.summarize(Collections.emptyList(), TIMEOUT);
    assertEquals(1, counters.summaryCallCount());
    assertEquals(2, adapter.counters().summaryCallCount());
  }

  @Test
  void declarationExpiryDuringFinalReconstructionPreservesLocalStatusAndReconcilesCache() {
    // Entry, worker, wait, post-worker, and pre-reconstruction reads remain in budget.
    ScriptedTicker ticker = new ScriptedTicker(0L, 0L, 0L, 0L, 0L, 10L);
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, ticker);
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));

    List<SchemaStatus> results =
        adapter.declare(Arrays.asList(null, schema), Duration.ofNanos(10));

    assertEquals(SchemaStatus.Code.INVALID_REQUEST, results.get(0).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, results.get(1).code());
    LocalMetricStorePlanningAdapter.PlanningCounterSnapshot counters = adapter.counters();
    assertEquals(1,
        counters.declarationOutcomeCount(SchemaStatus.Code.INVALID_REQUEST));
    assertEquals(1, counters.declarationOutcomeCount(SchemaStatus.Code.UNAVAILABLE));
    assertEquals(0, counters.declarationOutcomeCount(SchemaStatus.Code.ACCEPTED));
    assertEquals(1, counters.timeoutCallCount());
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));

    SummaryRequest request = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    assertEquals(Status.Code.OK,
        adapter.summarize(Collections.singletonList(request), TIMEOUT)
            .get(0).status().code());
    assertEquals(1, backend.summaryCalls);
  }

  @Test
  void summaryExpiryDuringFinalReconstructionPreservesLocalStatusAndCountsFallback() {
    ScriptedTicker ticker = new ScriptedTicker(0L);
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, new InlineExecutor(), ticker);
    // Only the read immediately before outcome counting observes expiration.
    ticker.reset(0L, 0L, 0L, 0L, 0L, 10L);
    SummaryRequest invalid = request(METRIC_V1, 1, Collections.emptyMap());
    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    backend.summaryResult =
        Collections.singletonList(SummaryResponse.ok(null, Coverage.COMPLETE));

    List<SummaryResponse> results =
        adapter.summarize(Arrays.asList(invalid, valid), Duration.ofNanos(10));

    assertEquals(Status.Code.INVALID_REQUEST, results.get(0).status().code());
    assertEquals(Status.Code.DEADLINE_EXCEEDED, results.get(1).status().code());
    LocalMetricStorePlanningAdapter.PlanningCounterSnapshot counters = adapter.counters();
    assertEquals(1, counters.summaryOutcomeCount(Status.Code.INVALID_REQUEST));
    assertEquals(1, counters.summaryOutcomeCount(Status.Code.DEADLINE_EXCEEDED));
    assertEquals(0, counters.summaryOutcomeCount(Status.Code.OK));
    assertEquals(1, counters.timeoutCallCount());
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void executorQueueWaitAndLateProviderOutputConsumeOneBudget() {
    MutableTicker queueTicker = new MutableTicker();
    FakeBackend queuedBackend = new FakeBackend();
    AdvancingExecutor queuedExecutor = new AdvancingExecutor(queueTicker, 10L);
    LocalMetricStorePlanningAdapter queued =
        adapter(queuedBackend, queuedExecutor, false, queueTicker);
    MetricSchema schema = schema(OTHER_V1, "table", Duration.ofDays(1));

    SchemaStatus queuedResult =
        queued.declare(Collections.singletonList(schema), Duration.ofNanos(10)).get(0);
    assertEquals(SchemaStatus.Code.UNAVAILABLE, queuedResult.code());
    assertEquals(0, queuedBackend.declareCalls);

    ScriptedTicker lateTicker = new ScriptedTicker(0L);
    FakeBackend lateBackend = new FakeBackend();
    LocalMetricStorePlanningAdapter late =
        declaredAdapter(lateBackend, new InlineExecutor(), lateTicker);
    // Entry through provider validation remains in budget; only final reconstruction expires.
    lateTicker.reset(0L, 0L, 0L, 0L, 0L, 0L, 10L);
    SummaryRequest valid = request(METRIC_V1, 0, Collections.singletonMap(
        "table", DimValue.of("a")));
    lateBackend.summaryResult =
        Collections.singletonList(SummaryResponse.ok(null, Coverage.COMPLETE));

    SummaryResponse lateResult =
        late.summarize(Collections.singletonList(valid), Duration.ofNanos(10)).get(0);
    assertEquals(Status.Code.DEADLINE_EXCEEDED, lateResult.status().code());
    assertEquals(2, late.counters().timeoutCallCount() + queued.counters().timeoutCallCount());
    assertEquals(2,
        late.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE) +
            queued.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void acceptedResultReconcilesInsideLateWorkerWithoutRegressingTerminalBreaker()
      throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      entered.countDown();
      awaitUninterruptibly(release);
      return Collections.singletonList(SchemaStatus.accepted(schemas.get(0).metric(), null));
    };
    HangingExecutor executor = new HangingExecutor(entered);
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend, executor, false, new MutableTicker(), breakerPolicy(1, 1, 1.0));

    SchemaStatus result = adapter.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
        TIMEOUT).get(0);

    assertEquals(SchemaStatus.Code.UNAVAILABLE, result.code());
    assertTrue(executor.cancelled);
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());
    LocalHistoryMetricsCounters terminalCounters = adapter.breakerCounters();

    release.countDown();
    adapter.stopPlanning();
    assertTrue(adapter.awaitPlanningTermination(TIMEOUT));

    SummaryRequest locallyInvalid = request(METRIC_V1, 0, Collections.singletonMap(
        "bucket", DimValue.of("a")));
    assertEquals(Status.Code.INVALID_REQUEST,
        adapter.summarize(Collections.singletonList(locallyInvalid), TIMEOUT)
            .get(0).status().code());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());
    assertBreakerUnchanged(terminalCounters, adapter.breakerCounters());
    assertEquals(1, adapter.counters().timeoutCallCount());
  }

  @Test
  void invalidLateResultsNeverReconcileDeclarationCache() throws Exception {
    assertLateResultDoesNotReconcile(
        Collections.singletonList(SchemaStatus.accepted(METRIC_V2, null)), false);
    assertLateResultDoesNotReconcile(Collections.<SchemaStatus>emptyList(), false);
    assertLateResultDoesNotReconcile(Collections.singletonList(SchemaStatus.of(
        METRIC_V1, SchemaStatus.Code.DENIED, "denied")), false);
    assertLateResultDoesNotReconcile(null, true);
  }

  @Test
  void recordDropsBeforeButAcceptsAfterLateAcceptedReconciliation() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      entered.countDown();
      awaitUninterruptibly(release);
      return Collections.singletonList(SchemaStatus.accepted(schemas.get(0).metric(), null));
    };
    HangingExecutor executor = new HangingExecutor(entered);
    LocalMetricStorePlanningAdapter adapter = recordingAdapter(
        backend, executor, new MutableTicker(), breakerPolicy(1, 1, 1.0));
    Observation observation = new Observation(
        METRIC_V1,
        Collections.singletonMap("table", DimValue.of("a")),
        1.0,
        1L);

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(
            Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
            TIMEOUT).get(0).code());
    adapter.record(Collections.singletonList(observation));
    assertEquals(1, adapter.recordCounters().undeclaredItemCount());
    assertEquals(0, backend.recordCalls.get());

    release.countDown();
    adapter.stopPlanning();
    assertTrue(adapter.awaitPlanningTermination(TIMEOUT));
    adapter.record(Collections.singletonList(observation));
    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(1, backend.recordCalls.get());
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void planningTerminationWaitsForActiveBackendNotificationWithoutPolling() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      entered.countDown();
      boolean interrupted = false;
      while (release.getCount() != 0) {
        try {
          release.await();
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return Collections.singletonList(SchemaStatus.accepted(schemas.get(0).metric(), null));
    };
    HangingExecutor executor = new HangingExecutor(entered);
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, executor, false, new MutableTicker());

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(
            Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
            TIMEOUT).get(0).code());
    adapter.stopPlanning();

    CountDownLatch waitStarted = new CountDownLatch(1);
    java.util.concurrent.ExecutorService waiter = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> completed = waiter.submit(() -> {
        waitStarted.countDown();
        return adapter.awaitPlanningTermination(TIMEOUT);
      });
      assertTrue(waitStarted.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class,
          () -> completed.get(100, TimeUnit.MILLISECONDS));

      release.countDown();
      assertTrue(completed.get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      waiter.shutdownNow();
    }
  }

  @Test
  void backendExceptionsLinkageAndExecutorRejectionAreContainedAndCounted() {
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));

    FakeBackend runtime = new FakeBackend();
    runtime.declareHandler = (schemas, timeout) -> {
      throw new IllegalStateException("failed");
    };
    LocalMetricStorePlanningAdapter runtimeAdapter =
        adapter(runtime, new InlineExecutor(), false, new MutableTicker());
    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        runtimeAdapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());

    FakeBackend linkage = new FakeBackend();
    linkage.declareHandler = (schemas, timeout) -> {
      throw new AbstractMethodError("old provider");
    };
    LocalMetricStorePlanningAdapter linkageAdapter =
        adapter(linkage, new InlineExecutor(), false, new MutableTicker());
    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        linkageAdapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());

    FakeBackend rejected = new FakeBackend();
    LocalMetricStorePlanningAdapter rejectedAdapter =
        adapter(rejected, new RejectingExecutor(), false, new MutableTicker());
    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        rejectedAdapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
    assertEquals(1, rejectedAdapter.counters().executorRejectionCount());
    assertEquals(0, rejected.declareCalls);
    assertEquals(1,
        runtimeAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1,
        linkageAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1,
        rejectedAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void invalidTimeoutsAreTotalAndCountersSnapshotEveryPositionalOutcome() {
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, new InlineExecutor(), false, new MutableTicker());
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));

    assertEquals(SchemaStatus.Code.INVALID_REQUEST,
        adapter.declare(Collections.singletonList(schema), null).get(0).code());
    assertEquals(SchemaStatus.Code.INVALID_REQUEST,
        adapter.declare(
            Collections.singletonList(schema), Duration.ofNanos(-1)).get(0).code());
    adapter.record(Arrays.asList(
        new Observation(METRIC_V1, Collections.singletonMap("table", DimValue.of("a")),
            1.0, 1L),
        null));

    LocalMetricStorePlanningAdapter.PlanningCounterSnapshot counters = adapter.counters();
    assertEquals(2, counters.declareCallCount());
    assertEquals(2,
        counters.declarationOutcomeCount(SchemaStatus.Code.INVALID_REQUEST));
    assertEquals(1, counters.recordCallCount());
    assertEquals(2, counters.recordDroppedCount());
    assertEquals(0, counters.suppressedCallCount());

    adapter.declare(Collections.emptyList(), TIMEOUT);
    assertEquals(2, counters.declareCallCount());
    assertEquals(3, adapter.counters().declareCallCount());
  }

  @Test
  void expiryImmediatelyBeforeOpenBreakerPreservesLocalPositionsWithoutBreakerAccess() {
    ScriptedTicker declarationTicker = new ScriptedTicker(0L);
    FakeBackend declarationBackend = new FakeBackend();
    LocalMetricStorePlanningAdapter declarationAdapter = adapter(
        declarationBackend,
        new InlineExecutor(),
        false,
        declarationTicker,
        breakerPolicy(1, 1, 1.0));
    MetricSchema first = schema(METRIC_V1, "table", Duration.ofDays(1));
    declarationBackend.declareResult = Collections.singletonList(SchemaStatus.of(
        METRIC_V1, SchemaStatus.Code.UNAVAILABLE, "offline"));
    declarationAdapter.declare(Collections.singletonList(first), TIMEOUT);
    assertEquals(LocalCircuitBreakerState.OPEN, declarationAdapter.breakerState());
    LocalHistoryMetricsCounters declarationBefore = declarationAdapter.breakerCounters();
    int declarationCalls = declarationBackend.declareCalls;

    declarationTicker.reset(0L, 10L);
    List<SchemaStatus> declarationResults = declarationAdapter.declare(
        Arrays.asList(null, schema(OTHER_V1, "table", Duration.ofDays(1))),
        Duration.ofNanos(10L));

    assertEquals(SchemaStatus.Code.INVALID_REQUEST, declarationResults.get(0).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, declarationResults.get(1).code());
    assertEquals(declarationCalls, declarationBackend.declareCalls);
    assertEquals(1, declarationAdapter.counters().timeoutCallCount());
    assertBreakerUnchanged(
        declarationBefore, declarationAdapter.breakerCounters());
    assertEquals(LocalCircuitBreakerState.OPEN, declarationAdapter.breakerState());

    ScriptedTicker summaryTicker = new ScriptedTicker(0L);
    FakeBackend summaryBackend = new FakeBackend();
    LocalMetricStorePlanningAdapter summaryAdapter = adapter(
        summaryBackend,
        new InlineExecutor(),
        false,
        summaryTicker,
        breakerPolicy(1, 1, 1.0));
    MetricSchema declared = schema(METRIC_V1, "table", Duration.ofDays(1));
    summaryBackend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    summaryAdapter.declare(Collections.singletonList(declared), TIMEOUT);
    summaryBackend.useSummaryResult = true;
    summaryBackend.summaryResult = Collections.singletonList(SummaryResponse.error(
        Status.of(Status.Code.UNAVAILABLE, "offline")));
    SummaryRequest valid = request(
        METRIC_V1, 0, Collections.singletonMap("table", DimValue.of("x")));
    summaryAdapter.summarize(Collections.singletonList(valid), TIMEOUT);
    assertEquals(LocalCircuitBreakerState.OPEN, summaryAdapter.breakerState());
    LocalHistoryMetricsCounters summaryBefore = summaryAdapter.breakerCounters();
    int summaryCalls = summaryBackend.summaryCalls;

    summaryTicker.reset(0L, 10L);
    List<SummaryResponse> summaryResults = summaryAdapter.summarize(
        Arrays.asList(request(METRIC_V2, 0, Collections.emptyMap()), valid),
        Duration.ofNanos(10L));

    assertEquals(Status.Code.DEADLINE_EXCEEDED, summaryResults.get(0).status().code());
    assertEquals(Status.Code.DEADLINE_EXCEEDED, summaryResults.get(1).status().code());
    assertEquals(summaryCalls, summaryBackend.summaryCalls);
    assertEquals(1, summaryAdapter.counters().timeoutCallCount());
    assertBreakerUnchanged(summaryBefore, summaryAdapter.breakerCounters());
    assertEquals(LocalCircuitBreakerState.OPEN, summaryAdapter.breakerState());
  }

  private static void assertBreakerUnchanged(
      LocalHistoryMetricsCounters before, LocalHistoryMetricsCounters after) {
    for (LocalMetricCounter counter : LocalMetricCounter.values()) {
      assertEquals(before.value(counter), after.value(counter), counter.name());
    }
  }

  @Test
  void breakerIsSharedAndSuppressesOnlyProviderEligiblePositions() {
    MutableTicker ticker = new MutableTicker();
    FakeBackend backend = new FakeBackend();
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend,
        new InlineExecutor(),
        false,
        ticker,
        breakerPolicy(2, 2, 0.5));
    MetricSchema declared = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(Collections.singletonList(declared), TIMEOUT).get(0).code());

    backend.useSummaryResult = true;
    backend.summaryResult = Collections.singletonList(SummaryResponse.error(
        Status.of(Status.Code.UNAVAILABLE, "provider unavailable")));
    SummaryRequest request = request(
        METRIC_V1, 0, Collections.singletonMap("table", DimValue.of("x")));
    assertEquals(Status.Code.UNAVAILABLE,
        adapter.summarize(Collections.singletonList(request), TIMEOUT).get(0).status().code());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());

    List<SchemaStatus> suppressed = adapter.declare(
        Arrays.asList(null, schema(OTHER_V1, "table", Duration.ofDays(1))), TIMEOUT);
    assertEquals(SchemaStatus.Code.INVALID_REQUEST, suppressed.get(0).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE, suppressed.get(1).code());
    assertEquals(1, backend.declareCalls);
    assertEquals(2,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SUPPRESSED));

    adapter.declare(
        Collections.singletonList(schema(OTHER_V1, "table", Duration.ofDays(1))),
        Duration.ZERO);
    adapter.record(Collections.<Observation>emptyList());
    assertEquals(2,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SUPPRESSED));
  }

  @Test
  void mixedUnavailableFailsSampleWhileDeniedIsNonfailure() {
    FakeBackend mixed = new FakeBackend();
    mixed.declareResult = Arrays.asList(
        SchemaStatus.accepted(METRIC_V1, null),
        SchemaStatus.of(OTHER_V1, SchemaStatus.Code.UNAVAILABLE, "offline"));
    LocalMetricStorePlanningAdapter mixedAdapter = adapter(
        mixed,
        new InlineExecutor(),
        false,
        new MutableTicker(),
        breakerPolicy(1, 1, 1.0));
    mixedAdapter.declare(
        Arrays.asList(
            schema(METRIC_V1, "table", Duration.ofDays(1)),
            schema(OTHER_V1, "table", Duration.ofDays(1))),
        TIMEOUT);
    assertEquals(LocalCircuitBreakerState.OPEN, mixedAdapter.breakerState());
    assertEquals(1,
        mixedAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));

    FakeBackend denied = new FakeBackend();
    denied.declareResult = Collections.singletonList(
        SchemaStatus.of(METRIC_V1, SchemaStatus.Code.DENIED, "policy"));
    LocalMetricStorePlanningAdapter deniedAdapter = adapter(
        denied,
        new InlineExecutor(),
        false,
        new MutableTicker(),
        breakerPolicy(1, 1, 1.0));
    deniedAdapter.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
        TIMEOUT);
    assertEquals(LocalCircuitBreakerState.CLOSED, deniedAdapter.breakerState());
    assertEquals(0,
        deniedAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));

    FakeBackend notDeclared = new FakeBackend();
    LocalMetricStorePlanningAdapter notDeclaredAdapter = adapter(
        notDeclared,
        new InlineExecutor(),
        false,
        new MutableTicker(),
        breakerPolicy(1, 1, 1.0));
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    notDeclared.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    notDeclaredAdapter.declare(Collections.singletonList(schema), TIMEOUT);
    notDeclared.useSummaryResult = true;
    notDeclared.summaryResult = Collections.singletonList(SummaryResponse.error(
        Status.of(Status.Code.NOT_DECLARED, "provider has no declaration")));
    SummaryRequest request = request(
        METRIC_V1, 0, Collections.singletonMap("table", DimValue.of("x")));
    assertEquals(Status.Code.NOT_DECLARED,
        notDeclaredAdapter.summarize(
            Collections.singletonList(request), TIMEOUT).get(0).status().code());
    assertEquals(LocalCircuitBreakerState.CLOSED, notDeclaredAdapter.breakerState());
    assertEquals(0,
        notDeclaredAdapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  @Test
  void lateOldGenerationCompletionReturnsButCannotChangeRecoveredState() throws Exception {
    MutableTicker ticker = new MutableTicker();
    FakeBackend backend = new FakeBackend();
    ThreadPoolExecutor planningExecutor =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(2, command -> {
          Thread thread = new Thread(command, "history-metrics-planning-test");
          thread.setDaemon(true);
          return thread;
        });
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend,
        planningExecutor,
        false,
        ticker,
        breakerPolicy(1, 1, 1.0));
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    AtomicInteger sequence = new AtomicInteger();
    backend.declareHandler = (schemas, timeout) -> {
      int call = sequence.incrementAndGet();
      if (call == 1) {
        firstEntered.countDown();
        try {
          releaseFirst.await();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
        return Collections.singletonList(
            SchemaStatus.accepted(schemas.get(0).metric(), null));
      }
      if (call == 2) {
        return Collections.singletonList(SchemaStatus.of(
            schemas.get(0).metric(), SchemaStatus.Code.UNAVAILABLE, "offline"));
      }
      return Collections.singletonList(
          SchemaStatus.accepted(schemas.get(0).metric(), null));
    };
    AtomicReference<List<SchemaStatus>> lateResult =
        new AtomicReference<List<SchemaStatus>>();
    Thread lateCaller = new Thread(() -> lateResult.set(adapter.declare(
        Collections.singletonList(
            schema(METRIC_V1, "table", Duration.ofDays(1))),
        TIMEOUT)), "late-declaration-caller");
    lateCaller.start();
    assertTrue(firstEntered.await(5, TimeUnit.SECONDS));

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(
            Collections.singletonList(
                schema(OTHER_V1, "table", Duration.ofDays(1))),
            TIMEOUT).get(0).code());
    assertEquals(LocalCircuitBreakerState.OPEN, adapter.breakerState());

    ticker.nanos.set(10L);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(
            Collections.singletonList(
                schema(OTHER_V1, "table", Duration.ofDays(1))),
            TIMEOUT).get(0).code());
    assertEquals(LocalCircuitBreakerState.CLOSED, adapter.breakerState());

    releaseFirst.countDown();
    lateCaller.join(5_000L);
    assertFalse(lateCaller.isAlive());
    assertEquals(SchemaStatus.Code.ACCEPTED, lateResult.get().get(0).code());
    assertEquals(LocalCircuitBreakerState.CLOSED, adapter.breakerState());
    assertEquals(3,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_HALF_OPEN));
    assertEquals(1,
        adapter.breakerCounters().value(LocalMetricCounter.BREAKER_CLOSE));
    planningExecutor.shutdownNow();
  }

  @Test
  void executorRejectionAndQueueExpiryAreTerminalBreakerFailures() {
    LocalMetricStorePlanningAdapter rejected = adapter(
        new FakeBackend(),
        new RejectingExecutor(),
        false,
        new MutableTicker(),
        breakerPolicy(1, 1, 1.0));
    rejected.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
        TIMEOUT);
    assertEquals(LocalCircuitBreakerState.OPEN, rejected.breakerState());

    MutableTicker ticker = new MutableTicker();
    LocalMetricStorePlanningAdapter expired = adapter(
        new FakeBackend(),
        new AdvancingExecutor(ticker, TIMEOUT.toNanos()),
        false,
        ticker,
        breakerPolicy(1, 1, 1.0));
    expired.declare(
        Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
        TIMEOUT);
    assertEquals(LocalCircuitBreakerState.OPEN, expired.breakerState());
    assertEquals(1,
        expired.breakerCounters().value(LocalMetricCounter.BREAKER_FAILURE));
  }

  private static void assertLateResultDoesNotReconcile(
      List<SchemaStatus> providerResult, boolean throwFailure) throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    FakeBackend backend = new FakeBackend();
    backend.declareHandler = (schemas, timeout) -> {
      entered.countDown();
      awaitUninterruptibly(release);
      if (throwFailure) {
        throw new IllegalStateException("provider failed");
      }
      return providerResult;
    };
    HangingExecutor executor = new HangingExecutor(entered);
    LocalMetricStorePlanningAdapter adapter = adapter(
        backend, executor, false, new MutableTicker(), breakerPolicy(1, 1, 1.0));

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        adapter.declare(
            Collections.singletonList(schema(METRIC_V1, "table", Duration.ofDays(1))),
            TIMEOUT).get(0).code());
    release.countDown();
    adapter.stopPlanning();
    assertTrue(adapter.awaitPlanningTermination(TIMEOUT));

    SummaryRequest locallyInvalid = request(METRIC_V1, 0, Collections.singletonMap(
        "bucket", DimValue.of("a")));
    assertEquals(Status.Code.UNAVAILABLE,
        adapter.summarize(Collections.singletonList(locallyInvalid), TIMEOUT)
            .get(0).status().code());
    assertEquals(0, backend.summaryCalls);
  }

  private static LocalMetricStorePlanningAdapter recordingAdapter(
      FakeBackend backend,
      AbstractExecutorService executor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalCircuitBreakerPolicy breakerPolicy) {
    return LocalMetricStorePlanningAdapter.createWithRecording(
        backend,
        catalog(),
        executor,
        false,
        ticker,
        breakerPolicy,
        Clock.fixed(Instant.ofEpochMilli(1L), ZoneOffset.UTC),
        () -> LocalProvenanceIdentity.of("app", null, "test-version"),
        new LocalAsyncRecordPipeline.QueuePolicy(8, 1),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (latch.getCount() != 0L) {
      try {
        latch.await();
      } catch (InterruptedException expected) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static LocalCircuitBreakerPolicy breakerPolicy(
      int windowSize, int minSamples, double failureRate) {
    return LocalCircuitBreakerPolicy.of(
        windowSize,
        minSamples,
        failureRate,
        Duration.ofDays(1),
        1.0,
        Duration.ofNanos(10));
  }

  private static LocalMetricStorePlanningAdapter declaredAdapter(
      FakeBackend backend,
      AbstractExecutorService executor,
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    LocalMetricStorePlanningAdapter adapter =
        adapter(backend, executor, false, ticker);
    MetricSchema schema = schema(METRIC_V1, "table", Duration.ofDays(1));
    backend.declareResult =
        Collections.singletonList(SchemaStatus.accepted(METRIC_V1, null));
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
    backend.declareCalls = 0;
    return adapter;
  }

  private static LocalMetricStorePlanningAdapter adapter(
      FakeBackend backend,
      AbstractExecutorService executor,
      boolean ownsExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    return adapter(backend, executor, ownsExecutor, ticker, BREAKER_POLICY);
  }

  private static LocalMetricStorePlanningAdapter adapter(
      FakeBackend backend,
      AbstractExecutorService executor,
      boolean ownsExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalCircuitBreakerPolicy breakerPolicy) {
    return LocalMetricStorePlanningAdapter.create(
        backend, catalog(), executor, ownsExecutor, ticker, breakerPolicy);
  }

  private static HistoryMetricCatalog catalog() {
    return TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.metric"),
        TestHistoryMetricCatalog.live(18, "test.other"));
  }

  private static MetricSchema schema(
      MetricVersionId metric, String dimension, Duration planningMaxAge) {
    return new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec(dimension, DimValue.Kind.STRING)),
        new Retention(planningMaxAge, Duration.ofDays(30)));
  }

  private static SummaryRequest request(
      MetricVersionId metric, int limit, Map<String, DimValue> bound) {
    return SummaryRequest.builder(metric)
        .bound(bound)
        .window(0L, 10L)
        .limit(limit)
        .build();
  }

  private static List<MetricSchema> repeatedSchemas(int size, MetricSchema schema) {
    List<MetricSchema> schemas = new ArrayList<MetricSchema>();
    for (int index = 0; index < size; index++) {
      schemas.add(schema);
    }
    return schemas;
  }

  private static List<SummaryRequest> repeatedRequests(int size, SummaryRequest request) {
    List<SummaryRequest> requests = new ArrayList<SummaryRequest>();
    for (int index = 0; index < size; index++) {
      requests.add(request);
    }
    return requests;
  }

  private static List<SummaryResponse> throwingSummaryList() {
    return new AbstractList<SummaryResponse>() {
      @Override
      public SummaryResponse get(int index) {
        throw new IllegalStateException("malformed accessor");
      }

      @Override
      public int size() {
        return 2;
      }
    };
  }

  private interface DeclareHandler {
    List<SchemaStatus> apply(List<MetricSchema> schemas, Duration timeout);
  }

  private static final class FakeBackend implements HistoryMetricsBackend {
    private BackendInfo info =
        new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "fake backend");
    private RuntimeException infoFailure;
    private LinkageError infoLinkage;
    private int infoCalls;
    private int declareCalls;
    private final AtomicInteger recordCalls = new AtomicInteger();
    private int summaryCalls;
    private List<MetricSchema> lastDeclared = Collections.emptyList();
    private List<SummaryRequest> lastSummaries = Collections.emptyList();
    private List<SchemaStatus> declareResult;
    private List<SummaryResponse> summaryResult;
    private boolean useSummaryResult;
    private DeclareHandler declareHandler;

    private static FakeBackend nullInfo() {
      FakeBackend backend = new FakeBackend();
      backend.info = null;
      return backend;
    }

    private static FakeBackend throwingInfo() {
      FakeBackend backend = new FakeBackend();
      backend.infoFailure = new IllegalStateException("info failed");
      return backend;
    }

    private static FakeBackend linkageInfo() {
      FakeBackend backend = new FakeBackend();
      backend.infoLinkage = new AbstractMethodError("old provider");
      return backend;
    }

    private static FakeBackend lowInfo() {
      FakeBackend backend = new FakeBackend();
      backend.infoFailure = new IllegalArgumentException("version zero");
      return backend;
    }

    private static FakeBackend highInfo() {
      FakeBackend backend = new FakeBackend();
      backend.info = new BackendInfo(2, "future backend");
      return backend;
    }

    @Override
    public BackendInfo info() {
      infoCalls++;
      if (infoFailure != null) {
        throw infoFailure;
      }
      if (infoLinkage != null) {
        throw infoLinkage;
      }
      return info;
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      declareCalls++;
      lastDeclared = schemas;
      if (declareHandler != null) {
        return declareHandler.apply(schemas, timeout);
      }
      if (declareResult != null) {
        return declareResult;
      }
      List<SchemaStatus> statuses = new ArrayList<SchemaStatus>();
      for (MetricSchema schema : schemas) {
        statuses.add(SchemaStatus.accepted(schema.metric(), null));
      }
      return statuses;
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      recordCalls.incrementAndGet();
      return WriteResult.ok(observations.size());
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      summaryCalls++;
      lastSummaries = requests;
      if (useSummaryResult || summaryResult != null) {
        return summaryResult;
      }
      List<SummaryResponse> responses = new ArrayList<SummaryResponse>();
      for (SummaryRequest ignored : requests) {
        responses.add(SummaryResponse.ok(null, Coverage.COMPLETE));
      }
      return responses;
    }

    @Override
    public void close() {
    }
  }

  private static class InlineExecutor extends AbstractExecutorService {
    private boolean shutdown;

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException("shutdown");
      }
      command.run();
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }
  }

  private static final class RejectingExecutor extends InlineExecutor {
    @Override
    public void execute(Runnable command) {
      throw new RejectedExecutionException("full");
    }
  }

  private static final class ToggleErrorExecutor extends InlineExecutor {
    private boolean fail;

    @Override
    public void execute(Runnable command) {
      if (fail) {
        throw new AssertionError("unexpected executor failure");
      }
      super.execute(command);
    }
  }

  private static final class AdvancingExecutor extends InlineExecutor {
    private final MutableTicker ticker;
    private final long advancedNanos;

    private AdvancingExecutor(MutableTicker ticker, long advancedNanos) {
      this.ticker = ticker;
      this.advancedNanos = advancedNanos;
    }

    @Override
    public void execute(Runnable command) {
      ticker.nanos.set(advancedNanos);
      super.execute(command);
    }
  }

  private static final class HangingExecutor extends InlineExecutor {
    private final CountDownLatch backendEntered;
    private volatile boolean cancelled;
    private FutureTask<?> task;
    private Thread thread;

    private HangingExecutor(CountDownLatch backendEntered) {
      this.backendEntered = backendEntered;
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
      FutureTask<T> submitted = new FutureTask<T>(callable);
      task = submitted;
      thread = new Thread(submitted, "history-metrics-test-hang");
      thread.setDaemon(true);
      thread.start();
      return new Future<T>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          cancelled = true;
          return submitted.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
          return submitted.isCancelled();
        }

        @Override
        public boolean isDone() {
          return submitted.isDone();
        }

        @Override
        public T get() throws java.util.concurrent.ExecutionException, InterruptedException {
          return submitted.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
          backendEntered.await();
          throw new TimeoutException("deterministic timeout");
        }
      };
    }

    @Override
    public List<Runnable> shutdownNow() {
      if (task != null) {
        task.cancel(true);
      }
      return super.shutdownNow();
    }
  }

  private static class MutableTicker implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.get();
    }
  }

  private static final class ThrowingTicker extends MutableTicker {
    private RuntimeException failure;
    private LinkageError linkage;
    private int throwOnRead = -1;
    private int reads;

    @Override
    public long readNanos() {
      reads++;
      if (throwOnRead < 0 || reads == throwOnRead) {
        if (failure != null) {
          throw failure;
        }
        if (linkage != null) {
          throw linkage;
        }
      }
      return super.readNanos();
    }
  }

  private static final class ScriptedTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private long[] values;
    private int index;

    private ScriptedTicker(long... values) {
      reset(values);
    }

    private void reset(long... newValues) {
      values = newValues.clone();
      index = 0;
    }

    @Override
    public long readNanos() {
      int current = Math.min(index, values.length - 1);
      index++;
      return values[current];
    }
  }
}
