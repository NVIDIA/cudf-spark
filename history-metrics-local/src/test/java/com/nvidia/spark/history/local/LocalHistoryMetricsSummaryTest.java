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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.Summary;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.TestHistoryMetricCatalog;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/**
 * Local-provider summary behaviors for FR-04, FR-09 through FR-13, FR-20/21 and
 * NFR-03/04/09/10.
 */
class LocalHistoryMetricsSummaryTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(1);
  private static final long NOW_MS = 1_000L;
  private static final MetricVersionId METRIC_V1 = new MetricVersionId(17, 1);
  private static final MetricVersionId METRIC_V2 = new MetricVersionId(17, 2);
  private static final MetricVersionId METRIC_V3 = new MetricVersionId(17, 3);

  @Test
  void exactAndWildcardEqualityAggregateIntoOneSummaryAndEmptyEvidenceIsSuccessful() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      assertOk(backend.summarize(Collections.singletonList(
          request(METRIC_V1, 0, 500, 0, dims("table", "missing"))), TIMEOUT).get(0),
          null, Coverage.COMPLETE);

      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 2L), 4.0, 200),
          stamped(METRIC_V1, dims("table", "b", "bucket", 1L), 100.0, 200));

      Summary exact = onlySummary(backend, request(
          METRIC_V1, 0, 500, 0, dims("table", "a", "bucket", 1L)));
      assertSummary(exact, 1, 2.0, 2.0, 2.0, 100, 100);

      Summary wildcard = onlySummary(backend, request(
          METRIC_V1, 0, 500, 0, dims("table", "a")));
      assertSummary(wildcard, 2, 3.0, 2.0, 4.0, 100, 200);
    } finally {
      backend.close();
    }
  }

  @Test
  void observationWindowIsInclusiveFromExclusiveToAndVersionsNeverMix() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      declare(backend, schema(METRIC_V2, Duration.ofDays(3)));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 200),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 3.0, 300),
          stamped(METRIC_V2, dims("table", "a", "bucket", 1L), 99.0, 200));

      Summary v1 = onlySummary(backend, request(
          METRIC_V1, 100, 300, 0, dims("table", "a", "bucket", 1L)));
      assertSummary(v1, 2, 1.5, 1.0, 2.0, 100, 200);
      assertSummary(onlySummary(backend, request(
          METRIC_V2, 100, 300, 0, dims("table", "a", "bucket", 1L))),
          1, 99.0, 99.0, 99.0, 200, 200);

      SummaryResponse undeclared = backend.summarize(
          Collections.singletonList(request(METRIC_V3, 100, 300, 0, Collections.emptyMap())),
          TIMEOUT).get(0);
      assertEquals(Status.Code.NOT_DECLARED, undeclared.status().code());
      assertNull(undeclared.summary());
      assertNull(undeclared.coverage());
    } finally {
      backend.close();
    }
  }

  @Test
  void schemaDependentInvalidRequestsRemainPositionalWhileValidRequestsProceed() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      record(backend, stamped(
          METRIC_V1, dims("table", "a", "bucket", 1L), 7.0, 100));

      List<SummaryRequest> requests = Arrays.asList(
          request(METRIC_V1, 0, 200, 0, dims("unknown", "a")),
          request(METRIC_V1, 0, 200, 0, dims("bucket", "wrong-kind")),
          request(METRIC_V1, 0, 200, 1, dims("table", "a")),
          request(METRIC_V1, 0, 200, 1, dims("table", "a", "bucket", 1L)));
      List<SummaryResponse> responses = backend.summarize(requests, TIMEOUT);

      assertEquals(requests.size(), responses.size());
      assertEquals(Status.Code.INVALID_REQUEST, responses.get(0).status().code());
      assertEquals(Status.Code.INVALID_REQUEST, responses.get(1).status().code());
      assertEquals(Status.Code.INVALID_REQUEST, responses.get(2).status().code());
      assertEquals(Status.Code.OK, responses.get(3).status().code());
      assertEquals(7.0, responses.get(3).summary().mean());
    } finally {
      backend.close();
    }
  }

  @Test
  void fullyBoundLimitUsesTimestampThenAcceptanceOrdinalDescending() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 10.0, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 20.0, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 30.0, 101));

      Summary newest = onlySummary(backend, request(
          METRIC_V1, 0, 200, 1, dims("table", "a", "bucket", 1L)));
      assertSummary(newest, 1, 30.0, 30.0, 30.0, 101, 101);

      Summary newestTwo = onlySummary(backend, request(
          METRIC_V1, 0, 200, 2, dims("table", "a", "bucket", 1L)));
      assertSummary(newestTwo, 2, 25.0, 20.0, 30.0, 100, 101);

      declare(backend, new MetricSchema(
          METRIC_V2,
          Collections.<DimensionSpec>emptyList(),
          new Retention(Duration.ofDays(3), Duration.ofDays(30))));
      record(backend,
          stamped(METRIC_V2, Collections.emptyMap(), 11.0, 100),
          stamped(METRIC_V2, Collections.emptyMap(), 12.0, 100));
      assertSummary(onlySummary(backend, request(
          METRIC_V2, 0, 200, 1, Collections.emptyMap())),
          1, 12.0, 12.0, 12.0, 100, 100);
    } finally {
      backend.close();
    }
  }

  @Test
  void effectivePlanningAgeControlsCoverageAndZeroDisablesVisibilityWithoutDeletion() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofMillis(100));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofMillis(500)));
      declare(backend, schema(METRIC_V2, Duration.ZERO));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 899),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 900),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 3.0, 999),
          stamped(METRIC_V2, dims("table", "a", "bucket", 1L), 4.0, 999));

      SummaryResponse clipped = response(backend, request(
          METRIC_V1, 0, 2_000, 0, dims("table", "a", "bucket", 1L)));
      assertEquals(Coverage.WINDOW_CLIPPED, clipped.coverage());
      assertSummary(clipped.summary(), 2, 2.5, 2.0, 3.0, 900, 999);

      SummaryResponse complete = response(backend, request(
          METRIC_V1, 900, 2_000, 0, dims("table", "a", "bucket", 1L)));
      assertEquals(Coverage.COMPLETE, complete.coverage());
      assertEquals(2, complete.summary().count());

      SummaryResponse disabled = response(backend, request(
          METRIC_V2, 0, 2_000, 0, dims("table", "a", "bucket", 1L)));
      assertOk(disabled, null, Coverage.WINDOW_CLIPPED);
      assertEquals(1, backend.testHandle().observations(METRIC_V2).size());
    } finally {
      backend.close();
    }
  }

  @Test
  void planningBoundaryArithmeticSaturatesInsteadOfOverflowing() {
    long now = Long.MIN_VALUE + 10;
    LocalHistoryMetricsBackend backend = backend(
        Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC), Duration.ofMillis(100));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofMillis(100)));
      record(backend, stamped(
          METRIC_V1, dims("table", "a", "bucket", 1L), 8.0, now + 5));
      SummaryResponse response = response(backend, request(
          METRIC_V1, Long.MIN_VALUE, now + 9, 0,
          dims("table", "a", "bucket", 1L)));
      assertEquals(Coverage.COMPLETE, response.coverage());
      assertEquals(1, response.summary().count());
    } finally {
      backend.close();
    }
  }

  @Test
  void positiveSubMillisecondAndHugePlanningAgesAreNotZeroOrOverflowed() {
    LocalHistoryMetricsBackend subMillisecond = backend(Duration.ofNanos(1));
    try {
      declare(subMillisecond, schema(METRIC_V1, Duration.ofNanos(1)));
      record(subMillisecond,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, NOW_MS - 1),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, NOW_MS));
      SummaryResponse response = response(subMillisecond, request(
          METRIC_V1, 0, 2_000, 0, dims("table", "a", "bucket", 1L)));
      assertEquals(Coverage.WINDOW_CLIPPED, response.coverage());
      assertSummary(response.summary(), 1, 2.0, 2.0, 2.0, NOW_MS, NOW_MS);
    } finally {
      subMillisecond.close();
    }

    Duration huge = Duration.ofSeconds(Long.MAX_VALUE);
    LocalHistoryMetricsBackend overflow =
        backend(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC), huge);
    try {
      declare(overflow, new MetricSchema(
          METRIC_V1,
          Arrays.asList(
              new DimensionSpec("table", DimValue.Kind.STRING),
              new DimensionSpec("bucket", DimValue.Kind.LONG)),
          new Retention(huge, huge)));
      record(overflow, stamped(
          METRIC_V1, dims("table", "a", "bucket", 1L), 3.0, 0));
      SummaryResponse response = response(overflow, request(
          METRIC_V1, Long.MIN_VALUE, 1, 0,
          dims("table", "a", "bucket", 1L)));
      assertEquals(Coverage.COMPLETE, response.coverage());
      assertEquals(1, response.summary().count());
    } finally {
      overflow.close();
    }
  }

  @Test
  void providerClockIsReadOncePerBatchAndClockFailureIsContained() {
    AtomicInteger calls = new AtomicInteger();
    Clock counting = new Clock() {
      @Override
      public ZoneId getZone() {
        return ZoneOffset.UTC;
      }

      @Override
      public Clock withZone(ZoneId zone) {
        return this;
      }

      @Override
      public Instant instant() {
        return Instant.ofEpochMilli(millis());
      }

      @Override
      public long millis() {
        calls.incrementAndGet();
        return NOW_MS;
      }
    };
    LocalHistoryMetricsBackend backend =
        backend(counting, Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      SummaryRequest request = request(
          METRIC_V1, 0, 2_000, 0, dims("table", "a", "bucket", 1L));
      List<SummaryResponse> responses =
          backend.summarize(Arrays.asList(request, request), TIMEOUT);
      assertEquals(Status.Code.OK, responses.get(0).status().code());
      assertEquals(Status.Code.OK, responses.get(1).status().code());
      assertEquals(1, calls.get());
    } finally {
      backend.close();
    }

    Clock failing = new Clock() {
      @Override
      public ZoneId getZone() {
        return ZoneOffset.UTC;
      }

      @Override
      public Clock withZone(ZoneId zone) {
        return this;
      }

      @Override
      public Instant instant() {
        throw new IllegalStateException("clock failed");
      }

      @Override
      public long millis() {
        throw new IllegalStateException("clock failed");
      }
    };
    LocalHistoryMetricsBackend unavailable =
        backend(failing, Duration.ofDays(7));
    try {
      declare(unavailable, schema(METRIC_V1, Duration.ofDays(3)));
      SummaryResponse response = unavailable.summarize(
          Collections.singletonList(request(
              METRIC_V1, 0, 2_000, 0, dims("table", "a", "bucket", 1L))),
          TIMEOUT).get(0);
      assertEquals(Status.Code.UNAVAILABLE, response.status().code());
      assertNull(response.summary());
      assertNull(response.coverage());
      assertEquals(1, unavailable.testHandle().counters()
          .summaryOutcomeCount(Status.Code.UNAVAILABLE));
    } finally {
      unavailable.close();
    }
  }

  @Test
  void compensatedMeanMeetsDocumentedAbsolutePlusRelativeTolerance() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0e16, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 101),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), -1.0e16, 102));

      record(backend,
          stamped(METRIC_V1, dims("table", "max", "bucket", 1L),
              Double.MAX_VALUE, 100),
          stamped(METRIC_V1, dims("table", "max", "bucket", 1L),
              Double.MAX_VALUE, 101),
          stamped(METRIC_V1, dims("table", "opposite", "bucket", 1L),
              Double.MAX_VALUE, 100),
          stamped(METRIC_V1, dims("table", "opposite", "bucket", 1L),
              -Double.MAX_VALUE, 101));

      assertClose(1.0 / 3.0, onlySummary(backend, request(
          METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L))).mean());
      assertEquals(Double.MAX_VALUE, onlySummary(backend, request(
          METRIC_V1, 0, 200, 0, dims("table", "max", "bucket", 1L))).mean());
      assertClose(0.0, onlySummary(backend, request(
          METRIC_V1, 0, 200, 0, dims("table", "opposite", "bucket", 1L))).mean());
    } finally {
      backend.close();
    }
  }

  @Test
  void defensiveBatchBoundsAndTimeoutResultsAreExactAndImmutable() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      SummaryRequest valid = request(
          METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L));
      assertTrue(backend.summarize(Collections.emptyList(), TIMEOUT).isEmpty());
      assertTrue(backend.summarize(null, TIMEOUT).isEmpty());

      List<SummaryRequest> oversized = new ArrayList<SummaryRequest>();
      for (int index = 0; index < 129; index++) {
        oversized.add(valid);
      }
      assertTrue(backend.summarize(oversized, TIMEOUT).isEmpty());

      List<SummaryResponse> nullBatch =
          backend.summarize(Arrays.asList(valid, null), TIMEOUT);
      assertEquals(2, nullBatch.size());
      assertEquals(Status.Code.INVALID_REQUEST, nullBatch.get(0).status().code());
      assertEquals(Status.Code.INVALID_REQUEST, nullBatch.get(1).status().code());

      List<SummaryRequest> maximum = new ArrayList<SummaryRequest>();
      for (int index = 0; index < 128; index++) {
        maximum.add(valid);
      }
      List<SummaryResponse> maximumResponses = backend.summarize(maximum, TIMEOUT);
      assertEquals(128, maximumResponses.size());
      assertEquals(Status.Code.OK, maximumResponses.get(127).status().code());
      assertThrows(UnsupportedOperationException.class, () -> maximumResponses.clear());

      List<SummaryResponse> zero =
          backend.summarize(Collections.singletonList(valid), Duration.ZERO);
      assertEquals(Status.Code.DEADLINE_EXCEEDED, zero.get(0).status().code());
      assertEquals(Status.Code.INVALID_REQUEST,
          backend.summarize(Collections.singletonList(valid), null).get(0).status().code());
      assertEquals(Status.Code.INVALID_REQUEST,
          backend.summarize(
              Collections.singletonList(valid), Duration.ofMillis(-1)).get(0).status().code());
    } finally {
      backend.close();
    }
  }

  @Test
  void summaryDeadlineDuringLockedSnapshotCopyStopsBeforeScanningAndReleasesTheLock()
      throws Exception {
    BlockingExpiringTicker ticker = new BlockingExpiringTicker(3, 10L);
    LocalHistoryMetricsBackend backend =
        backend(ticker, Duration.ofDays(7));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      List<StampedObservation> observations = new ArrayList<StampedObservation>();
      for (int index = 0; index < 128; index++) {
        observations.add(stamped(
            METRIC_V1,
            dims("table", "a", "bucket", 1L),
            index,
            100 + index));
      }
      assertEquals(Status.Code.OK, backend.record(observations).status().code());

      Future<SummaryResponse> summary = executor.submit(() -> backend.summarize(
          Collections.singletonList(request(
              METRIC_V1, 0, 500, 0, dims("table", "a", "bucket", 1L))),
          Duration.ofNanos(10)).get(0));
      assertTrue(ticker.expiringReadEntered.await(5, TimeUnit.SECONDS));

      CountDownLatch lockProbeStarted = new CountDownLatch(1);
      Future<Integer> lockProbe = executor.submit(() -> {
        lockProbeStarted.countDown();
        return backend.declarationCount();
      });
      assertTrue(lockProbeStarted.await(5, TimeUnit.SECONDS));
      assertFalse(lockProbe.isDone());

      ticker.allowExpiration.countDown();
      assertEquals(1, lockProbe.get(5, TimeUnit.SECONDS));
      assertEquals(Status.Code.DEADLINE_EXCEEDED,
          summary.get(5, TimeUnit.SECONDS).status().code());
      assertTrue(ticker.reads.get() >= 5,
          "the budget must be checked during copying and again after the lock is released");
      assertEquals(0, backend.testHandle().counters().summaryRowsExamined());
    } finally {
      ticker.allowExpiration.countDown();
      executor.shutdownNow();
      backend.close();
    }
  }

  @Test
  void positiveTimeoutDuringScanReturnsDeadlineAndCountsOnlyExaminedRows() {
    ScriptedTicker ticker = new ScriptedTicker(0L, 0L, 0L, 0L, 10L);
    LocalHistoryMetricsBackend backend =
        backend(ticker, Duration.ofDays(7));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 100),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 101));

      SummaryResponse response = backend.summarize(
          Collections.singletonList(request(
              METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L))),
          Duration.ofNanos(10)).get(0);

      assertEquals(Status.Code.DEADLINE_EXCEEDED, response.status().code());
      LocalBackendTestHandle.CounterSnapshot counters =
          backend.testHandle().counters();
      assertEquals(1, counters.summaryBatchCount());
      assertEquals(1, counters.summaryOutcomeCount(Status.Code.DEADLINE_EXCEEDED));
      assertEquals(1, counters.summaryRowsExamined());
      assertEquals(0, counters.summaryWindowClipped());
    } finally {
      backend.close();
    }
  }

  @Test
  void positiveTimeoutAfterSnapshotAndAtZeroRetentionBoundaryReturnsDeadline() {
    LocalHistoryMetricsBackend snapshotExpiry =
        backend(new ScriptedTicker(0L, 10L), Duration.ofDays(7));
    try {
      declare(snapshotExpiry, schema(METRIC_V1, Duration.ofDays(3)));
      SummaryResponse response = snapshotExpiry.summarize(
          Collections.singletonList(request(
              METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L))),
          Duration.ofNanos(10)).get(0);
      assertEquals(Status.Code.DEADLINE_EXCEEDED, response.status().code());
      assertEquals(0, snapshotExpiry.testHandle().counters().summaryRowsExamined());
    } finally {
      snapshotExpiry.close();
    }

    LocalHistoryMetricsBackend zeroRetention =
        backend(new ScriptedTicker(0L, 0L, 0L, 10L), Duration.ofDays(7));
    try {
      declare(zeroRetention, schema(METRIC_V1, Duration.ZERO));
      SummaryResponse response = zeroRetention.summarize(
          Collections.singletonList(request(
              METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L))),
          Duration.ofNanos(10)).get(0);
      assertEquals(Status.Code.DEADLINE_EXCEEDED, response.status().code());
      assertEquals(0, zeroRetention.testHandle().counters().summaryWindowClipped());
    } finally {
      zeroRetention.close();
    }
  }

  @Test
  void summaryCountersIncludeEveryReturnedOutcomeAndSnapshotsStayImmutable() {
    LocalHistoryMetricsBackend backend = backend(Duration.ofMillis(100));
    try {
      declare(backend, schema(METRIC_V1, Duration.ofMillis(100)));
      record(backend,
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 950),
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 960));
      SummaryRequest valid = request(
          METRIC_V1, 0, 2_000, 0, dims("table", "a", "bucket", 1L));
      SummaryRequest invalid = request(
          METRIC_V1, 0, 2_000, 1, dims("table", "a"));
      SummaryRequest undeclared = request(
          METRIC_V3, 0, 2_000, 0, Collections.emptyMap());

      backend.summarize(Arrays.asList(valid, invalid, undeclared), TIMEOUT);
      backend.summarize(null, TIMEOUT);
      backend.summarize(Collections.singletonList(valid), Duration.ZERO);
      backend.summarize(Arrays.asList(valid, null), TIMEOUT);

      LocalBackendTestHandle.CounterSnapshot counters =
          backend.testHandle().counters();
      assertEquals(4, counters.summaryBatchCount());
      assertEquals(1, counters.summaryOutcomeCount(Status.Code.OK));
      assertEquals(3, counters.summaryOutcomeCount(Status.Code.INVALID_REQUEST));
      assertEquals(1, counters.summaryOutcomeCount(Status.Code.NOT_DECLARED));
      assertEquals(1, counters.summaryOutcomeCount(Status.Code.DEADLINE_EXCEEDED));
      assertEquals(1, counters.summaryWindowClipped());
      assertEquals(2, counters.summaryRowsExamined());

      backend.summarize(Collections.emptyList(), TIMEOUT);
      assertEquals(4, counters.summaryBatchCount());
      assertEquals(5, backend.testHandle().counters().summaryBatchCount());
    } finally {
      backend.close();
    }
  }

  @Test
  void concurrentReadAndAtomicWriteExposeOnlyCoherentSnapshots() throws Exception {
    LocalHistoryMetricsBackend backend = backend(Duration.ofDays(7));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      declare(backend, schema(METRIC_V1, Duration.ofDays(3)));
      StampedObservation first =
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 1.0, 100);
      StampedObservation second =
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 2.0, 101);
      StampedObservation third =
          stamped(METRIC_V1, dims("table", "a", "bucket", 1L), 3.0, 102);
      record(backend, first);
      SummaryRequest request = request(
          METRIC_V1, 0, 200, 0, dims("table", "a", "bucket", 1L));
      CyclicBarrier barrier = new CyclicBarrier(3);

      Future<SummaryResponse> read = executor.submit(() -> {
        barrier.await();
        return backend.summarize(Collections.singletonList(request), TIMEOUT).get(0);
      });
      Future<WriteResult> write = executor.submit(() -> {
        barrier.await();
        return backend.record(Arrays.asList(second, third));
      });
      barrier.await();

      long count = read.get().summary().count();
      assertTrue(count == 1 || count == 3, "read must see the batch before or after atomic commit");
      assertEquals(Status.Code.OK, write.get().status().code());
      assertEquals(3, onlySummary(backend, request).count());
    } finally {
      executor.shutdownNow();
      backend.close();
    }
  }

  private static LocalHistoryMetricsBackend backend(Duration maximumPlanningAge) {
    return backend(
        Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC), maximumPlanningAge);
  }

  private static LocalHistoryMetricsBackend backend(
      Clock clock, Duration maximumPlanningAge) {
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.metric"));
    return LocalHistoryMetricsBackend.create(catalog, clock, maximumPlanningAge);
  }

  private static LocalHistoryMetricsBackend backend(
      LocalHistoryMetricsBackend.Ticker ticker, Duration maximumPlanningAge) {
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.metric"));
    return LocalHistoryMetricsBackend.createForTest(
        catalog,
        Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC),
        maximumPlanningAge,
        ticker);
  }

  private static void declare(LocalHistoryMetricsBackend backend, MetricSchema schema) {
    SchemaStatus status =
        backend.declare(Collections.singletonList(schema), TIMEOUT).get(0);
    assertEquals(SchemaStatus.Code.ACCEPTED, status.code());
  }

  private static MetricSchema schema(MetricVersionId metric, Duration planningMaxAge) {
    return new MetricSchema(
        metric,
        Arrays.asList(
            new DimensionSpec("table", DimValue.Kind.STRING),
            new DimensionSpec("bucket", DimValue.Kind.LONG)),
        new Retention(planningMaxAge, Duration.ofDays(30)));
  }

  private static SummaryRequest request(
      MetricVersionId metric,
      long fromMs,
      long toMs,
      int limit,
      Map<String, DimValue> bound) {
    return SummaryRequest.builder(metric)
        .bound(bound)
        .window(fromMs, toMs)
        .limit(limit)
        .build();
  }

  private static SummaryResponse response(
      LocalHistoryMetricsBackend backend, SummaryRequest request) {
    return backend.summarize(Collections.singletonList(request), TIMEOUT).get(0);
  }

  private static Summary onlySummary(
      LocalHistoryMetricsBackend backend, SummaryRequest request) {
    SummaryResponse response = response(backend, request);
    assertEquals(Status.Code.OK, response.status().code());
    assertEquals(Coverage.COMPLETE, response.coverage());
    return response.summary();
  }

  private static void assertOk(
      SummaryResponse response, Summary summary, Coverage coverage) {
    assertEquals(Status.Code.OK, response.status().code());
    assertEquals(summary, response.summary());
    assertEquals(coverage, response.coverage());
  }

  private static void assertClose(double expected, double actual) {
    double absoluteTolerance = 1.0e-12;
    double relativeTolerance = 1.0e-12;
    assertTrue(Math.abs(actual - expected) <=
        absoluteTolerance + relativeTolerance * Math.abs(expected),
        "mean must satisfy the documented absolute plus relative tolerance");
  }

  private static void assertSummary(
      Summary summary,
      long count,
      double mean,
      double min,
      double max,
      long first,
      long last) {
    assertEquals(count, summary.count());
    assertClose(mean, summary.mean());
    assertEquals(min, summary.min());
    assertEquals(max, summary.max());
    assertEquals(first, summary.firstObservedMs());
    assertEquals(last, summary.lastObservedMs());
  }

  private static void record(
      LocalHistoryMetricsBackend backend, StampedObservation... observations) {
    WriteResult result = backend.record(Arrays.asList(observations));
    assertEquals(observations.length, result.accepted());
    assertEquals(0, result.rejected());
    assertEquals(Status.Code.OK, result.status().code());
  }

  private static StampedObservation stamped(
      MetricVersionId metric,
      Map<String, DimValue> dimensions,
      double value,
      long timestampMs) {
    return new StampedObservation(
        new Observation(metric, dimensions, value, timestampMs),
        new Provenance("app-1", "attempt-1", "26.10.0", NOW_MS));
  }

  private static final class BlockingExpiringTicker
      implements LocalHistoryMetricsBackend.Ticker {
    private final int expiringRead;
    private final long expiredNanos;
    private final AtomicInteger reads = new AtomicInteger();
    private final CountDownLatch expiringReadEntered = new CountDownLatch(1);
    private final CountDownLatch allowExpiration = new CountDownLatch(1);

    private BlockingExpiringTicker(int expiringRead, long expiredNanos) {
      this.expiringRead = expiringRead;
      this.expiredNanos = expiredNanos;
    }

    @Override
    public long readNanos() {
      if (reads.incrementAndGet() < expiringRead) {
        return 0L;
      }
      expiringReadEntered.countDown();
      boolean interrupted = false;
      while (true) {
        try {
          allowExpiration.await();
          break;
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return expiredNanos;
    }
  }

  private static final class ScriptedTicker
      implements LocalHistoryMetricsBackend.Ticker {
    private final long[] values;
    private int index;

    private ScriptedTicker(long... values) {
      this.values = values.clone();
    }

    @Override
    public long readNanos() {
      int current = Math.min(index, values.length - 1);
      index++;
      return values[current];
    }
  }

  private static Map<String, DimValue> dims(Object... pairs) {
    Map<String, DimValue> dimensions = new HashMap<String, DimValue>();
    for (int index = 0; index < pairs.length; index += 2) {
      String name = (String) pairs[index];
      Object raw = pairs[index + 1];
      dimensions.put(name,
          raw instanceof String ? DimValue.of((String) raw) : DimValue.of((Long) raw));
    }
    return dimensions;
  }
}
