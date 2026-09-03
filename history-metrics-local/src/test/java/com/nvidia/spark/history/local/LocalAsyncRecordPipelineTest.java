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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.MetricStores;
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

/** Behavioral tests for the package-private asynchronous local record pipeline. */
class LocalAsyncRecordPipelineTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final LocalCircuitBreakerPolicy BREAKER_POLICY =
      LocalCircuitBreakerPolicy.of(
          128, 128, 1.0, Duration.ofDays(1), 1.0, Duration.ofDays(1));
  private static final MetricVersionId METRIC_A = new MetricVersionId(17, 1);
  private static final MetricVersionId METRIC_B = new MetricVersionId(18, 1);
  private static final MetricVersionId METRIC_BYTES = new MetricVersionId(19, 1);

  @Test
  void recordValidatesEachItemAndAcceptsTheExactFutureBoundary() {
    RecordingBackend backend = new RecordingBackend();
    CountingProvenanceSource source = new CountingProvenanceSource();
    LocalMetricStorePlanningAdapter adapter =
        declaredAdapter(backend, fixedClock(1_000L), source, policy(16, 16));

    Observation exactBoundary = observation(METRIC_A, "exact", 301_000L);
    adapter.record(Arrays.asList(
        exactBoundary,
        observation(METRIC_A, "future", 301_001L),
        observation(METRIC_B, "unknown", 1_000L),
        new Observation(METRIC_A, Collections.<String, DimValue>emptyMap(), 1.0, 1_000L),
        null));

    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(Collections.singletonList(exactBoundary),
        observations(backend.recorded()));
    assertEquals(1, source.calls.get());
    LocalAsyncRecordPipeline.RecordCounterSnapshot counters = adapter.recordCounters();
    assertEquals(1, counters.recordCallCount());
    assertEquals(2, counters.invalidItemCount());
    assertEquals(1, counters.undeclaredItemCount());
    assertEquals(1, counters.futureItemCount());
    assertEquals(1, counters.enqueuedItemCount());
    assertEquals(1, counters.backendAcceptedItemCount());
    assertEquals(0, counters.queueCurrent());
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void futureCutoffSaturatesAtLongMaxValue() {
    RecordingBackend backend = new RecordingBackend();
    LocalMetricStorePlanningAdapter adapter = declaredAdapter(
        backend,
        fixedClock(Long.MAX_VALUE - 1L),
        new CountingProvenanceSource(),
        policy(4, 4));

    Observation maximum = observation(METRIC_A, "max", Long.MAX_VALUE);
    adapter.record(Collections.singletonList(maximum));

    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(Collections.singletonList(maximum), observations(backend.recorded()));
    assertEquals(0, adapter.recordCounters().futureItemCount());
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void provenanceIsCapturedOncePerCallAndFrameworkOwnsWrittenAt() {
    RecordingBackend backend = new RecordingBackend();
    CountingProvenanceSource source =
        new CountingProvenanceSource("app-redacted", "attempt-2", "26.10.0");
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend,
        fixedClock(77L),
        source,
        policy(8, 8),
        new MutableTicker(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
    declare(adapter, METRIC_BYTES);
    byte[] bytes = new byte[] {1, 2, 3};
    Observation first = byteObservation(METRIC_BYTES, bytes, 70L);
    Observation second = byteObservation(METRIC_BYTES, new byte[] {4}, 71L);

    adapter.record(Arrays.asList(first, second));
    bytes[0] = 99;

    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(1, source.calls.get());
    List<StampedObservation> recorded = backend.recorded();
    assertEquals(2, recorded.size());
    for (StampedObservation stamped : recorded) {
      assertEquals("app-redacted", stamped.provenance().app());
      assertEquals("attempt-2", stamped.provenance().attempt());
      assertEquals("26.10.0", stamped.provenance().pluginVersion());
      assertEquals(77L, stamped.provenance().writtenAtMs());
    }
    assertEquals(1, recorded.get(0).observation().dimensions().get("key")
        .bytesValue()[0]);
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void clockAndProvenanceFailuresDropOtherwiseValidItemsWithoutThrowing() {
    RecordingBackend clockBackend = new RecordingBackend();
    LocalMetricStorePlanningAdapter clockFailure = declaredAdapter(
        clockBackend,
        new ThrowingClock(),
        new CountingProvenanceSource(),
        policy(4, 4));
    clockFailure.record(Collections.singletonList(observation(METRIC_A, "a", 1L)));
    assertEquals(1, clockFailure.recordCounters().clockFailureItemCount());
    assertEquals(0, clockBackend.recordCalls.get());
    assertTrue(clockFailure.stopRecording(TIMEOUT));

    RecordingBackend sourceBackend = new RecordingBackend();
    CountingProvenanceSource source = new CountingProvenanceSource();
    source.failure = new IllegalStateException("source unavailable");
    LocalMetricStorePlanningAdapter sourceFailure =
        declaredAdapter(sourceBackend, fixedClock(1L), source, policy(4, 4));
    sourceFailure.record(Collections.singletonList(observation(METRIC_A, "a", 1L)));
    assertEquals(1, sourceFailure.recordCounters().provenanceFailureItemCount());
    assertEquals(1, source.calls.get());
    assertEquals(0, sourceBackend.recordCalls.get());
    assertTrue(sourceFailure.stopRecording(TIMEOUT));
  }

  @Test
  void blockedBackendDoesNotBlockPrefixAdmissionAndOverflowAccounting() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      entered.countDown();
      release.await();
      return WriteResult.ok(batch.size());
    };
    LocalMetricStorePlanningAdapter adapter = declaredAdapter(
        backend, fixedClock(1L), new CountingProvenanceSource(), policy(2, 1));

    adapter.record(Collections.singletonList(observation(METRIC_A, "first", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    adapter.record(Arrays.asList(
        observation(METRIC_A, "second", 1L),
        observation(METRIC_A, "third", 1L),
        observation(METRIC_A, "overflow", 1L)));

    assertEquals(2, adapter.recordCounters().queueCurrent());
    assertEquals(1, adapter.recordCounters().overflowItemCount());
    release.countDown();
    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(Arrays.asList("first", "second", "third"), keys(backend.recorded()));
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void concurrentRecordCallsSerializeWholePrefixAdmission() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      if (backend.recordCalls.get() == 1) {
        entered.countDown();
        release.await();
      }
      return WriteResult.ok(batch.size());
    };
    LocalMetricStorePlanningAdapter adapter = declaredAdapter(
        backend, fixedClock(1L), new CountingProvenanceSource(), policy(2, 2));
    adapter.record(Collections.singletonList(observation(METRIC_A, "block", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));

    ExecutorService callers = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(3);
    try {
      callers.submit(() -> {
        start.await();
        adapter.record(Arrays.asList(
            observation(METRIC_A, "a1", 1L),
            observation(METRIC_A, "a2", 1L)));
        return null;
      });
      callers.submit(() -> {
        start.await();
        adapter.record(Arrays.asList(
            observation(METRIC_A, "b1", 1L),
            observation(METRIC_A, "b2", 1L)));
        return null;
      });
      start.await();
      callers.shutdown();
      assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      callers.shutdownNow();
      release.countDown();
    }

    assertTrue(adapter.drain(TIMEOUT));
    List<String> keys = keys(backend.recorded());
    assertEquals(3, keys.size());
    assertEquals("block", keys.get(0));
    assertTrue(keys.subList(1, 3).equals(Arrays.asList("a1", "a2")) ||
        keys.subList(1, 3).equals(Arrays.asList("b1", "b2")));
    assertEquals(2, adapter.recordCounters().overflowItemCount());
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void writerBatchesOnlyContiguousMetricVersionsUpToExplicitMaximum() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      if (backend.recordCalls.get() == 1) {
        entered.countDown();
        release.await();
      }
      return WriteResult.ok(batch.size());
    };
    LocalMetricStorePlanningAdapter adapter =
        adapterWithDeclarations(backend, policy(16, 3), METRIC_A, METRIC_B);
    adapter.record(Collections.singletonList(observation(METRIC_A, "block", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));

    adapter.record(Arrays.asList(
        observation(METRIC_A, "a1", 1L),
        observation(METRIC_A, "a2", 1L),
        observation(METRIC_B, "b1", 1L),
        observation(METRIC_A, "a3", 1L),
        observation(METRIC_A, "a4", 1L),
        observation(METRIC_A, "a5", 1L),
        observation(METRIC_A, "a6", 1L)));
    release.countDown();

    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(Arrays.asList(
        Arrays.asList("block"),
        Arrays.asList("a1", "a2"),
        Arrays.asList("b1"),
        Arrays.asList("a3", "a4", "a5"),
        Arrays.asList("a6")),
        backend.batchKeys());
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void backendFailuresMalformedAndPartialResultsAreTerminalWithoutRetry() {
    RecordingBackend backend = new RecordingBackend();
    AtomicInteger outcome = new AtomicInteger();
    backend.handler = batch -> {
      switch (outcome.getAndIncrement()) {
        case 0:
          throw new IllegalStateException("failed");
        case 1:
          throw new AbstractMethodError("old backend");
        case 2:
          return null;
        default:
          return WriteResult.ok(0);
      }
    };
    LocalMetricStorePlanningAdapter adapter =
        adapterWithDeclarations(backend, policy(16, 1), METRIC_A);

    for (int index = 0; index < 4; index++) {
      adapter.record(Collections.singletonList(
          observation(METRIC_A, "item-" + index, 1L)));
    }

    assertTrue(adapter.drain(TIMEOUT));
    assertEquals(4, backend.recordCalls.get());
    assertEquals(4, adapter.recordCounters().backendAmbiguousItemCount());

    RecordingBackend partialBackend = new RecordingBackend();
    partialBackend.handler =
        batch -> WriteResult.unavailable(1, 1, "partial");
    LocalMetricStorePlanningAdapter partial =
        adapterWithDeclarations(partialBackend, policy(4, 2), METRIC_A);
    partial.record(Arrays.asList(
        observation(METRIC_A, "accepted", 1L),
        observation(METRIC_A, "rejected", 1L)));

    assertTrue(partial.drain(TIMEOUT));
    assertEquals(1, partialBackend.recordCalls.get());
    LocalAsyncRecordPipeline.RecordCounterSnapshot counters = partial.recordCounters();
    assertEquals(0, counters.backendAmbiguousItemCount());
    assertEquals(1, counters.backendAcceptedItemCount());
    assertEquals(1, counters.backendRejectedItemCount());
    assertEquals(1, counters.backendStatusCount(Status.Code.UNAVAILABLE));
    assertTrue(partial.stopRecording(TIMEOUT));
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void drainWatermarkExcludesLaterEnqueuesAndPositiveTimeoutIsDeterministic()
      throws Exception {
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch firstRelease = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch secondRelease = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      if (backend.recordCalls.get() == 1) {
        firstEntered.countDown();
        firstRelease.await();
      } else {
        secondEntered.countDown();
        secondRelease.await();
      }
      return WriteResult.ok(batch.size());
    };
    MutableTicker ticker = new MutableTicker();
    SignallingDrainWaiter waiter = new SignallingDrainWaiter();
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend, fixedClock(1L), new CountingProvenanceSource(), policy(4, 1), ticker, waiter);
    declare(adapter, METRIC_A);
    adapter.record(Collections.singletonList(observation(METRIC_A, "first", 1L)));
    assertTrue(firstEntered.await(5, TimeUnit.SECONDS));

    ExecutorService drainer = Executors.newSingleThreadExecutor();
    java.util.concurrent.Future<Boolean> drained =
        drainer.submit(() -> adapter.drain(Duration.ofSeconds(1)));
    assertTrue(waiter.waiting.await(5, TimeUnit.SECONDS));
    adapter.record(Collections.singletonList(observation(METRIC_A, "second", 1L)));
    firstRelease.countDown();
    assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
    assertTrue(drained.get(5, TimeUnit.SECONDS));
    secondRelease.countDown();
    assertTrue(adapter.drain(TIMEOUT));
    drainer.shutdownNow();

    CountDownLatch timeoutEntered = new CountDownLatch(1);
    CountDownLatch timeoutRelease = new CountDownLatch(1);
    RecordingBackend timeoutBackend = new RecordingBackend();
    timeoutBackend.handler = batch -> {
      timeoutEntered.countDown();
      timeoutRelease.await();
      return WriteResult.ok(batch.size());
    };
    MutableTicker timeoutTicker = new MutableTicker();
    LocalMetricStorePlanningAdapter timeoutAdapter = adapterWithRecording(
        timeoutBackend,
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        timeoutTicker,
        new ExpiringDrainWaiter(timeoutTicker));
    declare(timeoutAdapter, METRIC_A);
    timeoutAdapter.record(Collections.singletonList(observation(METRIC_A, "timeout", 1L)));
    assertTrue(timeoutEntered.await(5, TimeUnit.SECONDS));
    assertFalse(timeoutAdapter.drain(Duration.ofNanos(10L)));
    assertEquals(1, timeoutAdapter.recordCounters().drainTimeoutCount());
    timeoutRelease.countDown();
    assertTrue(timeoutAdapter.drain(TIMEOUT));
    assertTrue(timeoutAdapter.stopRecording(TIMEOUT));
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void drainTimesOutWhenLockAcquisitionOrWatermarkCaptureConsumesTheBudget() {
    MutableTicker ticker = new MutableTicker();
    AcquiredAtDeadlineWaiter waiter = new AcquiredAtDeadlineWaiter(ticker);
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        new RecordingBackend(),
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        ticker,
        waiter);

    assertFalse(adapter.drain(Duration.ofNanos(10L)));
    assertEquals(1, adapter.recordCounters().drainTimeoutCount());
    assertTrue(adapter.stopRecording(TIMEOUT));

    ScriptedTicker captureTicker = new ScriptedTicker(0L, 0L, 0L, 10L);
    LocalMetricStorePlanningAdapter captureAdapter = adapterWithRecording(
        new RecordingBackend(),
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        captureTicker,
        new SignallingDrainWaiter());
    assertFalse(captureAdapter.drain(Duration.ofNanos(10L)));
    assertEquals(1, captureAdapter.recordCounters().drainTimeoutCount());
    assertTrue(captureAdapter.stopRecording(TIMEOUT));

    MutableTicker contendedTicker = new MutableTicker();
    ContendedLockWaiter contended = new ContendedLockWaiter(contendedTicker);
    LocalMetricStorePlanningAdapter contendedAdapter = adapterWithRecording(
        new RecordingBackend(),
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        contendedTicker,
        contended);
    assertFalse(contendedAdapter.drain(Duration.ofNanos(10L)));
    assertFalse(contendedAdapter.drain(Duration.ZERO));
    assertEquals(2, contendedAdapter.recordCounters().drainTimeoutCount());
    assertTrue(contendedAdapter.stopRecording(TIMEOUT));
  }

  @Test
  void drainRejectsTerminalCompletionObservedOnlyAfterWaitExpiry() throws Exception {
    CountDownLatch backendEntered = new CountDownLatch(1);
    CountDownLatch backendRelease = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      backendEntered.countDown();
      backendRelease.await();
      return WriteResult.ok(batch.size());
    };
    MutableTicker ticker = new MutableTicker();
    BoundaryDrainWaiter waiter = new BoundaryDrainWaiter(ticker, false);
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend,
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        ticker,
        waiter);
    declare(adapter, METRIC_A);
    adapter.record(Collections.singletonList(observation(METRIC_A, "late", 1L)));
    assertTrue(backendEntered.await(5, TimeUnit.SECONDS));

    ExecutorService drainer = Executors.newSingleThreadExecutor();
    java.util.concurrent.Future<Boolean> drained =
        drainer.submit(() -> adapter.drain(Duration.ofNanos(10L)));
    assertTrue(waiter.waiting.await(5, TimeUnit.SECONDS));
    backendRelease.countDown();

    assertFalse(drained.get(5, TimeUnit.SECONDS));
    assertEquals(1, adapter.recordCounters().backendAcceptedItemCount());
    assertEquals(1, adapter.recordCounters().drainTimeoutCount());
    drainer.shutdownNow();
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void drainAcceptsTerminalCompletionObservedJustBeforeBoundary() throws Exception {
    CountDownLatch backendEntered = new CountDownLatch(1);
    CountDownLatch backendRelease = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      backendEntered.countDown();
      backendRelease.await();
      return WriteResult.ok(batch.size());
    };
    MutableTicker ticker = new MutableTicker();
    BoundaryDrainWaiter waiter = new BoundaryDrainWaiter(ticker, true);
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend,
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy(2, 1),
        ticker,
        waiter);
    declare(adapter, METRIC_A);
    adapter.record(Collections.singletonList(observation(METRIC_A, "timely", 1L)));
    assertTrue(backendEntered.await(5, TimeUnit.SECONDS));

    ExecutorService drainer = Executors.newSingleThreadExecutor();
    java.util.concurrent.Future<Boolean> drained =
        drainer.submit(() -> adapter.drain(Duration.ofNanos(10L)));
    assertTrue(waiter.waiting.await(5, TimeUnit.SECONDS));
    backendRelease.countDown();

    assertTrue(drained.get(5, TimeUnit.SECONDS));
    assertEquals(1, adapter.recordCounters().drainSuccessCount());
    drainer.shutdownNow();
    assertTrue(adapter.stopRecording(TIMEOUT));
  }

  @Test
  void nullEmptyCleanupAndCounterSnapshotsAreTotalAndDoNotInstall() {
    MetricStore before = MetricStores.current();
    RecordingBackend backend = new RecordingBackend();
    LocalMetricStorePlanningAdapter adapter = declaredAdapter(
        backend, fixedClock(1L), new CountingProvenanceSource(), policy(2, 1));

    adapter.record(null);
    adapter.record(Collections.<Observation>emptyList());
    LocalAsyncRecordPipeline.RecordCounterSnapshot counters = adapter.recordCounters();
    assertEquals(2, counters.recordCallCount());
    assertEquals(1, counters.invalidCallCount());
    assertEquals(0, counters.enqueuedItemCount());
    assertEquals(1, counters.invalidCallCount());
    assertSame(before, MetricStores.current());
    assertThrows(NullPointerException.class, () -> adapter.drain(null));
    assertThrows(IllegalArgumentException.class,
        () -> adapter.drain(Duration.ofNanos(-1L)));
    assertTrue(adapter.stopRecording(TIMEOUT));
    assertTrue(adapter.drain(Duration.ZERO));
    assertSame(before, MetricStores.current());
  }

  @Test
  void cleanupStopsAdmissionDropsQueuedItemsAndJoinsTheWriter() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = batch -> {
      entered.countDown();
      new CountDownLatch(1).await();
      return WriteResult.ok(batch.size());
    };
    LocalMetricStorePlanningAdapter adapter = declaredAdapter(
        backend, fixedClock(1L), new CountingProvenanceSource(), policy(2, 1));

    adapter.record(Collections.singletonList(observation(METRIC_A, "in-flight", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    adapter.record(Arrays.asList(
        observation(METRIC_A, "queued-1", 1L),
        observation(METRIC_A, "queued-2", 1L)));

    assertTrue(adapter.stopRecording(TIMEOUT));
    LocalAsyncRecordPipeline.RecordCounterSnapshot counters = adapter.recordCounters();
    assertEquals(2, counters.stoppedItemCount());
    assertEquals(1, counters.backendAmbiguousItemCount());
    assertEquals(0, counters.queueCurrent());
    assertTrue(adapter.drain(Duration.ZERO));

    adapter.record(Collections.singletonList(observation(METRIC_A, "after-stop", 1L)));
    assertEquals(3, adapter.recordCounters().stoppedItemCount());
    assertEquals(1, backend.recordCalls.get());
  }

  @Test
  void queuePolicyRequiresPositiveCapacityAndBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> policy(0, 1));
    assertThrows(IllegalArgumentException.class, () -> policy(1, 0));
    assertThrows(IllegalArgumentException.class, () -> policy(-1, 1));
    assertThrows(IllegalArgumentException.class, () -> policy(1, -1));
  }

  private static LocalAsyncRecordPipeline.QueuePolicy policy(int capacity, int batchSize) {
    return new LocalAsyncRecordPipeline.QueuePolicy(capacity, batchSize);
  }

  private static LocalMetricStorePlanningAdapter declaredAdapter(
      RecordingBackend backend,
      Clock clock,
      LocalProvenanceSource source,
      LocalAsyncRecordPipeline.QueuePolicy policy) {
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend,
        clock,
        source,
        policy,
        new MutableTicker(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
    declare(adapter, METRIC_A);
    return adapter;
  }

  private static LocalMetricStorePlanningAdapter adapterWithDeclarations(
      RecordingBackend backend,
      LocalAsyncRecordPipeline.QueuePolicy policy,
      MetricVersionId... metrics) {
    LocalMetricStorePlanningAdapter adapter = adapterWithRecording(
        backend,
        fixedClock(1L),
        new CountingProvenanceSource(),
        policy,
        new MutableTicker(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
    for (MetricVersionId metric : metrics) {
      declare(adapter, metric);
    }
    return adapter;
  }

  private static LocalMetricStorePlanningAdapter adapterWithRecording(
      RecordingBackend backend,
      Clock clock,
      LocalProvenanceSource source,
      LocalAsyncRecordPipeline.QueuePolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalAsyncRecordPipeline.DrainWaiter waiter) {
    return LocalMetricStorePlanningAdapter.createWithRecording(
        backend,
        catalog(),
        new InlineExecutor(),
        false,
        ticker,
        BREAKER_POLICY,
        clock,
        source,
        policy,
        waiter);
  }

  private static void declare(LocalMetricStorePlanningAdapter adapter, MetricVersionId metric) {
    assertEquals(SchemaStatus.Code.ACCEPTED,
        adapter.declare(
            Collections.singletonList(schema(metric)),
            Duration.ofSeconds(1)).get(0).code());
  }

  private static MetricSchema schema(MetricVersionId metric) {
    DimValue.Kind kind = metric.equals(METRIC_BYTES)
        ? DimValue.Kind.BYTES
        : DimValue.Kind.STRING;
    return new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec("key", kind)),
        new Retention(Duration.ofDays(1), Duration.ofDays(30)));
  }

  private static HistoryMetricCatalog catalog() {
    return TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.a"),
        TestHistoryMetricCatalog.live(18, "test.b"),
        TestHistoryMetricCatalog.live(19, "test.bytes"));
  }

  private static Observation observation(MetricVersionId metric, String key, long timestampMs) {
    return new Observation(
        metric,
        Collections.singletonMap("key", DimValue.of(key)),
        1.0,
        timestampMs);
  }

  private static Observation byteObservation(MetricVersionId metric, byte[] key, long timestampMs) {
    return new Observation(
        metric,
        Collections.singletonMap("key", DimValue.of(key)),
        1.0,
        timestampMs);
  }

  private static Clock fixedClock(long millis) {
    return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
  }

  private static List<Observation> observations(List<StampedObservation> stamped) {
    List<Observation> observations = new ArrayList<Observation>();
    for (StampedObservation item : stamped) {
      observations.add(item.observation());
    }
    return observations;
  }

  private static List<String> keys(List<StampedObservation> stamped) {
    List<String> keys = new ArrayList<String>();
    for (StampedObservation item : stamped) {
      DimValue value = item.observation().dimensions().get("key");
      keys.add(value.kind() == DimValue.Kind.STRING
          ? value.stringValue()
          : Arrays.toString(value.bytesValue()));
    }
    return keys;
  }

  private interface RecordHandler {
    WriteResult apply(List<StampedObservation> batch) throws Exception;
  }

  private static final class RecordingBackend implements HistoryMetricsBackend {
    private final AtomicInteger recordCalls = new AtomicInteger();
    private final List<List<StampedObservation>> batches =
        Collections.synchronizedList(new ArrayList<List<StampedObservation>>());
    private volatile RecordHandler handler;

    @Override
    public BackendInfo info() {
      return new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "recording backend");
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      List<SchemaStatus> results = new ArrayList<SchemaStatus>();
      for (MetricSchema schema : schemas) {
        results.add(SchemaStatus.accepted(schema.metric(), null));
      }
      return results;
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      recordCalls.incrementAndGet();
      batches.add(Collections.unmodifiableList(
          new ArrayList<StampedObservation>(observations)));
      if (handler != null) {
        try {
          return handler.apply(observations);
        } catch (RuntimeException failure) {
          throw failure;
        } catch (Error failure) {
          throw failure;
        } catch (Exception failure) {
          throw new IllegalStateException(failure);
        }
      }
      return WriteResult.ok(observations.size());
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      List<SummaryResponse> results = new ArrayList<SummaryResponse>();
      for (SummaryRequest ignored : requests) {
        results.add(SummaryResponse.ok(null, Coverage.COMPLETE));
      }
      return results;
    }

    @Override
    public void close() {
    }

    private List<StampedObservation> recorded() {
      List<StampedObservation> flattened = new ArrayList<StampedObservation>();
      synchronized (batches) {
        for (List<StampedObservation> batch : batches) {
          flattened.addAll(batch);
        }
      }
      return flattened;
    }

    private List<List<String>> batchKeys() {
      List<List<String>> keys = new ArrayList<List<String>>();
      synchronized (batches) {
        for (List<StampedObservation> batch : batches) {
          keys.add(LocalAsyncRecordPipelineTest.keys(batch));
        }
      }
      return keys;
    }
  }

  private static final class CountingProvenanceSource implements LocalProvenanceSource {
    private final AtomicInteger calls = new AtomicInteger();
    private final LocalProvenanceIdentity identity;
    private volatile RuntimeException failure;

    private CountingProvenanceSource() {
      this("app", null, "test-version");
    }

    private CountingProvenanceSource(String app, String attempt, String pluginVersion) {
      identity = LocalProvenanceIdentity.of(app, attempt, pluginVersion);
    }

    @Override
    public LocalProvenanceIdentity current() {
      calls.incrementAndGet();
      if (failure != null) {
        throw failure;
      }
      return identity;
    }
  }

  private static final class MutableTicker implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.get();
    }
  }

  private static final class ScriptedTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private final long[] values;
    private final AtomicInteger index = new AtomicInteger();

    private ScriptedTicker(long... values) {
      this.values = values.clone();
    }

    @Override
    public long readNanos() {
      int current = index.getAndIncrement();
      return values[Math.min(current, values.length - 1)];
    }
  }

  private abstract static class TestDrainWaiter
      implements LocalAsyncRecordPipeline.DrainWaiter {
    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      return lock.tryLock();
    }
  }

  private static final class SignallingDrainWaiter extends TestDrainWaiter {
    private final CountDownLatch waiting = new CountDownLatch(1);

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      waiting.countDown();
      return condition.awaitNanos(remainingNanos);
    }
  }

  private static final class ExpiringDrainWaiter extends TestDrainWaiter {
    private final MutableTicker ticker;
    private boolean expired;

    private ExpiringDrainWaiter(MutableTicker ticker) {
      this.ticker = ticker;
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      if (!expired) {
        expired = true;
        ticker.nanos.addAndGet(remainingNanos);
        return 0L;
      }
      return condition.awaitNanos(remainingNanos);
    }
  }

  private static final class AcquiredAtDeadlineWaiter extends TestDrainWaiter {
    private final MutableTicker ticker;

    private AcquiredAtDeadlineWaiter(MutableTicker ticker) {
      this.ticker = ticker;
    }

    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos) {
      boolean acquired = lock.tryLock();
      ticker.nanos.addAndGet(remainingNanos);
      return acquired;
    }

    @Override
    public long await(Condition condition, long remainingNanos) {
      throw new AssertionError("terminal wait is not expected");
    }
  }

  private static final class ContendedLockWaiter extends TestDrainWaiter {
    private final MutableTicker ticker;

    private ContendedLockWaiter(MutableTicker ticker) {
      this.ticker = ticker;
    }

    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      CountDownLatch held = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      Thread holder = new Thread(() -> {
        lock.lock();
        try {
          held.countDown();
          release.await();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        } finally {
          lock.unlock();
        }
      }, "history-metrics-drain-contention-test");
      holder.start();
      if (!held.await(5, TimeUnit.SECONDS)) {
        release.countDown();
        throw new AssertionError("lock holder did not start");
      }
      boolean acquired = lock.tryLock();
      ticker.nanos.addAndGet(remainingNanos);
      release.countDown();
      holder.join(5_000L);
      if (holder.isAlive()) {
        holder.interrupt();
        throw new AssertionError("lock holder did not stop");
      }
      return acquired;
    }

    @Override
    public long await(Condition condition, long remainingNanos) {
      throw new AssertionError("terminal wait is not expected");
    }
  }

  private static final class BoundaryDrainWaiter extends TestDrainWaiter {
    private final MutableTicker ticker;
    private final boolean justBeforeBoundary;
    private final CountDownLatch waiting = new CountDownLatch(1);

    private BoundaryDrainWaiter(MutableTicker ticker, boolean justBeforeBoundary) {
      this.ticker = ticker;
      this.justBeforeBoundary = justBeforeBoundary;
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      waiting.countDown();
      if (!justBeforeBoundary) {
        ticker.nanos.addAndGet(remainingNanos);
        condition.await();
        return 0L;
      }
      condition.await();
      ticker.nanos.addAndGet(remainingNanos - 1L);
      return 1L;
    }
  }

  private static final class ThrowingClock extends Clock {
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
      throw new IllegalStateException("clock unavailable");
    }
  }

  private static final class InlineExecutor extends AbstractExecutorService {
    private boolean shutdown;

    @Override
    public void execute(Runnable command) {
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
}
