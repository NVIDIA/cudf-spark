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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

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
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.LocalTestCatalog;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for explicit standalone local ownership and lifecycle. */
class LocalHistoryMetricsLifecycleTest {
  private static final MetricVersionId METRIC = new MetricVersionId(41, 1);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  void standaloneOwnerDoesNotInstallAndSupportsTheCompleteHappyPath() {
    MetricStore initial = MetricStores.current();
    LocalHistoryMetrics local = open();
    assertSame(initial, MetricStores.current());

    MetricSchema schema = schema();
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 7.0)));
    assertTrue(local.drain(TIMEOUT));

    SummaryRequest request = SummaryRequest.builder(METRIC)
        .bind("key", DimValue.of("x"))
        .window(0L, 2_000L)
        .build();
    SummaryResponse response =
        local.store().summarize(Collections.singletonList(request), TIMEOUT).get(0);
    assertEquals(Status.Code.OK, response.status().code());
    assertEquals(1L, response.summary().count());
    assertEquals(7.0, response.summary().mean());

    LocalHistoryMetricsTestHandle handle = local.testHandle();
    assertEquals(1, handle.declarations().size());
    assertEquals(1, handle.observations().size());
    LocalObservationSnapshot stored = handle.observations().get(0);
    assertEquals(METRIC, stored.metric());
    assertEquals(Collections.singletonMap("key", DimValue.of("x")), stored.dimensions());
    assertEquals(7.0, stored.value());
    assertEquals(900L, stored.timestampMs());
    assertEquals("app-redacted", stored.provenance().app());
    assertEquals("attempt-redacted", stored.provenance().attempt());
    assertEquals("1.0-redacted", stored.provenance().pluginVersion());
    assertEquals(1_000L, stored.provenance().writtenAtMs());
    assertFalse(stored.toString().contains("app-redacted"));
    assertFalse(stored.toString().contains("attempt-redacted"));
    assertFalse(stored.toString().contains("1.0-redacted"));
    assertFalse(stored.toString().contains("x"));
    assertFalse(stored.toString().contains("7.0"));
    assertEquals(1L,
        handle.counters().value(LocalMetricCounter.DECLARATION_STATUS_ACCEPTED));
    assertEquals(1L, handle.counters().value(LocalMetricCounter.RECORD_ENQUEUED));
    assertEquals(1L, handle.counters().value(LocalMetricCounter.BACKEND_ACCEPTED));
    assertEquals(1L, handle.counters().value(LocalMetricCounter.SUMMARY_STATUS_OK));

    assertTrue(local.shutdown(TIMEOUT));
    assertTrue(local.shutdown(Duration.ZERO));
  }

  @Test
  void zeroShutdownInitiatesStopAndAppliesPostStopFallbacks() {
    LocalHistoryMetrics local = open();
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.shutdown(Duration.ZERO);

    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertEquals(1L,
        local.testHandle().counters().value(LocalMetricCounter.RECORD_POST_STOP));
    assertTrue(local.shutdown(TIMEOUT));
    assertEquals(1L,
        local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
  }

  @Test
  void zeroShutdownMakesWritesTerminalWithoutWaitingForPlanningStopOrDiagnostics()
      throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    BlockingPlanningExecutor planning = new BlockingPlanningExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));
    local.store().record(java.util.Arrays.asList(
        observation(901L, 2.0), observation(902L, 3.0)));

    CountingAmbiguityHandler handler = new CountingAmbiguityHandler();
    Logger logger = Logger.getLogger(LocalRecordDiagnostics.class.getName());
    ExecutorService callers = Executors.newFixedThreadPool(3);
    logger.addHandler(handler);
    try {
      Future<Boolean> first = callers.submit(() -> local.shutdown(Duration.ZERO));
      assertTrue(planning.shutdownEntered.await(5, TimeUnit.SECONDS));
      assertFalse(first.get(5, TimeUnit.SECONDS));

      local.store().record(Collections.singletonList(observation(903L, 4.0)));
      Future<SummaryResponse> stoppedPlanning = callers.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      Future<Boolean> joined = callers.submit(() -> local.shutdown(Duration.ZERO));
      assertEquals(Status.Code.UNAVAILABLE,
          stoppedPlanning.get(5, TimeUnit.SECONDS).status().code());
      assertFalse(joined.get(5, TimeUnit.SECONDS));

      LocalHistoryMetricsCounters stopped = local.testHandle().counters();
      assertEquals(1L, stopped.value(LocalMetricCounter.RECORD_POST_STOP));
      assertEquals(2L, stopped.value(LocalMetricCounter.SHUTDOWN_DROPPED));
      assertEquals(1L, stopped.value(LocalMetricCounter.BACKEND_AMBIGUOUS));
      assertTrue(local.drain(TIMEOUT));
      assertEquals(0, handler.count.get());

      planning.releaseShutdown.countDown();
      assertTrue(handler.published.await(5, TimeUnit.SECONDS));
      assertEquals(1, handler.count.get());

      backend.releaseRecord.countDown();
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, handler.count.get());
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
    } finally {
      planning.releaseShutdown.countDown();
      backend.releaseRecord.countDown();
      logger.removeHandler(handler);
      callers.shutdownNow();
      local.shutdown(TIMEOUT);
    }
  }

  @Test
  void positiveRecordingDeadlineDoesNotWaitForPlanningExecutorStop() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    BlockingPlanningExecutor planning = new BlockingPlanningExecutor();
    LocalHistoryMetrics local =
        openForTest(backend, planning, new IncrementingTicker());
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));
    local.store().record(Collections.singletonList(observation(901L, 2.0)));

    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> result =
          caller.submit(() -> local.shutdown(Duration.ofNanos(1L)));
      assertTrue(planning.shutdownEntered.await(5, TimeUnit.SECONDS));
      assertFalse(result.get(5, TimeUnit.SECONDS));
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_DROPPED));
      assertTrue(local.drain(TIMEOUT));
    } finally {
      planning.releaseShutdown.countDown();
      backend.releaseRecord.countDown();
      caller.shutdownNow();
      local.shutdown(TIMEOUT);
    }
  }

  @Test
  void zeroShutdownDiagnosticDoesNotHoldLifecycleLock() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    LocalHistoryMetrics local = openForTest(backend, planningExecutor());
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));

    BlockingLogHandler handler = new BlockingLogHandler();
    Logger logger = Logger.getLogger(LocalRecordDiagnostics.class.getName());
    ExecutorService callers = Executors.newFixedThreadPool(2);
    logger.addHandler(handler);
    try {
      Future<Boolean> first = callers.submit(() -> local.shutdown(Duration.ZERO));
      assertTrue(handler.publishEntered.await(5, TimeUnit.SECONDS));
      assertFalse(first.get(5, TimeUnit.SECONDS));

      Future<Boolean> joined = callers.submit(() -> local.shutdown(Duration.ZERO));
      assertFalse(joined.get(5, TimeUnit.SECONDS));

      handler.releasePublish.countDown();
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.BACKEND_AMBIGUOUS));
    } finally {
      handler.releasePublish.countDown();
      logger.removeHandler(handler);
      backend.releaseRecord.countDown();
      callers.shutdownNow();
      local.shutdown(TIMEOUT);
    }
  }

  @Test
  void runtimeTickerFailureDuringShutdownDoesNotWedgeCleanup() throws Exception {
    assertShutdownTickerFailureContained(ShutdownThrowingTicker.Failure.RUNTIME);
  }

  @Test
  void linkageTickerFailureDuringShutdownDoesNotWedgeCleanup() throws Exception {
    assertShutdownTickerFailureContained(ShutdownThrowingTicker.Failure.LINKAGE);
  }

  @Test
  void nonLinkageErrorDuringShutdownEscapesWithoutWedgingCleanup() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    ShutdownThrowingTicker ticker =
        new ShutdownThrowingTicker(ShutdownThrowingTicker.Failure.ASSERTION);
    LocalHistoryMetrics local = openForTest(backend, planningExecutor(), ticker);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));

    ticker.armForCurrentThread();
    try {
      assertThrows(AssertionError.class, () -> local.shutdown(TIMEOUT));
      assertEquals(0, backend.closeCalls.get());

      ticker.disarm();
      backend.releaseRecord.countDown();
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
    } finally {
      backend.releaseRecord.countDown();
      local.shutdown(Duration.ZERO);
    }
  }

  private static void assertShutdownTickerFailureContained(
      ShutdownThrowingTicker.Failure failure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    ShutdownThrowingTicker ticker = new ShutdownThrowingTicker(failure);
    LocalHistoryMetrics local = openForTest(backend, planningExecutor(), ticker);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));

    ticker.armForCurrentThread();
    try {
      assertFalse(local.shutdown(TIMEOUT));
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.BACKEND_AMBIGUOUS));
      assertEquals(0, backend.closeCalls.get());

      ticker.disarm();
      backend.releaseRecord.countDown();
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
    } finally {
      backend.releaseRecord.countDown();
      local.shutdown(Duration.ZERO);
    }
  }

  @Test
  void runtimePlanningShutdownFailureFallsBackWithoutWedgingCleanup() throws Exception {
    assertPlanningShutdownFailureContained(ThrowingPlanningExecutor.Failure.RUNTIME);
  }

  @Test
  void linkagePlanningShutdownFailureFallsBackWithoutWedgingCleanup() throws Exception {
    assertPlanningShutdownFailureContained(ThrowingPlanningExecutor.Failure.LINKAGE);
  }

  @Test
  void nestedRuntimePlanningShutdownFailuresRetryWithoutFalseCompletion() throws Exception {
    assertNestedPlanningShutdownFailureContained(
        ThrowingPlanningExecutor.Failure.RUNTIME,
        ThrowingPlanningExecutor.Failure.RUNTIME);
  }

  @Test
  void runtimeThenLinkagePlanningShutdownFailuresRetryWithoutFalseCompletion() throws Exception {
    assertNestedPlanningShutdownFailureContained(
        ThrowingPlanningExecutor.Failure.RUNTIME,
        ThrowingPlanningExecutor.Failure.LINKAGE);
  }

  @Test
  void linkageThenRuntimePlanningShutdownFailuresRetryWithoutFalseCompletion() throws Exception {
    assertNestedPlanningShutdownFailureContained(
        ThrowingPlanningExecutor.Failure.LINKAGE,
        ThrowingPlanningExecutor.Failure.RUNTIME);
  }

  @Test
  void nestedLinkagePlanningShutdownFailuresRetryWithoutFalseCompletion() throws Exception {
    assertNestedPlanningShutdownFailureContained(
        ThrowingPlanningExecutor.Failure.LINKAGE,
        ThrowingPlanningExecutor.Failure.LINKAGE);
  }

  @Test
  void planningShutdownAssertionDoesNotPublishFalseCompletion() throws Exception {
    assertNestedPlanningShutdownFailureContained(
        ThrowingPlanningExecutor.Failure.ASSERTION,
        ThrowingPlanningExecutor.Failure.NONE);
  }

  @Test
  void runtimeBackendCloseFailureDoesNotPublishCompletionOrRetryClose() throws Exception {
    assertBackendCloseFailureDoesNotComplete(ControlledBackend.CloseFailure.RUNTIME);
  }

  @Test
  void linkageBackendCloseFailureDoesNotPublishCompletionOrRetryClose() throws Exception {
    assertBackendCloseFailureDoesNotComplete(ControlledBackend.CloseFailure.LINKAGE);
  }

  @Test
  void rejectedCleanupExecutorShutdownFailureCannotPublishEarlyCompletion() throws Exception {
    ChainedRejectingLifecycleExecutor rejected = new ChainedRejectingLifecycleExecutor(
        ChainedRejectingLifecycleExecutor.Failure.RUNTIME,
        ChainedRejectingLifecycleExecutor.Failure.LINKAGE,
        null);
    assertChainedCleanupSchedulingFailureContained(rejected, null);
  }

  @Test
  void repeatedCleanupSchedulingFailuresUseBoundedOwnedFallback() throws Exception {
    ChainedRejectingLifecycleExecutor second = new ChainedRejectingLifecycleExecutor(
        ChainedRejectingLifecycleExecutor.Failure.LINKAGE,
        ChainedRejectingLifecycleExecutor.Failure.RUNTIME,
        null);
    ChainedRejectingLifecycleExecutor first = new ChainedRejectingLifecycleExecutor(
        ChainedRejectingLifecycleExecutor.Failure.RUNTIME,
        ChainedRejectingLifecycleExecutor.Failure.LINKAGE,
        second);
    assertChainedCleanupSchedulingFailureContained(first, second);
  }

  @Test
  void runtimeCurrentCleanupExecutorShutdownFailureRetriesWithoutCaller() throws Exception {
    assertCurrentCleanupExecutorShutdownFailureRetries(
        ChainedRejectingLifecycleExecutor.Failure.RUNTIME);
  }

  @Test
  void linkageCurrentCleanupExecutorShutdownFailureRetriesWithoutCaller() throws Exception {
    assertCurrentCleanupExecutorShutdownFailureRetries(
        ChainedRejectingLifecycleExecutor.Failure.LINKAGE);
  }

  @Test
  void supersededTerminationCallbackSchedulesCurrentTermination() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    DeferredTerminatingRejectingLifecycleExecutor rejected =
        new DeferredTerminatingRejectingLifecycleExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, rejected);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(rejected.firstShutdown.await(5, TimeUnit.SECONDS));
      Future<Boolean> completion = caller.submit(() -> local.shutdown(TIMEOUT));
      assertTrue(rejected.secondShutdown.await(5, TimeUnit.SECONDS));

      rejected.finishTermination();
      assertTrue(completion.get(5, TimeUnit.SECONDS));
      assertEquals(1, backend.closeCalls.get());
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    } finally {
      rejected.finishTermination();
      caller.shutdownNow();
      planning.shutdownNow();
    }
  }

  @Test
  void executorTerminationProbeCannotCallHostileOverrideUnderLifecycleLock() {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    ThrowingIsTerminatedLifecycleExecutor cleanup =
        new ThrowingIsTerminatedLifecycleExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);

    try {
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(0, cleanup.isTerminatedCalls.get());
      assertEquals(1, backend.closeCalls.get());
    } finally {
      planning.shutdownNow();
    }
  }

  @Test
  void blockedRejectedExecutorShutdownCannotExtendZeroCaller() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    BlockingShutdownRejectingLifecycleExecutor rejected =
        new BlockingShutdownRejectingLifecycleExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, rejected);

    try {
      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(rejected.shutdownEntered.await(5, TimeUnit.SECONDS));
      assertEquals(0, backend.closeCalls.get());

      rejected.releaseShutdown.countDown();
      assertTrue(local.shutdown(TIMEOUT));
      assertTrue(rejected.isTerminated());
      assertEquals(1, backend.closeCalls.get());
    } finally {
      rejected.releaseShutdown.countDown();
      rejected.shutdownNow();
      planning.shutdownNow();
    }
  }

  @Test
  void terminatingCleanupExecutorCannotInvertLifecycleLockOrder() throws Exception {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    BlockingTerminatedLifecycleExecutor cleanup = new BlockingTerminatedLifecycleExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(cleanup.terminatedEntered.await(5, TimeUnit.SECONDS));

      Future<Boolean> poll = caller.submit(() -> local.shutdown(Duration.ZERO));
      assertFalse(poll.get(5, TimeUnit.SECONDS));
      assertEquals(0L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));

      cleanup.releaseTerminated.countDown();
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
    } finally {
      cleanup.releaseTerminated.countDown();
      caller.shutdownNow();
      planning.shutdownNow();
    }
  }

  @Test
  void runtimeCleanupSchedulingFailureFallsBackWithoutReopeningAdmission() throws Exception {
    assertCleanupSchedulingFailureContained(RejectOnceLifecycleExecutor.Failure.RUNTIME);
  }

  @Test
  void linkageCleanupSchedulingFailureFallsBackWithoutReopeningAdmission() throws Exception {
    assertCleanupSchedulingFailureContained(RejectOnceLifecycleExecutor.Failure.LINKAGE);
  }

  @Test
  void nonLinkageCleanupSchedulingErrorEscapesBeforeLifecycleTransition() {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    RejectOnceLifecycleExecutor cleanup =
        new RejectOnceLifecycleExecutor(RejectOnceLifecycleExecutor.Failure.ASSERTION);
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);

    try {
      assertEquals(SchemaStatus.Code.ACCEPTED,
          local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
      assertThrows(AssertionError.class, () -> local.shutdown(Duration.ZERO));
      assertEquals(0, backend.closeCalls.get());
      local.store().record(Collections.singletonList(observation(900L, 1.0)));
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.RECORD_POST_STOP));
      assertEquals(0, backend.recordCalls.get());
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, cleanup.executeCalls.get());
      assertTrue(cleanup.isTerminated());
      assertEquals(1, backend.closeCalls.get());
    } finally {
      local.shutdown(TIMEOUT);
    }
  }

  private static void assertBackendCloseFailureDoesNotComplete(
      ControlledBackend.CloseFailure failure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.closeFailure = failure;
    ThreadPoolExecutor planning = planningExecutor();
    LocalHistoryMetricsImpl.LifecycleExecutor cleanup =
        new LocalHistoryMetricsImpl.LifecycleExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);

    try {
      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(backend.closeAttempted.await(5, TimeUnit.SECONDS));
      assertEquals(1, backend.closeCalls.get());
      assertEquals(0L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));

      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(1, backend.closeCalls.get());
      assertEquals(0L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    } finally {
      cleanup.shutdownNow();
      planning.shutdownNow();
    }
  }

  private static void assertCurrentCleanupExecutorShutdownFailureRetries(
      ChainedRejectingLifecycleExecutor.Failure failure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    ThreadPoolExecutor planning = planningExecutor();
    ChainedRejectingLifecycleExecutor cleanup = new ChainedRejectingLifecycleExecutor(
        ChainedRejectingLifecycleExecutor.Failure.NONE, failure, null);
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);

    try {
      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(cleanup.firstShutdownFailed.await(5, TimeUnit.SECONDS));
      assertTrue(cleanup.terminated.await(5, TimeUnit.SECONDS));
      assertTrue(cleanup.shutdownCalls.get() >= 2);
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    } finally {
      cleanup.shutdownNow();
      planning.shutdownNow();
    }
  }

  private static void assertChainedCleanupSchedulingFailureContained(
      ChainedRejectingLifecycleExecutor first,
      ChainedRejectingLifecycleExecutor second) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockSummary = true;
    ThreadPoolExecutor planning = planningExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning, first);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      Future<SummaryResponse> active = caller.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(backend.summaryEntered.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(1, first.executeCalls.get());
      if (second != null) {
        assertEquals(1, second.executeCalls.get());
      }
      assertEquals(0, backend.closeCalls.get());

      backend.releaseSummary.countDown();
      active.get(5, TimeUnit.SECONDS);
      assertTrue(local.shutdown(TIMEOUT));
      assertTrue(first.isTerminated());
      assertTrue(first.terminated.await(5, TimeUnit.SECONDS));
      assertTrue(first.shutdownCalls.get() >= 2);
      if (second != null) {
        assertTrue(second.isTerminated());
        assertTrue(second.terminated.await(5, TimeUnit.SECONDS));
        assertTrue(second.shutdownCalls.get() >= 2);
      }
      assertEquals(1, backend.closeCalls.get());
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    } finally {
      backend.releaseSummary.countDown();
      caller.shutdownNow();
      first.shutdownNow();
      if (second != null) {
        second.shutdownNow();
      }
      planning.shutdownNow();
    }
  }

  private static void assertCleanupSchedulingFailureContained(
      RejectOnceLifecycleExecutor.Failure failure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockSummary = true;
    ThreadPoolExecutor planning = planningExecutor();
    RejectOnceLifecycleExecutor cleanup = new RejectOnceLifecycleExecutor(failure);
    LocalHistoryMetrics local = openForTest(backend, planning, cleanup);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      assertEquals(SchemaStatus.Code.ACCEPTED,
          local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
      Future<SummaryResponse> active = caller.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(backend.summaryEntered.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(1, cleanup.executeCalls.get());
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_TIMEOUT));
      assertEquals(0, backend.closeCalls.get());

      local.store().record(Collections.singletonList(observation(900L, 1.0)));
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.RECORD_POST_STOP));
      assertEquals(0, backend.recordCalls.get());
      assertEquals(Status.Code.UNAVAILABLE,
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0).status().code());

      backend.releaseSummary.countDown();
      active.get(5, TimeUnit.SECONDS);
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
    } finally {
      backend.releaseSummary.countDown();
      caller.shutdownNow();
      local.shutdown(TIMEOUT);
    }
  }

  private static void assertNestedPlanningShutdownFailureContained(
      ThrowingPlanningExecutor.Failure shutdownNowFailure,
      ThrowingPlanningExecutor.Failure shutdownFailure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockSummary = true;
    ThrowingPlanningExecutor planning =
        new ThrowingPlanningExecutor(shutdownNowFailure, shutdownFailure);
    LocalHistoryMetrics local = openForTest(backend, planning);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      Future<SummaryResponse> active = caller.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(backend.summaryEntered.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(planning.shutdownAttempted.await(5, TimeUnit.SECONDS));
      if (shutdownFailure != ThrowingPlanningExecutor.Failure.NONE) {
        assertTrue(planning.gracefulShutdownAttempted.await(5, TimeUnit.SECONDS));
      }
      assertEquals(0L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
      assertEquals(0, backend.closeCalls.get());

      backend.releaseSummary.countDown();
      active.get(5, TimeUnit.SECONDS);
      assertTrue(local.shutdown(TIMEOUT));
      assertTrue(planning.isTerminated());
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
      assertEquals(1L,
          local.testHandle().counters().value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    } finally {
      backend.releaseSummary.countDown();
      caller.shutdownNow();
      local.shutdown(Duration.ZERO);
    }
  }

  private static void assertPlanningShutdownFailureContained(
      ThrowingPlanningExecutor.Failure failure) throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockSummary = true;
    ThrowingPlanningExecutor planning = new ThrowingPlanningExecutor(failure);
    LocalHistoryMetrics local = openForTest(backend, planning);
    ExecutorService caller = Executors.newFixedThreadPool(2);

    try {
      Future<SummaryResponse> active = caller.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(backend.summaryEntered.await(5, TimeUnit.SECONDS));
      Future<SummaryResponse> queued = caller.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(planning.queued.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertTrue(planning.shutdownAttempted.await(5, TimeUnit.SECONDS));
      assertEquals(Status.Code.UNAVAILABLE, queued.get(5, TimeUnit.SECONDS).status().code());
      assertEquals(0, backend.closeCalls.get());

      backend.releaseSummary.countDown();
      active.get(5, TimeUnit.SECONDS);
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, planning.shutdownNowCalls.get());
      assertEquals(1, planning.shutdownCalls.get());
      assertTrue(planning.isTerminated());
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
    } finally {
      backend.releaseSummary.countDown();
      caller.shutdownNow();
      local.shutdown(TIMEOUT);
    }
  }

  @Test
  void incompatibleBackendInfoFailsConstructionAndCleansOwnedResources() {
    ControlledBackend backend = new ControlledBackend();
    backend.info = new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION + 1, "too new");
    ThreadPoolExecutor planning = planningExecutor();

    assertThrows(IllegalArgumentException.class, () -> openForTest(backend, planning));

    assertEquals(1, backend.infoCalls.get());
    assertEquals(1, backend.closeCalls.get());
    assertTrue(planning.isShutdown());
    assertSame(MetricStores.current(), MetricStores.current());
  }

  @Test
  void shutdownCancelsQueuedPlanningAndClosesOnlyAfterTheActiveCallEnds()
      throws Exception {
    ControlledBackend backend = new ControlledBackend();
    SignallingPlanningExecutor planning = planningExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    backend.blockSummary = true;

    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<SummaryResponse> active = callers.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(backend.summaryEntered.await(5, TimeUnit.SECONDS));
      Future<SummaryResponse> queued = callers.submit(() ->
          local.store().summarize(
              Collections.singletonList(summaryRequest()), TIMEOUT).get(0));
      assertTrue(planning.queued.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(Status.Code.UNAVAILABLE, queued.get(5, TimeUnit.SECONDS).status().code());
      assertEquals(0, backend.closeCalls.get());
      assertEquals(3L, local.testHandle().counters().value(
          LocalMetricCounter.BREAKER_SAMPLE));
      assertEquals(1L, local.testHandle().counters().value(
          LocalMetricCounter.BREAKER_FAILURE));

      backend.releaseSummary.countDown();
      active.get(5, TimeUnit.SECONDS);
      assertTrue(local.shutdown(TIMEOUT));
      assertEquals(1, backend.closeCalls.get());
      assertFalse(backend.closeOverlapped);
      assertTrue(planning.isTerminated());
    } finally {
      backend.releaseSummary.countDown();
      callers.shutdownNow();
    }
  }

  @Test
  void zeroShutdownDropsQueuedWritesAndMarksInflightAmbiguousWithoutRetry()
      throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    LocalHistoryMetrics local = openForTest(backend, planningExecutor());
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());

    local.store().record(Collections.singletonList(observation(900L, 1.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));
    local.store().record(java.util.Arrays.asList(
        observation(901L, 2.0), observation(902L, 3.0)));

    assertFalse(local.shutdown(Duration.ZERO));
    LocalHistoryMetricsCounters stopped = local.testHandle().counters();
    assertEquals(2L, stopped.value(LocalMetricCounter.SHUTDOWN_DROPPED));
    assertTrue(local.drain(TIMEOUT));
    assertEquals(1L, stopped.value(LocalMetricCounter.BACKEND_AMBIGUOUS));
    assertEquals(1L, stopped.value(LocalMetricCounter.SHUTDOWN_TIMEOUT));
    assertEquals(0, backend.closeCalls.get());

    backend.releaseRecord.countDown();
    assertTrue(local.shutdown(TIMEOUT));
    assertTrue(local.shutdown(Duration.ZERO));
    LocalHistoryMetricsCounters complete = local.testHandle().counters();
    assertEquals(1L, complete.value(LocalMetricCounter.BACKEND_ACCEPTED));
    assertEquals(1L, complete.value(LocalMetricCounter.SHUTDOWN_COMPLETE));
    assertEquals(1, backend.recordCalls.get());
    assertEquals(1, backend.closeCalls.get());
    assertFalse(backend.closeOverlapped);
  }

  @Test
  void laterTimedOutShutdownMarksInflightAmbiguousOnceWithoutReplacingCompletion()
      throws Exception {
    ControlledBackend backend = new ControlledBackend();
    backend.blockRecord = true;
    SignallingPlanningExecutor planning = planningExecutor();
    LocalHistoryMetrics local = openForTest(backend, planning);
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.store().record(java.util.Arrays.asList(
        observation(900L, 1.0), observation(901L, 2.0)));
    assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));

    ExecutorService callers = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> first = callers.submit(() -> local.shutdown(TIMEOUT));
      assertTrue(planning.shutdownCalled.await(5, TimeUnit.SECONDS));

      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(2L, local.testHandle().counters().value(
          LocalMetricCounter.BACKEND_AMBIGUOUS));
      assertFalse(local.shutdown(Duration.ZERO));
      assertEquals(2L, local.testHandle().counters().value(
          LocalMetricCounter.BACKEND_AMBIGUOUS));

      backend.releaseRecord.countDown();
      assertTrue(first.get(5, TimeUnit.SECONDS));
      LocalHistoryMetricsCounters complete = local.testHandle().counters();
      assertEquals(2L, complete.value(LocalMetricCounter.BACKEND_AMBIGUOUS));
      assertEquals(2L, complete.value(LocalMetricCounter.BACKEND_ACCEPTED));
      assertEquals(0L, complete.value(LocalMetricCounter.SHUTDOWN_DROPPED));
      assertEquals(1L, complete.value(LocalMetricCounter.SHUTDOWN_COMPLETE));
      assertEquals(1, backend.closeCalls.get());
    } finally {
      backend.releaseRecord.countDown();
      callers.shutdownNow();
    }
  }

  @Test
  void stoppedStoreKeepsLocalValidationAndZeroTimeoutPrecedence() {
    LocalHistoryMetrics local = open();
    assertEquals(SchemaStatus.Code.ACCEPTED,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    local.shutdown(Duration.ZERO);

    assertEquals(SchemaStatus.Code.INVALID_REQUEST,
        local.store().declare(Collections.singletonList(null), TIMEOUT).get(0).code());
    assertEquals(SchemaStatus.Code.UNAVAILABLE,
        local.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
    assertEquals(Status.Code.UNAVAILABLE,
        local.store().summarize(
            Collections.singletonList(SummaryRequest.builder(new MetricVersionId(41, 2))
                .window(0L, 1L)
                .build()),
            TIMEOUT).get(0).status().code());
    assertEquals(Status.Code.DEADLINE_EXCEEDED,
        local.store().summarize(
            Collections.singletonList(summaryRequest()), Duration.ZERO)
            .get(0).status().code());
    assertEquals(Status.Code.UNAVAILABLE,
        local.store().summarize(
            Collections.singletonList(summaryRequest()), TIMEOUT)
            .get(0).status().code());
    assertTrue(local.shutdown(TIMEOUT));
  }

  private static LocalHistoryMetrics open() {
    HistoryMetricCatalog catalog =
        LocalTestCatalog.builder().addLive(41, "test.metric").build();
    return LocalHistoryMetricsFactory.open(
        catalog,
        Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
        () -> LocalProvenanceIdentity.of(
            "app-redacted", "attempt-redacted", "1.0-redacted"),
        Duration.ofDays(30),
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)));
  }

  private static MetricSchema schema() {
    return new MetricSchema(
        METRIC,
        Collections.singletonList(new DimensionSpec("key", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(1), Duration.ofDays(7)));
  }

  private static Observation observation(long timestampMs, double value) {
    return new Observation(
        METRIC, Collections.singletonMap("key", DimValue.of("x")), value, timestampMs);
  }

  private static SummaryRequest summaryRequest() {
    return SummaryRequest.builder(METRIC)
        .bind("key", DimValue.of("x"))
        .window(0L, 2_000L)
        .build();
  }

  private static SignallingPlanningExecutor planningExecutor() {
    return new SignallingPlanningExecutor();
  }

  private static LocalHistoryMetrics openForTest(
      ControlledBackend backend, ThreadPoolExecutor planning) {
    return openForTest(
        backend,
        planning,
        new LocalMetricStorePlanningAdapter.Ticker() {
          @Override
          public long readNanos() {
            return System.nanoTime();
          }
        });
  }

  private static LocalHistoryMetrics openForTest(
      ControlledBackend backend,
      ThreadPoolExecutor planning,
      LocalHistoryMetricsImpl.LifecycleExecutor shutdownExecutor) {
    return LocalHistoryMetricsFactory.openForTest(
        LocalTestCatalog.builder().addLive(41, "test.metric").build(),
        Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
        () -> LocalProvenanceIdentity.of("app", null, "1.0"),
        Duration.ofDays(30),
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)),
        backend,
        null,
        planning,
        new LocalMetricStorePlanningAdapter.Ticker() {
          @Override
          public long readNanos() {
            return System.nanoTime();
          }
        },
        shutdownExecutor);
  }

  private static LocalHistoryMetrics openForTest(
      ControlledBackend backend,
      ThreadPoolExecutor planning,
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    return LocalHistoryMetricsFactory.openForTest(
        LocalTestCatalog.builder().addLive(41, "test.metric").build(),
        Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
        () -> LocalProvenanceIdentity.of("app", null, "1.0"),
        Duration.ofDays(30),
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)),
        backend,
        null,
        planning,
        ticker);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException failure) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class IncrementingTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.getAndIncrement();
    }
  }

  private static final class ShutdownThrowingTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private enum Failure {
      RUNTIME,
      LINKAGE,
      ASSERTION
    }

    private final Failure failure;
    private final AtomicInteger targetReads = new AtomicInteger();
    private volatile Thread target;

    private ShutdownThrowingTicker(Failure failure) {
      this.failure = failure;
    }

    private void armForCurrentThread() {
      target = Thread.currentThread();
    }

    private void disarm() {
      target = null;
    }

    @Override
    public long readNanos() {
      if (Thread.currentThread() != target) {
        return System.nanoTime();
      }
      int read = targetReads.getAndIncrement();
      if (read == 0) {
        return 0L;
      }
      if (read == 1) {
        switch (failure) {
          case RUNTIME:
            throw new IllegalStateException("shutdown ticker failure");
          case LINKAGE:
            throw new LinkageError("shutdown ticker failure");
          case ASSERTION:
            throw new AssertionError("shutdown ticker failure");
          default:
            throw new AssertionError("unexpected failure kind");
        }
      }
      return (long) read * 10_000_000_000L;
    }
  }

  private static final class CountingAmbiguityHandler extends Handler {
    private final AtomicInteger count = new AtomicInteger();
    private final CountDownLatch published = new CountDownLatch(1);

    @Override
    public void publish(LogRecord record) {
      if (LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS.message()
          .equals(record.getMessage())) {
        count.incrementAndGet();
        published.countDown();
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }

  private static final class ChainedRejectingLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private enum Failure {
      NONE,
      RUNTIME,
      LINKAGE
    }

    private final Failure executeFailure;
    private final Failure shutdownFailure;
    private final AtomicInteger executeCalls = new AtomicInteger();
    private final AtomicInteger shutdownCalls = new AtomicInteger();
    private final CountDownLatch firstShutdownFailed = new CountDownLatch(1);
    private final CountDownLatch terminated = new CountDownLatch(1);

    private ChainedRejectingLifecycleExecutor(
        Failure executeFailure,
        Failure shutdownFailure,
        LocalHistoryMetricsImpl.LifecycleExecutor next) {
      this.executeFailure = executeFailure;
      this.shutdownFailure = shutdownFailure;
      if (next != null) {
        successor(next);
      }
    }

    @Override
    public void execute(Runnable command) {
      executeCalls.incrementAndGet();
      throwFailure(executeFailure);
      super.execute(command);
    }

    @Override
    public void shutdown() {
      int call = shutdownCalls.incrementAndGet();
      if (call == 1 && shutdownFailure != Failure.NONE) {
        firstShutdownFailed.countDown();
        throwFailure(shutdownFailure);
      }
      super.shutdown();
    }

    @Override
    protected void terminated() {
      try {
        super.terminated();
      } finally {
        terminated.countDown();
      }
    }

    private static void throwFailure(Failure failure) {
      if (failure == Failure.RUNTIME) {
        throw new IllegalStateException("cleanup executor failure");
      }
      if (failure == Failure.LINKAGE) {
        throw new LinkageError("cleanup executor failure");
      }
    }
  }

  private static final class DeferredTerminatingRejectingLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private final AtomicInteger shutdownCalls = new AtomicInteger();
    private final CountDownLatch firstShutdown = new CountDownLatch(1);
    private final CountDownLatch secondShutdown = new CountDownLatch(1);

    @Override
    public void execute(Runnable command) {
      throw new IllegalStateException("cleanup scheduling failure");
    }

    @Override
    public void shutdown() {
      int call = shutdownCalls.incrementAndGet();
      if (call == 1) {
        firstShutdown.countDown();
      } else {
        secondShutdown.countDown();
      }
      throw new IllegalStateException("deferred cleanup termination");
    }

    private void finishTermination() {
      super.shutdown();
    }
  }

  private static final class ThrowingIsTerminatedLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private final AtomicInteger isTerminatedCalls = new AtomicInteger();

    @Override
    public boolean isTerminated() {
      isTerminatedCalls.incrementAndGet();
      throw new IllegalStateException("hostile termination probe");
    }
  }

  private static final class BlockingShutdownRejectingLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private final CountDownLatch shutdownEntered = new CountDownLatch(1);
    private final CountDownLatch releaseShutdown = new CountDownLatch(1);

    @Override
    public void execute(Runnable command) {
      throw new IllegalStateException("cleanup scheduling failure");
    }

    @Override
    public void shutdown() {
      shutdownEntered.countDown();
      awaitUninterruptibly(releaseShutdown);
      super.shutdown();
    }
  }

  private static final class BlockingTerminatedLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private final CountDownLatch terminatedEntered = new CountDownLatch(1);
    private final CountDownLatch releaseTerminated = new CountDownLatch(1);

    @Override
    protected void terminated() {
      terminatedEntered.countDown();
      awaitUninterruptibly(releaseTerminated);
      super.terminated();
    }
  }

  private static final class RejectOnceLifecycleExecutor
      extends LocalHistoryMetricsImpl.LifecycleExecutor {
    private enum Failure {
      RUNTIME,
      LINKAGE,
      ASSERTION
    }

    private final Failure failure;
    private final AtomicInteger executeCalls = new AtomicInteger();

    private RejectOnceLifecycleExecutor(Failure failure) {
      this.failure = failure;
    }

    @Override
    public void execute(Runnable command) {
      if (executeCalls.getAndIncrement() == 0) {
        switch (failure) {
          case RUNTIME:
            throw new IllegalStateException("cleanup scheduling failure");
          case LINKAGE:
            throw new LinkageError("cleanup scheduling failure");
          case ASSERTION:
            throw new AssertionError("cleanup scheduling failure");
          default:
            throw new AssertionError("unexpected failure kind");
        }
      }
      super.execute(command);
    }
  }

  private static final class ThrowingPlanningExecutor extends ThreadPoolExecutor {
    private enum Failure {
      NONE,
      RUNTIME,
      LINKAGE,
      ASSERTION
    }

    private final Failure shutdownNowFailure;
    private final Failure shutdownFailure;
    private final AtomicInteger shutdownNowCalls = new AtomicInteger();
    private final AtomicInteger shutdownCalls = new AtomicInteger();
    private final CountDownLatch shutdownAttempted = new CountDownLatch(1);
    private final CountDownLatch gracefulShutdownAttempted = new CountDownLatch(1);
    private final CountDownLatch queued = new CountDownLatch(1);

    private ThrowingPlanningExecutor(Failure failure) {
      this(failure, Failure.NONE);
    }

    private ThrowingPlanningExecutor(Failure shutdownNowFailure, Failure shutdownFailure) {
      super(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(8),
          LocalHistoryMetricsFactory.daemonFactory("history-metrics-throwing-shutdown-test"),
          new ThreadPoolExecutor.AbortPolicy());
      this.shutdownNowFailure = shutdownNowFailure;
      this.shutdownFailure = shutdownFailure;
    }

    @Override
    public void execute(Runnable command) {
      super.execute(command);
      if (getQueue().contains(command)) {
        queued.countDown();
      }
    }

    @Override
    public List<Runnable> shutdownNow() {
      int call = shutdownNowCalls.incrementAndGet();
      shutdownAttempted.countDown();
      if (call == 1) {
        throwFailure(shutdownNowFailure);
      }
      return super.shutdownNow();
    }

    @Override
    public void shutdown() {
      int call = shutdownCalls.incrementAndGet();
      gracefulShutdownAttempted.countDown();
      if (call == 1) {
        throwFailure(shutdownFailure);
      }
      super.shutdown();
    }

    private static void throwFailure(Failure failure) {
      switch (failure) {
        case NONE:
          return;
        case RUNTIME:
          throw new IllegalStateException("planning shutdown failure");
        case LINKAGE:
          throw new LinkageError("planning shutdown failure");
        case ASSERTION:
          throw new AssertionError("planning shutdown failure");
        default:
          throw new AssertionError("unexpected failure kind");
      }
    }
  }

  private static final class BlockingPlanningExecutor extends ThreadPoolExecutor {
    private final CountDownLatch shutdownEntered = new CountDownLatch(1);
    private final CountDownLatch releaseShutdown = new CountDownLatch(1);

    private BlockingPlanningExecutor() {
      super(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(8),
          LocalHistoryMetricsFactory.daemonFactory("history-metrics-blocking-shutdown-test"),
          new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdownEntered.countDown();
      awaitUninterruptibly(releaseShutdown);
      return super.shutdownNow();
    }
  }

  private static final class BlockingLogHandler extends Handler {
    private final CountDownLatch publishEntered = new CountDownLatch(1);
    private final CountDownLatch releasePublish = new CountDownLatch(1);

    @Override
    public void publish(LogRecord record) {
      publishEntered.countDown();
      awaitUninterruptibly(releasePublish);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }

  private static final class SignallingPlanningExecutor extends ThreadPoolExecutor {
    private final CountDownLatch queued = new CountDownLatch(2);
    private final CountDownLatch shutdownCalled = new CountDownLatch(1);

    private SignallingPlanningExecutor() {
      super(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(8),
          LocalHistoryMetricsFactory.daemonFactory("history-metrics-lifecycle-test"),
          new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void execute(Runnable command) {
      super.execute(command);
      if (getQueue().contains(command)) {
        queued.countDown();
      }
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdownCalled.countDown();
      return super.shutdownNow();
    }
  }

  private static final class ControlledBackend implements HistoryMetricsBackend {
    private enum CloseFailure {
      NONE,
      RUNTIME,
      LINKAGE
    }

    private volatile BackendInfo info =
        new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "controlled backend");
    private final AtomicInteger infoCalls = new AtomicInteger();
    private final AtomicInteger recordCalls = new AtomicInteger();
    private final AtomicInteger closeCalls = new AtomicInteger();
    private final AtomicInteger activeCalls = new AtomicInteger();
    private final CountDownLatch summaryEntered = new CountDownLatch(1);
    private final CountDownLatch releaseSummary = new CountDownLatch(1);
    private final CountDownLatch recordEntered = new CountDownLatch(1);
    private final CountDownLatch releaseRecord = new CountDownLatch(1);
    private final CountDownLatch closeAttempted = new CountDownLatch(1);
    private volatile CloseFailure closeFailure = CloseFailure.NONE;
    private volatile boolean blockSummary;
    private volatile boolean blockRecord;
    private volatile boolean closeOverlapped;

    @Override
    public BackendInfo info() {
      infoCalls.incrementAndGet();
      return info;
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      activeCalls.incrementAndGet();
      try {
        List<SchemaStatus> statuses = new ArrayList<SchemaStatus>(schemas.size());
        for (MetricSchema declared : schemas) {
          statuses.add(SchemaStatus.accepted(declared.metric(), null));
        }
        return statuses;
      } finally {
        activeCalls.decrementAndGet();
      }
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      activeCalls.incrementAndGet();
      recordCalls.incrementAndGet();
      try {
        if (blockRecord) {
          recordEntered.countDown();
          awaitUninterruptibly(releaseRecord);
        }
        return WriteResult.ok(observations.size());
      } finally {
        activeCalls.decrementAndGet();
      }
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      activeCalls.incrementAndGet();
      try {
        if (blockSummary) {
          summaryEntered.countDown();
          awaitUninterruptibly(releaseSummary);
        }
        List<SummaryResponse> responses =
            new ArrayList<SummaryResponse>(requests.size());
        for (SummaryRequest ignored : requests) {
          responses.add(SummaryResponse.ok(null, Coverage.COMPLETE));
        }
        return responses;
      } finally {
        activeCalls.decrementAndGet();
      }
    }

    @Override
    public void close() {
      if (activeCalls.get() != 0) {
        closeOverlapped = true;
      }
      closeCalls.incrementAndGet();
      closeAttempted.countDown();
      if (closeFailure == CloseFailure.RUNTIME) {
        throw new IllegalStateException("backend close failure");
      }
      if (closeFailure == CloseFailure.LINKAGE) {
        throw new LinkageError("backend close failure");
      }
    }
  }
}
