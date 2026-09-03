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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.LocalTestCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Behavioral coverage for FR-22 owner-side snapshot save. */
class LocalHistoryMetricsSaveTest {
  private static final MetricVersionId METRIC = new MetricVersionId(51, 1);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final Duration MAXIMUM_PLANNING_AGE = Duration.ofDays(30);
  private static final HistoryMetricCatalog CATALOG =
      LocalTestCatalog.builder().addLive(51, "snapshot.metric").build();

  @TempDir
  Path temporaryDirectory;

  @Test
  void publicOwnerExposesOnlyTheFrozenCheckedSaveSignature() throws Exception {
    assertArrayEquals(
        new Class<?>[] {LocalSnapshotException.class},
        LocalHistoryMetrics.class
            .getMethod("save", Path.class, Duration.class)
            .getExceptionTypes());
  }

  @Test
  void savePublishesTheCoherentDurableStateThroughTheExactPublicApi() throws Exception {
    LocalHistoryMetrics owner = open();
    try {
      MetricSchema schema = schema();
      assertEquals(SchemaStatus.Code.ACCEPTED,
          owner.store().declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
      owner.store().record(Collections.singletonList(observation(900L, 7.0)));
      Path target = temporaryDirectory.resolve("state.bin");

      owner.save(target, TIMEOUT);

      LocalSnapshotState state =
          LocalSnapshotFiles.load(target, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT);
      assertEquals(1, state.declarations().size());
      assertEquals(schema, state.declarations().get(0).schema());
      assertEquals(1, state.observations().size());
      assertEquals(7.0, state.observations().get(0).value());
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void committedSaveWithOneCleanupFailureCountsAndDiagnosesOnceThenRecovers()
      throws Exception {
    DeleteFaultFileOperations operations = new DeleteFaultFileOperations(true, false);
    DiagnosticCollector diagnostics = new DiagnosticCollector();
    LocalHistoryMetrics owner = openForTest(systemTicker(), operations, diagnostics);
    Path target = temporaryDirectory.resolve("sensitive-target.bin");
    Path unrelated = temporaryDirectory.resolve("unrelated-history-temp.bin");
    Files.write(target, new byte[] {4, 5, 6});
    Files.write(unrelated, new byte[] {1});
    try {
      owner.save(target, TIMEOUT);

      assertEquals(1L, owner.testHandle().counters().value(
          LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE));
      assertEquals(Collections.singletonList(
          LocalHistoryMetricsImpl.SNAPSHOT_CLEANUP_DIAGNOSTIC), diagnostics.messages);
      assertTrue(Files.exists(operations.intendedTemporary));
      Path residualFromFirstSave = operations.intendedTemporary;
      assertFalse(Files.exists(operations.priorTemporary));
      assertTrue(Files.exists(unrelated));
      assertTrue(LocalSnapshotFiles.load(
          target, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT).observations().isEmpty());

      operations.failIntendedDelete = false;
      owner.save(target, TIMEOUT);
      assertEquals(1L, owner.testHandle().counters().value(
          LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE));
      assertEquals(1, diagnostics.messages.size());
      assertTrue(Files.exists(residualFromFirstSave));
      assertFalse(Files.exists(operations.intendedTemporary));
      assertTrue(Files.exists(unrelated));
      assertTrue(LocalSnapshotFiles.load(
          target, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT).observations().isEmpty());
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void committedSaveWithBothCleanupFailuresStillCountsAndDiagnosesOnce()
      throws Exception {
    DeleteFaultFileOperations operations = new DeleteFaultFileOperations(true, true);
    DiagnosticCollector diagnostics = new DiagnosticCollector();
    LocalHistoryMetrics owner = openForTest(systemTicker(), operations, diagnostics);
    Path target = temporaryDirectory.resolve("sensitive-target.bin");
    Files.write(target, new byte[] {7, 8, 9});
    try {
      owner.save(target, TIMEOUT);

      assertEquals(1L, owner.testHandle().counters().value(
          LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE));
      assertEquals(Collections.singletonList(
          LocalHistoryMetricsImpl.SNAPSHOT_CLEANUP_DIAGNOSTIC), diagnostics.messages);
      assertFalse(diagnostics.messages.get(0).contains(target.toString()));
      assertFalse(diagnostics.messages.get(0).contains("sensitive-delete-detail"));
      assertTrue(Files.exists(operations.intendedTemporary));
      assertTrue(Files.exists(operations.priorTemporary));
      assertTrue(LocalSnapshotFiles.load(
          target, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT).observations().isEmpty());
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void cleanupFailuresBeforeCommitRemainSuppressedAndDoNotCountOrDiagnose()
      throws Exception {
    DeleteFaultFileOperations operations = new DeleteFaultFileOperations(true, true);
    operations.failMove = true;
    DiagnosticCollector diagnostics = new DiagnosticCollector();
    LocalHistoryMetrics owner = openForTest(systemTicker(), operations, diagnostics);
    Path target = temporaryDirectory.resolve("sensitive-target.bin");
    byte[] prior = new byte[] {10, 11, 12};
    Files.write(target, prior);
    try {
      LocalSnapshotException failure = assertThrows(
          LocalSnapshotException.class, () -> owner.save(target, TIMEOUT));

      assertEquals(LocalSnapshotException.Reason.IO, failure.reason());
      assertEquals(2, failure.getSuppressed().length);
      assertArrayEquals(prior, Files.readAllBytes(target));
      assertEquals(0L, owner.testHandle().counters().value(
          LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE));
      assertTrue(diagnostics.messages.isEmpty());
      assertTrue(Files.exists(operations.intendedTemporary));
      assertTrue(Files.exists(operations.priorTemporary));
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void saveValidatesArgumentsBeforeStoppedStateAndNeverReopensStoppedOwner() {
    LocalHistoryMetrics owner = open();
    assertTrue(owner.shutdown(TIMEOUT));

    assertThrows(NullPointerException.class, () -> owner.save(null, TIMEOUT));
    assertThrows(NullPointerException.class,
        () -> owner.save(temporaryDirectory.resolve("state.bin"), null));
    assertThrows(IllegalArgumentException.class,
        () -> owner.save(temporaryDirectory.resolve("state.bin"), Duration.ofNanos(-1)));
    assertThrows(IllegalStateException.class,
        () -> owner.save(temporaryDirectory.resolve("state.bin"), TIMEOUT));
    assertThrows(IllegalStateException.class,
        () -> owner.save(temporaryDirectory.resolve("zero-stopped.bin"), Duration.ZERO));
  }

  @Test
  void zeroBudgetOnAFreeOwnerReportsTimeoutAndReleasesTheReservation() throws Exception {
    LocalHistoryMetrics owner = open();
    try {
      LocalSnapshotException failure = assertThrows(LocalSnapshotException.class,
          () -> owner.save(temporaryDirectory.resolve("zero.bin"), Duration.ZERO));
      assertEquals(LocalSnapshotException.Reason.TIMEOUT, failure.reason());

      owner.save(temporaryDirectory.resolve("retry.bin"), TIMEOUT);
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void pathNormalizationFollowsWatermarkReservationAndDoesNotBlockShutdown()
      throws Exception {
    BlockingNormalizationFileOperations operations =
        new BlockingNormalizationFileOperations();
    WatermarkSignallingDrainWaiter drainWaiter =
        new WatermarkSignallingDrainWaiter();
    LocalHistoryMetrics owner = openForTest(
        systemTicker(),
        operations,
        LocalSnapshotFiles.systemGuardWaiter(),
        drainWaiter);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<?> saving = callers.submit(() -> {
        owner.save(temporaryDirectory.resolve("normalization.bin"), TIMEOUT);
        return null;
      });
      assertTrue(drainWaiter.watermarkEntered.await(5, TimeUnit.SECONDS));
      assertTrue(operations.normalizationEntered.await(5, TimeUnit.SECONDS));

      Future<Boolean> stopping =
          callers.submit(() -> owner.shutdown(Duration.ZERO));
      assertFalse(stopping.get(5, TimeUnit.SECONDS));

      operations.releaseNormalization.countDown();
      saving.get(5, TimeUnit.SECONDS);
      assertTrue(owner.shutdown(TIMEOUT));
    } finally {
      operations.releaseNormalization.countDown();
      owner.shutdown(TIMEOUT);
      callers.shutdownNow();
    }
  }

  @Test
  void positiveBudgetExpiredAfterWatermarkSkipsPathNormalization()
      throws Exception {
    ScriptedTicker ticker = new ScriptedTicker();
    RecordingNormalizationFileOperations operations =
        new RecordingNormalizationFileOperations();
    WatermarkExpiringDrainWaiter drainWaiter =
        new WatermarkExpiringDrainWaiter(ticker);
    LocalHistoryMetrics owner = openForTest(
        ticker,
        operations,
        LocalSnapshotFiles.systemGuardWaiter(),
        drainWaiter);
    try {
      LocalSnapshotException failure = assertThrows(LocalSnapshotException.class,
          () -> owner.save(
              temporaryDirectory.resolve("expired-before-normalization.bin"),
              Duration.ofNanos(5)));
      assertEquals(LocalSnapshotException.Reason.TIMEOUT, failure.reason());
      assertFalse(operations.normalizationCalled.get());
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  @Test
  void distinctTargetsPublishConcurrentlyAndShutdownWaitsOnlyForCapture()
      throws Exception {
    BlockingFileOperations operations = new BlockingFileOperations(2);
    LocalHistoryMetrics owner = openForTest(systemTicker(), operations);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      MetricSchema schema = schema();
      assertEquals(SchemaStatus.Code.ACCEPTED,
          owner.store().declare(Collections.singletonList(schema), TIMEOUT).get(0).code());
      owner.store().record(Collections.singletonList(observation(900L, 1.0)));
      assertTrue(owner.drain(TIMEOUT));

      Path first = temporaryDirectory.resolve("first.bin");
      Path second = temporaryDirectory.resolve("second.bin");
      Future<?> firstSave = callers.submit(() -> {
        owner.save(first, TIMEOUT);
        return null;
      });
      Future<?> secondSave = callers.submit(() -> {
        owner.save(second, TIMEOUT);
        return null;
      });
      assertTrue(operations.writesEntered.await(5, TimeUnit.SECONDS));

      assertTrue(owner.shutdown(TIMEOUT));
      operations.releaseWrites.countDown();
      firstSave.get(5, TimeUnit.SECONDS);
      secondSave.get(5, TimeUnit.SECONDS);

      assertEquals(1, LocalSnapshotFiles.load(
          first, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT).observations().size());
      assertEquals(1, LocalSnapshotFiles.load(
          second, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT).observations().size());
      assertThrows(IllegalStateException.class,
          () -> owner.save(temporaryDirectory.resolve("after-stop.bin"), TIMEOUT));
    } finally {
      operations.releaseWrites.countDown();
      owner.shutdown(TIMEOUT);
      callers.shutdownNow();
    }
  }

  @Test
  void sameTargetZeroIsBusyPositiveTimeoutDoesNotLeakAndRetrySucceeds()
      throws Exception {
    BlockingFileOperations operations = new BlockingFileOperations(1);
    LocalSnapshotFiles.GuardWaiter timeoutWaiter =
        new LocalSnapshotFiles.GuardWaiter() {
          @Override
          public long await(Condition condition, long remainingNanos) {
            return 0L;
          }
        };
    LocalHistoryMetrics owner = openForTest(
        systemTicker(), operations, timeoutWaiter);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    Path target = temporaryDirectory.resolve("same.bin");
    try {
      Future<?> firstSave = caller.submit(() -> {
        owner.save(target, TIMEOUT);
        return null;
      });
      assertTrue(operations.writesEntered.await(5, TimeUnit.SECONDS));

      LocalSnapshotException zeroBusy = assertThrows(LocalSnapshotException.class,
          () -> owner.save(target, Duration.ZERO));
      assertEquals(LocalSnapshotException.Reason.BUSY, zeroBusy.reason());

      LocalSnapshotException timedOut = assertThrows(LocalSnapshotException.class,
          () -> owner.save(target, TIMEOUT));
      assertEquals(LocalSnapshotException.Reason.TIMEOUT, timedOut.reason());

      operations.releaseWrites.countDown();
      firstSave.get(5, TimeUnit.SECONDS);
      owner.save(target, TIMEOUT);
      assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
    } finally {
      operations.releaseWrites.countDown();
      assertTrue(owner.shutdown(TIMEOUT));
      caller.shutdownNow();
    }
  }

  @Test
  void shutdownWaitsForEveryReservedCaptureButNotTheirFilePublication()
      throws Exception {
    Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC);
    LocalHistoryMetricsBackend durable =
        LocalHistoryMetricsBackend.create(CATALOG, clock, MAXIMUM_PLANNING_AGE);
    BlockingRecordBackend backend = new BlockingRecordBackend(durable);
    BlockingSnapshotDrainWaiter drainWaiter = new BlockingSnapshotDrainWaiter(2);
    BlockingFileOperations operations = new BlockingFileOperations(2);
    LocalHistoryMetrics owner = LocalHistoryMetricsFactory.openForTest(
        CATALOG,
        clock,
        () -> LocalProvenanceIdentity.of("app", null, "1.0"),
        MAXIMUM_PLANNING_AGE,
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)),
        backend,
        durable,
        Executors.newSingleThreadExecutor(
            LocalHistoryMetricsFactory.daemonFactory("snapshot-ambiguous-planning-test")),
        systemTicker(),
        operations,
        LocalSnapshotFiles.systemGuardWaiter(),
        drainWaiter);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      assertEquals(SchemaStatus.Code.ACCEPTED,
          owner.store().declare(Collections.singletonList(schema()), TIMEOUT).get(0).code());
      owner.store().record(Collections.singletonList(observation(900L, 1.0)));
      assertTrue(backend.recordEntered.await(5, TimeUnit.SECONDS));

      Path first = temporaryDirectory.resolve("ambiguous-first.bin");
      Path second = temporaryDirectory.resolve("ambiguous-second.bin");
      Future<?> firstSave = callers.submit(() -> {
        owner.save(first, TIMEOUT);
        return null;
      });
      Future<?> secondSave = callers.submit(() -> {
        owner.save(second, TIMEOUT);
        return null;
      });
      assertTrue(drainWaiter.awaitEntered.await(5, TimeUnit.SECONDS));

      assertFalse(owner.shutdown(Duration.ZERO));
      backend.releaseRecord.countDown();
      assertFalse(owner.shutdown(Duration.ofMillis(50)));
      assertFalse(backend.closeCalled.get());

      drainWaiter.release();
      assertTrue(operations.writesEntered.await(5, TimeUnit.SECONDS));
      assertTrue(owner.shutdown(TIMEOUT));

      operations.releaseWrites.countDown();
      firstSave.get(5, TimeUnit.SECONDS);
      secondSave.get(5, TimeUnit.SECONDS);
      LocalSnapshotState firstState =
          LocalSnapshotFiles.load(first, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT);
      LocalSnapshotState secondState =
          LocalSnapshotFiles.load(second, CATALOG, MAXIMUM_PLANNING_AGE, TIMEOUT);
      assertEquals(1, firstState.observations().size());
      assertEquals(1.0, firstState.observations().get(0).value());
      assertEquals(1, secondState.observations().size());
      assertEquals(1.0, secondState.observations().get(0).value());
      assertTrue(backend.closeCalled.get());
      assertEquals(1L, owner.testHandle().counters().value(
          LocalMetricCounter.BACKEND_AMBIGUOUS));
      assertEquals(1, owner.testHandle().observations().size());
    } finally {
      backend.releaseRecord.countDown();
      drainWaiter.release();
      operations.releaseWrites.countDown();
      owner.shutdown(TIMEOUT);
      callers.shutdownNow();
    }
  }

  @Test
  void oneEntryDeadlineCoversFileWorkAndEveryFailureReleasesReservation()
      throws Exception {
    ScriptedTicker ticker = new ScriptedTicker();
    ExpiringFileOperations operations = new ExpiringFileOperations(ticker);
    LocalHistoryMetrics owner = openForTest(ticker, operations);
    try {
      LocalSnapshotException timeout = assertThrows(LocalSnapshotException.class,
          () -> owner.save(temporaryDirectory.resolve("expired.bin"), Duration.ofNanos(5)));
      assertEquals(LocalSnapshotException.Reason.TIMEOUT, timeout.reason());

      owner.save(temporaryDirectory.resolve("retry.bin"), Duration.ofNanos(100));
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  private static LocalHistoryMetrics open() {
    return LocalHistoryMetricsFactory.open(
        CATALOG,
        Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
        () -> LocalProvenanceIdentity.of("app", null, "1.0"),
        MAXIMUM_PLANNING_AGE,
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)));
  }

  private static LocalHistoryMetrics openForTest(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations) {
    return openForTest(ticker, operations, LocalSnapshotFiles.systemGuardWaiter());
  }

  private static LocalHistoryMetrics openForTest(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotDiagnosticSink diagnosticSink) {
    return openForTest(
        ticker,
        operations,
        LocalSnapshotFiles.systemGuardWaiter(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
        diagnosticSink);
  }

  private static LocalHistoryMetrics openForTest(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotFiles.GuardWaiter guardWaiter) {
    return openForTest(
        ticker,
        operations,
        guardWaiter,
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
  }

  private static LocalHistoryMetrics openForTest(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotFiles.GuardWaiter guardWaiter,
      LocalAsyncRecordPipeline.DrainWaiter drainWaiter) {
    return openForTest(
        ticker,
        operations,
        guardWaiter,
        drainWaiter,
        LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink());
  }

  private static LocalHistoryMetrics openForTest(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotFiles.GuardWaiter guardWaiter,
      LocalAsyncRecordPipeline.DrainWaiter drainWaiter,
      LocalSnapshotDiagnosticSink diagnosticSink) {
    Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC);
    LocalHistoryMetricsBackend backend =
        LocalHistoryMetricsBackend.create(CATALOG, clock, MAXIMUM_PLANNING_AGE);
    return LocalHistoryMetricsFactory.openForTest(
        CATALOG,
        clock,
        () -> LocalProvenanceIdentity.of("app", null, "1.0"),
        MAXIMUM_PLANNING_AGE,
        LocalQueuePolicy.of(16, 8),
        LocalExecutionPolicy.of(1, 8),
        LocalCircuitBreakerPolicy.of(
            8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1)),
        backend,
        backend,
        Executors.newSingleThreadExecutor(
            LocalHistoryMetricsFactory.daemonFactory("snapshot-save-planning-test")),
        ticker,
        operations,
        guardWaiter,
        drainWaiter,
        diagnosticSink);
  }

  private static LocalMetricStorePlanningAdapter.Ticker systemTicker() {
    return new LocalMetricStorePlanningAdapter.Ticker() {
      @Override
      public long readNanos() {
        return System.nanoTime();
      }
    };
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

  private static final class BlockingRecordBackend
      implements HistoryMetricsBackend {
    private final LocalHistoryMetricsBackend delegate;
    private final CountDownLatch recordEntered = new CountDownLatch(1);
    private final CountDownLatch releaseRecord = new CountDownLatch(1);
    private final AtomicBoolean closeCalled = new AtomicBoolean();

    private BlockingRecordBackend(LocalHistoryMetricsBackend delegate) {
      this.delegate = delegate;
    }

    @Override
    public BackendInfo info() {
      return delegate.info();
    }

    @Override
    public List<SchemaStatus> declare(
        List<MetricSchema> schemas, Duration timeout) {
      return delegate.declare(schemas, timeout);
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      recordEntered.countDown();
      awaitUninterruptibly(releaseRecord);
      return delegate.record(observations);
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      return delegate.summarize(requests, timeout);
    }

    @Override
    public void close() {
      closeCalled.set(true);
      delegate.close();
    }
  }

  private static final class WatermarkSignallingDrainWaiter
      implements LocalAsyncRecordPipeline.DrainWaiter {
    private final CountDownLatch watermarkEntered = new CountDownLatch(1);

    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      watermarkEntered.countDown();
      return LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER.tryLock(
          lock, remainingNanos);
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      return LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER.await(
          condition, remainingNanos);
    }
  }

  private static final class WatermarkExpiringDrainWaiter
      implements LocalAsyncRecordPipeline.DrainWaiter {
    private final ScriptedTicker ticker;
    private final AtomicBoolean first = new AtomicBoolean(true);

    private WatermarkExpiringDrainWaiter(ScriptedTicker ticker) {
      this.ticker = ticker;
    }

    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      boolean acquired = LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER.tryLock(
          lock, remainingNanos);
      if (first.compareAndSet(true, false)) {
        ticker.advance(remainingNanos);
      }
      return acquired;
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      return LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER.await(
          condition, remainingNanos);
    }
  }

  private static final class BlockingSnapshotDrainWaiter
      implements LocalAsyncRecordPipeline.DrainWaiter {
    private final CountDownLatch awaitEntered;
    private final AtomicBoolean released = new AtomicBoolean();

    private BlockingSnapshotDrainWaiter(int expectedCaptures) {
      awaitEntered = new CountDownLatch(expectedCaptures);
    }

    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      return LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER.tryLock(
          lock, remainingNanos);
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      awaitEntered.countDown();
      long started = System.nanoTime();
      long remaining = remainingNanos;
      while (!released.get() && remaining > 0L) {
        condition.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
        long elapsed = System.nanoTime() - started;
        remaining = elapsed < 0L || elapsed >= remainingNanos
            ? 0L
            : remainingNanos - elapsed;
      }
      return remaining;
    }

    private void release() {
      released.set(true);
    }
  }

  private static class DelegatingFileOperations
      implements LocalSnapshotFiles.FileOperations {
    private final LocalSnapshotFiles.FileOperations delegate =
        LocalSnapshotFiles.systemFileOperations();

    @Override
    public Path normalize(Path path) {
      return delegate.normalize(path);
    }

    @Override
    public void requireDirectory(Path parent) throws IOException {
      delegate.requireDirectory(parent);
    }

    @Override
    public void requireRegularFile(Path path) throws IOException {
      delegate.requireRegularFile(path);
    }

    @Override
    public Path createTemp(Path parent, String prefix, String suffix)
        throws IOException {
      return delegate.createTemp(parent, prefix, suffix);
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      delegate.writeAndSync(path, image);
    }

    @Override
    public void copyAndSync(
        Path source,
        Path destination,
        long expectedLength,
        LocalSnapshotDeadline deadline) throws IOException, LocalSnapshotException {
      delegate.copyAndSync(source, destination, expectedLength, deadline);
    }

    @Override
    public long size(Path path) throws IOException {
      return delegate.size(path);
    }

    @Override
    public InputStream open(Path path) throws IOException {
      return delegate.open(path);
    }

    @Override
    public Path move(Path source, Path destination, CopyOption... options)
        throws IOException {
      return delegate.move(source, destination, options);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException {
      return delegate.deleteIfExists(path);
    }
  }

  private static final class DiagnosticCollector
      implements LocalSnapshotDiagnosticSink {
    private final List<String> messages = new ArrayList<String>();

    @Override
    public void snapshotCleanupFailed(String message) {
      messages.add(message);
    }
  }

  private static final class DeleteFaultFileOperations
      extends DelegatingFileOperations {
    private boolean failIntendedDelete;
    private final boolean failPriorDelete;
    private boolean failMove;
    private Path intendedTemporary;
    private Path priorTemporary;

    private DeleteFaultFileOperations(
        boolean failIntendedDelete, boolean failPriorDelete) {
      this.failIntendedDelete = failIntendedDelete;
      this.failPriorDelete = failPriorDelete;
    }

    @Override
    public Path createTemp(Path parent, String prefix, String suffix)
        throws IOException {
      Path path = super.createTemp(parent, prefix, suffix);
      if (".prior.tmp".equals(suffix)) {
        priorTemporary = path;
      } else {
        intendedTemporary = path;
      }
      return path;
    }

    @Override
    public Path move(Path source, Path destination, CopyOption... options)
        throws IOException {
      if (failMove) {
        throw new IOException("sensitive-move-detail");
      }
      Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
      return destination;
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException {
      if ((path.equals(intendedTemporary) && failIntendedDelete) ||
          (path.equals(priorTemporary) && failPriorDelete)) {
        throw new IOException("sensitive-delete-detail");
      }
      return super.deleteIfExists(path);
    }
  }

  private static final class BlockingNormalizationFileOperations
      extends DelegatingFileOperations {
    private final CountDownLatch normalizationEntered = new CountDownLatch(1);
    private final CountDownLatch releaseNormalization = new CountDownLatch(1);

    @Override
    public Path normalize(Path path) {
      normalizationEntered.countDown();
      awaitUninterruptibly(releaseNormalization);
      return super.normalize(path);
    }
  }

  private static final class RecordingNormalizationFileOperations
      extends DelegatingFileOperations {
    private final AtomicBoolean normalizationCalled = new AtomicBoolean();

    @Override
    public Path normalize(Path path) {
      normalizationCalled.set(true);
      return super.normalize(path);
    }
  }

  private static final class BlockingFileOperations
      extends DelegatingFileOperations {
    private final CountDownLatch writesEntered;
    private final CountDownLatch releaseWrites = new CountDownLatch(1);

    private BlockingFileOperations(int expectedWrites) {
      writesEntered = new CountDownLatch(expectedWrites);
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      writesEntered.countDown();
      awaitUninterruptibly(releaseWrites);
      super.writeAndSync(path, image);
    }
  }

  private static final class ExpiringFileOperations
      extends DelegatingFileOperations {
    private final ScriptedTicker ticker;
    private final AtomicBoolean first = new AtomicBoolean(true);

    private ExpiringFileOperations(ScriptedTicker ticker) {
      this.ticker = ticker;
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      super.writeAndSync(path, image);
      if (first.compareAndSet(true, false)) {
        ticker.advance(5L);
      }
    }
  }

  private static final class ScriptedTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong now = new AtomicLong();

    @Override
    public long readNanos() {
      return now.get();
    }

    private void advance(long nanos) {
      now.addAndGet(nanos);
    }
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
}
