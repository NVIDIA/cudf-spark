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

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsBackend;

/** Explicit construction boundary for standalone driver-local history metrics. */
public final class LocalHistoryMetricsFactory {
  // Implementation bounds for the driver-local provider. These are deliberately not part of the
  // consumer API; callers describe metric semantics and operation deadlines, not executor tuning.
  private static final int DEFAULT_RECORD_QUEUE_CAPACITY = 4096;
  private static final int DEFAULT_BACKEND_BATCH_SIZE = 64;
  private static final int DEFAULT_PLANNING_THREADS = 2;
  private static final int DEFAULT_PLANNING_QUEUE_CAPACITY = 64;
  private static final int DEFAULT_BREAKER_WINDOW_SIZE = 32;
  private static final int DEFAULT_BREAKER_MIN_SAMPLES = 8;
  private static final double DEFAULT_BREAKER_FAILURE_RATE = 0.5;
  private static final Duration DEFAULT_BREAKER_SLOW_CALL = Duration.ofMillis(100);
  private static final double DEFAULT_BREAKER_SLOW_RATE = 0.5;
  private static final Duration DEFAULT_BREAKER_OPEN_DURATION = Duration.ofSeconds(30);

  private static final LocalMetricStorePlanningAdapter.Ticker SYSTEM_TICKER =
      new LocalMetricStorePlanningAdapter.Ticker() {
        @Override
        public long readNanos() {
          return System.nanoTime();
        }
      };
  private static final SnapshotOpenHook NOOP_SNAPSHOT_OPEN_HOOK =
      new SnapshotOpenHook() {
        @Override
        public void beforePublication() {
        }
      };

  private LocalHistoryMetricsFactory() {
  }

  /**
   * Opens a fresh explicitly owned driver-local store with bounded implementation defaults.
   *
   * <p>This method does not install the store, read process configuration, access a network, or
   * touch a file. The embedding plugin owns configuration, installation, persistence, and lifecycle.
   * Metric producers should use the returned {@link LocalHistoryMetrics#store() store} and should
   * not tune the provider's queues, executors, or circuit breaker.
   *
   * @param catalog governed production catalog or isolated companion test catalog
   * @param driverClock clock used for provenance write time, retention, and future-time validation
   * @param provenanceSource caller-redacted identity source sampled once per record call
   * @param maximumPlanningAge nonnegative provider clamp on planning-visible history
   * @return a fresh standalone owner; never installed in {@code MetricStores}
   * @throws NullPointerException if an argument is null
   * @throws IllegalArgumentException if an argument violates its documented range or construction
   *     is binary-incompatible
   */
  public static LocalHistoryMetrics open(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge) {
    return open(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        defaultQueuePolicy(),
        defaultExecutionPolicy(),
        defaultCircuitBreakerPolicy());
  }

  /** Test seam for exercising implementation bounds. */
  static LocalHistoryMetrics open(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy) {
    validate(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy);
    LocalHistoryMetricsBackend backend =
        LocalHistoryMetricsBackend.create(catalog, driverClock, maximumPlanningAge);
    ExecutorService planningExecutor = planningExecutor(executionPolicy);
    return openOwned(
        catalog,
        maximumPlanningAge,
        driverClock,
        provenanceSource,
        queuePolicy,
        circuitBreakerPolicy,
        backend,
        backend,
        planningExecutor,
        SYSTEM_TICKER,
        LocalSnapshotFiles.systemFileOperations(),
        LocalSnapshotFiles.systemGuardWaiter(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
        LocalRecordDiagnostics.system(SYSTEM_TICKER),
        LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink(),
        new LocalHistoryMetricsImpl.LifecycleExecutor());
  }

  /**
   * Restores one explicitly owned driver-local store with bounded implementation defaults.
   *
   * <p>The relative nonnegative {@code timeout} is one monotonic budget from method entry through
   * final owner publication. Runtime queues, counters, circuit-breaker state, and lifecycle state
   * start fresh. The returned store is not installed.
   *
   * @param source explicit snapshot source path
   * @param catalog exact governed catalog expected in the image
   * @param clock newly supplied driver clock
   * @param provenanceSource newly supplied caller-redacted identity source
   * @param maximumPlanningAge nonnegative clamp for restored and future declarations
   * @param timeout relative nonnegative end-to-end budget
   * @return a fully validated and explicitly owned restored store
   * @throws NullPointerException if an argument is null
   * @throws IllegalArgumentException if {@code maximumPlanningAge} or {@code timeout} is negative
   * @throws LocalSnapshotException for a checked snapshot failure; branch on
   *     {@link LocalSnapshotException#reason()}
   */
  public static LocalHistoryMetrics openSnapshot(
      Path source,
      HistoryMetricCatalog catalog,
      Clock clock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      Duration timeout) throws LocalSnapshotException {
    return openSnapshot(
        source,
        catalog,
        clock,
        provenanceSource,
        maximumPlanningAge,
        defaultQueuePolicy(),
        defaultExecutionPolicy(),
        defaultCircuitBreakerPolicy(),
        timeout);
  }

  /** Test seam for exercising implementation bounds during restore. */
  static LocalHistoryMetrics openSnapshot(
      Path source,
      HistoryMetricCatalog catalog,
      Clock clock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy breakerPolicy,
      Duration timeout) throws LocalSnapshotException {
    return openSnapshotWithDependencies(
        source,
        catalog,
        clock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        breakerPolicy,
        LocalSnapshotDeadline.start(timeout, SYSTEM_TICKER),
        SYSTEM_TICKER,
        LocalSnapshotFiles.systemFileOperations(),
        LocalSnapshotFiles.systemGuardWaiter(),
        NOOP_SNAPSHOT_OPEN_HOOK);
  }

  static LocalHistoryMetrics openSnapshotForTest(
      Path source,
      HistoryMetricCatalog catalog,
      Clock clock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy breakerPolicy,
      Duration timeout,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotFiles.GuardWaiter waiter,
      SnapshotOpenHook hook) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline = LocalSnapshotDeadline.start(timeout, ticker);
    return openSnapshotWithDependencies(
        source,
        catalog,
        clock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        breakerPolicy,
        deadline,
        ticker,
        operations,
        waiter,
        hook);
  }

  private static LocalHistoryMetrics openSnapshotWithDependencies(
      Path source,
      HistoryMetricCatalog catalog,
      Clock clock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy breakerPolicy,
      LocalSnapshotDeadline deadline,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations operations,
      LocalSnapshotFiles.GuardWaiter waiter,
      SnapshotOpenHook hook) throws LocalSnapshotException {
    Objects.requireNonNull(source, "source");
    validate(
        catalog,
        clock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        breakerPolicy);
    Objects.requireNonNull(deadline, "deadline");
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(operations, "operations");
    Objects.requireNonNull(waiter, "waiter");
    Objects.requireNonNull(hook, "hook");

    return LocalSnapshotFiles.loadAndPublishWithDeadline(
        source,
        catalog,
        maximumPlanningAge,
        deadline,
        operations,
        waiter,
        new LocalSnapshotFiles.StatePublisher<LocalHistoryMetrics>() {
          @Override
          public LocalHistoryMetrics publish(
              LocalSnapshotState state, LocalSnapshotDeadline operationDeadline)
              throws LocalSnapshotException {
            return publishRestored(
                state,
                catalog,
                clock,
                provenanceSource,
                maximumPlanningAge,
                queuePolicy,
                executionPolicy,
                breakerPolicy,
                operationDeadline,
                ticker,
                operations,
                waiter,
                hook);
          }
        });
  }

  interface SnapshotOpenHook {
    void beforePublication();
  }

  static LocalHistoryMetrics openForTest(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    return openForTest(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy,
        backend,
        inspectionBackend,
        planningExecutor,
        ticker,
        LocalSnapshotFiles.systemFileOperations(),
        LocalSnapshotFiles.systemGuardWaiter(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER);
  }

  static LocalHistoryMetrics openForTest(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalHistoryMetricsImpl.LifecycleExecutor shutdownExecutor) {
    validate(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy);
    return openOwned(
        catalog,
        maximumPlanningAge,
        driverClock,
        provenanceSource,
        queuePolicy,
        circuitBreakerPolicy,
        Objects.requireNonNull(backend, "backend"),
        inspectionBackend,
        Objects.requireNonNull(planningExecutor, "planningExecutor"),
        Objects.requireNonNull(ticker, "ticker"),
        LocalSnapshotFiles.systemFileOperations(),
        LocalSnapshotFiles.systemGuardWaiter(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
        LocalRecordDiagnostics.system(ticker),
        LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink(),
        Objects.requireNonNull(shutdownExecutor, "shutdownExecutor"));
  }

  static LocalHistoryMetrics openForTest(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalHistoryMetricsImpl.LifecycleExecutor shutdownExecutor,
      LocalRecordDiagnosticSink recordDiagnosticSink) {
    validate(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy);
    return openOwned(
        catalog,
        maximumPlanningAge,
        driverClock,
        provenanceSource,
        queuePolicy,
        circuitBreakerPolicy,
        Objects.requireNonNull(backend, "backend"),
        inspectionBackend,
        Objects.requireNonNull(planningExecutor, "planningExecutor"),
        Objects.requireNonNull(ticker, "ticker"),
        LocalSnapshotFiles.systemFileOperations(),
        LocalSnapshotFiles.systemGuardWaiter(),
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
        new LocalRecordDiagnostics(ticker,
            Objects.requireNonNull(recordDiagnosticSink, "recordDiagnosticSink")),
        LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink(),
        Objects.requireNonNull(shutdownExecutor, "shutdownExecutor"));
  }

  static LocalHistoryMetrics openForTest(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      LocalAsyncRecordPipeline.DrainWaiter recordDrainWaiter) {
    return openForTest(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy,
        backend,
        inspectionBackend,
        planningExecutor,
        ticker,
        snapshotFileOperations,
        snapshotGuardWaiter,
        recordDrainWaiter,
        LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink());
  }

  static LocalHistoryMetrics openForTest(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      LocalAsyncRecordPipeline.DrainWaiter recordDrainWaiter,
      LocalSnapshotDiagnosticSink snapshotDiagnosticSink) {
    validate(
        catalog,
        driverClock,
        provenanceSource,
        maximumPlanningAge,
        queuePolicy,
        executionPolicy,
        circuitBreakerPolicy);
    Objects.requireNonNull(backend, "backend");
    Objects.requireNonNull(planningExecutor, "planningExecutor");
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(snapshotFileOperations, "snapshotFileOperations");
    Objects.requireNonNull(snapshotGuardWaiter, "snapshotGuardWaiter");
    Objects.requireNonNull(recordDrainWaiter, "recordDrainWaiter");
    Objects.requireNonNull(snapshotDiagnosticSink, "snapshotDiagnosticSink");
    return openOwned(
        catalog,
        maximumPlanningAge,
        driverClock,
        provenanceSource,
        queuePolicy,
        circuitBreakerPolicy,
        backend,
        inspectionBackend,
        planningExecutor,
        ticker,
        snapshotFileOperations,
        snapshotGuardWaiter,
        recordDrainWaiter,
        LocalRecordDiagnostics.system(ticker),
        snapshotDiagnosticSink,
        new LocalHistoryMetricsImpl.LifecycleExecutor());
  }

  private static LocalHistoryMetrics openOwned(
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      LocalQueuePolicy queuePolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      ExecutorService planningExecutor,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      LocalAsyncRecordPipeline.DrainWaiter recordDrainWaiter,
      LocalRecordDiagnostics recordDiagnostics,
      LocalSnapshotDiagnosticSink snapshotDiagnosticSink,
      LocalHistoryMetricsImpl.LifecycleExecutor shutdownExecutor) {
    LocalMetricStorePlanningAdapter adapter = null;
    try {
      adapter = LocalMetricStorePlanningAdapter.createWithRecording(
          backend,
          catalog,
          planningExecutor,
          true,
          ticker,
          circuitBreakerPolicy,
          driverClock,
          provenanceSource,
          new LocalAsyncRecordPipeline.QueuePolicy(
              queuePolicy.capacityObservations(), queuePolicy.maxBackendBatchSize()),
          recordDrainWaiter,
          Collections.<LocalDeclarationSnapshot>emptyList(),
          Objects.requireNonNull(recordDiagnostics, "recordDiagnostics"));
      return new LocalHistoryMetricsImpl(
          catalog,
          maximumPlanningAge,
          backend,
          inspectionBackend,
          adapter,
          ticker,
          snapshotFileOperations,
          snapshotGuardWaiter,
          snapshotDiagnosticSink,
          shutdownExecutor);
    } catch (RuntimeException failure) {
      cleanupFailure(adapter, backend, planningExecutor);
      throw failure;
    } catch (LinkageError failure) {
      cleanupFailure(adapter, backend, planningExecutor);
      throw new IllegalArgumentException(
          "history metrics local construction is binary-incompatible", failure);
    }
  }

  private static LocalHistoryMetrics publishRestored(
      LocalSnapshotState state,
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy,
      LocalSnapshotDeadline deadline,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      SnapshotOpenHook hook) throws LocalSnapshotException {
    LocalHistoryMetricsBackend backend = null;
    ExecutorService planningExecutor = null;
    LocalMetricStorePlanningAdapter adapter = null;
    try {
      deadline.throwIfExpired();
      backend =
          LocalHistoryMetricsBackend.restore(
              catalog, driverClock, maximumPlanningAge, state);
      if (!state.equals(backend.captureSnapshotState())) {
        throw new IllegalArgumentException(
            "restored backend state differs from the validated snapshot");
      }
      deadline.throwIfExpired();

      planningExecutor = planningExecutor(executionPolicy);
      adapter = LocalMetricStorePlanningAdapter.createWithRecording(
          backend,
          catalog,
          planningExecutor,
          true,
          ticker,
          circuitBreakerPolicy,
          driverClock,
          provenanceSource,
          new LocalAsyncRecordPipeline.QueuePolicy(
              queuePolicy.capacityObservations(), queuePolicy.maxBackendBatchSize()),
          LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
          state.declarations());
      deadline.throwIfExpired();

      LocalHistoryMetrics owner = new LocalHistoryMetricsImpl(
          catalog,
          maximumPlanningAge,
          backend,
          backend,
          adapter,
          ticker,
          snapshotFileOperations,
          snapshotGuardWaiter,
          LocalHistoryMetricsImpl.systemSnapshotDiagnosticSink());
      hook.beforePublication();
      deadline.throwIfExpired();
      return owner;
    } catch (LocalSnapshotException failure) {
      cleanupFailure(adapter, backend, planningExecutor, failure);
      throw failure;
    } catch (RuntimeException failure) {
      LocalSnapshotException mapped = snapshotConstructionFailure();
      cleanupFailure(adapter, backend, planningExecutor, mapped);
      throw mapped;
    } catch (LinkageError failure) {
      LocalSnapshotException mapped = snapshotConstructionFailure();
      cleanupFailure(adapter, backend, planningExecutor, mapped);
      throw mapped;
    }
  }

  private static LocalSnapshotException snapshotConstructionFailure() {
    return new LocalSnapshotException(
        LocalSnapshotException.Reason.IO,
        "local snapshot owner construction failed",
        new IllegalStateException("local snapshot owner construction failed"));
  }

  private static LocalQueuePolicy defaultQueuePolicy() {
    return LocalQueuePolicy.of(DEFAULT_RECORD_QUEUE_CAPACITY, DEFAULT_BACKEND_BATCH_SIZE);
  }

  private static LocalExecutionPolicy defaultExecutionPolicy() {
    return LocalExecutionPolicy.of(DEFAULT_PLANNING_THREADS, DEFAULT_PLANNING_QUEUE_CAPACITY);
  }

  private static LocalCircuitBreakerPolicy defaultCircuitBreakerPolicy() {
    return LocalCircuitBreakerPolicy.of(
        DEFAULT_BREAKER_WINDOW_SIZE,
        DEFAULT_BREAKER_MIN_SAMPLES,
        DEFAULT_BREAKER_FAILURE_RATE,
        DEFAULT_BREAKER_SLOW_CALL,
        DEFAULT_BREAKER_SLOW_RATE,
        DEFAULT_BREAKER_OPEN_DURATION);
  }

  private static void validate(
      HistoryMetricCatalog catalog,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      Duration maximumPlanningAge,
      LocalQueuePolicy queuePolicy,
      LocalExecutionPolicy executionPolicy,
      LocalCircuitBreakerPolicy circuitBreakerPolicy) {
    Objects.requireNonNull(catalog, "catalog");
    Objects.requireNonNull(driverClock, "driverClock");
    Objects.requireNonNull(provenanceSource, "provenanceSource");
    Objects.requireNonNull(maximumPlanningAge, "maximumPlanningAge");
    Objects.requireNonNull(queuePolicy, "queuePolicy");
    Objects.requireNonNull(executionPolicy, "executionPolicy");
    Objects.requireNonNull(circuitBreakerPolicy, "circuitBreakerPolicy");
    if (maximumPlanningAge.isNegative()) {
      throw new IllegalArgumentException("maximumPlanningAge must not be negative");
    }
  }

  private static ExecutorService planningExecutor(LocalExecutionPolicy policy) {
    return new ThreadPoolExecutor(
        policy.planningThreads(),
        policy.planningThreads(),
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(policy.planningQueueCapacity()),
        daemonFactory("history-metrics-local-planning"),
        new ThreadPoolExecutor.AbortPolicy());
  }

  static ThreadFactory daemonFactory(final String prefix) {
    final AtomicInteger sequence = new AtomicInteger();
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable command) {
        Thread thread =
            new Thread(command, prefix + "-" + sequence.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  private static void cleanupFailure(
      LocalMetricStorePlanningAdapter adapter,
      HistoryMetricsBackend backend,
      ExecutorService planningExecutor) {
    cleanupFailure(adapter, backend, planningExecutor, null);
  }

  private static void cleanupFailure(
      LocalMetricStorePlanningAdapter adapter,
      HistoryMetricsBackend backend,
      ExecutorService planningExecutor,
      LocalSnapshotException primary) {
    boolean cleanupFailed = false;
    if (adapter != null) {
      try {
        adapter.stopPlanning();
      } catch (RuntimeException | LinkageError failure) {
        cleanupFailed = true;
      }
      try {
        adapter.stopRecording(Duration.ofNanos(Long.MAX_VALUE));
        adapter.awaitWriterTermination();
        adapter.awaitPlanningTermination(Duration.ofNanos(Long.MAX_VALUE));
      } catch (RuntimeException | LinkageError failure) {
        cleanupFailed = true;
      }
    } else if (planningExecutor != null) {
      try {
        planningExecutor.shutdownNow();
      } catch (RuntimeException | LinkageError failure) {
        cleanupFailed = true;
      }
    }
    if (backend != null) {
      try {
        backend.close();
      } catch (RuntimeException | LinkageError failure) {
        cleanupFailed = true;
      }
    }
    if (primary != null && cleanupFailed) {
      primary.addSuppressed(
          new IllegalStateException("local snapshot owner cleanup failed"));
    }
  }
}
