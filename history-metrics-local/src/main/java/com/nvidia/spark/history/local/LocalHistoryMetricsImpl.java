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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.Status;

/** Package-private owner implementation behind the explicit public companion factory. */
final class LocalHistoryMetricsImpl implements LocalHistoryMetrics {
  static final String SNAPSHOT_CLEANUP_DIAGNOSTIC =
      "local history metrics snapshot cleanup failed after commit";

  private static final Logger LOGGER =
      Logger.getLogger(LocalHistoryMetricsImpl.class.getName());
  private static final LocalSnapshotDiagnosticSink SYSTEM_SNAPSHOT_DIAGNOSTIC_SINK =
      new LocalSnapshotDiagnosticSink() {
        @Override
        public void snapshotCleanupFailed(String message) {
          LOGGER.warning(message);
        }
      };

  private enum Lifecycle {
    RUNNING,
    STARTING,
    STOPPING,
    COMPLETE
  }

  private final HistoryMetricCatalog catalog;
  private final Duration maximumPlanningAge;
  private final HistoryMetricsBackend backend;
  private final LocalHistoryMetricsBackend inspectionBackend;
  private final LocalMetricStorePlanningAdapter adapter;
  private final LocalMetricStorePlanningAdapter.Ticker ticker;
  private final LocalSnapshotFiles.FileOperations snapshotFileOperations;
  private final LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter;
  private final LocalSnapshotDiagnosticSink snapshotDiagnosticSink;
  private LifecycleExecutor shutdownExecutor;
  private final List<LifecycleExecutor> ownedShutdownExecutors =
      new ArrayList<LifecycleExecutor>();
  private final List<LifecycleExecutor> terminationRequestedExecutors =
      new ArrayList<LifecycleExecutor>();
  private final Object lifecycleLock = new Object();
  private final LocalHistoryMetricsTestHandle testHandle = new TestHandle();

  private Lifecycle lifecycle = Lifecycle.RUNNING;
  private long shutdownWatermark;
  private long shutdownStartedNanos;
  private long shutdownTimeoutNanos;
  private long shutdownTimeouts;
  private long shutdownComplete;
  private long snapshotCleanupFailures;
  private int activeSnapshotCaptures;
  private boolean cleanupScheduled;
  private boolean terminationRetryScheduled;
  private boolean cleanupPrerequisitesComplete;
  private boolean backendCloseAttempted;
  private boolean backendCloseSucceeded;

  LocalHistoryMetricsImpl(
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      LocalMetricStorePlanningAdapter adapter,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      LocalSnapshotDiagnosticSink snapshotDiagnosticSink) {
    this(
        catalog,
        maximumPlanningAge,
        backend,
        inspectionBackend,
        adapter,
        ticker,
        snapshotFileOperations,
        snapshotGuardWaiter,
        snapshotDiagnosticSink,
        new LifecycleExecutor());
  }

  LocalHistoryMetricsImpl(
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      HistoryMetricsBackend backend,
      LocalHistoryMetricsBackend inspectionBackend,
      LocalMetricStorePlanningAdapter adapter,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalSnapshotFiles.FileOperations snapshotFileOperations,
      LocalSnapshotFiles.GuardWaiter snapshotGuardWaiter,
      LocalSnapshotDiagnosticSink snapshotDiagnosticSink,
      LifecycleExecutor shutdownExecutor) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.maximumPlanningAge =
        Objects.requireNonNull(maximumPlanningAge, "maximumPlanningAge");
    this.backend = Objects.requireNonNull(backend, "backend");
    this.inspectionBackend = inspectionBackend;
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.snapshotFileOperations =
        Objects.requireNonNull(snapshotFileOperations, "snapshotFileOperations");
    this.snapshotGuardWaiter =
        Objects.requireNonNull(snapshotGuardWaiter, "snapshotGuardWaiter");
    this.snapshotDiagnosticSink =
        Objects.requireNonNull(snapshotDiagnosticSink, "snapshotDiagnosticSink");
    installShutdownExecutor(
        Objects.requireNonNull(shutdownExecutor, "shutdownExecutor"));
  }

  private void installShutdownExecutor(final LifecycleExecutor executor) {
    executor.onTerminated = new Runnable() {
      @Override
      public void run() {
        shutdownExecutorTerminated(executor);
      }
    };
    synchronized (lifecycleLock) {
      shutdownExecutor = executor;
      ownedShutdownExecutors.add(executor);
    }
    if (executor.isOwnedTerminated()) {
      shutdownExecutorTerminated(executor);
    }
  }

  @Override
  public MetricStore store() {
    return adapter;
  }

  @Override
  public LocalHistoryMetricsTestHandle testHandle() {
    return testHandle;
  }

  @Override
  public void save(Path target, Duration timeout) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline = LocalSnapshotDeadline.start(timeout, ticker);
    Objects.requireNonNull(target, "target");

    long watermark;
    synchronized (lifecycleLock) {
      if (lifecycle != Lifecycle.RUNNING) {
        throw new IllegalStateException("local history metrics owner is stopped");
      }
      activeSnapshotCaptures++;
      boolean watermarkSelected = false;
      try {
        watermark = adapter.captureRecordingWatermark(deadline);
        watermarkSelected = true;
      } finally {
        if (!watermarkSelected) {
          activeSnapshotCaptures--;
          lifecycleLock.notifyAll();
        }
      }
    }

    final SnapshotCapture capture = new SnapshotCapture(watermark, deadline);
    try {
      LocalSnapshotFiles.SaveResult result =
          LocalSnapshotFiles.saveCapturedWithDeadline(
              target,
              catalog,
              maximumPlanningAge,
              deadline,
              snapshotFileOperations,
              snapshotGuardWaiter,
              new LocalSnapshotFiles.SnapshotStateCapture() {
                @Override
                public LocalSnapshotState capture() throws LocalSnapshotException {
                  return capture.captureState();
                }
              });
      if (result.cleanupFailed()) {
        recordSnapshotCleanupFailure();
      }
    } finally {
      capture.release();
    }
  }

  static LocalSnapshotDiagnosticSink systemSnapshotDiagnosticSink() {
    return SYSTEM_SNAPSHOT_DIAGNOSTIC_SINK;
  }

  private void recordSnapshotCleanupFailure() {
    synchronized (lifecycleLock) {
      snapshotCleanupFailures = incrementSaturated(snapshotCleanupFailures);
    }
    try {
      snapshotDiagnosticSink.snapshotCleanupFailed(SNAPSHOT_CLEANUP_DIAGNOSTIC);
    } catch (RuntimeException ignored) {
      // Diagnostics cannot turn an externally committed snapshot into a failed save.
    } catch (LinkageError ignored) {
      // Diagnostic linkage failure cannot replace successful snapshot publication.
    }
  }

  private final class SnapshotCapture {
    private final long watermark;
    private final LocalSnapshotDeadline deadline;
    private boolean active = true;

    private SnapshotCapture(long watermark, LocalSnapshotDeadline deadline) {
      this.watermark = watermark;
      this.deadline = deadline;
    }

    private LocalSnapshotState captureState() throws LocalSnapshotException {
      adapter.awaitRecordingWatermark(watermark, deadline);
      if (inspectionBackend == null) {
        throw new LocalSnapshotException(
            LocalSnapshotException.Reason.IO,
            "local snapshot state is unavailable");
      }
      LocalSnapshotState state = inspectionBackend.captureSnapshotState();
      release();
      deadline.throwIfExpired();
      return state;
    }

    private void release() {
      synchronized (lifecycleLock) {
        if (active) {
          active = false;
          activeSnapshotCaptures--;
          lifecycleLock.notifyAll();
        }
      }
    }
  }

  @Override
  public boolean drain(Duration timeout) {
    return adapter.drain(timeout);
  }

  @Override
  public boolean shutdown(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long callerStarted = ticker.readNanos();
    long callerTimeout = durationToNanosSaturated(timeout);
    boolean initiate = false;
    boolean scheduleCleanup = false;
    boolean scheduleTerminationRetry = false;
    LifecycleExecutor cleanupTarget = null;
    synchronized (lifecycleLock) {
      if (lifecycle == Lifecycle.RUNNING) {
        shutdownStartedNanos = callerStarted;
        shutdownTimeoutNanos = callerTimeout;
        adapter.stopPlanningAdmission();
        shutdownWatermark = adapter.beginRecordingShutdown();
        lifecycle = Lifecycle.STARTING;
        cleanupScheduled = true;
        cleanupTarget = shutdownExecutor;
        scheduleCleanup = true;
        initiate = true;
      } else if (lifecycle == Lifecycle.STOPPING) {
        if (cleanupPrerequisitesComplete &&
            hasUnrequestedTerminationLocked() && !terminationRetryScheduled) {
          terminationRetryScheduled = true;
          scheduleTerminationRetry = true;
        } else if (!cleanupPrerequisitesComplete && !cleanupScheduled) {
          cleanupScheduled = true;
          cleanupTarget = shutdownExecutor;
          scheduleCleanup = true;
        }
      }
    }
    Error schedulingError = null;
    if (scheduleCleanup) {
      schedulingError = scheduleCleanup(cleanupTarget);
      synchronized (lifecycleLock) {
        if (lifecycle == Lifecycle.STARTING) {
          lifecycle = Lifecycle.STOPPING;
          lifecycleLock.notifyAll();
        }
      }
    }
    if (scheduleTerminationRetry) {
      scheduleTerminationRetry();
    }
    if (initiate) {
      adapter.finishRecordingShutdown(
          shutdownWatermark, shutdownStartedNanos, shutdownTimeoutNanos);
    }
    if (schedulingError != null) {
      throw schedulingError;
    }
    boolean complete = awaitComplete(callerStarted, callerTimeout);
    if (!complete) {
      adapter.reserveInFlightRecordAmbiguousForShutdown();
      synchronized (lifecycleLock) {
        shutdownTimeouts = incrementSaturated(shutdownTimeouts);
      }
    }
    return complete;
  }

  private Error scheduleCleanup(LifecycleExecutor initialTarget) {
    final Runnable cleanupTask = new Runnable() {
      @Override
      public void run() {
        cleanupAfterStart();
      }
    };
    LifecycleExecutor target = initialTarget;
    try {
      if (!tryScheduleCleanup(target, cleanupTask)) {
        target = replaceRejectedShutdownExecutor(target.replacement());
        if (!tryScheduleCleanup(target, cleanupTask)) {
          target = replaceRejectedShutdownExecutor(new LifecycleExecutor());
          target.executeOwned(cleanupTask);
        }
      }
      return null;
    } catch (Error failure) {
      target = replaceRejectedShutdownExecutor(new LifecycleExecutor());
      target.executeOwned(cleanupTask);
      return failure;
    }
  }

  private boolean tryScheduleCleanup(LifecycleExecutor target, Runnable cleanupTask) {
    try {
      target.execute(cleanupTask);
      return true;
    } catch (RuntimeException failure) {
      return false;
    } catch (LinkageError failure) {
      return false;
    }
  }

  private LifecycleExecutor replaceRejectedShutdownExecutor(
      LifecycleExecutor replacement) {
    installShutdownExecutor(replacement);
    return replacement;
  }

  private void cleanupAfterStart() {
    boolean interrupted = false;
    synchronized (lifecycleLock) {
      while (lifecycle == Lifecycle.STARTING) {
        try {
          lifecycleLock.wait();
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    cleanupIfStopping();
  }

  private void cleanupIfStopping() {
    boolean succeeded = false;
    try {
      synchronized (lifecycleLock) {
        if (lifecycle != Lifecycle.STOPPING) {
          return;
        }
      }
      requestSupersededCleanupExecutorTermination();
      succeeded = cleanup();
    } catch (RuntimeException ignored) {
      // Ordinary cleanup failures remain retryable from a later bounded shutdown call.
    } catch (LinkageError ignored) {
      // Binary-incompatible cleanup remains retryable without publishing false completion.
    } finally {
      synchronized (lifecycleLock) {
        cleanupScheduled = false;
        if (succeeded) {
          cleanupPrerequisitesComplete = true;
        }
        lifecycleLock.notifyAll();
      }
      if (succeeded) {
        continueOwnedExecutorTermination(true, false);
      }
    }
  }

  private void requestSupersededCleanupExecutorTermination() {
    final List<LifecycleExecutor> superseded;
    synchronized (lifecycleLock) {
      superseded = new ArrayList<LifecycleExecutor>(ownedShutdownExecutors);
      superseded.remove(shutdownExecutor);
    }
    for (LifecycleExecutor executor : superseded) {
      requestCleanupExecutorTermination(executor);
    }
  }

  private void continueOwnedExecutorTermination(
      boolean scheduleFollowUp, boolean coordinatorTask) {
    requestSupersededCleanupExecutorTermination();
    final LifecycleExecutor current;
    synchronized (lifecycleLock) {
      current = ownedShutdownExecutors.size() == 1 &&
          ownedShutdownExecutors.contains(shutdownExecutor) &&
          (coordinatorTask || !terminationRetryScheduled) ? shutdownExecutor : null;
    }
    if (current != null && !requestCleanupExecutorTermination(current) && scheduleFollowUp) {
      boolean scheduleRetry = false;
      synchronized (lifecycleLock) {
        if (lifecycle == Lifecycle.STOPPING && cleanupPrerequisitesComplete &&
            ownedShutdownExecutors.contains(current) && !terminationRetryScheduled) {
          terminationRetryScheduled = true;
          scheduleRetry = true;
        }
      }
      if (scheduleRetry) {
        scheduleTerminationRetry();
      }
    }
  }

  private void scheduleTerminationRetry() {
    final LifecycleExecutor target;
    synchronized (lifecycleLock) {
      target = shutdownExecutor;
    }
    try {
      target.executeOwned(new Runnable() {
        @Override
        public void run() {
          try {
            continueOwnedExecutorTermination(false, true);
          } finally {
            synchronized (lifecycleLock) {
              terminationRetryScheduled = false;
              lifecycleLock.notifyAll();
            }
          }
        }
      });
    } catch (RuntimeException ignored) {
      clearTerminationRetryScheduled();
    } catch (LinkageError ignored) {
      clearTerminationRetryScheduled();
    }
  }

  private void clearTerminationRetryScheduled() {
    synchronized (lifecycleLock) {
      terminationRetryScheduled = false;
      lifecycleLock.notifyAll();
    }
  }

  private boolean requestCleanupExecutorTermination(LifecycleExecutor executor) {
    synchronized (lifecycleLock) {
      if (ownedShutdownExecutors.contains(executor) &&
          !terminationRequestedExecutors.contains(executor)) {
        terminationRequestedExecutors.add(executor);
      }
    }
    try {
      executor.shutdown();
      return true;
    } catch (RuntimeException ignored) {
      clearTerminationRequested(executor);
      return false;
    } catch (LinkageError ignored) {
      clearTerminationRequested(executor);
      return false;
    }
  }

  private void clearTerminationRequested(LifecycleExecutor executor) {
    synchronized (lifecycleLock) {
      terminationRequestedExecutors.remove(executor);
      lifecycleLock.notifyAll();
    }
  }

  private boolean hasUnrequestedTerminationLocked() {
    for (LifecycleExecutor executor : ownedShutdownExecutors) {
      if (!terminationRequestedExecutors.contains(executor)) {
        return true;
      }
    }
    return false;
  }

  private boolean cleanup() {
    if (!adapter.finishPlanningStop()) {
      return false;
    }
    adapter.emitRecordingShutdownAmbiguityAfterFinalized();
    adapter.awaitWriterTermination();
    while (!adapter.awaitPlanningTermination(Duration.ofNanos(Long.MAX_VALUE))) {
      if (Thread.currentThread().isInterrupted()) {
        Thread.interrupted();
      }
    }
    awaitSnapshotCaptures();
    synchronized (lifecycleLock) {
      if (backendCloseAttempted) {
        return backendCloseSucceeded;
      }
      backendCloseAttempted = true;
    }
    try {
      backend.close();
    } catch (RuntimeException ignored) {
      return false;
    } catch (LinkageError ignored) {
      return false;
    }
    synchronized (lifecycleLock) {
      backendCloseSucceeded = true;
    }
    return true;
  }

  private void awaitSnapshotCaptures() {
    boolean interrupted = false;
    synchronized (lifecycleLock) {
      while (activeSnapshotCaptures != 0) {
        try {
          lifecycleLock.wait();
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void shutdownExecutorTerminated(LifecycleExecutor executor) {
    boolean scheduleRetry = false;
    synchronized (lifecycleLock) {
      ownedShutdownExecutors.remove(executor);
      terminationRequestedExecutors.remove(executor);
      if (lifecycle == Lifecycle.STOPPING && cleanupPrerequisitesComplete) {
        if (ownedShutdownExecutors.isEmpty()) {
          lifecycle = Lifecycle.COMPLETE;
          shutdownComplete = incrementSaturated(shutdownComplete);
        } else if (ownedShutdownExecutors.size() == 1 &&
            ownedShutdownExecutors.contains(shutdownExecutor) &&
            !terminationRequestedExecutors.contains(shutdownExecutor) &&
            !terminationRetryScheduled) {
          terminationRetryScheduled = true;
          scheduleRetry = true;
        }
      }
      lifecycleLock.notifyAll();
    }
    if (scheduleRetry) {
      scheduleTerminationRetry();
    }
  }

  private boolean awaitComplete(long startedNanos, long timeoutNanos) {
    synchronized (lifecycleLock) {
      if (lifecycle == Lifecycle.COMPLETE) {
        return true;
      }
      if (timeoutNanos == 0L) {
        return false;
      }
      while (lifecycle != Lifecycle.COMPLETE) {
        long remaining = remainingNanos(startedNanos, timeoutNanos);
        if (remaining == 0L) {
          return false;
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
        try {
          lifecycleLock.wait(millis, nanos);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return remainingNanos(startedNanos, timeoutNanos) > 0L;
    }
  }

  private long remainingNanos(long startedNanos, long timeoutNanos) {
    long elapsed = ticker.readNanos() - startedNanos;
    if (elapsed < 0L) {
      return 0L;
    }
    long remaining = timeoutNanos - elapsed;
    return remaining <= 0L ? 0L : remaining;
  }

  private LocalHistoryMetricsCounters counters() {
    EnumMap<LocalMetricCounter, Long> values =
        new EnumMap<LocalMetricCounter, Long>(LocalMetricCounter.class);
    for (LocalMetricCounter counter : LocalMetricCounter.values()) {
      values.put(counter, 0L);
    }

    LocalMetricStorePlanningAdapter.PlanningCounterSnapshot planning = adapter.counters();
    put(values, LocalMetricCounter.DECLARATION_BATCH, planning.declareCallCount());
    put(values, LocalMetricCounter.SUMMARY_BATCH, planning.summaryCallCount());
    put(values, LocalMetricCounter.SUMMARY_WINDOW_CLIPPED,
        planning.summaryWindowClippedCount());
    put(values, LocalMetricCounter.SUMMARY_ROWS, planning.summaryRowCount());
    put(values, LocalMetricCounter.DECLARATION_STATUS_ACCEPTED,
        planning.declarationOutcomeCount(SchemaStatus.Code.ACCEPTED));
    put(values, LocalMetricCounter.DECLARATION_STATUS_INCOMPATIBLE,
        planning.declarationOutcomeCount(SchemaStatus.Code.INCOMPATIBLE));
    put(values, LocalMetricCounter.DECLARATION_STATUS_INVALID_REQUEST,
        planning.declarationOutcomeCount(SchemaStatus.Code.INVALID_REQUEST));
    put(values, LocalMetricCounter.DECLARATION_STATUS_UNAVAILABLE,
        planning.declarationOutcomeCount(SchemaStatus.Code.UNAVAILABLE));
    put(values, LocalMetricCounter.DECLARATION_STATUS_DENIED,
        planning.declarationOutcomeCount(SchemaStatus.Code.DENIED));
    put(values, LocalMetricCounter.SUMMARY_STATUS_OK,
        planning.summaryOutcomeCount(Status.Code.OK));
    put(values, LocalMetricCounter.SUMMARY_STATUS_NOT_DECLARED,
        planning.summaryOutcomeCount(Status.Code.NOT_DECLARED));
    put(values, LocalMetricCounter.SUMMARY_STATUS_INVALID_REQUEST,
        planning.summaryOutcomeCount(Status.Code.INVALID_REQUEST));
    put(values, LocalMetricCounter.SUMMARY_STATUS_DEADLINE_EXCEEDED,
        planning.summaryOutcomeCount(Status.Code.DEADLINE_EXCEEDED));
    put(values, LocalMetricCounter.SUMMARY_STATUS_UNAVAILABLE,
        planning.summaryOutcomeCount(Status.Code.UNAVAILABLE));
    put(values, LocalMetricCounter.SUMMARY_STATUS_DENIED,
        planning.summaryOutcomeCount(Status.Code.DENIED));

    LocalAsyncRecordPipeline.RecordCounterSnapshot recording = adapter.recordCounters();
    put(values, LocalMetricCounter.RECORD_INVALID,
        saturatedAdd(recording.invalidCallCount(), recording.invalidItemCount()));
    put(values, LocalMetricCounter.RECORD_NOT_DECLARED, recording.undeclaredItemCount());
    put(values, LocalMetricCounter.RECORD_FUTURE_TIMESTAMP, recording.futureItemCount());
    put(values, LocalMetricCounter.RECORD_CLOCK_FAILURE, recording.clockFailureItemCount());
    put(values, LocalMetricCounter.RECORD_PROVENANCE_FAILURE,
        recording.provenanceFailureItemCount());
    put(values, LocalMetricCounter.RECORD_OVERFLOW, recording.overflowItemCount());
    put(values, LocalMetricCounter.RECORD_POST_STOP, recording.stoppedItemCount());
    put(values, LocalMetricCounter.RECORD_ENQUEUED, recording.enqueuedItemCount());
    put(values, LocalMetricCounter.BACKEND_ACCEPTED, recording.backendAcceptedItemCount());
    put(values, LocalMetricCounter.BACKEND_REJECTED, recording.backendRejectedItemCount());
    put(values, LocalMetricCounter.BACKEND_AMBIGUOUS, recording.backendAmbiguousItemCount());
    put(values, LocalMetricCounter.QUEUE_CURRENT, recording.queueCurrent());
    put(values, LocalMetricCounter.QUEUE_HIGH_WATER, recording.queueHighWater());
    put(values, LocalMetricCounter.DRAIN_SUCCESS, recording.drainSuccessCount());
    put(values, LocalMetricCounter.DRAIN_TIMEOUT, recording.drainTimeoutCount());

    LocalHistoryMetricsCounters breaker = adapter.breakerCounters();
    for (LocalMetricCounter counter : LocalMetricCounter.values()) {
      if (counter.name().startsWith("BREAKER_")) {
        put(values, counter, breaker.value(counter));
      }
    }
    synchronized (lifecycleLock) {
      put(values, LocalMetricCounter.SHUTDOWN_DROPPED,
          adapter.shutdownDroppedItemCount());
      put(values, LocalMetricCounter.SHUTDOWN_TIMEOUT, shutdownTimeouts);
      put(values, LocalMetricCounter.SHUTDOWN_COMPLETE, shutdownComplete);
      put(values, LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE, snapshotCleanupFailures);
    }
    return new ImmutableLocalHistoryMetricsCounters(values);
  }

  private static void put(
      EnumMap<LocalMetricCounter, Long> values,
      LocalMetricCounter counter,
      long value) {
    values.put(counter, value);
  }

  private static long saturatedAdd(long left, long right) {
    return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
  }

  private static long durationToNanosSaturated(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  private static long incrementSaturated(long value) {
    return value == Long.MAX_VALUE ? value : value + 1L;
  }

  private final class TestHandle implements LocalHistoryMetricsTestHandle {
    @Override
    public List<LocalObservationSnapshot> observations() {
      return inspectionBackend == null
          ? Collections.<LocalObservationSnapshot>emptyList()
          : inspectionBackend.observationSnapshots();
    }

    @Override
    public List<LocalDeclarationSnapshot> declarations() {
      return inspectionBackend == null
          ? Collections.<LocalDeclarationSnapshot>emptyList()
          : inspectionBackend.declarationSnapshots();
    }

    @Override
    public LocalHistoryMetricsCounters counters() {
      return LocalHistoryMetricsImpl.this.counters();
    }

    @Override
    public LocalCircuitBreakerState breakerState() {
      return adapter.breakerState();
    }
  }

  static class LifecycleExecutor extends ThreadPoolExecutor {
    private volatile Runnable onTerminated;
    private LifecycleExecutor successor;

    LifecycleExecutor() {
      super(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(1),
          LocalHistoryMetricsFactory.daemonFactory(
              "history-metrics-local-shutdown"),
          new ThreadPoolExecutor.AbortPolicy());
    }

    final LifecycleExecutor replacement() {
      LifecycleExecutor replacement = successor;
      successor = null;
      return replacement == null ? new LifecycleExecutor() : replacement;
    }

    final void successor(LifecycleExecutor executor) {
      successor = executor;
    }

    final void executeOwned(Runnable command) {
      super.execute(command);
    }

    final boolean isOwnedTerminated() {
      return super.isTerminated();
    }

    @Override
    protected void terminated() {
      Runnable callback = onTerminated;
      if (callback != null) {
        callback.run();
      }
    }
  }
}
