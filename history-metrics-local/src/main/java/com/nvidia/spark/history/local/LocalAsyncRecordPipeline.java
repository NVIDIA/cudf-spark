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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.WriteResult;

/** Package-private single-writer queue behind the local planning adapter's raw record boundary. */
final class LocalAsyncRecordPipeline {
  private static final long FUTURE_TOLERANCE_MS = 300_000L;

  static final DrainWaiter SYSTEM_DRAIN_WAITER = new DrainWaiter() {
    @Override
    public boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException {
      return remainingNanos == 0L
          ? lock.tryLock()
          : lock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long await(Condition condition, long remainingNanos)
        throws InterruptedException {
      return condition.awaitNanos(remainingNanos);
    }
  };

  interface SchemaLookup {
    MetricSchema find(MetricVersionId metric);
  }

  interface DrainWaiter {
    boolean tryLock(ReentrantLock lock, long remainingNanos)
        throws InterruptedException;

    long await(Condition condition, long remainingNanos)
        throws InterruptedException;
  }

  static final class QueuePolicy {
    private final int observationCapacity;
    private final int maxBackendBatchSize;

    QueuePolicy(int observationCapacity, int maxBackendBatchSize) {
      if (observationCapacity <= 0) {
        throw new IllegalArgumentException("observationCapacity must be positive");
      }
      if (maxBackendBatchSize <= 0) {
        throw new IllegalArgumentException("maxBackendBatchSize must be positive");
      }
      this.observationCapacity = observationCapacity;
      this.maxBackendBatchSize = maxBackendBatchSize;
    }

    int observationCapacity() {
      return observationCapacity;
    }

    int maxBackendBatchSize() {
      return maxBackendBatchSize;
    }
  }

  private final HistoryMetricsBackend backend;
  private final SchemaLookup schemas;
  private final Clock driverClock;
  private final LocalProvenanceSource provenanceSource;
  private final QueuePolicy policy;
  private final LocalMetricStorePlanningAdapter.Ticker ticker;
  private final DrainWaiter drainWaiter;
  private final LocalRecordDiagnostics diagnostics;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition workAvailable = lock.newCondition();
  private final Condition terminalChanged = lock.newCondition();
  private final ArrayDeque<QueuedObservation> queue = new ArrayDeque<QueuedObservation>();
  private final EnumMap<Status.Code, Long> backendStatuses =
      new EnumMap<Status.Code, Long>(Status.Code.class);
  private final Thread writer;

  private boolean accepting = true;
  private boolean inFlight;
  private int inFlightItems;
  private boolean inFlightReportedAmbiguous;
  private boolean writerTerminated;
  private long nextSequence = 1L;
  private long highestEnqueued;
  private long terminalSequence;
  private long cleanupTerminalSequence;

  private long recordCalls;
  private long invalidCalls;
  private long invalidItems;
  private long undeclaredItems;
  private long futureItems;
  private long clockFailureItems;
  private long provenanceFailureItems;
  private long overflowItems;
  private long stoppedItems;
  private long shutdownDroppedItems;
  private boolean shutdownFinalized;
  private boolean shutdownAmbiguityDiagnosticPending;
  private boolean shutdownAmbiguityDiagnosticEmitted;
  private long enqueuedItems;
  private long backendBatches;
  private long backendAcceptedItems;
  private long backendRejectedItems;
  private long backendAmbiguousItems;
  private long queueHighWater;
  private long drainSuccesses;
  private final AtomicLong drainTimeouts = new AtomicLong();

  LocalAsyncRecordPipeline(
      HistoryMetricsBackend backend,
      SchemaLookup schemas,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      QueuePolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      DrainWaiter drainWaiter) {
    this(
        backend,
        schemas,
        driverClock,
        provenanceSource,
        policy,
        ticker,
        drainWaiter,
        LocalRecordDiagnostics.system(ticker));
  }

  LocalAsyncRecordPipeline(
      HistoryMetricsBackend backend,
      SchemaLookup schemas,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      QueuePolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      DrainWaiter drainWaiter,
      LocalRecordDiagnosticSink diagnosticSink) {
    this(
        backend,
        schemas,
        driverClock,
        provenanceSource,
        policy,
        ticker,
        drainWaiter,
        new LocalRecordDiagnostics(ticker, diagnosticSink));
  }

  private LocalAsyncRecordPipeline(
      HistoryMetricsBackend backend,
      SchemaLookup schemas,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      QueuePolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      DrainWaiter drainWaiter,
      LocalRecordDiagnostics diagnostics) {
    this.backend = Objects.requireNonNull(backend, "backend");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.driverClock = Objects.requireNonNull(driverClock, "driverClock");
    this.provenanceSource = Objects.requireNonNull(provenanceSource, "provenanceSource");
    this.policy = Objects.requireNonNull(policy, "policy");
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.drainWaiter = Objects.requireNonNull(drainWaiter, "drainWaiter");
    this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
    for (Status.Code code : Status.Code.values()) {
      backendStatuses.put(code, 0L);
    }
    writer = new Thread(new Runnable() {
      @Override
      public void run() {
        runWriter();
      }
    }, "history-metrics-local-writer");
    writer.setDaemon(true);
    writer.start();
  }

  void record(List<Observation> observations) {
    incrementRecordCalls();
    if (observations == null) {
      lock.lock();
      try {
        invalidCalls++;
      } finally {
        lock.unlock();
      }
      diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
      return;
    }
    try {
      if (observations.isEmpty()) {
        return;
      }
    } catch (RuntimeException failure) {
      incrementInvalidCalls();
      diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
      return;
    } catch (LinkageError failure) {
      incrementInvalidCalls();
      diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
      return;
    }

    List<Observation> structurallyValid = new ArrayList<Observation>();
    long invalid = 0L;
    long undeclared = 0L;
    try {
      for (Observation observation : observations) {
        if (observation == null) {
          invalid++;
          continue;
        }
        MetricSchema schema = schemas.find(observation.metric());
        if (schema == null) {
          undeclared++;
          continue;
        }
        if (!matchesSchema(observation, schema)) {
          invalid++;
          continue;
        }
        structurallyValid.add(observation);
      }
    } catch (RuntimeException failure) {
      invalid += structurallyValid.size();
      structurallyValid.clear();
    } catch (LinkageError failure) {
      invalid += structurallyValid.size();
      structurallyValid.clear();
    }
    addValidationCounts(invalid, undeclared);
    if (invalid > 0L) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
    }
    if (undeclared > 0L) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.NOT_DECLARED);
    }
    if (structurallyValid.isEmpty()) {
      return;
    }

    final long nowMs;
    try {
      nowMs = driverClock.millis();
    } catch (RuntimeException failure) {
      addClockFailures(structurallyValid.size());
      diagnostics.candidate(LocalRecordDiagnostics.Category.CLOCK_FAILURE);
      return;
    } catch (LinkageError failure) {
      addClockFailures(structurallyValid.size());
      diagnostics.candidate(LocalRecordDiagnostics.Category.CLOCK_FAILURE);
      return;
    }

    long cutoffMs = nowMs > Long.MAX_VALUE - FUTURE_TOLERANCE_MS
        ? Long.MAX_VALUE
        : nowMs + FUTURE_TOLERANCE_MS;
    List<Observation> timeValid = new ArrayList<Observation>(structurallyValid.size());
    long future = 0L;
    for (Observation observation : structurallyValid) {
      if (observation.timestampMs() > cutoffMs) {
        future++;
      } else {
        timeValid.add(observation);
      }
    }
    addFutureItems(future);
    if (future > 0L) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.FUTURE_TIMESTAMP);
    }
    if (timeValid.isEmpty()) {
      return;
    }

    final Provenance provenance;
    try {
      LocalProvenanceIdentity identity =
          Objects.requireNonNull(provenanceSource.current(), "provenance identity");
      provenance = new Provenance(
          identity.applicationId(), identity.attemptId(), identity.pluginVersion(), nowMs);
    } catch (RuntimeException failure) {
      addProvenanceFailures(timeValid.size());
      diagnostics.candidate(LocalRecordDiagnostics.Category.PROVENANCE_FAILURE);
      return;
    } catch (LinkageError failure) {
      addProvenanceFailures(timeValid.size());
      diagnostics.candidate(LocalRecordDiagnostics.Category.PROVENANCE_FAILURE);
      return;
    }

    boolean stopped = false;
    boolean overflow = false;
    lock.lock();
    try {
      if (!accepting) {
        stoppedItems += timeValid.size();
        stopped = true;
      } else {
        int available = policy.observationCapacity() - queue.size();
        int admitted = Math.min(available, timeValid.size());
        int actuallyAdmitted = 0;
        for (int index = 0; index < admitted; index++) {
          if (nextSequence <= 0L) {
            break;
          }
          long sequence = nextSequence++;
          queue.addLast(new QueuedObservation(
              new StampedObservation(timeValid.get(index), provenance), sequence));
          highestEnqueued = sequence;
          enqueuedItems++;
          actuallyAdmitted++;
        }
        long rejected = timeValid.size() - actuallyAdmitted;
        overflowItems += rejected;
        overflow = rejected > 0L;
        queueHighWater = Math.max(queueHighWater, queue.size());
        if (actuallyAdmitted > 0) {
          workAvailable.signal();
        }
      }
    } finally {
      lock.unlock();
    }
    if (stopped) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.POST_STOP);
    }
    if (overflow) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.OVERFLOW);
    }
  }

  long captureWatermark(LocalSnapshotDeadline deadline)
      throws LocalSnapshotException {
    Objects.requireNonNull(deadline, "deadline");
    if (!acquireForSnapshot(deadline)) {
      throw snapshotTimeout("snapshot watermark selection exceeded its monotonic budget");
    }
    try {
      // An uncontended zero-budget selection may proceed so path BUSY wins over TIMEOUT.
      if (!deadline.isZero()) {
        deadline.throwIfExpired();
      }
      return highestEnqueued;
    } finally {
      lock.unlock();
    }
  }

  void awaitWatermark(long watermark, LocalSnapshotDeadline deadline)
      throws LocalSnapshotException {
    Objects.requireNonNull(deadline, "deadline");
    if (!acquireForSnapshot(deadline)) {
      throw snapshotTimeout("snapshot watermark completion exceeded its monotonic budget");
    }
    try {
      deadline.throwIfExpired();
      while (terminalSequence < watermark) {
        long remaining = deadline.remainingNanos();
        if (remaining == 0L) {
          throw snapshotTimeout(
              "snapshot watermark completion exceeded its monotonic budget");
        }
        final long waitRemaining;
        try {
          waitRemaining = drainWaiter.await(terminalChanged, remaining);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new LocalSnapshotException(
              LocalSnapshotException.Reason.IO,
              "snapshot watermark coordination was interrupted",
              new IllegalStateException("snapshot watermark coordination was interrupted"));
        }
        if (waitRemaining <= 0L || deadline.isExpired()) {
          throw snapshotTimeout(
              "snapshot watermark completion exceeded its monotonic budget");
        }
      }
      deadline.throwIfExpired();
    } finally {
      lock.unlock();
    }
  }

  private boolean acquireForSnapshot(LocalSnapshotDeadline deadline)
      throws LocalSnapshotException {
    try {
      return drainWaiter.tryLock(lock, deadline.remainingNanos());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new LocalSnapshotException(
          LocalSnapshotException.Reason.IO,
          "snapshot watermark coordination was interrupted",
          new IllegalStateException("snapshot watermark coordination was interrupted"));
    } catch (RuntimeException failure) {
      throw new LocalSnapshotException(
          LocalSnapshotException.Reason.IO,
          "snapshot watermark coordination failed",
          new IllegalStateException("snapshot watermark coordination failed"));
    } catch (LinkageError failure) {
      throw new LocalSnapshotException(
          LocalSnapshotException.Reason.IO,
          "snapshot watermark coordination failed",
          new IllegalStateException("snapshot watermark coordination failed"));
    }
  }

  private static LocalSnapshotException snapshotTimeout(String message) {
    return new LocalSnapshotException(LocalSnapshotException.Reason.TIMEOUT, message);
  }

  boolean drain(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long startedNanos = ticker.readNanos();
    long timeoutNanos = durationToNanosSaturated(timeout);
    long lockBudget = timeoutNanos == 0L
        ? 0L
        : remainingNanos(startedNanos, timeoutNanos);

    final boolean acquired;
    try {
      acquired = drainWaiter.tryLock(lock, lockBudget);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      drainTimeouts.incrementAndGet();
      return false;
    } catch (RuntimeException failure) {
      drainTimeouts.incrementAndGet();
      return false;
    } catch (LinkageError failure) {
      drainTimeouts.incrementAndGet();
      return false;
    }
    if (!acquired) {
      drainTimeouts.incrementAndGet();
      return false;
    }

    try {
      long watermark = highestEnqueued;
      if (timeoutNanos == 0L) {
        if (terminalSequence >= watermark) {
          drainSuccesses++;
          return true;
        }
        drainTimeouts.incrementAndGet();
        return false;
      }
      if (remainingNanos(startedNanos, timeoutNanos) == 0L) {
        drainTimeouts.incrementAndGet();
        return false;
      }
      while (terminalSequence < watermark) {
        long remaining = remainingNanos(startedNanos, timeoutNanos);
        if (remaining == 0L) {
          drainTimeouts.incrementAndGet();
          return false;
        }
        final long waitRemaining;
        try {
          waitRemaining = drainWaiter.await(terminalChanged, remaining);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          drainTimeouts.incrementAndGet();
          return false;
        } catch (RuntimeException failure) {
          drainTimeouts.incrementAndGet();
          return false;
        } catch (LinkageError failure) {
          drainTimeouts.incrementAndGet();
          return false;
        }
        if (waitRemaining <= 0L ||
            remainingNanos(startedNanos, timeoutNanos) == 0L) {
          drainTimeouts.incrementAndGet();
          return false;
        }
      }
      if (remainingNanos(startedNanos, timeoutNanos) == 0L) {
        drainTimeouts.incrementAndGet();
        return false;
      }
      drainSuccesses++;
      return true;
    } finally {
      lock.unlock();
    }
  }

  void markInFlightAmbiguous() {
    boolean newlyAmbiguous;
    lock.lock();
    try {
      newlyAmbiguous = markInFlightAmbiguousUnderLock();
    } finally {
      lock.unlock();
    }
    if (newlyAmbiguous) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS);
    }
  }

  void reserveInFlightAmbiguousForShutdown() {
    lock.lock();
    try {
      reserveInFlightAmbiguousForShutdownUnderLock();
    } finally {
      lock.unlock();
    }
  }

  private boolean markInFlightAmbiguousUnderLock() {
    if (inFlight && !inFlightReportedAmbiguous) {
      backendAmbiguousItems += inFlightItems;
      inFlightReportedAmbiguous = true;
      return true;
    }
    return false;
  }

  private void reserveInFlightAmbiguousForShutdownUnderLock() {
    if (markInFlightAmbiguousUnderLock()) {
      shutdownAmbiguityDiagnosticPending = true;
    }
  }

  long beginShutdown() {
    lock.lock();
    try {
      accepting = false;
      workAvailable.signalAll();
      return highestEnqueued;
    } finally {
      lock.unlock();
    }
  }

  void finishShutdown(long watermark, long startedNanos, long timeoutNanos) {
    lock.lock();
    try {
      try {
        while (terminalSequence < watermark) {
          long remaining = remainingNanos(startedNanos, timeoutNanos);
          if (remaining == 0L) {
            dropForShutdown(watermark);
            break;
          }
          try {
            terminalChanged.awaitNanos(remaining);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            dropForShutdown(watermark);
            break;
          }
        }
      } catch (RuntimeException failure) {
        dropForShutdown(watermark);
      } catch (LinkageError failure) {
        dropForShutdown(watermark);
      } finally {
        shutdownFinalized = true;
        terminalChanged.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  void emitShutdownAmbiguityAfterFinalized() {
    boolean emit = false;
    boolean interrupted = false;
    lock.lock();
    try {
      while (!shutdownFinalized) {
        try {
          terminalChanged.await();
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
      if (shutdownAmbiguityDiagnosticPending &&
          !shutdownAmbiguityDiagnosticEmitted) {
        shutdownAmbiguityDiagnosticEmitted = true;
        emit = true;
      }
    } finally {
      lock.unlock();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (emit) {
      diagnostics.candidate(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS);
    }
  }

  private void dropForShutdown(long watermark) {
    long dropped = queue.size();
    queue.clear();
    shutdownDroppedItems += dropped;
    reserveInFlightAmbiguousForShutdownUnderLock();
    terminalSequence = Math.max(terminalSequence, watermark);
    cleanupTerminalSequence = 0L;
    terminalChanged.signalAll();
    workAvailable.signalAll();
  }

  void awaitWriterTermination() {
    if (Thread.currentThread() == writer) {
      return;
    }
    boolean interrupted = false;
    while (writer.isAlive()) {
      try {
        writer.join();
      } catch (InterruptedException failure) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  long shutdownDroppedItemCount() {
    lock.lock();
    try {
      return shutdownDroppedItems;
    } finally {
      lock.unlock();
    }
  }

  boolean writerIsTerminated() {
    return !writer.isAlive();
  }

  boolean stop(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }

    lock.lock();
    try {
      accepting = false;
      long clearedLast = 0L;
      while (!queue.isEmpty()) {
        QueuedObservation cleared = queue.removeFirst();
        clearedLast = cleared.sequence;
        stoppedItems++;
      }
      if (clearedLast != 0L) {
        if (inFlight) {
          cleanupTerminalSequence = Math.max(cleanupTerminalSequence, clearedLast);
        } else {
          terminalSequence = Math.max(terminalSequence, clearedLast);
          terminalChanged.signalAll();
        }
      }
      workAvailable.signalAll();
    } finally {
      lock.unlock();
    }

    writer.interrupt();
    if (Thread.currentThread() == writer) {
      return false;
    }
    long nanos = durationToNanosSaturated(timeout);
    if (nanos == 0L) {
      return !writer.isAlive();
    }
    try {
      long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
      int extraNanos = (int) (nanos - TimeUnit.MILLISECONDS.toNanos(millis));
      writer.join(millis, extraNanos);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
    return !writer.isAlive();
  }

  RecordCounterSnapshot counters() {
    lock.lock();
    try {
      return new RecordCounterSnapshot(
          recordCalls,
          invalidCalls,
          invalidItems,
          undeclaredItems,
          futureItems,
          clockFailureItems,
          provenanceFailureItems,
          overflowItems,
          stoppedItems,
          enqueuedItems,
          backendBatches,
          backendAcceptedItems,
          backendRejectedItems,
          backendAmbiguousItems,
          queue.size(),
          queueHighWater,
          drainSuccesses,
          drainTimeouts.get(),
          backendStatuses);
    } finally {
      lock.unlock();
    }
  }

  private void runWriter() {
    while (true) {
      List<QueuedObservation> batch = takeBatch();
      if (batch == null) {
        return;
      }
      completeBatch(batch, write(batch));
    }
  }

  private List<QueuedObservation> takeBatch() {
    lock.lock();
    try {
      while (queue.isEmpty() && accepting) {
        try {
          workAvailable.await();
        } catch (InterruptedException interrupted) {
          if (!accepting) {
            return null;
          }
        }
      }
      if (queue.isEmpty()) {
        return null;
      }

      List<QueuedObservation> batch = new ArrayList<QueuedObservation>();
      QueuedObservation first = queue.removeFirst();
      batch.add(first);
      MetricVersionId metric = first.stamped.observation().metric();
      while (batch.size() < policy.maxBackendBatchSize() && !queue.isEmpty() &&
          queue.peekFirst().stamped.observation().metric().equals(metric)) {
        batch.add(queue.removeFirst());
      }
      inFlight = true;
      inFlightItems = batch.size();
      inFlightReportedAmbiguous = false;
      return batch;
    } finally {
      lock.unlock();
    }
  }

  private BackendWriteOutcome write(List<QueuedObservation> batch) {
    List<StampedObservation> observations =
        new ArrayList<StampedObservation>(batch.size());
    for (QueuedObservation queued : batch) {
      observations.add(queued.stamped);
    }
    try {
      WriteResult result =
          backend.record(Collections.unmodifiableList(observations));
      if (!validResult(result, batch.size())) {
        return BackendWriteOutcome.ambiguous(batch.size());
      }
      return BackendWriteOutcome.valid(
          result.accepted(),
          result.rejected(),
          result.status().code(),
          diagnosticCategory(result.status().code()));
    } catch (RuntimeException failure) {
      return BackendWriteOutcome.ambiguous(batch.size());
    } catch (LinkageError failure) {
      return BackendWriteOutcome.ambiguous(batch.size());
    }
  }

  private void completeBatch(
      List<QueuedObservation> batch, BackendWriteOutcome outcome) {
    boolean diagnose;
    lock.lock();
    try {
      backendBatches++;
      backendAcceptedItems += outcome.accepted;
      backendRejectedItems += outcome.rejected;
      if (!inFlightReportedAmbiguous) {
        backendAmbiguousItems += outcome.ambiguous;
      }
      if (outcome.status != null) {
        backendStatuses.put(
            outcome.status, backendStatuses.get(outcome.status) + 1L);
      }
      terminalSequence = Math.max(
          terminalSequence, batch.get(batch.size() - 1).sequence);
      diagnose = !inFlightReportedAmbiguous && outcome.diagnosticCategory != null;
      inFlight = false;
      inFlightItems = 0;
      inFlightReportedAmbiguous = false;
      if (cleanupTerminalSequence > terminalSequence) {
        terminalSequence = cleanupTerminalSequence;
        cleanupTerminalSequence = 0L;
      }
      terminalChanged.signalAll();
      if (!queue.isEmpty()) {
        workAvailable.signal();
      }
    } finally {
      lock.unlock();
    }
    if (diagnose) {
      diagnostics.candidate(outcome.diagnosticCategory);
    }
  }

  private static LocalRecordDiagnostics.Category diagnosticCategory(Status.Code status) {
    switch (status) {
      case OK:
        return null;
      case NOT_DECLARED:
        return LocalRecordDiagnostics.Category.NOT_DECLARED;
      case INVALID_REQUEST:
        return LocalRecordDiagnostics.Category.INVALID;
      case DENIED:
        return LocalRecordDiagnostics.Category.BACKEND_DENIED;
      case UNAVAILABLE:
        return LocalRecordDiagnostics.Category.BACKEND_UNAVAILABLE;
      default:
        return LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS;
    }
  }

  private static boolean matchesSchema(
      Observation observation, MetricSchema schema) {
    try {
      if (!Double.isFinite(observation.value()) ||
          observation.dimensions().size() != schema.dimensions().size()) {
        return false;
      }
      for (DimensionSpec dimension : schema.dimensions()) {
        DimValue value = observation.dimensions().get(dimension.name());
        if (value == null ||
            value.kind() != dimension.kind() ||
            value.canonicalBytes().length > DimValue.MAX_CANONICAL_BYTES) {
          return false;
        }
      }
      return true;
    } catch (RuntimeException failure) {
      return false;
    } catch (LinkageError failure) {
      return false;
    }
  }

  private static boolean validResult(WriteResult result, int expected) {
    try {
      if (result == null ||
          result.status() == null ||
          result.status().code() == null ||
          (long) result.accepted() + result.rejected() != expected) {
        return false;
      }
      switch (result.status().code()) {
        case OK:
          return result.accepted() == expected && result.rejected() == 0;
        case NOT_DECLARED:
        case INVALID_REQUEST:
        case DENIED:
          return result.accepted() == 0 && result.rejected() == expected;
        case UNAVAILABLE:
          return result.rejected() > 0;
        default:
          return false;
      }
    } catch (RuntimeException failure) {
      return false;
    } catch (LinkageError failure) {
      return false;
    }
  }

  private void incrementRecordCalls() {
    lock.lock();
    try {
      recordCalls++;
    } finally {
      lock.unlock();
    }
  }

  private void incrementInvalidCalls() {
    lock.lock();
    try {
      invalidCalls++;
    } finally {
      lock.unlock();
    }
  }

  private void addValidationCounts(long invalid, long undeclared) {
    lock.lock();
    try {
      invalidItems += invalid;
      undeclaredItems += undeclared;
    } finally {
      lock.unlock();
    }
  }

  private void addClockFailures(long count) {
    lock.lock();
    try {
      clockFailureItems += count;
    } finally {
      lock.unlock();
    }
  }

  private void addFutureItems(long count) {
    lock.lock();
    try {
      futureItems += count;
    } finally {
      lock.unlock();
    }
  }

  private void addProvenanceFailures(long count) {
    lock.lock();
    try {
      provenanceFailureItems += count;
    } finally {
      lock.unlock();
    }
  }

  private static long durationToNanosSaturated(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
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

  private static final class QueuedObservation {
    private final StampedObservation stamped;
    private final long sequence;

    private QueuedObservation(StampedObservation stamped, long sequence) {
      this.stamped = stamped;
      this.sequence = sequence;
    }
  }

  private static final class BackendWriteOutcome {
    private final int accepted;
    private final int rejected;
    private final int ambiguous;
    private final Status.Code status;
    private final LocalRecordDiagnostics.Category diagnosticCategory;

    private BackendWriteOutcome(
        int accepted,
        int rejected,
        int ambiguous,
        Status.Code status,
        LocalRecordDiagnostics.Category diagnosticCategory) {
      this.accepted = accepted;
      this.rejected = rejected;
      this.ambiguous = ambiguous;
      this.status = status;
      this.diagnosticCategory = diagnosticCategory;
    }

    private static BackendWriteOutcome valid(
        int accepted,
        int rejected,
        Status.Code status,
        LocalRecordDiagnostics.Category diagnosticCategory) {
      return new BackendWriteOutcome(
          accepted, rejected, 0, status, diagnosticCategory);
    }

    private static BackendWriteOutcome ambiguous(int count) {
      return new BackendWriteOutcome(
          0, 0, count, null, LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS);
    }
  }

  /** Immutable companion-only record-pipeline counter snapshot. */
  static final class RecordCounterSnapshot {
    private final long recordCalls;
    private final long invalidCalls;
    private final long invalidItems;
    private final long undeclaredItems;
    private final long futureItems;
    private final long clockFailureItems;
    private final long provenanceFailureItems;
    private final long overflowItems;
    private final long stoppedItems;
    private final long enqueuedItems;
    private final long backendBatches;
    private final long backendAcceptedItems;
    private final long backendRejectedItems;
    private final long backendAmbiguousItems;
    private final long queueCurrent;
    private final long queueHighWater;
    private final long drainSuccesses;
    private final long drainTimeouts;
    private final EnumMap<Status.Code, Long> backendStatuses;

    private RecordCounterSnapshot(
        long recordCalls,
        long invalidCalls,
        long invalidItems,
        long undeclaredItems,
        long futureItems,
        long clockFailureItems,
        long provenanceFailureItems,
        long overflowItems,
        long stoppedItems,
        long enqueuedItems,
        long backendBatches,
        long backendAcceptedItems,
        long backendRejectedItems,
        long backendAmbiguousItems,
        long queueCurrent,
        long queueHighWater,
        long drainSuccesses,
        long drainTimeouts,
        EnumMap<Status.Code, Long> backendStatuses) {
      this.recordCalls = recordCalls;
      this.invalidCalls = invalidCalls;
      this.invalidItems = invalidItems;
      this.undeclaredItems = undeclaredItems;
      this.futureItems = futureItems;
      this.clockFailureItems = clockFailureItems;
      this.provenanceFailureItems = provenanceFailureItems;
      this.overflowItems = overflowItems;
      this.stoppedItems = stoppedItems;
      this.enqueuedItems = enqueuedItems;
      this.backendBatches = backendBatches;
      this.backendAcceptedItems = backendAcceptedItems;
      this.backendRejectedItems = backendRejectedItems;
      this.backendAmbiguousItems = backendAmbiguousItems;
      this.queueCurrent = queueCurrent;
      this.queueHighWater = queueHighWater;
      this.drainSuccesses = drainSuccesses;
      this.drainTimeouts = drainTimeouts;
      this.backendStatuses = new EnumMap<Status.Code, Long>(backendStatuses);
    }

    long recordCallCount() {
      return recordCalls;
    }

    long invalidCallCount() {
      return invalidCalls;
    }

    long invalidItemCount() {
      return invalidItems;
    }

    long undeclaredItemCount() {
      return undeclaredItems;
    }

    long futureItemCount() {
      return futureItems;
    }

    long clockFailureItemCount() {
      return clockFailureItems;
    }

    long provenanceFailureItemCount() {
      return provenanceFailureItems;
    }

    long overflowItemCount() {
      return overflowItems;
    }

    long stoppedItemCount() {
      return stoppedItems;
    }

    long enqueuedItemCount() {
      return enqueuedItems;
    }

    long backendBatchCount() {
      return backendBatches;
    }

    long backendAcceptedItemCount() {
      return backendAcceptedItems;
    }

    long backendRejectedItemCount() {
      return backendRejectedItems;
    }

    long backendAmbiguousItemCount() {
      return backendAmbiguousItems;
    }

    long queueCurrent() {
      return queueCurrent;
    }

    long queueHighWater() {
      return queueHighWater;
    }

    long drainSuccessCount() {
      return drainSuccesses;
    }

    long drainTimeoutCount() {
      return drainTimeouts;
    }

    long backendStatusCount(Status.Code code) {
      Long count = backendStatuses.get(Objects.requireNonNull(code, "code"));
      return count == null ? 0L : count;
    }
  }
}
