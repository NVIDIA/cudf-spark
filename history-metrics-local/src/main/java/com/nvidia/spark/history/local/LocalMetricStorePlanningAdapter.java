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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.Summary;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;

/**
 * Package-private local {@code MetricStore} wrapper for a synchronous history metrics backend.
 *
 * <p>The local owner constructs this wrapper with a dedicated bounded planning executor and record
 * pipeline. It is distinct from the future Spark consumer integration that builds requests before
 * calling {@code MetricStore}.
 */
final class LocalMetricStorePlanningAdapter implements MetricStore {
  private static final int MAX_BATCH_SIZE = 128;
  private static final String PROVIDER_UNAVAILABLE = "history metrics backend is unavailable";
  private static final String DEADLINE_EXCEEDED = "planning deadline exceeded";
  private static final String OWNER_STOPPED = "local history metrics owner is stopping";

  interface Ticker {
    long readNanos();
  }

  private final HistoryMetricsBackend backend;
  private final HistoryMetricCatalog catalog;
  private final ExecutorService executor;
  private final Ticker ticker;
  private final BackendInfo info;
  private final LocalPlanningCircuitBreaker breaker;
  private final Object declarationLock = new Object();
  private volatile LocalAsyncRecordPipeline recordPipeline;
  private final Object counterLock = new Object();
  private final Object activeLock = new Object();
  private final Object submissionLock = new Object();
  private final Map<Future<?>, LocalPlanningCircuitBreaker.Attempt> pendingAttempts =
      new HashMap<Future<?>, LocalPlanningCircuitBreaker.Attempt>();
  private volatile boolean acceptingPlanning = true;
  private int activeBackendInvocations;
  private final Map<MetricVersionId, MetricSchema> declarations =
      new HashMap<MetricVersionId, MetricSchema>();
  private final EnumMap<SchemaStatus.Code, Long> declarationOutcomes =
      new EnumMap<SchemaStatus.Code, Long>(SchemaStatus.Code.class);
  private final EnumMap<Status.Code, Long> summaryOutcomes =
      new EnumMap<Status.Code, Long>(Status.Code.class);

  private long declareCalls;
  private long summaryCalls;
  private long summaryWindowClipped;
  private long summaryRows;
  private long timeoutCalls;
  private long malformedProviderResults;
  private long executorRejections;
  private long providerFailures;
  private long suppressedCalls;
  private long recordCalls;
  private long recordDropped;

  private LocalMetricStorePlanningAdapter(
      HistoryMetricsBackend backend,
      HistoryMetricCatalog catalog,
      ExecutorService executor,
      Ticker ticker,
      BackendInfo info,
      LocalCircuitBreakerPolicy breakerPolicy) {
    this.backend = backend;
    this.catalog = catalog;
    this.executor = executor;
    this.ticker = ticker;
    this.info = info;
    this.breaker = new LocalPlanningCircuitBreaker(breakerPolicy, ticker);
    for (SchemaStatus.Code code : SchemaStatus.Code.values()) {
      declarationOutcomes.put(code, 0L);
    }
    for (Status.Code code : Status.Code.values()) {
      summaryOutcomes.put(code, 0L);
    }
  }

  static LocalMetricStorePlanningAdapter create(
      HistoryMetricsBackend backend,
      HistoryMetricCatalog catalog,
      ExecutorService executor,
      boolean ownsExecutor,
      Ticker ticker,
      LocalCircuitBreakerPolicy breakerPolicy) {
    try {
      Objects.requireNonNull(backend, "backend");
      Objects.requireNonNull(catalog, "catalog");
      Objects.requireNonNull(executor, "executor");
      Objects.requireNonNull(ticker, "ticker");
      Objects.requireNonNull(breakerPolicy, "breakerPolicy");
      BackendInfo info = backend.info();
      if (info == null ||
          info.apiVersion() != HistoryMetricsApi.CURRENT_API_VERSION ||
          info.description() == null ||
          info.description().isEmpty()) {
        throw new IllegalArgumentException("incompatible history metrics backend information");
      }
      return new LocalMetricStorePlanningAdapter(
          backend, catalog, executor, ticker, info, breakerPolicy);
    } catch (RuntimeException failure) {
      shutdownAfterConstructionFailure(executor, ownsExecutor);
      throw new IllegalArgumentException("history metrics backend construction failed", failure);
    } catch (LinkageError failure) {
      shutdownAfterConstructionFailure(executor, ownsExecutor);
      throw new IllegalArgumentException("history metrics backend is binary-incompatible", failure);
    }
  }

  static LocalMetricStorePlanningAdapter createWithRecording(
      HistoryMetricsBackend backend,
      HistoryMetricCatalog catalog,
      ExecutorService executor,
      boolean ownsExecutor,
      Ticker ticker,
      LocalCircuitBreakerPolicy breakerPolicy,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      LocalAsyncRecordPipeline.QueuePolicy queuePolicy,
      LocalAsyncRecordPipeline.DrainWaiter drainWaiter) {
    return createWithRecording(
        backend,
        catalog,
        executor,
        ownsExecutor,
        ticker,
        breakerPolicy,
        driverClock,
        provenanceSource,
        queuePolicy,
        drainWaiter,
        Collections.<LocalDeclarationSnapshot>emptyList());
  }

  static LocalMetricStorePlanningAdapter createWithRecording(
      HistoryMetricsBackend backend,
      HistoryMetricCatalog catalog,
      ExecutorService executor,
      boolean ownsExecutor,
      Ticker ticker,
      LocalCircuitBreakerPolicy breakerPolicy,
      Clock driverClock,
      LocalProvenanceSource provenanceSource,
      LocalAsyncRecordPipeline.QueuePolicy queuePolicy,
      LocalAsyncRecordPipeline.DrainWaiter drainWaiter,
      List<LocalDeclarationSnapshot> restoredDeclarations) {
    final LocalMetricStorePlanningAdapter adapter =
        create(backend, catalog, executor, ownsExecutor, ticker, breakerPolicy);
    try {
      adapter.seedDeclarations(restoredDeclarations);
      adapter.recordPipeline = new LocalAsyncRecordPipeline(
          backend,
          new LocalAsyncRecordPipeline.SchemaLookup() {
            @Override
            public MetricSchema find(MetricVersionId metric) {
              return adapter.declaredSchema(metric);
            }
          },
          driverClock,
          provenanceSource,
          queuePolicy,
          ticker,
          drainWaiter);
      return adapter;
    } catch (RuntimeException failure) {
      adapter.stopPlanning();
      throw failure;
    } catch (LinkageError failure) {
      adapter.stopPlanning();
      throw failure;
    }
  }

  private void seedDeclarations(List<LocalDeclarationSnapshot> restoredDeclarations) {
    Objects.requireNonNull(restoredDeclarations, "restoredDeclarations");
    synchronized (declarationLock) {
      for (LocalDeclarationSnapshot declaration : restoredDeclarations) {
        LocalDeclarationSnapshot checked =
            Objects.requireNonNull(declaration, "restored declaration");
        MetricSchema previous =
            declarations.put(checked.schema().metric(), checked.schema());
        if (previous != null) {
          throw new IllegalArgumentException(
              "restored declarations contain a duplicate metric version");
        }
      }
    }
  }

  private static void shutdownAfterConstructionFailure(
      ExecutorService executor, boolean ownsExecutor) {
    if (ownsExecutor && executor != null) {
      try {
        executor.shutdownNow();
      } catch (RuntimeException ignored) {
        // Construction is already failing; executor cleanup is best effort.
      } catch (LinkageError ignored) {
        // Preserve the original compatibility failure.
      }
    }
  }

  @Override
  public BackendInfo info() {
    return info;
  }

  LocalCircuitBreakerState breakerState() {
    return breaker.state();
  }

  LocalHistoryMetricsCounters breakerCounters() {
    return breaker.counters();
  }

  @Override
  public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
    Budget budget = null;
    try {
      budget = Budget.start(timeout, ticker);
    } catch (RuntimeException failure) {
      countProviderFailure();
    } catch (LinkageError failure) {
      countProviderFailure();
    }

    CapturedInput<MetricSchema> captured = captureInput(schemas);
    if (!captured.validShape) {
      return countedDeclarations(Collections.<SchemaStatus>emptyList());
    }
    if (captured.accessFailed) {
      return countedDeclarations(invalidDeclarations(captured.size));
    }
    try {
      return declareCaptured(captured.values, timeout, budget);
    } catch (RuntimeException failure) {
      countProviderFailure();
      return declareCaptured(captured.values, timeout, null);
    } catch (LinkageError failure) {
      countProviderFailure();
      return declareCaptured(captured.values, timeout, null);
    }
  }

  private List<SchemaStatus> declareCaptured(
      List<MetricSchema> schemas, Duration timeout, Budget budget) {
    if (schemas.isEmpty()) {
      return countedDeclarations(Collections.<SchemaStatus>emptyList());
    }

    Set<MetricVersionId> conflicts = findBatchConflicts(schemas);
    List<SchemaStatus> results =
        new ArrayList<SchemaStatus>(Collections.nCopies(schemas.size(), (SchemaStatus) null));
    List<MetricSchema> eligible = new ArrayList<MetricSchema>();
    List<List<Integer>> eligiblePositions = new ArrayList<List<Integer>>();
    Map<DeclarationInput, Integer> duplicateInputs =
        new HashMap<DeclarationInput, Integer>();

    for (int position = 0; position < schemas.size(); position++) {
      MetricSchema schema = schemas.get(position);
      if (schema == null) {
        results.set(position, SchemaStatus.of(
            null, SchemaStatus.Code.INVALID_REQUEST, "schema must not be null"));
        continue;
      }
      if (conflicts.contains(schema.metric())) {
        results.set(position, SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INCOMPATIBLE,
            "conflicting canonical schemas in one declaration batch"));
        continue;
      }
      try {
        if (!catalog.find(schema.metric().metricId()).isPresent()) {
          results.set(position, SchemaStatus.of(
              schema.metric(),
              SchemaStatus.Code.INVALID_REQUEST,
              "metric ID is not present in the supplied catalog"));
          continue;
        }
        MetricSchema declared = declaredSchema(schema.metric());
        if (declared != null && !declared.equals(schema)) {
          results.set(position, SchemaStatus.of(
              schema.metric(),
              SchemaStatus.Code.INCOMPATIBLE,
              "schema conflicts with the accepted declaration"));
          continue;
        }
      } catch (RuntimeException failure) {
        countProviderFailure();
        fillUnresolvedDeclarationFallback(results, schemas, conflicts);
        return countedDeclarations(immutable(results));
      } catch (LinkageError failure) {
        countProviderFailure();
        fillUnresolvedDeclarationFallback(results, schemas, conflicts);
        return countedDeclarations(immutable(results));
      }
      if (timeout == null || timeout.isNegative()) {
        results.set(position, SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INVALID_REQUEST,
            "timeout must be non-null and nonnegative"));
        continue;
      }
      if (timeout.isZero()) {
        results.set(position, SchemaStatus.of(
            schema.metric(), SchemaStatus.Code.UNAVAILABLE, DEADLINE_EXCEEDED));
        continue;
      }

      DeclarationInput input = new DeclarationInput(schema);
      Integer unique = duplicateInputs.get(input);
      if (unique == null) {
        unique = eligible.size();
        duplicateInputs.put(input, unique);
        eligible.add(schema);
        eligiblePositions.add(new ArrayList<Integer>());
      }
      eligiblePositions.get(unique).add(position);
    }

    if (eligible.isEmpty()) {
      return countedDeclarations(immutable(results));
    }

    final List<MetricSchema> backendInput =
        Collections.unmodifiableList(new ArrayList<MetricSchema>(eligible));
    if (budget == null) {
      fillDeclarationFallback(results, eligiblePositions, eligible, PROVIDER_UNAVAILABLE);
      return countedDeclarations(immutable(results));
    }
    if (!acceptingPlanning) {
      fillDeclarationFallback(results, eligiblePositions, eligible, OWNER_STOPPED);
      return countedDeclarations(immutable(results));
    }
    if (budget.expired()) {
      countTimeout();
      fillDeclarationFallback(results, eligiblePositions, eligible, DEADLINE_EXCEEDED);
      return countedDeclarations(immutable(results));
    }
    LocalPlanningCircuitBreaker.Attempt attempt =
        breaker.tryAcquire(budget.startedNanos());
    if (attempt == null) {
      fillDeclarationFallback(results, eligiblePositions, eligible, PROVIDER_UNAVAILABLE);
      return countedDeclarations(immutable(results));
    }
    boolean attemptFailed = true;
    try {
      Invocation<ValidatedBatch<SchemaStatus>> invocation = invoke(
          budget,
          attempt,
          new BackendCall<ValidatedBatch<SchemaStatus>>() {
            @Override
            public ValidatedBatch<SchemaStatus> call(Duration remaining) {
              List<SchemaStatus> copied = copyValidDeclarationResults(
                  backend.declare(backendInput, remaining), backendInput);
              if (copied == null) {
                return ValidatedBatch.<SchemaStatus>malformed();
              }
              // Publish validated ACCEPTED results before this worker relinquishes backend
              // invocation ownership, even if its caller has already timed out.
              reconcileAcceptedDeclarations(copied, backendInput);
              return ValidatedBatch.valid(copied);
            }
          });

      if (!invocation.succeeded()) {
        fillDeclarationFallback(results, eligiblePositions, eligible, PROVIDER_UNAVAILABLE);
        return countedDeclarations(immutable(results));
      }

      ValidatedBatch<SchemaStatus> providerBatch = invocation.value;
      if (providerBatch.malformed) {
        countMalformedProviderResult();
        fillDeclarationFallback(
            results,
            eligiblePositions,
            eligible,
            "history metrics backend returned malformed results");
        return countedDeclarations(immutable(results));
      }
      List<SchemaStatus> providerResults = providerBatch.values;
      if (budget.expired()) {
        countTimeout();
        fillDeclarationFallback(results, eligiblePositions, eligible, DEADLINE_EXCEEDED);
        return countedDeclarations(immutable(results));
      }

      for (int unique = 0; unique < eligible.size(); unique++) {
        SchemaStatus result = providerResults.get(unique);
        for (Integer position : eligiblePositions.get(unique)) {
          results.set(position, result);
        }
      }
      if (budget.expired()) {
        countTimeout();
        fillDeclarationFallback(results, eligiblePositions, eligible, DEADLINE_EXCEEDED);
        return countedDeclarations(immutable(results));
      }
      List<SchemaStatus> counted = countedDeclarations(immutable(results));
      attemptFailed = hasUnavailableDeclaration(providerResults);
      return counted;
    } finally {
      attempt.complete(attemptFailed);
    }
  }

  /**
   * Stops new planning submissions without waiting for executor shutdown.
   */
  void stopPlanningAdmission() {
    synchronized (submissionLock) {
      acceptingPlanning = false;
    }
  }

  /**
   * Stops the executor and terminally fails tasks removed before backend invocation.
   */
  boolean finishPlanningStop() {
    final List<Runnable> removed;
    try {
      removed = executor.shutdownNow();
    } catch (RuntimeException failure) {
      cancelQueuedPlanningTasks();
      return finishPlanningStopGracefully();
    } catch (LinkageError failure) {
      cancelQueuedPlanningTasks();
      return finishPlanningStopGracefully();
    }
    terminalizeRemovedPlanningTasks(removed);
    return true;
  }

  private boolean finishPlanningStopGracefully() {
    try {
      executor.shutdown();
      return true;
    } catch (RuntimeException failure) {
      return false;
    } catch (LinkageError failure) {
      return false;
    }
  }

  private void cancelQueuedPlanningTasks() {
    if (!(executor instanceof ThreadPoolExecutor)) {
      return;
    }
    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
    List<Runnable> queued = new ArrayList<Runnable>(threadPool.getQueue());
    for (Runnable runnable : queued) {
      if (threadPool.remove(runnable)) {
        terminalizeRemovedPlanningTask(runnable);
      }
    }
  }

  private void terminalizeRemovedPlanningTasks(List<Runnable> removed) {
    for (Runnable runnable : removed) {
      terminalizeRemovedPlanningTask(runnable);
    }
  }

  private void terminalizeRemovedPlanningTask(Runnable runnable) {
    if (runnable instanceof Future) {
      Future<?> future = (Future<?>) runnable;
      LocalPlanningCircuitBreaker.Attempt attempt;
      synchronized (submissionLock) {
        attempt = pendingAttempts.remove(future);
      }
      future.cancel(false);
      if (attempt != null) {
        attempt.executorQueueFailure();
      }
    }
  }

  /**
   * Stops new planning submissions and terminally fails tasks removed before backend invocation.
   */
  void stopPlanning() {
    stopPlanningAdmission();
    finishPlanningStop();
  }

  boolean awaitPlanningTermination(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long startedNanos = System.nanoTime();
    long timeoutNanos = durationToNanosSaturated(timeout);
    try {
      if (!executor.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
      synchronized (activeLock) {
        while (activeBackendInvocations != 0) {
          long remainingNanos = remainingWaitNanos(startedNanos, timeoutNanos);
          if (remainingNanos == 0L) {
            return false;
          }
          TimeUnit.NANOSECONDS.timedWait(activeLock, remainingNanos);
        }
        return true;
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void beginBackendInvocation() {
    synchronized (activeLock) {
      activeBackendInvocations++;
    }
  }

  private void endBackendInvocation() {
    synchronized (activeLock) {
      activeBackendInvocations--;
      activeLock.notifyAll();
    }
  }

  @Override
  public void record(List<Observation> observations) {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.record(observations);
      return;
    }

    long dropped = 0L;
    try {
      if (observations != null) {
        dropped = observations.size();
      }
    } catch (RuntimeException ignored) {
      // The raw write boundary is total; the future writer owns detailed rejection accounting.
    } catch (LinkageError ignored) {
      // A binary-incompatible caller must not escape through the raw write boundary.
    }
    synchronized (counterLock) {
      recordCalls++;
      recordDropped += dropped;
    }
  }

  LocalAsyncRecordPipeline.RecordCounterSnapshot recordCounters() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline == null) {
      throw new IllegalStateException("record pipeline is not configured");
    }
    return pipeline.counters();
  }

  boolean drain(Duration timeout) {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    return pipeline == null || pipeline.drain(timeout);
  }

  long captureRecordingWatermark(LocalSnapshotDeadline deadline)
      throws LocalSnapshotException {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    return pipeline == null ? 0L : pipeline.captureWatermark(deadline);
  }

  void awaitRecordingWatermark(
      long watermark, LocalSnapshotDeadline deadline) throws LocalSnapshotException {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.awaitWatermark(watermark, deadline);
    }
  }

  boolean stopRecording(Duration timeout) {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    return pipeline == null || pipeline.stop(timeout);
  }

  void reserveInFlightRecordAmbiguousForShutdown() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.reserveInFlightAmbiguousForShutdown();
    }
  }

  void emitRecordingShutdownAmbiguityAfterFinalized() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.emitShutdownAmbiguityAfterFinalized();
    }
  }

  long beginRecordingShutdown() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    return pipeline == null ? 0L : pipeline.beginShutdown();
  }

  void finishRecordingShutdown(long watermark, long startedNanos, long timeoutNanos) {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.finishShutdown(watermark, startedNanos, timeoutNanos);
    }
  }

  void awaitWriterTermination() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    if (pipeline != null) {
      pipeline.awaitWriterTermination();
    }
  }

  long shutdownDroppedItemCount() {
    LocalAsyncRecordPipeline pipeline = recordPipeline;
    return pipeline == null ? 0L : pipeline.shutdownDroppedItemCount();
  }

  @Override
  public List<SummaryResponse> summarize(
      List<SummaryRequest> requests, Duration timeout) {
    Budget budget = null;
    try {
      budget = Budget.start(timeout, ticker);
    } catch (RuntimeException failure) {
      countProviderFailure();
    } catch (LinkageError failure) {
      countProviderFailure();
    }

    CapturedInput<SummaryRequest> captured = captureInput(requests);
    if (!captured.validShape) {
      return countedSummaries(Collections.<SummaryResponse>emptyList());
    }
    if (captured.accessFailed) {
      return countedSummaries(invalidSummaries(captured.size));
    }
    try {
      return summarizeCaptured(captured.values, timeout, budget);
    } catch (RuntimeException failure) {
      countProviderFailure();
      return summarizeCaptured(captured.values, timeout, null);
    } catch (LinkageError failure) {
      countProviderFailure();
      return summarizeCaptured(captured.values, timeout, null);
    }
  }

  private List<SummaryResponse> summarizeCaptured(
      List<SummaryRequest> requests, Duration timeout, Budget budget) {
    if (requests.isEmpty()) {
      return countedSummaries(Collections.<SummaryResponse>emptyList());
    }
    for (SummaryRequest request : requests) {
      if (request == null) {
        List<SummaryResponse> invalid = new ArrayList<SummaryResponse>(requests.size());
        for (int index = 0; index < requests.size(); index++) {
          invalid.add(summaryError(
              Status.Code.INVALID_REQUEST, "summary request batch contains a null element"));
        }
        return countedSummaries(immutable(invalid));
      }
    }

    List<SummaryResponse> results =
        new ArrayList<SummaryResponse>(
            Collections.nCopies(requests.size(), (SummaryResponse) null));
    List<SummaryRequest> eligible = new ArrayList<SummaryRequest>();
    List<Integer> eligiblePositions = new ArrayList<Integer>();

    for (int position = 0; position < requests.size(); position++) {
      SummaryRequest request = requests.get(position);
      MetricSchema schema = declaredSchema(request.metric());
      if (schema != null) {
        String invalidReason = invalidSummaryReason(request, schema);
        if (invalidReason != null) {
          results.set(position, summaryError(Status.Code.INVALID_REQUEST, invalidReason));
          continue;
        }
      }
      if (timeout == null || timeout.isNegative()) {
        results.set(position, summaryError(
            Status.Code.INVALID_REQUEST, "timeout must be non-null and nonnegative"));
        continue;
      }
      if (timeout.isZero()) {
        results.set(position, summaryError(Status.Code.DEADLINE_EXCEEDED, DEADLINE_EXCEEDED));
        continue;
      }
      eligible.add(request);
      eligiblePositions.add(position);
    }

    if (eligible.isEmpty()) {
      return countedSummaries(immutable(results));
    }

    final List<SummaryRequest> backendInput =
        Collections.unmodifiableList(new ArrayList<SummaryRequest>(eligible));
    if (budget == null) {
      fillSummaryFallback(results, eligiblePositions, false);
      return countedSummaries(immutable(results));
    }
    if (!acceptingPlanning) {
      fillSummaryFallback(results, eligiblePositions, false);
      return countedSummaries(immutable(results));
    }
    if (budget.expired()) {
      countTimeout();
      fillSummaryFallback(results, eligiblePositions, true);
      return countedSummaries(immutable(results));
    }
    LocalPlanningCircuitBreaker.Attempt attempt =
        breaker.tryAcquire(budget.startedNanos());
    if (attempt == null) {
      fillSummaryFallback(results, eligiblePositions, false);
      return countedSummaries(immutable(results));
    }
    boolean attemptFailed = true;
    try {
      Invocation<ValidatedBatch<SummaryResponse>> invocation = invoke(
          budget,
          attempt,
          new BackendCall<ValidatedBatch<SummaryResponse>>() {
            @Override
            public ValidatedBatch<SummaryResponse> call(Duration remaining) {
              List<SummaryResponse> copied = copyValidSummaryResults(
                  backend.summarize(backendInput, remaining), backendInput.size());
              return copied == null
                  ? ValidatedBatch.<SummaryResponse>malformed()
                  : ValidatedBatch.valid(copied);
            }
          });

      if (!invocation.succeeded()) {
        fillSummaryFallback(results, eligiblePositions, invocation.timedOut);
        return countedSummaries(immutable(results));
      }

      ValidatedBatch<SummaryResponse> providerBatch = invocation.value;
      if (providerBatch.malformed) {
        countMalformedProviderResult();
        fillSummaryFallback(results, eligiblePositions, false);
        return countedSummaries(immutable(results));
      }
      List<SummaryResponse> providerResults = providerBatch.values;
      if (budget.expired()) {
        countTimeout();
        fillSummaryFallback(results, eligiblePositions, true);
        return countedSummaries(immutable(results));
      }

      for (int index = 0; index < eligiblePositions.size(); index++) {
        results.set(eligiblePositions.get(index), providerResults.get(index));
      }
      if (budget.expired()) {
        countTimeout();
        fillSummaryFallback(results, eligiblePositions, true);
        return countedSummaries(immutable(results));
      }
      List<SummaryResponse> counted = countedSummaries(immutable(results));
      attemptFailed = hasUnavailableSummary(providerResults);
      return counted;
    } finally {
      attempt.complete(attemptFailed);
    }
  }

  private void reconcileAcceptedDeclarations(
      List<SchemaStatus> results, List<MetricSchema> schemas) {
    synchronized (declarationLock) {
      for (int index = 0; index < results.size(); index++) {
        if (results.get(index).code() == SchemaStatus.Code.ACCEPTED) {
          MetricSchema schema = schemas.get(index);
          MetricSchema existing = declarations.get(schema.metric());
          if (existing == null || existing.equals(schema)) {
            declarations.put(schema.metric(), schema);
          }
        }
      }
    }
  }

  private MetricSchema declaredSchema(MetricVersionId metric) {
    synchronized (declarationLock) {
      return declarations.get(metric);
    }
  }

  private static Set<MetricVersionId> findBatchConflicts(List<MetricSchema> schemas) {
    Map<MetricVersionId, MetricSchema> firstByMetric = new HashMap<MetricVersionId, MetricSchema>();
    Set<MetricVersionId> conflicts = new HashSet<MetricVersionId>();
    for (MetricSchema schema : schemas) {
      if (schema != null) {
        MetricSchema first = firstByMetric.get(schema.metric());
        if (first == null) {
          firstByMetric.put(schema.metric(), schema);
        } else if (!first.equals(schema)) {
          conflicts.add(schema.metric());
        }
      }
    }
    return conflicts;
  }

  private static String invalidSummaryReason(SummaryRequest request, MetricSchema schema) {
    Map<String, DimValue.Kind> declared = new HashMap<String, DimValue.Kind>();
    for (DimensionSpec dimension : schema.dimensions()) {
      declared.put(dimension.name(), dimension.kind());
    }
    for (Map.Entry<String, DimValue> bound : request.bound().entrySet()) {
      DimValue.Kind expected = declared.get(bound.getKey());
      if (expected == null) {
        return "bound dimension is not declared";
      }
      if (bound.getValue().kind() != expected) {
        return "bound dimension kind does not match the declaration";
      }
      if (bound.getValue().canonicalBytes().length > DimValue.MAX_CANONICAL_BYTES) {
        return "bound dimension exceeds the canonical framing limit";
      }
    }
    if (request.bound().size() < schema.dimensions().size() && request.limit() != 0) {
      return "wildcard summary requests require limit zero";
    }
    return null;
  }

  private static boolean hasUnavailableDeclaration(List<SchemaStatus> results) {
    for (SchemaStatus result : results) {
      if (result.code() == SchemaStatus.Code.UNAVAILABLE) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasUnavailableSummary(List<SummaryResponse> results) {
    for (SummaryResponse result : results) {
      if (result.status().code() == Status.Code.UNAVAILABLE) {
        return true;
      }
    }
    return false;
  }

  private <T> Invocation<T> invoke(
      Budget budget,
      final LocalPlanningCircuitBreaker.Attempt attempt,
      final BackendCall<T> call) {
    Future<Invocation<T>> future;
    try {
      synchronized (submissionLock) {
        if (!acceptingPlanning) {
          attempt.executorQueueFailure();
          return Invocation.failure();
        }
        future = executor.submit(new java.util.concurrent.Callable<Invocation<T>>() {
          @Override
          public Invocation<T> call() {
            Duration remaining = budget.remaining();
            if (remaining.isZero()) {
              attempt.executorQueueFailure();
              return Invocation.timeout();
            }
            beginBackendInvocation();
            try {
              return Invocation.success(call.call(remaining));
            } catch (RuntimeException failure) {
              return Invocation.failure();
            } catch (LinkageError failure) {
              return Invocation.failure();
            } finally {
              endBackendInvocation();
            }
          }
        });
        pendingAttempts.put(future, attempt);
      }
    } catch (RejectedExecutionException rejected) {
      countExecutorRejection();
      attempt.executorQueueFailure();
      return Invocation.failure();
    } catch (RuntimeException failure) {
      countProviderFailure();
      return Invocation.failure();
    } catch (LinkageError failure) {
      countProviderFailure();
      return Invocation.failure();
    }

    try {
      Duration remaining = budget.remaining();
      if (remaining.isZero()) {
        future.cancel(true);
        countTimeout();
        return Invocation.timeout();
      }

      try {
        Invocation<T> result =
            future.get(durationToNanosSaturated(remaining), TimeUnit.NANOSECONDS);
        if (result.timedOut || budget.expired()) {
          future.cancel(true);
          countTimeout();
          return Invocation.timeout();
        }
        if (!result.succeeded()) {
          countProviderFailure();
        }
        return result;
      } catch (TimeoutException timeout) {
        future.cancel(true);
        countTimeout();
        return Invocation.timeout();
      } catch (InterruptedException interrupted) {
        future.cancel(true);
        Thread.currentThread().interrupt();
        countProviderFailure();
        return Invocation.failure();
      } catch (CancellationException cancelled) {
        countProviderFailure();
        return Invocation.failure();
      } catch (ExecutionException failed) {
        Throwable cause = failed.getCause();
        if (cause instanceof VirtualMachineError) {
          throw (VirtualMachineError) cause;
        }
        if (cause instanceof ThreadDeath) {
          throw (ThreadDeath) cause;
        }
        if (cause instanceof Error && !(cause instanceof LinkageError)) {
          throw (Error) cause;
        }
        countProviderFailure();
        return Invocation.failure();
      } catch (RuntimeException failure) {
        countProviderFailure();
        return Invocation.failure();
      } catch (LinkageError failure) {
        countProviderFailure();
        return Invocation.failure();
      }
    } finally {
      synchronized (submissionLock) {
        pendingAttempts.remove(future);
      }
    }
  }

  private static List<SchemaStatus> copyValidDeclarationResults(
      List<SchemaStatus> results, List<MetricSchema> schemas) {
    try {
      if (results == null || results.size() != schemas.size()) {
        return null;
      }
      List<SchemaStatus> copied = new ArrayList<SchemaStatus>(results.size());
      for (int index = 0; index < results.size(); index++) {
        SchemaStatus result = results.get(index);
        if (result == null ||
            result.metric() == null ||
            !result.metric().equals(schemas.get(index).metric()) ||
            result.code() == null) {
          return null;
        }
        if (result.code() == SchemaStatus.Code.ACCEPTED) {
          SchemaStatus.accepted(result.metric(), result.reason());
        } else {
          SchemaStatus.of(result.metric(), result.code(), result.reason());
        }
        copied.add(result);
      }
      return copied;
    } catch (RuntimeException failure) {
      return null;
    } catch (LinkageError failure) {
      return null;
    }
  }

  private static List<SummaryResponse> copyValidSummaryResults(
      List<SummaryResponse> results, int expectedSize) {
    try {
      if (results == null || results.size() != expectedSize) {
        return null;
      }
      List<SummaryResponse> copied = new ArrayList<SummaryResponse>(results.size());
      for (int index = 0; index < results.size(); index++) {
        SummaryResponse response = results.get(index);
        if (response == null || response.status() == null || response.status().code() == null) {
          return null;
        }
        Status status = response.status();
        if (status.code() == Status.Code.OK) {
          if (status.reason() != null || response.coverage() == null) {
            return null;
          }
          Summary summary = response.summary();
          if (summary != null && (
              summary.count() <= 0 ||
              !Double.isFinite(summary.mean()) ||
              !Double.isFinite(summary.min()) ||
              !Double.isFinite(summary.max()) ||
              summary.min() > summary.mean() ||
              summary.mean() > summary.max() ||
              summary.firstObservedMs() > summary.lastObservedMs())) {
            return null;
          }
        } else {
          Status.of(status.code(), status.reason());
          if (response.coverage() != null || response.summary() != null) {
            return null;
          }
        }
        copied.add(response);
      }
      return copied;
    } catch (RuntimeException failure) {
      return null;
    } catch (LinkageError failure) {
      return null;
    }
  }

  private static void fillUnresolvedDeclarationFallback(
      List<SchemaStatus> results,
      List<MetricSchema> schemas,
      Set<MetricVersionId> conflicts) {
    for (int position = 0; position < results.size(); position++) {
      if (results.get(position) != null) {
        continue;
      }
      MetricSchema schema = schemas.get(position);
      if (schema == null) {
        results.set(position, SchemaStatus.of(
            null, SchemaStatus.Code.INVALID_REQUEST, "schema must not be null"));
      } else if (conflicts.contains(schema.metric())) {
        results.set(position, SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INCOMPATIBLE,
            "conflicting canonical schemas in one declaration batch"));
      } else {
        results.set(position, SchemaStatus.of(
            schema.metric(), SchemaStatus.Code.UNAVAILABLE, PROVIDER_UNAVAILABLE));
      }
    }
  }

  private static void fillDeclarationFallback(
      List<SchemaStatus> results,
      List<List<Integer>> positions,
      List<MetricSchema> schemas,
      String reason) {
    for (int unique = 0; unique < positions.size(); unique++) {
      SchemaStatus fallback = SchemaStatus.of(
          schemas.get(unique).metric(), SchemaStatus.Code.UNAVAILABLE, reason);
      for (Integer position : positions.get(unique)) {
        results.set(position, fallback);
      }
    }
  }

  private static void fillSummaryFallback(
      List<SummaryResponse> results, List<Integer> positions, boolean timedOut) {
    SummaryResponse fallback = summaryError(
        timedOut ? Status.Code.DEADLINE_EXCEEDED : Status.Code.UNAVAILABLE,
        timedOut ? DEADLINE_EXCEEDED : PROVIDER_UNAVAILABLE);
    for (Integer position : positions) {
      results.set(position, fallback);
    }
  }

  private List<SchemaStatus> declarationBoundaryFallback(
      List<MetricSchema> schemas, String reason) {
    CapturedInput<MetricSchema> captured = captureInput(schemas);
    if (!captured.validShape) {
      return countedDeclarations(Collections.<SchemaStatus>emptyList());
    }
    if (captured.accessFailed) {
      return countedDeclarations(invalidDeclarations(captured.size));
    }
    return countedDeclarations(unavailableDeclarations(captured.values, reason));
  }

  private List<SummaryResponse> summaryBoundaryFallback(
      List<SummaryRequest> requests, String reason) {
    CapturedInput<SummaryRequest> captured = captureInput(requests);
    if (!captured.validShape) {
      return countedSummaries(Collections.<SummaryResponse>emptyList());
    }
    if (captured.accessFailed) {
      return countedSummaries(invalidSummaries(captured.size));
    }
    return countedSummaries(unavailableSummaries(captured.size, reason));
  }

  private static List<SchemaStatus> invalidDeclarations(int size) {
    List<SchemaStatus> results = new ArrayList<SchemaStatus>(size);
    for (int index = 0; index < size; index++) {
      results.add(SchemaStatus.of(
          null, SchemaStatus.Code.INVALID_REQUEST, "declaration batch could not be inspected"));
    }
    return immutable(results);
  }

  private static List<SchemaStatus> unavailableDeclarations(List<MetricSchema> schemas) {
    return unavailableDeclarations(schemas, PROVIDER_UNAVAILABLE);
  }

  private static List<SchemaStatus> unavailableDeclarations(
      List<MetricSchema> schemas, String reason) {
    List<SchemaStatus> results = new ArrayList<SchemaStatus>(schemas.size());
    for (MetricSchema schema : schemas) {
      if (schema == null) {
        results.add(SchemaStatus.of(
            null, SchemaStatus.Code.INVALID_REQUEST, "schema must not be null"));
      } else {
        results.add(SchemaStatus.of(
            schema.metric(), SchemaStatus.Code.UNAVAILABLE, reason));
      }
    }
    return immutable(results);
  }

  private static List<SummaryResponse> invalidSummaries(int size) {
    List<SummaryResponse> results = new ArrayList<SummaryResponse>(size);
    for (int index = 0; index < size; index++) {
      results.add(summaryError(
          Status.Code.INVALID_REQUEST, "summary batch could not be inspected"));
    }
    return immutable(results);
  }

  private static List<SummaryResponse> unavailableSummaries(int size) {
    return unavailableSummaries(size, PROVIDER_UNAVAILABLE);
  }

  private static List<SummaryResponse> unavailableSummaries(int size, String reason) {
    List<SummaryResponse> results = new ArrayList<SummaryResponse>(size);
    for (int index = 0; index < size; index++) {
      results.add(summaryError(Status.Code.UNAVAILABLE, reason));
    }
    return immutable(results);
  }

  private static <T> CapturedInput<T> captureInput(List<T> input) {
    if (input == null) {
      return CapturedInput.invalidShape();
    }
    final int size;
    try {
      size = input.size();
    } catch (RuntimeException failure) {
      return CapturedInput.invalidShape();
    } catch (LinkageError failure) {
      return CapturedInput.invalidShape();
    }
    if (size < 0 || size > MAX_BATCH_SIZE) {
      return CapturedInput.invalidShape();
    }

    List<T> copied = new ArrayList<T>(size);
    try {
      for (int index = 0; index < size; index++) {
        copied.add(input.get(index));
      }
    } catch (RuntimeException failure) {
      return CapturedInput.accessFailure(size);
    } catch (LinkageError failure) {
      return CapturedInput.accessFailure(size);
    }
    return CapturedInput.success(Collections.unmodifiableList(copied));
  }

  private List<SchemaStatus> countedDeclarations(List<SchemaStatus> results) {
    synchronized (counterLock) {
      declareCalls++;
      for (SchemaStatus result : results) {
        SchemaStatus.Code code = result.code();
        declarationOutcomes.put(code, declarationOutcomes.get(code) + 1L);
      }
    }
    return results;
  }

  private List<SummaryResponse> countedSummaries(List<SummaryResponse> results) {
    synchronized (counterLock) {
      summaryCalls++;
      for (SummaryResponse response : results) {
        Status.Code code = response.status().code();
        summaryOutcomes.put(code, summaryOutcomes.get(code) + 1L);
        if (response.coverage() == com.nvidia.spark.history.Coverage.WINDOW_CLIPPED) {
          summaryWindowClipped++;
        }
        if (response.summary() != null) {
          long count = response.summary().count();
          summaryRows = Long.MAX_VALUE - summaryRows < count
              ? Long.MAX_VALUE
              : summaryRows + count;
        }
      }
    }
    return results;
  }

  private void countTimeout() {
    synchronized (counterLock) {
      timeoutCalls++;
    }
  }

  private void countMalformedProviderResult() {
    synchronized (counterLock) {
      malformedProviderResults++;
    }
  }

  private void countExecutorRejection() {
    synchronized (counterLock) {
      executorRejections++;
    }
  }

  private void countProviderFailure() {
    synchronized (counterLock) {
      providerFailures++;
    }
  }

  PlanningCounterSnapshot counters() {
    synchronized (counterLock) {
      return new PlanningCounterSnapshot(
          declareCalls,
          summaryCalls,
          summaryWindowClipped,
          summaryRows,
          timeoutCalls,
          malformedProviderResults,
          executorRejections,
          providerFailures,
          suppressedCalls,
          recordCalls,
          recordDropped,
          declarationOutcomes,
          summaryOutcomes);
    }
  }

  private static SummaryResponse summaryError(Status.Code code, String reason) {
    return SummaryResponse.error(Status.of(code, reason));
  }

  private static long remainingWaitNanos(long startedNanos, long timeoutNanos) {
    long elapsedNanos = System.nanoTime() - startedNanos;
    if (elapsedNanos < 0L) {
      return 0L;
    }
    long remainingNanos = timeoutNanos - elapsedNanos;
    return remainingNanos <= 0L ? 0L : remainingNanos;
  }

  private static long durationToNanosSaturated(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  private static <T> List<T> immutable(List<T> values) {
    return Collections.unmodifiableList(values);
  }

  private interface BackendCall<T> {
    T call(Duration remaining);
  }

  private static final class CapturedInput<T> {
    private final List<T> values;
    private final int size;
    private final boolean validShape;
    private final boolean accessFailed;

    private CapturedInput(
        List<T> values, int size, boolean validShape, boolean accessFailed) {
      this.values = values;
      this.size = size;
      this.validShape = validShape;
      this.accessFailed = accessFailed;
    }

    private static <T> CapturedInput<T> success(List<T> values) {
      return new CapturedInput<T>(values, values.size(), true, false);
    }

    private static <T> CapturedInput<T> accessFailure(int size) {
      return new CapturedInput<T>(null, size, true, true);
    }

    private static <T> CapturedInput<T> invalidShape() {
      return new CapturedInput<T>(null, 0, false, false);
    }
  }

  private static final class ValidatedBatch<T> {
    private final List<T> values;
    private final boolean malformed;

    private ValidatedBatch(List<T> values, boolean malformed) {
      this.values = values;
      this.malformed = malformed;
    }

    private static <T> ValidatedBatch<T> valid(List<T> values) {
      return new ValidatedBatch<T>(values, false);
    }

    private static <T> ValidatedBatch<T> malformed() {
      return new ValidatedBatch<T>(null, true);
    }
  }

  private static final class Invocation<T> {
    private final T value;
    private final boolean succeeded;
    private final boolean timedOut;

    private Invocation(T value, boolean succeeded, boolean timedOut) {
      this.value = value;
      this.succeeded = succeeded;
      this.timedOut = timedOut;
    }

    private static <T> Invocation<T> success(T value) {
      return new Invocation<T>(value, true, false);
    }

    private static <T> Invocation<T> failure() {
      return new Invocation<T>(null, false, false);
    }

    private static <T> Invocation<T> timeout() {
      return new Invocation<T>(null, false, true);
    }

    private boolean succeeded() {
      return succeeded;
    }
  }

  private static final class Budget {
    private final Ticker ticker;
    private final long startedNanos;
    private final long timeoutNanos;

    private Budget(Ticker ticker, long startedNanos, long timeoutNanos) {
      this.ticker = ticker;
      this.startedNanos = startedNanos;
      this.timeoutNanos = timeoutNanos;
    }

    private static Budget start(Duration timeout, Ticker ticker) {
      return new Budget(
          ticker,
          ticker.readNanos(),
          timeout == null || timeout.isNegative()
              ? 0L
              : durationToNanosSaturated(timeout));
    }

    private long startedNanos() {
      return startedNanos;
    }

    private Duration remaining() {
      long elapsed = ticker.readNanos() - startedNanos;
      if (elapsed < 0L) {
        return Duration.ZERO;
      }
      long remaining = timeoutNanos - elapsed;
      return remaining <= 0L ? Duration.ZERO : Duration.ofNanos(remaining);
    }

    private boolean expired() {
      return remaining().isZero();
    }
  }

  private static final class DeclarationInput {
    private final MetricSchema schema;

    private DeclarationInput(MetricSchema schema) {
      this.schema = schema;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof DeclarationInput)) {
        return false;
      }
      DeclarationInput that = (DeclarationInput) other;
      return schema.equals(that.schema) &&
          schema.recommendedRetention().equals(that.schema.recommendedRetention());
    }

    @Override
    public int hashCode() {
      return 31 * schema.hashCode() + schema.recommendedRetention().hashCode();
    }
  }

  static final class PlanningCounterSnapshot {
    private final long declareCalls;
    private final long summaryCalls;
    private final long summaryWindowClipped;
    private final long summaryRows;
    private final long timeoutCalls;
    private final long malformedProviderResults;
    private final long executorRejections;
    private final long providerFailures;
    private final long suppressedCalls;
    private final long recordCalls;
    private final long recordDropped;
    private final EnumMap<SchemaStatus.Code, Long> declarationOutcomes;
    private final EnumMap<Status.Code, Long> summaryOutcomes;

    private PlanningCounterSnapshot(
        long declareCalls,
        long summaryCalls,
        long summaryWindowClipped,
        long summaryRows,
        long timeoutCalls,
        long malformedProviderResults,
        long executorRejections,
        long providerFailures,
        long suppressedCalls,
        long recordCalls,
        long recordDropped,
        EnumMap<SchemaStatus.Code, Long> declarationOutcomes,
        EnumMap<Status.Code, Long> summaryOutcomes) {
      this.declareCalls = declareCalls;
      this.summaryCalls = summaryCalls;
      this.summaryWindowClipped = summaryWindowClipped;
      this.summaryRows = summaryRows;
      this.timeoutCalls = timeoutCalls;
      this.malformedProviderResults = malformedProviderResults;
      this.executorRejections = executorRejections;
      this.providerFailures = providerFailures;
      this.suppressedCalls = suppressedCalls;
      this.recordCalls = recordCalls;
      this.recordDropped = recordDropped;
      this.declarationOutcomes =
          new EnumMap<SchemaStatus.Code, Long>(declarationOutcomes);
      this.summaryOutcomes = new EnumMap<Status.Code, Long>(summaryOutcomes);
    }

    long declareCallCount() {
      return declareCalls;
    }

    long summaryCallCount() {
      return summaryCalls;
    }

    long summaryWindowClippedCount() {
      return summaryWindowClipped;
    }

    long summaryRowCount() {
      return summaryRows;
    }

    long timeoutCallCount() {
      return timeoutCalls;
    }

    long malformedProviderResultCount() {
      return malformedProviderResults;
    }

    long executorRejectionCount() {
      return executorRejections;
    }

    long providerFailureCount() {
      return providerFailures;
    }

    long suppressedCallCount() {
      return suppressedCalls;
    }

    long recordCallCount() {
      return recordCalls;
    }

    long recordDroppedCount() {
      return recordDropped;
    }

    long declarationOutcomeCount(SchemaStatus.Code code) {
      Long count = declarationOutcomes.get(Objects.requireNonNull(code, "code"));
      return count == null ? 0L : count;
    }

    long summaryOutcomeCount(Status.Code code) {
      Long count = summaryOutcomes.get(Objects.requireNonNull(code, "code"));
      return count == null ? 0L : count;
    }
  }
}
