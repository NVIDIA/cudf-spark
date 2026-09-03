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

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

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
import com.nvidia.spark.history.Summary;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.WriteResult;

/**
 * Explicit synchronous driver-local backend for conformance tests and early prototypes.
 *
 * <p>Construction never registers this backend, reads configuration, starts a thread, or performs
 * file or network I/O. Canonical declarations are permanent for this backend instance.
 */
final class LocalHistoryMetricsBackend implements HistoryMetricsBackend {
  private static final int MAX_BATCH_SIZE = 128;
  private static final int SNAPSHOT_COPY_DEADLINE_INTERVAL = 32;
  private static final BackendInfo INFO = new BackendInfo(
      HistoryMetricsApi.CURRENT_API_VERSION, "driver-local history metrics backend");
  private static final Ticker SYSTEM_TICKER = new Ticker() {
    @Override
    public long readNanos() {
      return System.nanoTime();
    }
  };
  private static final Comparator<LocalBackendTestHandle.StoredObservation> OLDEST_FIRST =
      new Comparator<LocalBackendTestHandle.StoredObservation>() {
        @Override
        public int compare(
            LocalBackendTestHandle.StoredObservation left,
            LocalBackendTestHandle.StoredObservation right) {
          int timestamp = Long.compare(
              left.internalStamped().observation().timestampMs(),
              right.internalStamped().observation().timestampMs());
          return timestamp != 0
              ? timestamp
              : Long.compare(left.acceptanceOrdinal(), right.acceptanceOrdinal());
        }
      };

  private final HistoryMetricCatalog catalog;
  private final Clock providerClock;
  private final Duration maximumPlanningAge;
  private final Ticker ticker;
  private final Map<MetricVersionId, Declaration> declarations =
      new HashMap<MetricVersionId, Declaration>();
  private final Map<MetricVersionId, List<LocalBackendTestHandle.StoredObservation>> observations =
      new HashMap<MetricVersionId, List<LocalBackendTestHandle.StoredObservation>>();
  private final EnumMap<SchemaStatus.Code, Long> declarationOutcomes =
      new EnumMap<SchemaStatus.Code, Long>(SchemaStatus.Code.class);
  private final EnumMap<Status.Code, Long> recordOutcomes =
      new EnumMap<Status.Code, Long>(Status.Code.class);
  private final EnumMap<Status.Code, Long> summaryOutcomes =
      new EnumMap<Status.Code, Long>(Status.Code.class);
  private final EnumMap<LocalBackendTestHandle.RejectionReason, Long> rejectionReasons =
      new EnumMap<LocalBackendTestHandle.RejectionReason, Long>(
          LocalBackendTestHandle.RejectionReason.class);
  private final LocalBackendTestHandle testHandle;
  private final Object summaryCounterLock = new Object();

  private long nextAcceptanceOrdinal = 1L;
  private long recordAccepted;
  private long recordRejected;
  private long summaryBatchCount;
  private long summaryWindowClipped;
  private long summaryRowsExamined;
  private boolean closed;

  private LocalHistoryMetricsBackend(
      HistoryMetricCatalog catalog,
      Clock providerClock,
      Duration maximumPlanningAge,
      Ticker ticker) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.providerClock = Objects.requireNonNull(providerClock, "providerClock");
    this.maximumPlanningAge =
        Objects.requireNonNull(maximumPlanningAge, "maximumPlanningAge");
    if (maximumPlanningAge.isNegative()) {
      throw new IllegalArgumentException("maximumPlanningAge must not be negative");
    }
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    initializeCounters();
    testHandle = new LocalBackendTestHandle(this);
  }

  /**
   * Creates an isolated backend with an explicit catalog, provider clock, and planning-age
   * envelope.
   */
  static LocalHistoryMetricsBackend create(
      HistoryMetricCatalog catalog, Clock providerClock, Duration maximumPlanningAge) {
    return new LocalHistoryMetricsBackend(
        catalog, providerClock, maximumPlanningAge, SYSTEM_TICKER);
  }

  static LocalHistoryMetricsBackend restore(
      HistoryMetricCatalog catalog,
      Clock providerClock,
      Duration maximumPlanningAge,
      LocalSnapshotState state) {
    Objects.requireNonNull(state, "state");
    LocalHistoryMetricsBackend backend =
        new LocalHistoryMetricsBackend(
            catalog, providerClock, maximumPlanningAge, SYSTEM_TICKER);
    for (LocalDeclarationSnapshot declaration : state.declarations()) {
      MetricSchema schema = declaration.schema();
      backend.declarations.put(
          schema.metric(), new Declaration(schema, declaration.effectiveRetention()));
    }
    for (LocalObservationSnapshot observation : state.observations()) {
      StampedObservation stamped = new StampedObservation(
          new Observation(
              observation.metric(),
              observation.dimensions(),
              observation.value(),
              observation.timestampMs()),
          observation.provenance());
      if (backend.validate(stamped) != null) {
        throw new IllegalArgumentException(
            "snapshot observation is incompatible with its declaration");
      }
      List<LocalBackendTestHandle.StoredObservation> stored =
          backend.observations.get(observation.metric());
      if (stored == null) {
        stored = new ArrayList<LocalBackendTestHandle.StoredObservation>();
        backend.observations.put(observation.metric(), stored);
      }
      stored.add(new LocalBackendTestHandle.StoredObservation(
          stamped, observation.acceptanceOrdinal()));
    }
    backend.nextAcceptanceOrdinal = state.nextAcceptanceOrdinal();
    return backend;
  }

  static LocalHistoryMetricsBackend createForTest(
      HistoryMetricCatalog catalog,
      Clock providerClock,
      Duration maximumPlanningAge,
      Ticker ticker) {
    return new LocalHistoryMetricsBackend(
        catalog, providerClock, maximumPlanningAge, ticker);
  }

  /** Returns the provider-specific, non-production inspection handle. */
  LocalBackendTestHandle testHandle() {
    return testHandle;
  }

  @Override
  public BackendInfo info() {
    return INFO;
  }

  @Override
  public synchronized List<SchemaStatus> declare(
      List<MetricSchema> schemas, Duration timeout) {
    if (schemas == null || schemas.size() > MAX_BATCH_SIZE) {
      return Collections.emptyList();
    }
    if (schemas.isEmpty()) {
      return Collections.emptyList();
    }

    Set<MetricVersionId> conflicts = findBatchConflicts(schemas);
    boolean invalidTimeout = timeout == null || timeout.isNegative();
    Map<DeclarationInput, SchemaStatus> duplicateResults =
        new HashMap<DeclarationInput, SchemaStatus>();
    List<SchemaStatus> results = new ArrayList<SchemaStatus>(schemas.size());
    for (MetricSchema schema : schemas) {
      DeclarationInput input = schema == null ? null : new DeclarationInput(schema);
      SchemaStatus duplicate = input == null ? null : duplicateResults.get(input);
      SchemaStatus result;
      if (duplicate != null) {
        result = duplicate;
      } else if (schema == null) {
        result = SchemaStatus.of(
            null, SchemaStatus.Code.INVALID_REQUEST, "schema must not be null");
      } else if (conflicts.contains(schema.metric())) {
        result = SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INCOMPATIBLE,
            "conflicting canonical schemas in one declaration batch");
      } else if (invalidTimeout) {
        result = SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INVALID_REQUEST,
            "timeout must be non-null and nonnegative");
      } else if (timeout.isZero() || closed) {
        result = SchemaStatus.of(
            schema.metric(), SchemaStatus.Code.UNAVAILABLE, "backend is unavailable");
      } else if (!catalog.find(schema.metric().metricId()).isPresent()) {
        result = SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INVALID_REQUEST,
            "metric ID is not present in the supplied catalog");
      } else {
        result = establish(schema);
      }
      if (input != null && duplicate == null) {
        duplicateResults.put(input, result);
      }
      results.add(result);
      increment(declarationOutcomes, result.code(), 1);
    }
    return Collections.unmodifiableList(results);
  }

  @Override
  public synchronized WriteResult record(List<StampedObservation> batch) {
    if (batch == null || batch.isEmpty()) {
      return counted(WriteResult.ok(0));
    }
    int size = batch.size();
    if (closed) {
      increment(rejectionReasons,
          LocalBackendTestHandle.RejectionReason.BACKEND_FAILURE, size);
      return counted(WriteResult.unavailable(0, size, "backend is closed"));
    }

    List<LocalBackendTestHandle.RejectionReason> validations =
        new ArrayList<LocalBackendTestHandle.RejectionReason>(size);
    try {
      for (StampedObservation stamped : batch) {
        validations.add(validate(stamped));
      }
    } catch (RuntimeException failure) {
      increment(rejectionReasons,
          LocalBackendTestHandle.RejectionReason.BACKEND_FAILURE, size);
      return counted(WriteResult.unavailable(0, size, "backend validation failed"));
    }

    Set<LocalBackendTestHandle.RejectionReason> distinct =
        new HashSet<LocalBackendTestHandle.RejectionReason>();
    int validCount = 0;
    for (LocalBackendTestHandle.RejectionReason reason : validations) {
      if (reason == null) {
        validCount++;
      } else {
        distinct.add(reason);
      }
    }
    if (!distinct.isEmpty()) {
      for (LocalBackendTestHandle.RejectionReason reason : validations) {
        increment(
            rejectionReasons,
            reason == null
                ? LocalBackendTestHandle.RejectionReason.BATCH_ABORTED
                : reason,
            1);
      }
      if (validCount == 0 && distinct.size() == 1) {
        LocalBackendTestHandle.RejectionReason reason = distinct.iterator().next();
        if (reason == LocalBackendTestHandle.RejectionReason.NOT_DECLARED) {
          return counted(WriteResult.rejected(
              size, Status.of(Status.Code.NOT_DECLARED, "metric is not declared")));
        }
        if (reason != LocalBackendTestHandle.RejectionReason.BACKEND_FAILURE) {
          return counted(WriteResult.rejected(
              size, Status.of(Status.Code.INVALID_REQUEST, "observation is malformed")));
        }
      }
      return counted(WriteResult.unavailable(
          0, size, "write batch contains mixed acceptance or rejection reasons"));
    }

    if (size > Long.MAX_VALUE - nextAcceptanceOrdinal) {
      increment(rejectionReasons,
          LocalBackendTestHandle.RejectionReason.BACKEND_FAILURE, size);
      return counted(WriteResult.unavailable(
          0, size, "acceptance ordinal space is exhausted"));
    }
    for (StampedObservation stamped : batch) {
      store(stamped);
    }
    return counted(WriteResult.ok(size));
  }

  @Override
  public List<SummaryResponse> summarize(
      List<SummaryRequest> requests, Duration timeout) {
    long startedNanos = ticker.readNanos();
    SummaryWork work = new SummaryWork();
    if (requests == null || requests.size() > MAX_BATCH_SIZE) {
      return countedSummary(Collections.<SummaryResponse>emptyList(), work);
    }
    if (requests.isEmpty()) {
      return countedSummary(Collections.<SummaryResponse>emptyList(), work);
    }
    for (SummaryRequest request : requests) {
      if (request == null) {
        return countedSummary(
            repeatedSummaryError(
                requests.size(),
                Status.Code.INVALID_REQUEST,
                "summary request batch contains a null element"),
            work);
      }
    }
    if (timeout == null || timeout.isNegative()) {
      return countedSummary(
          repeatedSummaryError(
              requests.size(), Status.Code.INVALID_REQUEST, "invalid summary timeout"),
          work);
    }
    if (timeout.isZero()) {
      return countedSummary(
          repeatedSummaryError(
              requests.size(), Status.Code.DEADLINE_EXCEEDED, "summary timeout expired"),
          work);
    }

    TimeoutBudget budget = new TimeoutBudget(startedNanos, timeout, ticker);
    List<PreparedSummary> prepared = new ArrayList<PreparedSummary>(requests.size());
    long providerNowMs = 0L;
    RuntimeException clockFailure = null;
    boolean snapshotCopyAborted = false;
    synchronized (this) {
      Map<MetricVersionId, List<LocalBackendTestHandle.StoredObservation>> snapshots =
          new HashMap<MetricVersionId, List<LocalBackendTestHandle.StoredObservation>>();
      boolean hasEligibleRequest = false;
      for (SummaryRequest request : requests) {
        Declaration declaration = declarations.get(request.metric());
        if (declaration == null) {
          prepared.add(PreparedSummary.error(
              Status.Code.NOT_DECLARED, "metric version is not declared"));
        } else {
          String invalidReason = invalidSummaryReason(request, declaration.schema);
          if (invalidReason != null) {
            prepared.add(PreparedSummary.error(Status.Code.INVALID_REQUEST, invalidReason));
          } else if (closed) {
            prepared.add(PreparedSummary.error(Status.Code.UNAVAILABLE, "backend is closed"));
          } else {
            hasEligibleRequest = true;
            List<LocalBackendTestHandle.StoredObservation> snapshot =
                snapshots.get(request.metric());
            if (snapshot == null) {
              List<LocalBackendTestHandle.StoredObservation> stored =
                  observations.get(request.metric());
              if (stored == null || snapshotCopyAborted) {
                snapshot = Collections.<LocalBackendTestHandle.StoredObservation>emptyList();
              } else {
                snapshot = copySummarySnapshot(stored, budget);
                if (snapshot == null) {
                  snapshotCopyAborted = true;
                  snapshot = Collections.<LocalBackendTestHandle.StoredObservation>emptyList();
                } else {
                  snapshots.put(request.metric(), snapshot);
                }
              }
            }
            prepared.add(PreparedSummary.eligible(request, declaration, snapshot));
          }
        }
      }
      if (hasEligibleRequest && !budget.expired() && !snapshotCopyAborted) {
        try {
          providerNowMs = providerClock.millis();
        } catch (RuntimeException failure) {
          clockFailure = failure;
        }
      }
    }

    boolean budgetExpired = budget.expired();
    boolean deadlineReached = snapshotCopyAborted || budgetExpired;
    List<SummaryResponse> results = new ArrayList<SummaryResponse>(requests.size());
    for (PreparedSummary item : prepared) {
      if (item.error != null) {
        results.add(item.error);
      } else if (deadlineReached) {
        results.add(deadlineExceeded());
      } else if (clockFailure != null) {
        results.add(SummaryResponse.error(
            Status.of(Status.Code.UNAVAILABLE, "provider clock is unavailable")));
      } else {
        SummaryResponse response = summarizeOne(item, providerNowMs, budget, work);
        if (response == null) {
          deadlineReached = true;
          results.add(deadlineExceeded());
        } else {
          results.add(response);
        }
      }
    }
    return countedSummary(Collections.unmodifiableList(results), work);
  }

  private static List<LocalBackendTestHandle.StoredObservation> copySummarySnapshot(
      List<LocalBackendTestHandle.StoredObservation> stored,
      TimeoutBudget budget) {
    if (Thread.currentThread().isInterrupted()) {
      return null;
    }
    List<LocalBackendTestHandle.StoredObservation> snapshot =
        new ArrayList<LocalBackendTestHandle.StoredObservation>(stored.size());
    for (int index = 0; index < stored.size(); index++) {
      if (index != 0 && index % SNAPSHOT_COPY_DEADLINE_INTERVAL == 0 &&
          (Thread.currentThread().isInterrupted() || budget.expired())) {
        return null;
      }
      snapshot.add(stored.get(index));
    }
    return snapshot;
  }

  @Override
  public synchronized void close() {
    closed = true;
  }

  synchronized java.util.Optional<MetricSchema> declaration(MetricVersionId metric) {
    Declaration declaration = declarations.get(metric);
    return declaration == null
        ? java.util.Optional.<MetricSchema>empty()
        : java.util.Optional.of(declaration.schema);
  }

  synchronized java.util.Optional<Retention> effectiveRetention(MetricVersionId metric) {
    Declaration declaration = declarations.get(metric);
    return declaration == null
        ? java.util.Optional.<Retention>empty()
        : java.util.Optional.of(declaration.effectiveRetention);
  }

  synchronized int declarationCount() {
    return declarations.size();
  }

  synchronized List<LocalBackendTestHandle.StoredObservation> observations(
      MetricVersionId metric) {
    List<LocalBackendTestHandle.StoredObservation> stored = observations.get(metric);
    if (stored == null) {
      return Collections.emptyList();
    }
    List<LocalBackendTestHandle.StoredObservation> copied =
        new ArrayList<LocalBackendTestHandle.StoredObservation>(stored.size());
    for (LocalBackendTestHandle.StoredObservation observation : stored) {
      copied.add(new LocalBackendTestHandle.StoredObservation(
          observation.stamped(), observation.acceptanceOrdinal()));
    }
    return Collections.unmodifiableList(copied);
  }

  synchronized List<LocalDeclarationSnapshot> declarationSnapshots() {
    List<Declaration> ordered = new ArrayList<Declaration>(declarations.values());
    Collections.sort(ordered, new Comparator<Declaration>() {
      @Override
      public int compare(Declaration left, Declaration right) {
        return Long.compare(left.schema.metric().packedKey(), right.schema.metric().packedKey());
      }
    });
    List<LocalDeclarationSnapshot> snapshots =
        new ArrayList<LocalDeclarationSnapshot>(ordered.size());
    for (Declaration declaration : ordered) {
      snapshots.add(new LocalDeclarationSnapshot(
          declaration.schema, declaration.effectiveRetention));
    }
    return Collections.unmodifiableList(snapshots);
  }

  synchronized List<LocalObservationSnapshot> observationSnapshots() {
    List<LocalBackendTestHandle.StoredObservation> ordered =
        new ArrayList<LocalBackendTestHandle.StoredObservation>();
    for (List<LocalBackendTestHandle.StoredObservation> metricObservations :
        observations.values()) {
      ordered.addAll(metricObservations);
    }
    Collections.sort(ordered, new Comparator<LocalBackendTestHandle.StoredObservation>() {
      @Override
      public int compare(
          LocalBackendTestHandle.StoredObservation left,
          LocalBackendTestHandle.StoredObservation right) {
        return Long.compare(left.acceptanceOrdinal(), right.acceptanceOrdinal());
      }
    });
    List<LocalObservationSnapshot> snapshots =
        new ArrayList<LocalObservationSnapshot>(ordered.size());
    for (LocalBackendTestHandle.StoredObservation stored : ordered) {
      StampedObservation stamped = stored.stamped();
      Observation observation = stamped.observation();
      snapshots.add(new LocalObservationSnapshot(
          observation.metric(),
          observation.dimensions(),
          observation.value(),
          observation.timestampMs(),
          stamped.provenance(),
          stored.acceptanceOrdinal()));
    }
    return Collections.unmodifiableList(snapshots);
  }

  synchronized long nextAcceptanceOrdinal() {
    return nextAcceptanceOrdinal;
  }

  synchronized LocalSnapshotState captureSnapshotState() {
    return LocalSnapshotState.capture(
        catalog,
        declarationSnapshots(),
        observationSnapshots(),
        nextAcceptanceOrdinal);
  }

  synchronized LocalBackendTestHandle.CounterSnapshot counterSnapshot() {
    synchronized (summaryCounterLock) {
      return new LocalBackendTestHandle.CounterSnapshot(
          declarationOutcomes,
          recordOutcomes,
          summaryOutcomes,
          rejectionReasons,
          recordAccepted,
          recordRejected,
          summaryBatchCount,
          summaryWindowClipped,
          summaryRowsExamined);
    }
  }

  Clock providerClock() {
    return providerClock;
  }

  private SchemaStatus establish(MetricSchema schema) {
    Declaration current = declarations.get(schema.metric());
    if (current != null) {
      if (!current.schema.equals(schema)) {
        return SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INCOMPATIBLE,
            "canonical schema conflicts with the permanent declaration");
      }
      return SchemaStatus.accepted(
          schema.metric(),
          retentionWarning(schema.recommendedRetention(), current.effectiveRetention));
    }

    Duration planningMaxAge = schema.recommendedRetention().planningMaxAge();
    String warning = null;
    if (planningMaxAge.compareTo(maximumPlanningAge) > 0) {
      planningMaxAge = maximumPlanningAge;
      warning = "planningMaxAge clamped to the provider maximum";
    }
    Retention effective =
        new Retention(planningMaxAge, schema.recommendedRetention().storageRetention());
    declarations.put(schema.metric(), new Declaration(schema, effective));
    return SchemaStatus.accepted(schema.metric(), warning);
  }

  private static String retentionWarning(Retention recommended, Retention effective) {
    return recommended.equals(effective)
        ? null
        : "first effective retention remains unchanged";
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

  private LocalBackendTestHandle.RejectionReason validate(StampedObservation stamped) {
    if (stamped == null || stamped.observation() == null || stamped.provenance() == null) {
      return LocalBackendTestHandle.RejectionReason.MALFORMED_OBSERVATION;
    }
    Observation observation = stamped.observation();
    Declaration declaration = declarations.get(observation.metric());
    if (declaration == null) {
      return LocalBackendTestHandle.RejectionReason.NOT_DECLARED;
    }
    if (!Double.isFinite(observation.value())) {
      return LocalBackendTestHandle.RejectionReason.MALFORMED_OBSERVATION;
    }
    if (observation.dimensions().size() != declaration.schema.dimensions().size()) {
      return LocalBackendTestHandle.RejectionReason.DIMENSION_MISMATCH;
    }
    for (DimensionSpec dimension : declaration.schema.dimensions()) {
      DimValue value = observation.dimensions().get(dimension.name());
      if (value == null || value.kind() != dimension.kind() ||
          value.canonicalBytes().length > DimValue.MAX_CANONICAL_BYTES) {
        return LocalBackendTestHandle.RejectionReason.DIMENSION_MISMATCH;
      }
    }
    return null;
  }

  private void store(StampedObservation stamped) {
    List<LocalBackendTestHandle.StoredObservation> stored =
        observations.get(stamped.observation().metric());
    if (stored == null) {
      stored = new ArrayList<LocalBackendTestHandle.StoredObservation>();
      observations.put(stamped.observation().metric(), stored);
    }
    stored.add(new LocalBackendTestHandle.StoredObservation(
        stamped, nextAcceptanceOrdinal++));
  }

  private WriteResult counted(WriteResult result) {
    increment(recordOutcomes, result.status().code(), 1);
    recordAccepted += result.accepted();
    recordRejected += result.rejected();
    return result;
  }

  private void initializeCounters() {
    for (SchemaStatus.Code code : SchemaStatus.Code.values()) {
      declarationOutcomes.put(code, 0L);
    }
    for (Status.Code code : Status.Code.values()) {
      recordOutcomes.put(code, 0L);
      summaryOutcomes.put(code, 0L);
    }
    for (LocalBackendTestHandle.RejectionReason reason :
        LocalBackendTestHandle.RejectionReason.values()) {
      rejectionReasons.put(reason, 0L);
    }
  }

  private static String invalidSummaryReason(
      SummaryRequest request, MetricSchema schema) {
    Map<String, DimValue.Kind> declared = new HashMap<String, DimValue.Kind>();
    for (DimensionSpec dimension : schema.dimensions()) {
      declared.put(dimension.name(), dimension.kind());
    }
    for (Map.Entry<String, DimValue> bound : request.bound().entrySet()) {
      DimValue.Kind kind = declared.get(bound.getKey());
      if (kind == null) {
        return "bound dimension is not declared";
      }
      if (bound.getValue().kind() != kind) {
        return "bound dimension kind does not match the declaration";
      }
    }
    if (request.bound().size() < schema.dimensions().size() && request.limit() != 0) {
      return "wildcard summary requests require limit zero";
    }
    return null;
  }

  private static SummaryResponse summarizeOne(
      PreparedSummary item,
      long providerNowMs,
      TimeoutBudget budget,
      SummaryWork work) {
    Duration planningMaxAge = item.declaration.effectiveRetention.planningMaxAge();
    if (planningMaxAge.isZero()) {
      return budget.expired()
          ? null
          : SummaryResponse.ok(null, Coverage.WINDOW_CLIPPED);
    }

    long cutoffMs = subtractAgeSaturated(providerNowMs, planningMaxAge);
    long effectiveFromMs = Math.max(item.request.fromMs(), cutoffMs);
    Coverage coverage = effectiveFromMs == item.request.fromMs()
        ? Coverage.COMPLETE
        : Coverage.WINDOW_CLIPPED;
    SummaryAccumulator accumulator = new SummaryAccumulator();

    if (item.request.limit() == 0) {
      for (LocalBackendTestHandle.StoredObservation stored : item.observations) {
        if (budget.expired()) {
          return null;
        }
        Observation observation = stored.internalStamped().observation();
        work.rowsExamined++;
        if (eligible(observation, item.request, effectiveFromMs)) {
          accumulator.add(observation);
        }
      }
    } else {
      int initialCapacity = Math.max(
          1, Math.min(item.request.limit(), item.observations.size()));
      PriorityQueue<LocalBackendTestHandle.StoredObservation> newest =
          new PriorityQueue<LocalBackendTestHandle.StoredObservation>(
              initialCapacity, OLDEST_FIRST);
      for (LocalBackendTestHandle.StoredObservation stored : item.observations) {
        if (budget.expired()) {
          return null;
        }
        Observation observation = stored.internalStamped().observation();
        work.rowsExamined++;
        if (eligible(observation, item.request, effectiveFromMs)) {
          if (newest.size() < item.request.limit()) {
            newest.add(stored);
          } else if (OLDEST_FIRST.compare(stored, newest.peek()) > 0) {
            newest.remove();
            newest.add(stored);
          }
        }
      }
      for (LocalBackendTestHandle.StoredObservation stored : newest) {
        if (budget.expired()) {
          return null;
        }
        accumulator.add(stored.internalStamped().observation());
      }
    }

    if (budget.expired()) {
      return null;
    }
    return SummaryResponse.ok(accumulator.summary(), coverage);
  }

  private static boolean eligible(
      Observation observation, SummaryRequest request, long effectiveFromMs) {
    long timestampMs = observation.timestampMs();
    if (timestampMs < effectiveFromMs || timestampMs >= request.toMs()) {
      return false;
    }
    for (Map.Entry<String, DimValue> bound : request.bound().entrySet()) {
      if (!bound.getValue().equals(observation.dimensions().get(bound.getKey()))) {
        return false;
      }
    }
    return true;
  }

  private static long subtractAgeSaturated(long nowMs, Duration age) {
    try {
      long ageMs = age.toMillis();
      if (ageMs > 0 && nowMs < Long.MIN_VALUE + ageMs) {
        return Long.MIN_VALUE;
      }
      return nowMs - ageMs;
    } catch (ArithmeticException overflow) {
      BigInteger ageMs = BigInteger.valueOf(age.getSeconds())
          .multiply(BigInteger.valueOf(1_000L))
          .add(BigInteger.valueOf(age.getNano() / 1_000_000L));
      BigInteger cutoff = BigInteger.valueOf(nowMs).subtract(ageMs);
      return cutoff.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
          ? Long.MIN_VALUE
          : cutoff.longValue();
    }
  }

  private static SummaryResponse deadlineExceeded() {
    return SummaryResponse.error(
        Status.of(Status.Code.DEADLINE_EXCEEDED, "summary timeout expired"));
  }

  private List<SummaryResponse> countedSummary(
      List<SummaryResponse> results, SummaryWork work) {
    synchronized (summaryCounterLock) {
      summaryBatchCount++;
      summaryRowsExamined += work.rowsExamined;
      for (SummaryResponse result : results) {
        increment(summaryOutcomes, result.status().code(), 1);
        if (result.coverage() == Coverage.WINDOW_CLIPPED) {
          summaryWindowClipped++;
        }
      }
      return results;
    }
  }

  private static <K> void increment(Map<K, Long> counters, K key, long amount) {
    counters.put(key, counters.get(key) + amount);
  }

  private static List<SummaryResponse> repeatedSummaryError(
      int count, Status.Code code, String reason) {
    SummaryResponse response = SummaryResponse.error(Status.of(code, reason));
    List<SummaryResponse> results = new ArrayList<SummaryResponse>(count);
    for (int index = 0; index < count; index++) {
      results.add(response);
    }
    return Collections.unmodifiableList(results);
  }

  interface Ticker {
    long readNanos();
  }

  private static final class TimeoutBudget {
    private final long startedNanos;
    private final long timeoutNanos;
    private final Ticker ticker;

    private TimeoutBudget(long startedNanos, Duration timeout, Ticker ticker) {
      this.startedNanos = startedNanos;
      this.ticker = ticker;
      long nanos;
      try {
        nanos = timeout.toNanos();
      } catch (ArithmeticException overflow) {
        nanos = Long.MAX_VALUE;
      }
      timeoutNanos = nanos;
    }

    private boolean expired() {
      return ticker.readNanos() - startedNanos >= timeoutNanos;
    }
  }

  private static final class SummaryWork {
    private long rowsExamined;
  }

  private static final class PreparedSummary {
    private final SummaryRequest request;
    private final Declaration declaration;
    private final List<LocalBackendTestHandle.StoredObservation> observations;
    private final SummaryResponse error;

    private PreparedSummary(
        SummaryRequest request,
        Declaration declaration,
        List<LocalBackendTestHandle.StoredObservation> observations,
        SummaryResponse error) {
      this.request = request;
      this.declaration = declaration;
      this.observations = observations;
      this.error = error;
    }

    private static PreparedSummary eligible(
        SummaryRequest request,
        Declaration declaration,
        List<LocalBackendTestHandle.StoredObservation> observations) {
      return new PreparedSummary(request, declaration, observations, null);
    }

    private static PreparedSummary error(Status.Code code, String reason) {
      return new PreparedSummary(
          null, null, null, SummaryResponse.error(Status.of(code, reason)));
    }
  }

  private static final class SummaryAccumulator {
    private long count;
    private double scale;
    private double scaledSum;
    private double compensation;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private long firstObservedMs = Long.MAX_VALUE;
    private long lastObservedMs = Long.MIN_VALUE;

    private void add(Observation observation) {
      double value = observation.value();
      double absolute = Math.abs(value);
      if (absolute > scale) {
        double factor = scale == 0.0 ? 0.0 : scale / absolute;
        scaledSum *= factor;
        compensation *= factor;
        scale = absolute;
      }
      double scaledValue = scale == 0.0 ? 0.0 : value / scale;
      double next = scaledSum + scaledValue;
      if (Math.abs(scaledSum) >= Math.abs(scaledValue)) {
        compensation += (scaledSum - next) + scaledValue;
      } else {
        compensation += (scaledValue - next) + scaledSum;
      }
      scaledSum = next;
      count++;
      min = Math.min(min, value);
      max = Math.max(max, value);
      firstObservedMs = Math.min(firstObservedMs, observation.timestampMs());
      lastObservedMs = Math.max(lastObservedMs, observation.timestampMs());
    }

    private Summary summary() {
      if (count == 0) {
        return null;
      }
      double normalizedMean = (scaledSum + compensation) / count;
      double mean = normalizedMean * scale;
      if (!Double.isFinite(mean)) {
        mean = normalizedMean < 0.0 ? min : max;
      } else {
        mean = Math.max(min, Math.min(max, mean));
      }
      return Summary.of(count, mean, min, max, firstObservedMs, lastObservedMs);
    }
  }

  private static final class DeclarationInput {
    private final MetricSchema schema;

    private DeclarationInput(MetricSchema schema) {
      this.schema = schema;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
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

  private static final class Declaration {
    private final MetricSchema schema;
    private final Retention effectiveRetention;

    private Declaration(MetricSchema schema, Retention effectiveRetention) {
      this.schema = schema;
      this.effectiveRetention = effectiveRetention;
    }
  }
}
