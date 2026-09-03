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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;

/** Non-production inspection handle for the explicit local backend. */
final class LocalBackendTestHandle {
  /** Closed record-rejection reasons exposed only by the companion local artifact. */
  public enum RejectionReason {
    NOT_DECLARED,
    DIMENSION_MISMATCH,
    MALFORMED_OBSERVATION,
    BATCH_ABORTED,
    BACKEND_FAILURE
  }

  private final LocalHistoryMetricsBackend backend;

  LocalBackendTestHandle(LocalHistoryMetricsBackend backend) {
    this.backend = backend;
  }

  public Optional<MetricSchema> declaration(MetricVersionId metric) {
    return backend.declaration(Objects.requireNonNull(metric, "metric"));
  }

  public Optional<Retention> effectiveRetention(MetricVersionId metric) {
    return backend.effectiveRetention(Objects.requireNonNull(metric, "metric"));
  }

  public int declarationCount() {
    return backend.declarationCount();
  }

  public List<StoredObservation> observations(MetricVersionId metric) {
    return backend.observations(Objects.requireNonNull(metric, "metric"));
  }

  /** Returns the next unassigned ordinal so snapshot tests can preserve the sequence later. */
  public long nextAcceptanceOrdinal() {
    return backend.nextAcceptanceOrdinal();
  }

  /** Returns one immutable point-in-time counter snapshot. */
  public CounterSnapshot counters() {
    return backend.counterSnapshot();
  }

  public long rejectionCount(RejectionReason reason) {
    return counters().rejectionCount(reason);
  }

  /** Immutable stored record plus provider acceptance order. */
  public static final class StoredObservation {
    private final StampedObservation stamped;
    private final long acceptanceOrdinal;

    StoredObservation(StampedObservation stamped, long acceptanceOrdinal) {
      this.stamped = copyStamped(Objects.requireNonNull(stamped, "stamped"));
      this.acceptanceOrdinal = acceptanceOrdinal;
    }

    public StampedObservation stamped() {
      return copyStamped(stamped);
    }

    StampedObservation internalStamped() {
      return stamped;
    }

    public long acceptanceOrdinal() {
      return acceptanceOrdinal;
    }
  }

  /** Immutable deterministic declaration, write, and summary counter snapshot. */
  public static final class CounterSnapshot {
    private final Map<SchemaStatus.Code, Long> declarationOutcomes;
    private final Map<Status.Code, Long> recordOutcomes;
    private final Map<Status.Code, Long> summaryOutcomes;
    private final Map<RejectionReason, Long> rejectionReasons;
    private final long recordAccepted;
    private final long recordRejected;
    private final long summaryBatchCount;
    private final long summaryWindowClipped;
    private final long summaryRowsExamined;

    CounterSnapshot(
        Map<SchemaStatus.Code, Long> declarationOutcomes,
        Map<Status.Code, Long> recordOutcomes,
        Map<Status.Code, Long> summaryOutcomes,
        Map<RejectionReason, Long> rejectionReasons,
        long recordAccepted,
        long recordRejected,
        long summaryBatchCount,
        long summaryWindowClipped,
        long summaryRowsExamined) {
      this.declarationOutcomes = Collections.unmodifiableMap(
          new java.util.EnumMap<SchemaStatus.Code, Long>(declarationOutcomes));
      this.recordOutcomes = Collections.unmodifiableMap(
          new java.util.EnumMap<Status.Code, Long>(recordOutcomes));
      this.summaryOutcomes = Collections.unmodifiableMap(
          new java.util.EnumMap<Status.Code, Long>(summaryOutcomes));
      this.rejectionReasons = Collections.unmodifiableMap(
          new java.util.EnumMap<RejectionReason, Long>(rejectionReasons));
      this.recordAccepted = recordAccepted;
      this.recordRejected = recordRejected;
      this.summaryBatchCount = summaryBatchCount;
      this.summaryWindowClipped = summaryWindowClipped;
      this.summaryRowsExamined = summaryRowsExamined;
    }

    public long declarationOutcomeCount(SchemaStatus.Code code) {
      return count(declarationOutcomes, Objects.requireNonNull(code, "code"));
    }

    public long recordOutcomeCount(Status.Code code) {
      return count(recordOutcomes, Objects.requireNonNull(code, "code"));
    }

    public long rejectionCount(RejectionReason reason) {
      return count(rejectionReasons, Objects.requireNonNull(reason, "reason"));
    }

    public long recordAccepted() {
      return recordAccepted;
    }

    public long recordRejected() {
      return recordRejected;
    }

    public long summaryBatchCount() {
      return summaryBatchCount;
    }

    public long summaryOutcomeCount(Status.Code code) {
      return count(summaryOutcomes, Objects.requireNonNull(code, "code"));
    }

    public long summaryWindowClipped() {
      return summaryWindowClipped;
    }

    public long summaryRowsExamined() {
      return summaryRowsExamined;
    }

    private static <K> long count(Map<K, Long> counts, K key) {
      Long count = counts.get(key);
      return count == null ? 0L : count;
    }
  }

  private static StampedObservation copyStamped(StampedObservation stamped) {
    Observation observation = stamped.observation();
    Map<String, DimValue> dimensions = new LinkedHashMap<String, DimValue>();
    for (Map.Entry<String, DimValue> entry : observation.dimensions().entrySet()) {
      dimensions.put(entry.getKey(), copyValue(entry.getValue()));
    }
    Observation observationCopy = new Observation(
        observation.metric(), dimensions, observation.value(), observation.timestampMs());
    Provenance provenance = stamped.provenance();
    Provenance provenanceCopy = new Provenance(
        provenance.app(),
        provenance.attempt(),
        provenance.pluginVersion(),
        provenance.writtenAtMs());
    return new StampedObservation(observationCopy, provenanceCopy);
  }

  private static DimValue copyValue(DimValue value) {
    switch (value.kind()) {
      case STRING:
        return DimValue.of(value.stringValue());
      case LONG:
        return DimValue.of(value.longValue());
      case BYTES:
        return DimValue.of(value.bytesValue());
      default:
        throw new IllegalArgumentException("unsupported dimension kind: " + value.kind());
    }
  }
}
