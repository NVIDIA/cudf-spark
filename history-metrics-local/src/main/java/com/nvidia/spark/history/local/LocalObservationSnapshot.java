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

import java.util.Map;
import java.util.Objects;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;

/** Immutable defensive point-in-time view of one accepted local observation. */
public final class LocalObservationSnapshot {
  private final Observation observation;
  private final Provenance provenance;
  private final long acceptanceOrdinal;

  LocalObservationSnapshot(
      MetricVersionId metric,
      Map<String, DimValue> dimensions,
      double value,
      long timestampMs,
      Provenance provenance,
      long acceptanceOrdinal) {
    this.observation = new Observation(metric, dimensions, value, timestampMs);
    this.provenance = copyProvenance(Objects.requireNonNull(provenance, "provenance"));
    this.acceptanceOrdinal = acceptanceOrdinal;
  }

  public MetricVersionId metric() {
    return observation.metric();
  }

  public Map<String, DimValue> dimensions() {
    return observation.dimensions();
  }

  public double value() {
    return observation.value();
  }

  public long timestampMs() {
    return observation.timestampMs();
  }

  public Provenance provenance() {
    return copyProvenance(provenance);
  }

  public long acceptanceOrdinal() {
    return acceptanceOrdinal;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalObservationSnapshot)) {
      return false;
    }
    LocalObservationSnapshot that = (LocalObservationSnapshot) other;
    return acceptanceOrdinal == that.acceptanceOrdinal &&
        observation.equals(that.observation) &&
        provenance.equals(that.provenance);
  }

  @Override
  public int hashCode() {
    int result = observation.hashCode();
    result = 31 * result + provenance.hashCode();
    return 31 * result +
        (int) (acceptanceOrdinal ^ (acceptanceOrdinal >>> 32));
  }

  @Override
  public String toString() {
    return "LocalObservationSnapshot{" +
        "metric=" + metric() +
        ", dimensionCount=" + dimensions().size() +
        ", value=<redacted>" +
        ", timestampMs=" + timestampMs() +
        ", provenance=<redacted>" +
        ", acceptanceOrdinal=" + acceptanceOrdinal + '}';
  }

  private static Provenance copyProvenance(Provenance provenance) {
    return new Provenance(
        provenance.app(),
        provenance.attempt(),
        provenance.pluginVersion(),
        provenance.writtenAtMs());
  }
}
