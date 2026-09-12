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
package com.nvidia.spark.history;

import java.util.Objects;

/** Backend-facing observation containing framework-owned diagnostic provenance. */
public final class StampedObservation {
  private final Observation observation;
  private final Provenance provenance;

  /**
   * Creates the backend-facing value after the framework has sampled provenance.
   *
   * @param observation producer-supplied raw observation
   * @param provenance framework-owned diagnostic provenance
   * @throws NullPointerException if either argument is null
   */
  public StampedObservation(Observation observation, Provenance provenance) {
    this.observation = Objects.requireNonNull(observation, "observation");
    this.provenance = Objects.requireNonNull(provenance, "provenance");
  }

  /** @return the producer-supplied raw observation */
  public Observation observation() {
    return observation;
  }

  /** @return the framework-owned diagnostic provenance */
  public Provenance provenance() {
    return provenance;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StampedObservation)) {
      return false;
    }
    StampedObservation that = (StampedObservation) other;
    return observation.equals(that.observation) && provenance.equals(that.provenance);
  }

  @Override
  public int hashCode() {
    return 31 * observation.hashCode() + provenance.hashCode();
  }

  @Override
  public String toString() {
    return "StampedObservation{" +
        "observation=" + observation + ", provenance=" + provenance + '}';
  }
}
