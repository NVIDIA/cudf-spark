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

/** Explicit immutable observation queue and backend batch bounds for a local provider. */
public final class LocalQueuePolicy {
  private final int capacityObservations;
  private final int maxBackendBatchSize;

  private LocalQueuePolicy(int capacityObservations, int maxBackendBatchSize) {
    if (capacityObservations <= 0) {
      throw new IllegalArgumentException("capacityObservations must be positive");
    }
    if (maxBackendBatchSize <= 0) {
      throw new IllegalArgumentException("maxBackendBatchSize must be positive");
    }
    this.capacityObservations = capacityObservations;
    this.maxBackendBatchSize = maxBackendBatchSize;
  }

  /**
   * Creates explicit queue bounds.
   *
   * @param capacityObservations positive observation capacity across queued work
   * @param maxBackendBatchSize positive maximum observations in one backend write batch
   * @return immutable queue policy
   */
  public static LocalQueuePolicy of(int capacityObservations, int maxBackendBatchSize) {
    return new LocalQueuePolicy(capacityObservations, maxBackendBatchSize);
  }

  /**
   * Returns the positive queue capacity in observations.
   *
   * @return observation capacity
   */
  public int capacityObservations() {
    return capacityObservations;
  }

  /**
   * Returns the positive maximum backend write batch size in observations.
   *
   * @return maximum observations per backend batch
   */
  public int maxBackendBatchSize() {
    return maxBackendBatchSize;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalQueuePolicy)) {
      return false;
    }
    LocalQueuePolicy that = (LocalQueuePolicy) other;
    return capacityObservations == that.capacityObservations &&
        maxBackendBatchSize == that.maxBackendBatchSize;
  }

  @Override
  public int hashCode() {
    return 31 * capacityObservations + maxBackendBatchSize;
  }

  @Override
  public String toString() {
    return "LocalQueuePolicy{" +
        "capacityObservations=" + capacityObservations +
        ", maxBackendBatchSize=" + maxBackendBatchSize + '}';
  }
}
