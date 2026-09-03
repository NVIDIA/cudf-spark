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

import java.time.Duration;
import java.util.Objects;

/** Recommended planning visibility and minimum storage retention for a metric version. */
public final class Retention {
  private final Duration planningMaxAge;
  private final Duration storageRetention;

  /**
   * Creates a retention recommendation.
   *
   * @param planningMaxAge nonnegative maximum age recommended for planning visibility
   * @param storageRetention nonnegative minimum recommended storage duration, not shorter than
   *     {@code planningMaxAge}
   * @throws NullPointerException if either duration is null
   * @throws IllegalArgumentException if a duration is negative or storage is shorter than planning
   */
  public Retention(Duration planningMaxAge, Duration storageRetention) {
    this.planningMaxAge = requireNonnegative(
        Objects.requireNonNull(planningMaxAge, "planningMaxAge"), "planningMaxAge");
    this.storageRetention = requireNonnegative(
        Objects.requireNonNull(storageRetention, "storageRetention"), "storageRetention");
    if (storageRetention.compareTo(planningMaxAge) < 0) {
      throw new IllegalArgumentException(
          "storageRetention must be greater than or equal to planningMaxAge");
    }
  }

  /** @return the nonnegative recommended maximum planning age */
  public Duration planningMaxAge() {
    return planningMaxAge;
  }

  /** @return the nonnegative recommended storage duration */
  public Duration storageRetention() {
    return storageRetention;
  }

  private static Duration requireNonnegative(Duration value, String name) {
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Retention)) {
      return false;
    }
    Retention that = (Retention) other;
    return planningMaxAge.equals(that.planningMaxAge) &&
        storageRetention.equals(that.storageRetention);
  }

  @Override
  public int hashCode() {
    return 31 * planningMaxAge.hashCode() + storageRetention.hashCode();
  }

  @Override
  public String toString() {
    return "Retention{" +
        "planningMaxAge=" + planningMaxAge +
        ", storageRetention=" + storageRetention +
        '}';
  }
}
