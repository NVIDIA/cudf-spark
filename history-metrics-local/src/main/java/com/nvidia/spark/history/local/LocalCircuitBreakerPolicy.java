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

import java.time.Duration;
import java.util.Objects;

/** Explicit immutable rolling-window circuit-breaker policy for local planning calls. */
public final class LocalCircuitBreakerPolicy {
  private final int windowSize;
  private final int minSamples;
  private final double failureRateThreshold;
  private final Duration slowCallThreshold;
  private final double slowRateThreshold;
  private final Duration openDuration;

  private LocalCircuitBreakerPolicy(
      int windowSize,
      int minSamples,
      double failureRateThreshold,
      Duration slowCallThreshold,
      double slowRateThreshold,
      Duration openDuration) {
    if (windowSize <= 0) {
      throw new IllegalArgumentException("windowSize must be positive");
    }
    if (minSamples <= 0 || minSamples > windowSize) {
      throw new IllegalArgumentException("minSamples must be from one through windowSize");
    }
    validateRate(failureRateThreshold, "failureRateThreshold");
    validatePositive(slowCallThreshold, "slowCallThreshold");
    validateRate(slowRateThreshold, "slowRateThreshold");
    validatePositive(openDuration, "openDuration");
    this.windowSize = windowSize;
    this.minSamples = minSamples;
    this.failureRateThreshold = failureRateThreshold;
    this.slowCallThreshold = slowCallThreshold;
    this.slowRateThreshold = slowRateThreshold;
    this.openDuration = openDuration;
  }

  /**
   * Creates an explicit rolling-window breaker policy.
   *
   * @param windowSize positive maximum terminal samples retained
   * @param minSamples samples required before tripping, from one through {@code windowSize}
   * @param failureRateThreshold failure fraction in {@code (0, 1]} that trips at or above
   * @param slowCallThreshold positive end-to-end provider-eligible elapsed-time threshold
   * @param slowRateThreshold slow-call fraction in {@code (0, 1]} that trips at or above
   * @param openDuration positive monotonic elapsed time before one half-open probe is eligible
   * @return immutable circuit-breaker policy
   */
  public static LocalCircuitBreakerPolicy of(
      int windowSize,
      int minSamples,
      double failureRateThreshold,
      Duration slowCallThreshold,
      double slowRateThreshold,
      Duration openDuration) {
    return new LocalCircuitBreakerPolicy(
        windowSize,
        minSamples,
        failureRateThreshold,
        slowCallThreshold,
        slowRateThreshold,
        openDuration);
  }

  /**
   * Returns the positive maximum terminal-sample window size.
   *
   * @return window size in samples
   */
  public int windowSize() {
    return windowSize;
  }

  /**
   * Returns the trip sample floor, from one through {@link #windowSize()}.
   *
   * @return minimum samples
   */
  public int minSamples() {
    return minSamples;
  }

  /**
   * Returns the trip fraction in {@code (0, 1]} for failed samples.
   *
   * @return failure-rate threshold
   */
  public double failureRateThreshold() {
    return failureRateThreshold;
  }

  /**
   * Returns the positive elapsed-time boundary at which a sample is slow.
   *
   * @return slow-call threshold
   */
  public Duration slowCallThreshold() {
    return slowCallThreshold;
  }

  /**
   * Returns the trip fraction in {@code (0, 1]} for slow samples.
   *
   * @return slow-rate threshold
   */
  public double slowRateThreshold() {
    return slowRateThreshold;
  }

  /**
   * Returns the positive monotonic recovery interval before a half-open probe.
   *
   * @return open-state duration
   */
  public Duration openDuration() {
    return openDuration;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalCircuitBreakerPolicy)) {
      return false;
    }
    LocalCircuitBreakerPolicy that = (LocalCircuitBreakerPolicy) other;
    return windowSize == that.windowSize &&
        minSamples == that.minSamples &&
        Double.compare(failureRateThreshold, that.failureRateThreshold) == 0 &&
        slowCallThreshold.equals(that.slowCallThreshold) &&
        Double.compare(slowRateThreshold, that.slowRateThreshold) == 0 &&
        openDuration.equals(that.openDuration);
  }

  @Override
  public int hashCode() {
    int result = windowSize;
    result = 31 * result + minSamples;
    long failureBits = Double.doubleToLongBits(failureRateThreshold);
    result = 31 * result + (int) (failureBits ^ (failureBits >>> 32));
    result = 31 * result + slowCallThreshold.hashCode();
    long slowBits = Double.doubleToLongBits(slowRateThreshold);
    result = 31 * result + (int) (slowBits ^ (slowBits >>> 32));
    return 31 * result + openDuration.hashCode();
  }

  @Override
  public String toString() {
    return "LocalCircuitBreakerPolicy{" +
        "windowSize=" + windowSize +
        ", minSamples=" + minSamples +
        ", failureRateThreshold=" + failureRateThreshold +
        ", slowCallThreshold=" + slowCallThreshold +
        ", slowRateThreshold=" + slowRateThreshold +
        ", openDuration=" + openDuration + '}';
  }

  private static void validateRate(double rate, String name) {
    if (!(rate > 0.0) || rate > 1.0) {
      throw new IllegalArgumentException(name + " must be in (0, 1]");
    }
  }

  private static void validatePositive(Duration duration, String name) {
    Objects.requireNonNull(duration, name);
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }
}
