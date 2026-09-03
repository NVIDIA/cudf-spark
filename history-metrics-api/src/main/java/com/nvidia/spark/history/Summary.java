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

/** Fixed-size unweighted evidence summary for one request. */
public final class Summary {
  private final long count;
  private final double mean;
  private final double min;
  private final double max;
  private final long firstObservedMs;
  private final long lastObservedMs;

  private Summary(
      long count,
      double mean,
      double min,
      double max,
      long firstObservedMs,
      long lastObservedMs) {
    this.count = count;
    this.mean = mean;
    this.min = min;
    this.max = max;
    this.firstObservedMs = firstObservedMs;
    this.lastObservedMs = lastObservedMs;
  }

  /**
   * Creates a nonempty unweighted evidence summary.
   *
   * @param count positive number of observations
   * @param mean finite arithmetic mean
   * @param min finite minimum no greater than {@code mean}
   * @param max finite maximum no less than {@code mean}
   * @param firstObservedMs earliest selected observation time in epoch milliseconds
   * @param lastObservedMs latest selected observation time in epoch milliseconds
   * @return the validated fixed-size summary
   * @throws IllegalArgumentException if count, values, ordering, or timestamps are invalid
   */
  public static Summary of(
      long count,
      double mean,
      double min,
      double max,
      long firstObservedMs,
      long lastObservedMs) {
    if (count <= 0) {
      throw new IllegalArgumentException("summary count must be positive");
    }
    if (!Double.isFinite(mean) || !Double.isFinite(min) || !Double.isFinite(max)) {
      throw new IllegalArgumentException("summary values must be finite");
    }
    if (min > mean || mean > max) {
      throw new IllegalArgumentException("summary requires min <= mean <= max");
    }
    if (firstObservedMs > lastObservedMs) {
      throw new IllegalArgumentException(
          "summary requires firstObservedMs <= lastObservedMs");
    }
    return new Summary(count, mean, min, max, firstObservedMs, lastObservedMs);
  }

  /** @return the positive observation count */
  public long count() {
    return count;
  }

  /** @return the finite unweighted arithmetic mean */
  public double mean() {
    return mean;
  }

  /** @return the finite minimum */
  public double min() {
    return min;
  }

  /** @return the finite maximum */
  public double max() {
    return max;
  }

  /** @return the earliest selected observation time in epoch milliseconds */
  public long firstObservedMs() {
    return firstObservedMs;
  }

  /** @return the latest selected observation time in epoch milliseconds */
  public long lastObservedMs() {
    return lastObservedMs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Summary)) {
      return false;
    }
    Summary that = (Summary) other;
    return count == that.count &&
        Double.compare(mean, that.mean) == 0 &&
        Double.compare(min, that.min) == 0 &&
        Double.compare(max, that.max) == 0 &&
        firstObservedMs == that.firstObservedMs &&
        lastObservedMs == that.lastObservedMs;
  }

  @Override
  public int hashCode() {
    int result = (int) (count ^ (count >>> 32));
    long bits = Double.doubleToLongBits(mean);
    result = 31 * result + (int) (bits ^ (bits >>> 32));
    bits = Double.doubleToLongBits(min);
    result = 31 * result + (int) (bits ^ (bits >>> 32));
    bits = Double.doubleToLongBits(max);
    result = 31 * result + (int) (bits ^ (bits >>> 32));
    result = 31 * result + (int) (firstObservedMs ^ (firstObservedMs >>> 32));
    return 31 * result + (int) (lastObservedMs ^ (lastObservedMs >>> 32));
  }

  @Override
  public String toString() {
    return "Summary{" + "count=" + count + ", mean=" + mean + ", min=" + min +
        ", max=" + max + ", firstObservedMs=" + firstObservedMs +
        ", lastObservedMs=" + lastObservedMs + '}';
  }
}
