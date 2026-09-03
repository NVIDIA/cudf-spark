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

/** One monotonic timeout budget captured before public-style argument validation. */
final class LocalSnapshotDeadline {
  private final LocalMetricStorePlanningAdapter.Ticker ticker;
  private final long startedNanos;
  private final long timeoutNanos;
  private final boolean zero;

  private LocalSnapshotDeadline(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      long startedNanos,
      long timeoutNanos,
      boolean zero) {
    this.ticker = ticker;
    this.startedNanos = startedNanos;
    this.timeoutNanos = timeoutNanos;
    this.zero = zero;
  }

  static LocalSnapshotDeadline start(
      Duration timeout, LocalMetricStorePlanningAdapter.Ticker ticker) {
    LocalMetricStorePlanningAdapter.Ticker checkedTicker =
        Objects.requireNonNull(ticker, "ticker");
    long startedNanos = checkedTicker.readNanos();
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    boolean zero = timeout.isZero();
    long timeoutNanos;
    try {
      timeoutNanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      timeoutNanos = Long.MAX_VALUE;
    }
    return new LocalSnapshotDeadline(
        checkedTicker, startedNanos, timeoutNanos, zero);
  }

  boolean isZero() {
    return zero;
  }

  long remainingNanos() {
    long elapsed = elapsedNanos();
    if (zero || elapsed >= timeoutNanos) {
      return 0L;
    }
    return timeoutNanos - elapsed;
  }

  boolean isExpired() {
    long elapsed = elapsedNanos();
    return zero ? elapsed > 0L : elapsed >= timeoutNanos;
  }

  void throwIfExpired() throws LocalSnapshotException {
    if (isExpired()) {
      throw new LocalSnapshotException(
          LocalSnapshotException.Reason.TIMEOUT,
          "local snapshot operation exceeded its monotonic budget");
    }
  }

  private long elapsedNanos() {
    long elapsed = ticker.readNanos() - startedNanos;
    return elapsed < 0L ? Long.MAX_VALUE : elapsed;
  }
}
