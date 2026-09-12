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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Objects;

/** Completion-ordered circuit breaker shared by local declaration and summary attempts. */
final class LocalPlanningCircuitBreaker {
  private final LocalCircuitBreakerPolicy policy;
  private final LocalMetricStorePlanningAdapter.Ticker ticker;
  private final long slowThresholdNanos;
  private final long openDurationNanos;
  private final Deque<Sample> window = new ArrayDeque<Sample>();
  private final EnumMap<LocalMetricCounter, Long> counters =
      new EnumMap<LocalMetricCounter, Long>(LocalMetricCounter.class);

  private LocalCircuitBreakerState state = LocalCircuitBreakerState.CLOSED;
  private Object generation = new Object();
  private long openStartedNanos;
  private long completionSequence;
  private boolean halfOpenReserved;

  LocalPlanningCircuitBreaker(
      LocalCircuitBreakerPolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    slowThresholdNanos = durationToNanosSaturated(policy.slowCallThreshold());
    openDurationNanos = durationToNanosSaturated(policy.openDuration());
    for (LocalMetricCounter counter : LocalMetricCounter.values()) {
      counters.put(counter, 0L);
    }
  }

  synchronized Attempt tryAcquire(long startedNanos) {
    if (state == LocalCircuitBreakerState.OPEN) {
      long now = ticker.readNanos();
      if (!elapsedAtLeast(openStartedNanos, now, openDurationNanos)) {
        increment(LocalMetricCounter.BREAKER_SUPPRESSED);
        return null;
      }
      state = LocalCircuitBreakerState.HALF_OPEN;
      generation = new Object();
      halfOpenReserved = true;
      increment(LocalMetricCounter.BREAKER_HALF_OPEN);
    } else if (state == LocalCircuitBreakerState.HALF_OPEN) {
      if (halfOpenReserved) {
        increment(LocalMetricCounter.BREAKER_SUPPRESSED);
        return null;
      }
      halfOpenReserved = true;
    }
    increment(LocalMetricCounter.BREAKER_SAMPLE);
    return new Attempt(this, generation, startedNanos);
  }

  synchronized LocalCircuitBreakerState state() {
    return state;
  }

  synchronized LocalHistoryMetricsCounters counters() {
    return new ImmutableLocalHistoryMetricsCounters(
        new EnumMap<LocalMetricCounter, Long>(counters));
  }

  private synchronized void complete(Attempt attempt, boolean failed) {
    if (attempt.completed) {
      return;
    }
    long completedNanos;
    boolean terminalFailed = failed;
    boolean slow;
    try {
      completedNanos = ticker.readNanos();
      slow = elapsedAtLeast(attempt.startedNanos, completedNanos, slowThresholdNanos);
    } catch (RuntimeException clockFailure) {
      completedNanos = attempt.startedNanos;
      terminalFailed = true;
      slow = true;
    } catch (LinkageError clockFailure) {
      completedNanos = attempt.startedNanos;
      terminalFailed = true;
      slow = true;
    }
    attempt.completed = true;
    completionSequence = incrementSaturated(completionSequence);
    if (terminalFailed) {
      increment(LocalMetricCounter.BREAKER_FAILURE);
    }
    if (slow) {
      increment(LocalMetricCounter.BREAKER_SLOW);
    }

    if (attempt.generation != generation) {
      return;
    }
    if (state == LocalCircuitBreakerState.HALF_OPEN) {
      halfOpenReserved = false;
      if (terminalFailed || slow) {
        open(completedNanos);
      } else {
        state = LocalCircuitBreakerState.CLOSED;
        window.clear();
        increment(LocalMetricCounter.BREAKER_CLOSE);
      }
      return;
    }
    if (state != LocalCircuitBreakerState.CLOSED) {
      return;
    }

    window.addLast(new Sample(completionSequence, terminalFailed, slow));
    while (window.size() > policy.windowSize()) {
      window.removeFirst();
    }
    if (window.size() >= policy.minSamples() &&
        (failureRate() >= policy.failureRateThreshold() ||
            slowRate() >= policy.slowRateThreshold())) {
      open(completedNanos);
    }
  }

  private void open(long nowNanos) {
    state = LocalCircuitBreakerState.OPEN;
    generation = new Object();
    openStartedNanos = nowNanos;
    halfOpenReserved = false;
    window.clear();
    increment(LocalMetricCounter.BREAKER_OPEN);
  }

  private double failureRate() {
    int failures = 0;
    for (Sample sample : window) {
      if (sample.failed) {
        failures++;
      }
    }
    return (double) failures / window.size();
  }

  private double slowRate() {
    int slow = 0;
    for (Sample sample : window) {
      if (sample.slow) {
        slow++;
      }
    }
    return (double) slow / window.size();
  }

  private void increment(LocalMetricCounter counter) {
    long value = counters.get(counter);
    counters.put(counter, value == Long.MAX_VALUE ? value : value + 1L);
  }

  private static boolean elapsedAtLeast(long startedNanos, long nowNanos, long thresholdNanos) {
    long elapsed = nowNanos - startedNanos;
    return elapsed < 0L || elapsed >= thresholdNanos;
  }

  private static long durationToNanosSaturated(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  private static long incrementSaturated(long value) {
    return value == Long.MAX_VALUE ? value : value + 1L;
  }

  static final class Attempt {
    private final LocalPlanningCircuitBreaker owner;
    private final Object generation;
    private final long startedNanos;
    private boolean completed;

    private Attempt(
        LocalPlanningCircuitBreaker owner, Object generation, long startedNanos) {
      this.owner = owner;
      this.generation = generation;
      this.startedNanos = startedNanos;
    }

    void complete(boolean failed) {
      owner.complete(this, failed);
    }

    /** Terminal seam for a reserved task rejected, expired, or removed before backend invocation. */
    void executorQueueFailure() {
      owner.complete(this, true);
    }
  }

  private static final class Sample {
    private final long completionSequence;
    private final boolean failed;
    private final boolean slow;

    private Sample(long completionSequence, boolean failed, boolean slow) {
      this.completionSequence = completionSequence;
      this.failed = failed;
      this.slow = slow;
    }
  }
}
