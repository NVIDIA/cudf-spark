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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/** Behavioral tests for the local planning breaker's NFR-07 state machine. */
class LocalPlanningCircuitBreakerTest {
  @Test
  void completionOrderedWindowUsesCurrentDenominatorAndThresholdEquality() {
    MutableTicker ticker = new MutableTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(3, 3, 2.0 / 3.0, Duration.ofDays(1), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt first = breaker.tryAcquire(0L);
    LocalPlanningCircuitBreaker.Attempt second = breaker.tryAcquire(0L);
    LocalPlanningCircuitBreaker.Attempt third = breaker.tryAcquire(0L);
    LocalPlanningCircuitBreaker.Attempt fourth = breaker.tryAcquire(0L);

    second.complete(false);
    third.complete(true);
    fourth.complete(false);
    assertEquals(LocalCircuitBreakerState.CLOSED, breaker.state());

    first.complete(true);
    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(4L, breaker.counters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(2L, breaker.counters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_OPEN));
  }

  @Test
  void slowThresholdIsInclusiveAndOldGenerationCompletionCannotReopen() {
    MutableTicker ticker = new MutableTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(2, 1, 1.0, Duration.ofNanos(10), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt late = breaker.tryAcquire(0L);
    LocalPlanningCircuitBreaker.Attempt slow = breaker.tryAcquire(0L);

    ticker.nanos.set(10L);
    slow.complete(false);
    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SLOW));

    ticker.nanos.set(20L);
    LocalPlanningCircuitBreaker.Attempt probe = breaker.tryAcquire(20L);
    assertNotNull(probe);
    assertEquals(LocalCircuitBreakerState.HALF_OPEN, breaker.state());
    probe.complete(false);
    assertEquals(LocalCircuitBreakerState.CLOSED, breaker.state());

    late.complete(true);
    assertEquals(LocalCircuitBreakerState.CLOSED, breaker.state());
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_CLOSE));
  }

  @Test
  void halfOpenGrantsExactlyOneConcurrentProbeAndSnapshotsAreImmutable() throws Exception {
    MutableTicker ticker = new MutableTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofDays(1), 1.0), ticker);
    breaker.tryAcquire(0L).complete(true);
    LocalHistoryMetricsCounters before = breaker.counters();
    ticker.nanos.set(10L);

    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    List<LocalPlanningCircuitBreaker.Attempt> attempts =
        Collections.synchronizedList(new ArrayList<LocalPlanningCircuitBreaker.Attempt>());
    Runnable contender = () -> {
      ready.countDown();
      await(release);
      attempts.add(breaker.tryAcquire(10L));
    };
    Thread first = new Thread(contender, "breaker-probe-one");
    Thread second = new Thread(contender, "breaker-probe-two");
    first.start();
    second.start();
    assertTrue(ready.await(5, TimeUnit.SECONDS));
    release.countDown();
    first.join(5_000L);
    second.join(5_000L);

    assertFalse(first.isAlive());
    assertFalse(second.isAlive());
    assertEquals(2, attempts.size());
    assertEquals(1L, attempts.stream().filter(value -> value != null).count());
    assertEquals(1L, attempts.stream().filter(value -> value == null).count());
    assertEquals(1L, before.value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(2L, breaker.counters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SUPPRESSED));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_HALF_OPEN));
  }

  @Test
  void failedOrSlowProbeReopensAndExecutorQueueFailureIsIdempotent() {
    MutableTicker ticker = new MutableTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofNanos(5), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt initial = breaker.tryAcquire(0L);
    initial.executorQueueFailure();
    initial.complete(true);
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_FAILURE));

    ticker.nanos.set(10L);
    LocalPlanningCircuitBreaker.Attempt probe = breaker.tryAcquire(10L);
    ticker.nanos.set(15L);
    probe.complete(false);
    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(2L, breaker.counters().value(LocalMetricCounter.BREAKER_OPEN));
  }

  @Test
  void completionTickerFailureIsConservativeNonthrowingAndExactlyOnce() {
    ThrowingTicker ticker = new ThrowingTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofDays(1), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt attempt = breaker.tryAcquire(0L);

    ticker.failure = new IllegalStateException("clock failed");
    attempt.complete(false);
    attempt.complete(false);

    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SAMPLE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SLOW));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_OPEN));
  }

  @Test
  void completionTickerLinkageFailureReopensHalfOpenWithoutWedging() {
    ThrowingTicker ticker = new ThrowingTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofDays(1), 1.0), ticker);
    breaker.tryAcquire(0L).complete(true);
    ticker.nanos.set(10L);
    LocalPlanningCircuitBreaker.Attempt probe = breaker.tryAcquire(10L);

    ticker.linkage = new AbstractMethodError("clock linkage failed");
    probe.complete(false);
    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(2L, breaker.counters().value(LocalMetricCounter.BREAKER_FAILURE));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SLOW));

    ticker.linkage = null;
    ticker.nanos.set(20L);
    LocalPlanningCircuitBreaker.Attempt recovery = breaker.tryAcquire(20L);
    assertNotNull(recovery);
    recovery.complete(false);
    assertEquals(LocalCircuitBreakerState.CLOSED, breaker.state());
  }

  @Test
  void completionDoesNotContainNonLinkageErrors() {
    ThrowingTicker ticker = new ThrowingTicker();
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofDays(1), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt attempt = breaker.tryAcquire(0L);
    ticker.fatal = new AssertionError("fatal clock failure");

    org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
        () -> attempt.complete(false));
  }

  @Test
  void elapsedChecksRemainConservativeAcrossTickerWrap() {
    MutableTicker ticker = new MutableTicker();
    ticker.nanos.set(Long.MAX_VALUE - 2L);
    LocalPlanningCircuitBreaker breaker = new LocalPlanningCircuitBreaker(
        policy(1, 1, 1.0, Duration.ofNanos(5), 1.0), ticker);
    LocalPlanningCircuitBreaker.Attempt attempt = breaker.tryAcquire(Long.MAX_VALUE - 2L);
    ticker.nanos.set(Long.MIN_VALUE + 2L);
    attempt.complete(false);
    assertEquals(LocalCircuitBreakerState.OPEN, breaker.state());
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SLOW));

    assertNull(breaker.tryAcquire(Long.MIN_VALUE + 2L));
    assertEquals(1L, breaker.counters().value(LocalMetricCounter.BREAKER_SUPPRESSED));
  }

  private static LocalCircuitBreakerPolicy policy(
      int window,
      int minimum,
      double failureRate,
      Duration slowThreshold,
      double slowRate) {
    return LocalCircuitBreakerPolicy.of(
        window,
        minimum,
        failureRate,
        slowThreshold,
        slowRate,
        Duration.ofNanos(10));
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static class MutableTicker implements LocalMetricStorePlanningAdapter.Ticker {
    protected final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.get();
    }
  }

  private static final class ThrowingTicker extends MutableTicker {
    private RuntimeException failure;
    private LinkageError linkage;
    private Error fatal;

    @Override
    public long readNanos() {
      if (failure != null) {
        throw failure;
      }
      if (linkage != null) {
        throw linkage;
      }
      if (fatal != null) {
        throw fatal;
      }
      return super.readNanos();
    }
  }
}
