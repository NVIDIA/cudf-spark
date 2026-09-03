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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/** FR-05 tests for bounded, fixed, redacted local record diagnostics. */
class LocalRecordDiagnosticsTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final MetricVersionId METRIC = new MetricVersionId(17, 1);
  private static final MetricVersionId UNKNOWN = new MetricVersionId(18, 1);
  private static final MetricSchema SCHEMA = new MetricSchema(
      METRIC,
      Collections.singletonList(new DimensionSpec("key", DimValue.Kind.STRING)),
      new Retention(Duration.ofDays(1), Duration.ofDays(30)));

  @Test
  void limiterIsPerCategoryConcurrentAndReemitsAtCooldownBoundary() throws Exception {
    MutableTicker ticker = new MutableTicker();
    CapturingSink sink = new CapturingSink();
    LocalRecordDiagnostics diagnostics = new LocalRecordDiagnostics(ticker, sink);
    int callers = 16;
    ExecutorService executor = Executors.newFixedThreadPool(callers);
    CyclicBarrier start = new CyclicBarrier(callers + 1);
    try {
      for (int index = 0; index < callers; index++) {
        executor.submit(() -> {
          start.await();
          diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
          return null;
        });
      }
      start.await();
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.INVALID));
    diagnostics.candidate(LocalRecordDiagnostics.Category.NOT_DECLARED);
    diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.INVALID));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.NOT_DECLARED));

    ticker.nanos.set(LocalRecordDiagnostics.COOLDOWN_NANOS - 1L);
    diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.INVALID));
    ticker.nanos.set(LocalRecordDiagnostics.COOLDOWN_NANOS);
    diagnostics.candidate(LocalRecordDiagnostics.Category.INVALID);
    assertEquals(2, sink.count(LocalRecordDiagnostics.Category.INVALID));
  }

  @Test
  void tickerAndSinkFailuresCannotEscapeTheLimiter() {
    CapturingSink unused = new CapturingSink();
    LocalRecordDiagnostics throwingTicker = new LocalRecordDiagnostics(
        () -> { throw new IllegalStateException("ticker-secret"); }, unused);
    assertDoesNotThrow(() ->
        throwingTicker.candidate(LocalRecordDiagnostics.Category.INVALID));
    assertEquals(0, unused.messages().size());

    LocalRecordDiagnostics linkageSink = new LocalRecordDiagnostics(
        new MutableTicker(),
        (category, message) -> { throw new AbstractMethodError("sink-secret"); });
    assertDoesNotThrow(() ->
        linkageSink.candidate(LocalRecordDiagnostics.Category.INVALID));
  }

  @Test
  void synchronousFailuresUseOnlyFixedRedactedCategoryMessages() throws Exception {
    MutableTicker ticker = new MutableTicker();
    CapturingSink sink = new CapturingSink();
    RecordingBackend backend = new RecordingBackend();
    LocalAsyncRecordPipeline pipeline = pipeline(
        backend, fixedClock(1_000L), new FixedSource(), policy(1, 1), ticker, sink);

    pipeline.record(null);
    pipeline.record(hostileList("caller-secret"));
    pipeline.record(Arrays.asList(
        null,
        new Observation(
            METRIC, Collections.<String, DimValue>emptyMap(), 1.0, 1_000L),
        observation(UNKNOWN, "undeclared-secret", 1_000L),
        observation(METRIC, "future-secret", 301_001L)));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.INVALID));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.NOT_DECLARED));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.FUTURE_TIMESTAMP));

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    backend.handler = observations -> {
      entered.countDown();
      release.await();
      return WriteResult.ok(observations.size());
    };
    pipeline.record(Collections.singletonList(observation(METRIC, "in-flight-secret", 1_000L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    pipeline.record(Arrays.asList(
        observation(METRIC, "queued-secret", 1_000L),
        observation(METRIC, "overflow-secret", 1_000L)));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.OVERFLOW));
    release.countDown();
    assertTrue(pipeline.drain(TIMEOUT));
    assertTrue(pipeline.stop(TIMEOUT));
    pipeline.record(Collections.singletonList(
        observation(METRIC, "post-stop-secret", 1_000L)));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.POST_STOP));

    CapturingSink clockSink = new CapturingSink();
    LocalAsyncRecordPipeline clockFailure = pipeline(
        new RecordingBackend(), new ThrowingClock("clock-secret"), new FixedSource(),
        policy(1, 1), new MutableTicker(), clockSink);
    clockFailure.record(Collections.singletonList(observation(METRIC, "clock-item", 1L)));
    assertEquals(1, clockSink.count(LocalRecordDiagnostics.Category.CLOCK_FAILURE));
    assertTrue(clockFailure.stop(TIMEOUT));

    CapturingSink provenanceSink = new CapturingSink();
    LocalAsyncRecordPipeline provenanceFailure = pipeline(
        new RecordingBackend(), fixedClock(1L),
        () -> { throw new IllegalStateException("provenance-secret"); },
        policy(1, 1), new MutableTicker(), provenanceSink);
    provenanceFailure.record(Collections.singletonList(
        observation(METRIC, "provenance-item", 1L)));
    assertEquals(1, provenanceSink.count(
        LocalRecordDiagnostics.Category.PROVENANCE_FAILURE));
    assertTrue(provenanceFailure.stop(TIMEOUT));

    for (String message : concat(sink.messages(), clockSink.messages(),
        provenanceSink.messages())) {
      assertFalse(message.contains("secret"));
      assertTrue(isFixedMessage(message));
    }
  }

  @Test
  void asynchronousBackendOutcomesCreateOneCandidatePerBatchAndCategory() throws Exception {
    MutableTicker ticker = new MutableTicker();
    CapturingSink sink = new CapturingSink();
    RecordingBackend backend = new RecordingBackend();
    AtomicInteger outcome = new AtomicInteger();
    backend.handler = observations -> {
      switch (outcome.getAndIncrement()) {
        case 0:
          return WriteResult.rejected(
              observations.size(),
              Status.of(Status.Code.NOT_DECLARED, "provider-secret"));
        case 1:
          return WriteResult.rejected(
              observations.size(),
              Status.of(Status.Code.INVALID_REQUEST, "provider-secret"));
        case 2:
          return WriteResult.rejected(
              observations.size(),
              Status.of(Status.Code.DENIED, "provider-secret"));
        case 3:
          return WriteResult.unavailable(0, observations.size(), "provider-secret");
        case 4:
          throw new IllegalStateException("exception-secret");
        default:
          return null;
      }
    };
    LocalAsyncRecordPipeline pipeline = pipeline(
        backend, fixedClock(1L), new FixedSource(), policy(16, 1), ticker, sink);

    for (int index = 0; index < 6; index++) {
      pipeline.record(Collections.singletonList(
          observation(METRIC, "provider-item-secret-" + index, 1L)));
    }
    assertTrue(pipeline.drain(TIMEOUT));
    assertTrue(sink.awaitTotal(5, TIMEOUT));

    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.NOT_DECLARED));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.INVALID));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_DENIED));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_UNAVAILABLE));
    // Throwing and malformed results are separate candidates, then limiter suppression applies.
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));
    for (String message : sink.messages()) {
      assertFalse(message.contains("secret"));
      assertTrue(isFixedMessage(message));
    }
    assertTrue(pipeline.stop(TIMEOUT));
  }

  @Test
  void externallyAmbiguousBatchIsNotDiagnosedAgainWhenItEventuallyCompletes()
      throws Exception {
    MutableTicker ticker = new MutableTicker();
    CapturingSink sink = new CapturingSink();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = observations -> {
      entered.countDown();
      release.await();
      return null;
    };
    LocalAsyncRecordPipeline pipeline = pipeline(
        backend, fixedClock(1L), new FixedSource(), policy(1, 1), ticker, sink);

    pipeline.record(Collections.singletonList(observation(METRIC, "secret", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    pipeline.markInFlightAmbiguous();
    assertEquals(1L, pipeline.counters().backendAmbiguousItemCount());
    ticker.nanos.set(LocalRecordDiagnostics.COOLDOWN_NANOS);
    release.countDown();
    assertTrue(pipeline.drain(TIMEOUT));
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));
    assertEquals(1L, pipeline.counters().backendAmbiguousItemCount());
    assertTrue(pipeline.stop(TIMEOUT));
  }

  @Test
  void shutdownTimeoutDiagnosesInFlightOnceWithoutDiagnosingQueuedDrops() throws Exception {
    MutableTicker ticker = new MutableTicker();
    CapturingSink sink = new CapturingSink();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RecordingBackend backend = new RecordingBackend();
    backend.handler = observations -> {
      entered.countDown();
      release.await();
      return WriteResult.ok(observations.size());
    };
    LocalAsyncRecordPipeline pipeline = pipeline(
        backend, fixedClock(1L), new FixedSource(), policy(3, 1), ticker, sink);

    pipeline.record(Collections.singletonList(observation(METRIC, "in-flight-secret", 1L)));
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    pipeline.record(Arrays.asList(
        observation(METRIC, "queued-secret-1", 1L),
        observation(METRIC, "queued-secret-2", 1L)));

    long watermark = pipeline.beginShutdown();
    pipeline.finishShutdown(watermark, ticker.readNanos(), 0L);

    assertEquals(0, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));
    assertEquals(1L, pipeline.counters().backendAmbiguousItemCount());
    pipeline.emitShutdownAmbiguityAfterFinalized();
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));
    assertEquals(1, sink.messages().size());
    assertEquals(2L, pipeline.shutdownDroppedItemCount());

    ticker.nanos.set(LocalRecordDiagnostics.COOLDOWN_NANOS);
    pipeline.markInFlightAmbiguous();
    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));

    release.countDown();
    pipeline.awaitWriterTermination();
    pipeline.markInFlightAmbiguous();

    assertEquals(1, sink.count(LocalRecordDiagnostics.Category.BACKEND_AMBIGUOUS));
    assertEquals(1, sink.messages().size());
    assertEquals(1L, pipeline.counters().backendAmbiguousItemCount());
    assertEquals(1L, pipeline.counters().backendAcceptedItemCount());
    assertEquals(2L, pipeline.shutdownDroppedItemCount());
  }

  @Test
  void partialUnavailableBatchCreatesOneCandidateAndThrowingSinkCannotEscape() throws Exception {
    ThrowingSink sink = new ThrowingSink();
    RecordingBackend backend = new RecordingBackend();
    backend.handler = observations ->
        WriteResult.unavailable(1, 1, "partial-provider-secret");
    LocalAsyncRecordPipeline pipeline = pipeline(
        backend, fixedClock(1L), new FixedSource(), policy(4, 2),
        new MutableTicker(), sink);

    assertDoesNotThrow(() -> pipeline.record(Arrays.asList(
        observation(METRIC, "first-secret", 1L),
        observation(METRIC, "second-secret", 1L))));
    assertTrue(pipeline.drain(TIMEOUT));
    assertTrue(sink.called.await(5, TimeUnit.SECONDS));
    assertEquals(1, sink.calls.get());

    assertDoesNotThrow(() -> pipeline.record(null));
    assertEquals(2, sink.calls.get());
    assertTrue(pipeline.stop(TIMEOUT));
  }

  private static LocalAsyncRecordPipeline pipeline(
      RecordingBackend backend,
      Clock clock,
      LocalProvenanceSource source,
      LocalAsyncRecordPipeline.QueuePolicy policy,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalRecordDiagnosticSink sink) {
    return new LocalAsyncRecordPipeline(
        backend,
        metric -> METRIC.equals(metric) ? SCHEMA : null,
        clock,
        source,
        policy,
        ticker,
        LocalAsyncRecordPipeline.SYSTEM_DRAIN_WAITER,
        sink);
  }

  private static LocalAsyncRecordPipeline.QueuePolicy policy(int capacity, int batchSize) {
    return new LocalAsyncRecordPipeline.QueuePolicy(capacity, batchSize);
  }

  private static Observation observation(MetricVersionId metric, String key, long timestampMs) {
    return new Observation(
        metric,
        Collections.singletonMap("key", DimValue.of(key)),
        1.0,
        timestampMs);
  }

  private static Clock fixedClock(long millis) {
    return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
  }

  private static List<Observation> hostileList(String secret) {
    return new AbstractList<Observation>() {
      @Override
      public Observation get(int index) {
        throw new IllegalStateException(secret);
      }

      @Override
      public int size() {
        throw new IllegalStateException(secret);
      }
    };
  }

  private static boolean isFixedMessage(String message) {
    for (LocalRecordDiagnostics.Category category :
        LocalRecordDiagnostics.Category.values()) {
      if (category.message().equals(message)) {
        return true;
      }
    }
    return false;
  }

  @SafeVarargs
  private static List<String> concat(List<String>... values) {
    List<String> all = new ArrayList<String>();
    for (List<String> value : values) {
      all.addAll(value);
    }
    return all;
  }

  private static final class CapturingSink implements LocalRecordDiagnosticSink {
    private final List<String> messages =
        Collections.synchronizedList(new ArrayList<String>());
    private final EnumMap<LocalRecordDiagnostics.Category, AtomicInteger> counts =
        new EnumMap<LocalRecordDiagnostics.Category, AtomicInteger>(
            LocalRecordDiagnostics.Category.class);

    private CapturingSink() {
      for (LocalRecordDiagnostics.Category category :
          LocalRecordDiagnostics.Category.values()) {
        counts.put(category, new AtomicInteger());
      }
    }

    @Override
    public void recordFailure(
        LocalRecordDiagnostics.Category category, String message) {
      counts.get(category).incrementAndGet();
      messages.add(message);
      synchronized (this) {
        notifyAll();
      }
    }

    private boolean awaitTotal(int expected, Duration timeout)
        throws InterruptedException {
      long deadline = System.nanoTime() + timeout.toNanos();
      synchronized (this) {
        while (messages.size() < expected) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0L) {
            return false;
          }
          long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
          int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
          wait(millis, nanos);
        }
        return true;
      }
    }

    private int count(LocalRecordDiagnostics.Category category) {
      return counts.get(category).get();
    }

    private List<String> messages() {
      synchronized (messages) {
        return new ArrayList<String>(messages);
      }
    }
  }

  private static final class ThrowingSink implements LocalRecordDiagnosticSink {
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch called = new CountDownLatch(1);

    @Override
    public void recordFailure(
        LocalRecordDiagnostics.Category category, String message) {
      calls.incrementAndGet();
      called.countDown();
      throw new IllegalStateException("sink-secret");
    }
  }

  private interface RecordHandler {
    WriteResult apply(List<StampedObservation> observations) throws Exception;
  }

  private static final class RecordingBackend implements HistoryMetricsBackend {
    private volatile RecordHandler handler;

    @Override
    public BackendInfo info() {
      return new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "record backend");
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      if (handler == null) {
        return WriteResult.ok(observations.size());
      }
      try {
        return handler.apply(observations);
      } catch (RuntimeException failure) {
        throw failure;
      } catch (Error failure) {
        throw failure;
      } catch (Exception failure) {
        throw new IllegalStateException(failure);
      }
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      return Collections.singletonList(SummaryResponse.ok(null, Coverage.COMPLETE));
    }

    @Override
    public void close() {
    }
  }

  private static final class FixedSource implements LocalProvenanceSource {
    @Override
    public LocalProvenanceIdentity current() {
      return LocalProvenanceIdentity.of("app", null, "version");
    }
  }

  private static final class MutableTicker implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.get();
    }
  }

  private static final class ThrowingClock extends Clock {
    private final String message;

    private ThrowingClock(String message) {
      this.message = message;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      throw new IllegalStateException(message);
    }

    @Override
    public long millis() {
      throw new IllegalStateException(message);
    }
  }
}
