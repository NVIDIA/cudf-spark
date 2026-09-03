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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.LocalTestCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.MetricStores;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Behavioral coverage for FR-22 explicit snapshot owner restoration. */
class LocalHistoryMetricsOpenSnapshotTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final Duration MAXIMUM_PLANNING_AGE = Duration.ofDays(30);
  private static final Clock CLOCK =
      Clock.fixed(Instant.ofEpochMilli(10_000L), ZoneOffset.UTC);
  private static final HistoryMetricCatalog CATALOG = LocalTestCatalog.builder()
      .addLive(61, "snapshot.live")
      .addRetired(62, "snapshot.retired")
      .build();
  private static final MetricVersionId LIVE_V1 = new MetricVersionId(61, 1);
  private static final MetricVersionId LIVE_V2 = new MetricVersionId(61, 2);
  private static final MetricVersionId RETIRED_V1 = new MetricVersionId(62, 1);

  @TempDir
  Path temporaryDirectory;

  @Test
  void factoryExposesTheExactFrozenCheckedSignature() throws Exception {
    assertArrayEquals(
        new Class<?>[] {LocalSnapshotException.class},
        LocalHistoryMetricsFactory.class.getMethod(
            "openSnapshot",
            Path.class,
            HistoryMetricCatalog.class,
            Clock.class,
            LocalProvenanceSource.class,
            Duration.class,
            LocalQueuePolicy.class,
            LocalExecutionPolicy.class,
            LocalCircuitBreakerPolicy.class,
            Duration.class).getExceptionTypes());
  }

  @Test
  void restoredOwnerPreservesDurableStateAndStartsRuntimeStateFresh()
      throws Exception {
    Path source = temporaryDirectory.resolve("state.bin");
    LocalHistoryMetrics original = open();
    MetricStore locator = MetricStores.current();
    try {
      for (MetricSchema schema : Arrays.asList(
          schema(LIVE_V1), schema(LIVE_V2), schema(RETIRED_V1))) {
        assertEquals(SchemaStatus.Code.ACCEPTED,
            original.store().declare(Collections.singletonList(schema), TIMEOUT)
                .get(0).code());
      }
      original.store().record(Arrays.asList(
          observation(LIVE_V1, 9_000L, 1.0),
          observation(LIVE_V2, 9_001L, 2.0),
          observation(RETIRED_V1, 9_002L, 3.0)));
      assertTrue(original.drain(TIMEOUT));
      original.save(source, TIMEOUT);
    } finally {
      assertTrue(original.shutdown(TIMEOUT));
    }

    LocalHistoryMetrics restored = openSnapshot(source, CATALOG, MAXIMUM_PLANNING_AGE);
    Files.delete(source);
    try {
      assertFalse(Files.exists(source));
      assertNotSame(original, restored);
      assertEquals(locator, MetricStores.current());
      assertEquals(3, restored.testHandle().declarations().size());
      assertEquals(3, restored.testHandle().observations().size());
      LocalDeclarationSnapshot firstDeclaration =
          restored.testHandle().declarations().get(0);
      assertEquals(schema(LIVE_V1).recommendedRetention(),
          firstDeclaration.schema().recommendedRetention());
      assertEquals(new Retention(Duration.ofDays(1), Duration.ofDays(7)),
          firstDeclaration.effectiveRetention());
      assertEquals("original-app",
          restored.testHandle().observations().get(0).provenance().app());
      for (LocalMetricCounter counter : LocalMetricCounter.values()) {
        assertEquals(0L, restored.testHandle().counters().value(counter));
      }
      assertEquals(LocalCircuitBreakerState.CLOSED, restored.testHandle().breakerState());

      SummaryResponse summary = restored.store().summarize(
          Collections.singletonList(SummaryRequest.builder(RETIRED_V1)
              .bind("key", DimValue.of("x"))
              .window(0L, 10_000L)
              .build()),
          TIMEOUT).get(0);
      assertEquals(Status.Code.OK, summary.status().code());
      assertEquals(Coverage.COMPLETE, summary.coverage());
      assertEquals(3.0, summary.summary().mean());

      long previousOrdinal = restored.testHandle().observations().get(2)
          .acceptanceOrdinal();
      restored.store().record(Collections.singletonList(
          observation(LIVE_V1, 9_003L, 4.0)));
      assertTrue(restored.drain(TIMEOUT));
      LocalObservationSnapshot newlyAccepted =
          restored.testHandle().observations().get(3);
      assertTrue(newlyAccepted.acceptanceOrdinal() > previousOrdinal);
      assertEquals("restored-app", newlyAccepted.provenance().app());
      assertFalse(Files.exists(source));
      assertEquals(SchemaStatus.Code.ACCEPTED,
          restored.store().declare(
              Collections.singletonList(schema(RETIRED_V1)), TIMEOUT)
              .get(0).code());
    } finally {
      assertTrue(restored.shutdown(TIMEOUT));
    }
  }

  @Test
  void allArgumentsValidateBeforeSourceAccessAndFailuresNeverPublishAnOwner()
      throws Exception {
    Path missing = temporaryDirectory.resolve("missing.bin");
    assertThrows(NullPointerException.class, () -> openSnapshot(
        null, CATALOG, MAXIMUM_PLANNING_AGE));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, null, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, null, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, null, MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), null,
            queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            null, executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), null, breakerPolicy(), TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), null, TIMEOUT));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), breakerPolicy(), null));
    assertThrows(IllegalArgumentException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), Duration.ofNanos(-1),
            queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT));
    assertThrows(IllegalArgumentException.class, () ->
        LocalHistoryMetricsFactory.openSnapshot(
            missing, CATALOG, CLOCK, provenance(), MAXIMUM_PLANNING_AGE,
            queuePolicy(), executionPolicy(), breakerPolicy(), Duration.ofNanos(-1)));

    LocalSnapshotException io = assertThrows(LocalSnapshotException.class, () ->
        openSnapshot(missing, CATALOG, MAXIMUM_PLANNING_AGE));
    assertEquals(LocalSnapshotException.Reason.IO, io.reason());
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
    assertFalse(Files.exists(missing));
  }

  @Test
  void codecReasonsPropagateThroughFactoryWithoutGuardOrLocatorLeaks() throws Exception {
    Path source = temporaryDirectory.resolve("reasons.bin");
    writeSnapshot(source);
    MetricStore locator = MetricStores.current();

    byte[] valid = Files.readAllBytes(source);
    byte[] corrupt = valid.clone();
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(source, corrupt);
    LocalSnapshotException integrity = assertThrows(LocalSnapshotException.class, () ->
        openSnapshot(source, CATALOG, MAXIMUM_PLANNING_AGE));
    assertEquals(LocalSnapshotException.Reason.INTEGRITY, integrity.reason());

    Files.write(source, valid);
    HistoryMetricCatalog renamed = LocalTestCatalog.builder()
        .addLive(61, "snapshot.renamed")
        .addRetired(62, "snapshot.retired")
        .build();
    LocalSnapshotException catalogConflict =
        assertThrows(LocalSnapshotException.class, () ->
            openSnapshot(source, renamed, MAXIMUM_PLANNING_AGE));
    assertEquals(LocalSnapshotException.Reason.CATALOG_CONFLICT,
        catalogConflict.reason());

    LocalSnapshotException policyConflict =
        assertThrows(LocalSnapshotException.class, () ->
            openSnapshot(source, CATALOG, Duration.ofHours(12)));
    assertEquals(LocalSnapshotException.Reason.POLICY_CONFLICT,
        policyConflict.reason());
    assertEquals(locator, MetricStores.current());
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
  }

  @Test
  void finalPrepublicationDeadlineCleansUnpublishedOwnerAndReleasesGuard()
      throws Exception {
    Path source = temporaryDirectory.resolve("deadline.bin");
    writeSnapshot(source);
    MutableTicker ticker = new MutableTicker();

    LocalSnapshotException timeout = assertThrows(LocalSnapshotException.class, () ->
        LocalHistoryMetricsFactory.openSnapshotForTest(
            source,
            CATALOG,
            CLOCK,
            provenance(),
            MAXIMUM_PLANNING_AGE,
            queuePolicy(),
            executionPolicy(),
            breakerPolicy(),
            Duration.ofNanos(10),
            ticker,
            LocalSnapshotFiles.systemFileOperations(),
            LocalSnapshotFiles.systemGuardWaiter(),
            new LocalHistoryMetricsFactory.SnapshotOpenHook() {
              @Override
              public void beforePublication() {
                ticker.set(10L);
              }
            }));
    assertEquals(LocalSnapshotException.Reason.TIMEOUT, timeout.reason());
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());

    LocalHistoryMetrics retry = openSnapshot(source, CATALOG, MAXIMUM_PLANNING_AGE);
    assertTrue(retry.shutdown(TIMEOUT));
  }

  @Test
  void sourceGuardRemainsHeldThroughUnpublishedOwnerConstruction() throws Exception {
    Path source = temporaryDirectory.resolve("guarded.bin");
    writeSnapshot(source);
    MutableTicker ticker = new MutableTicker();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<LocalHistoryMetrics> restored =
        new AtomicReference<LocalHistoryMetrics>();
    AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

    Thread opener = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          restored.set(LocalHistoryMetricsFactory.openSnapshotForTest(
              source,
              CATALOG,
              CLOCK,
              provenance(),
              MAXIMUM_PLANNING_AGE,
              queuePolicy(),
              executionPolicy(),
              breakerPolicy(),
              TIMEOUT,
              ticker,
              LocalSnapshotFiles.systemFileOperations(),
              LocalSnapshotFiles.systemGuardWaiter(),
              new LocalHistoryMetricsFactory.SnapshotOpenHook() {
                @Override
                public void beforePublication() {
                  entered.countDown();
                  await(release);
                }
              }));
        } catch (Throwable throwable) {
          failure.set(throwable);
        } finally {
          done.countDown();
        }
      }
    }, "history-metrics-open-snapshot-test");
    opener.setDaemon(true);
    opener.start();
    entered.await();

    LocalSnapshotException busy = assertThrows(LocalSnapshotException.class, () ->
        LocalSnapshotFiles.loadForTest(
            source,
            CATALOG,
            MAXIMUM_PLANNING_AGE,
            Duration.ZERO,
            ticker,
            LocalSnapshotFiles.systemFileOperations(),
            LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(LocalSnapshotException.Reason.BUSY, busy.reason());

    release.countDown();
    done.await();
    assertEquals(null, failure.get());
    assertTrue(restored.get().shutdown(TIMEOUT));
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
  }

  private static void writeSnapshot(Path target) throws Exception {
    LocalHistoryMetrics owner = open();
    try {
      assertEquals(SchemaStatus.Code.ACCEPTED,
          owner.store().declare(Collections.singletonList(schema(LIVE_V1)), TIMEOUT)
              .get(0).code());
      owner.store().record(
          Collections.singletonList(observation(LIVE_V1, 9_000L, 1.0)));
      assertTrue(owner.drain(TIMEOUT));
      owner.save(target, TIMEOUT);
    } finally {
      assertTrue(owner.shutdown(TIMEOUT));
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError("unexpected interruption", interrupted);
    }
  }

  private static LocalHistoryMetrics open() {
    return LocalHistoryMetricsFactory.open(
        CATALOG, CLOCK, provenance("original-app"), MAXIMUM_PLANNING_AGE,
        queuePolicy(), executionPolicy(), breakerPolicy());
  }

  private static LocalHistoryMetrics openSnapshot(
      Path source, HistoryMetricCatalog catalog, Duration maximumPlanningAge)
      throws LocalSnapshotException {
    return LocalHistoryMetricsFactory.openSnapshot(
        source, catalog, CLOCK, provenance("restored-app"), maximumPlanningAge,
        queuePolicy(), executionPolicy(), breakerPolicy(), TIMEOUT);
  }

  private static LocalProvenanceSource provenance() {
    return provenance("restored-app");
  }

  private static LocalProvenanceSource provenance(String app) {
    return () -> LocalProvenanceIdentity.of(app, "attempt", "2.0");
  }

  private static LocalQueuePolicy queuePolicy() {
    return LocalQueuePolicy.of(16, 8);
  }

  private static LocalExecutionPolicy executionPolicy() {
    return LocalExecutionPolicy.of(1, 8);
  }

  private static LocalCircuitBreakerPolicy breakerPolicy() {
    return LocalCircuitBreakerPolicy.of(
        8, 4, 1.0, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1));
  }

  private static MetricSchema schema(MetricVersionId metric) {
    return new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec("key", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(1), Duration.ofDays(7)));
  }

  private static Observation observation(
      MetricVersionId metric, long timestampMs, double value) {
    return new Observation(
        metric, Collections.singletonMap("key", DimValue.of("x")),
        value, timestampMs);
  }

  private static final class MutableTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private volatile long value;

    @Override
    public long readNanos() {
      return value;
    }

    private void set(long value) {
      this.value = value;
    }
  }
}
