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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.MetricStores;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.TestHistoryMetricCatalog;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/** Local-provider behaviors for FR-05, FR-07/08/20/21 and NFR-13/16/20. */
class LocalHistoryMetricsBackendTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(1);
  private static final Duration MAX_PLANNING_AGE = Duration.ofDays(7);
  private static final long NOW_MS = 1_000_000L;
  private static final MetricVersionId METRIC_V1 = new MetricVersionId(17, 1);
  private static final MetricVersionId METRIC_V2 = new MetricVersionId(17, 2);

  @Test
  void explicitFactoryDoesNotRegisterOrActivateExternalState() {
    MetricStore before = MetricStores.current();

    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.metric"));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsBackend.create(null, fixedClock(), MAX_PLANNING_AGE));
    assertThrows(NullPointerException.class, () ->
        LocalHistoryMetricsBackend.create(catalog, null, MAX_PLANNING_AGE));
    assertThrows(IllegalArgumentException.class, () ->
        LocalHistoryMetricsBackend.create(catalog, fixedClock(), Duration.ofMillis(-1)));

    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      assertNotNull(backend.testHandle());
      assertEquals(HistoryMetricsApi.CURRENT_API_VERSION, backend.info().apiVersion());
      assertEquals(before, MetricStores.current());
    } finally {
      backend.close();
    }
  }

  @Test
  void identicalConcurrentDeclarationsConvergeOnOnePermanentMapping() throws Exception {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    MetricSchema schema = schema(METRIC_V1, dimensions("table", "bucket"), Duration.ofDays(3));
    CyclicBarrier barrier = new CyclicBarrier(3);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<SchemaStatus> first =
          executor.submit(() -> declareAfterBarrier(backend, schema, barrier));
      Future<SchemaStatus> second =
          executor.submit(() -> declareAfterBarrier(backend, schema, barrier));
      barrier.await();

      assertEquals(SchemaStatus.Code.ACCEPTED, first.get().code());
      assertEquals(SchemaStatus.Code.ACCEPTED, second.get().code());
      assertEquals(schema, backend.testHandle().declaration(METRIC_V1).get());
      assertEquals(1, backend.testHandle().declarationCount());
    } finally {
      executor.shutdownNow();
      backend.close();
    }
  }

  @Test
  void conflictingConcurrentDeclarationsChooseOneMappingAndNeverReuseIt() throws Exception {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    MetricSchema ordered =
        schema(METRIC_V1, dimensions("table", "bucket"), Duration.ofDays(3));
    MetricSchema reordered =
        schema(METRIC_V1, dimensions("bucket", "table"), Duration.ofDays(3));
    CyclicBarrier barrier = new CyclicBarrier(3);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<SchemaStatus> first =
          executor.submit(() -> declareAfterBarrier(backend, ordered, barrier));
      Future<SchemaStatus> second =
          executor.submit(() -> declareAfterBarrier(backend, reordered, barrier));
      barrier.await();

      Set<SchemaStatus.Code> outcomes = new HashSet<SchemaStatus.Code>();
      outcomes.add(first.get().code());
      outcomes.add(second.get().code());
      assertEquals(new HashSet<SchemaStatus.Code>(
          Arrays.asList(SchemaStatus.Code.ACCEPTED, SchemaStatus.Code.INCOMPATIBLE)), outcomes);

      MetricSchema established = backend.testHandle().declaration(METRIC_V1).get();
      MetricSchema rejected = established.equals(ordered) ? reordered : ordered;
      assertEquals(SchemaStatus.Code.INCOMPATIBLE, declare(backend, rejected).code());
      assertEquals(established, backend.testHandle().declaration(METRIC_V1).get());
      assertEquals(1, backend.testHandle().declarationCount());
    } finally {
      executor.shutdownNow();
      backend.close();
    }
  }

  @Test
  void sameBatchCanonicalConflictHasNoMappingSideEffect() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      MetricSchema ordered =
          schema(METRIC_V1, dimensions("table", "bucket"), Duration.ofDays(3));
      MetricSchema reordered =
          schema(METRIC_V1, dimensions("bucket", "table"), Duration.ofDays(3));
      List<SchemaStatus> statuses =
          backend.declare(Arrays.asList(ordered, reordered), TIMEOUT);

      assertEquals(SchemaStatus.Code.INCOMPATIBLE, statuses.get(0).code());
      assertEquals(SchemaStatus.Code.INCOMPATIBLE, statuses.get(1).code());
      assertFalse(backend.testHandle().declaration(METRIC_V1).isPresent());
    } finally {
      backend.close();
    }
  }

  @Test
  void orderKindAndShapeConflictWhileVersionsRemainIndependent() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      MetricSchema v1 = schema(METRIC_V1, dimensions("table", "bucket"), Duration.ofDays(3));
      assertEquals(SchemaStatus.Code.ACCEPTED, declare(backend, v1).code());

      assertEquals(SchemaStatus.Code.INCOMPATIBLE, declare(backend,
          schema(METRIC_V1, dimensions("bucket", "table"), Duration.ofDays(3))).code());
      assertEquals(SchemaStatus.Code.INCOMPATIBLE, declare(backend,
          schema(METRIC_V1, Arrays.asList(
              new DimensionSpec("table", DimValue.Kind.BYTES),
              new DimensionSpec("bucket", DimValue.Kind.STRING)), Duration.ofDays(3))).code());
      assertEquals(SchemaStatus.Code.INCOMPATIBLE, declare(backend,
          schema(METRIC_V1, dimensions("table"), Duration.ofDays(3))).code());

      MetricSchema v2 =
          schema(METRIC_V2, dimensions("bucket", "table"), Duration.ofDays(4));
      assertEquals(SchemaStatus.Code.ACCEPTED, declare(backend, v2).code());
      MetricSchema absentCatalogMetric =
          schema(new MetricVersionId(18, 1), dimensions("table"), Duration.ofDays(4));
      assertEquals(
          SchemaStatus.Code.INVALID_REQUEST, declare(backend, absentCatalogMetric).code());
      assertEquals(v1, backend.testHandle().declaration(METRIC_V1).get());
      assertEquals(v2, backend.testHandle().declaration(METRIC_V2).get());
      assertEquals(2, backend.testHandle().declarationCount());
    } finally {
      backend.close();
    }
  }

  @Test
  void exactDuplicateDeclarationsReuseTheFirstClampedOrUnclampedStatus() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      MetricSchema clamped = schema(
          METRIC_V1, dimensions("table"), Duration.ofDays(30), Duration.ofDays(90));
      MetricSchema clampedDuplicate = schema(
          METRIC_V1, dimensions("table"), Duration.ofDays(30), Duration.ofDays(90));
      List<SchemaStatus> clampedStatuses =
          backend.declare(Arrays.asList(clamped, clampedDuplicate), TIMEOUT);
      assertEquals(clampedStatuses.get(0), clampedStatuses.get(1));
      assertSame(clampedStatuses.get(0), clampedStatuses.get(1));
      assertNotNull(clampedStatuses.get(0).reason());
      assertTrue(clampedStatuses.get(0).reason().contains("clamped"));

      MetricSchema unchanged =
          schema(METRIC_V2, dimensions("table"), Duration.ofDays(3));
      MetricSchema unchangedDuplicate =
          schema(METRIC_V2, dimensions("table"), Duration.ofDays(3));
      List<SchemaStatus> unchangedStatuses =
          backend.declare(Arrays.asList(unchanged, unchangedDuplicate), TIMEOUT);
      assertEquals(unchangedStatuses.get(0), unchangedStatuses.get(1));
      assertSame(unchangedStatuses.get(0), unchangedStatuses.get(1));
      assertEquals(SchemaStatus.accepted(METRIC_V2, null), unchangedStatuses.get(0));
    } finally {
      backend.close();
    }
  }

  @Test
  void firstEffectiveRetentionIsClampedWarnedAndNeverOverwritten() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      MetricSchema initial = schema(
          METRIC_V1, dimensions("table"), Duration.ofDays(30), Duration.ofDays(90));
      MetricSchema later = schema(
          METRIC_V1, dimensions("table"), Duration.ofDays(1), Duration.ofDays(2));
      List<SchemaStatus> ordered =
          backend.declare(Arrays.asList(initial, later), TIMEOUT);
      SchemaStatus first = ordered.get(0);
      assertEquals(SchemaStatus.Code.ACCEPTED, first.code());
      assertNotNull(first.reason());
      assertTrue(first.reason().contains("clamped"));
      assertEquals(SchemaStatus.Code.ACCEPTED, ordered.get(1).code());
      assertNotSame(first, ordered.get(1));
      assertFalse(first.reason().equals(ordered.get(1).reason()));
      assertEquals(
          new Retention(MAX_PLANNING_AGE, Duration.ofDays(90)),
          backend.testHandle().effectiveRetention(METRIC_V1).get());

      MetricSchema disabled =
          schema(METRIC_V2, dimensions("table"), Duration.ZERO, Duration.ofDays(2));
      assertEquals(SchemaStatus.Code.ACCEPTED, declare(backend, disabled).code());
      assertEquals(
          new Retention(Duration.ZERO, Duration.ofDays(2)),
          backend.testHandle().effectiveRetention(METRIC_V2).get());
      assertEquals(3, backend.testHandle().counters()
          .declarationOutcomeCount(SchemaStatus.Code.ACCEPTED));
    } finally {
      backend.close();
    }
  }

  @Test
  void exactSchemaValidationProducesTruthfulWriteResultsWithoutFrameworkClockPolicy() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      declare(backend, recordSchema(METRIC_V1));

      StampedObservation undeclaredObservation = stamped(
          new MetricVersionId(17, 2), dimensions(DimValue.of("t"), DimValue.of(1L)), NOW_MS);
      WriteResult undeclared =
          backend.record(Arrays.asList(undeclaredObservation, undeclaredObservation));
      assertWrite(undeclared, 0, 2, Status.Code.NOT_DECLARED);

      assertWrite(backend.record(Collections.singletonList(stamped(
          METRIC_V1, Collections.singletonMap("table", DimValue.of("t")), NOW_MS))),
          0, 1, Status.Code.INVALID_REQUEST);
      assertWrite(backend.record(Collections.singletonList(stamped(
          METRIC_V1, dimensions(DimValue.of("t"), DimValue.of("wrong")), NOW_MS))),
          0, 1, Status.Code.INVALID_REQUEST);

      Map<String, DimValue> extra = dimensions(DimValue.of("t"), DimValue.of(1L));
      extra.put("other", DimValue.of("x"));
      assertWrite(backend.record(Collections.singletonList(stamped(METRIC_V1, extra, NOW_MS))),
          0, 1, Status.Code.INVALID_REQUEST);

      // The future-time policy belongs to the raw MetricStore/framework boundary, not this SPI.
      assertWrite(backend.record(Collections.singletonList(stamped(
          METRIC_V1, dimensions(DimValue.of("t"), DimValue.of(1L)),
          NOW_MS + Duration.ofMinutes(5).toMillis() + 1))), 1, 0, Status.Code.OK);

      assertEquals(2, backend.testHandle().rejectionCount(
          LocalBackendTestHandle.RejectionReason.NOT_DECLARED));
      LocalBackendTestHandle.CounterSnapshot counters =
          backend.testHandle().counters();
      assertEquals(1, counters.recordAccepted());
      assertEquals(5, counters.recordRejected());
      assertEquals(1, counters.recordOutcomeCount(Status.Code.NOT_DECLARED));
      assertEquals(3, counters.recordOutcomeCount(Status.Code.INVALID_REQUEST));
      assertEquals(1, counters.recordOutcomeCount(Status.Code.OK));
      assertEquals(3, backend.testHandle().rejectionCount(
          LocalBackendTestHandle.RejectionReason.DIMENSION_MISMATCH));
    } finally {
      backend.close();
    }
  }

  @Test
  void acceptedWritesAreImmediatelyInspectableWithStableUniqueOrdinalsAndCopies()
      throws Exception {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    byte[] originalBytes = new byte[253];
    originalBytes[0] = 7;
    try {
      declare(backend, byteSchema(METRIC_V1));
      DimValue maximumFrame = DimValue.of(originalBytes);
      assertEquals(DimValue.MAX_CANONICAL_BYTES, maximumFrame.canonicalBytes().length);
      StampedObservation first = stamped(
          METRIC_V1, Collections.singletonMap("fingerprint", maximumFrame), NOW_MS);
      originalBytes[0] = 99;
      StampedObservation second = stamped(
          METRIC_V1, Collections.singletonMap("fingerprint", DimValue.of(new byte[] {8})), NOW_MS);

      assertWrite(backend.record(Arrays.asList(first, second)), 2, 0, Status.Code.OK);
      List<LocalBackendTestHandle.StoredObservation> stored =
          backend.testHandle().observations(METRIC_V1);
      assertEquals(2, stored.size());
      assertTrue(stored.get(0).acceptanceOrdinal() < stored.get(1).acceptanceOrdinal());
      assertEquals(first.provenance(), stored.get(0).stamped().provenance());
      assertNotSame(first, stored.get(0).stamped());
      assertNotSame(first.provenance(), stored.get(0).stamped().provenance());
      byte[] inspected = stored.get(0).stamped().observation()
          .dimensions().get("fingerprint").bytesValue();
      assertEquals(7, inspected[0]);
      inspected[0] = 42;
      assertArrayEquals(
          new byte[] {7}, Arrays.copyOf(
              backend.testHandle().observations(METRIC_V1).get(0).stamped().observation()
                  .dimensions().get("fingerprint").bytesValue(), 1));
      assertThrows(UnsupportedOperationException.class, () -> stored.clear());

      CyclicBarrier barrier = new CyclicBarrier(3);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<WriteResult> left = executor.submit(
            () -> recordAfterBarrier(backend, second, barrier));
        Future<WriteResult> right = executor.submit(
            () -> recordAfterBarrier(backend, second, barrier));
        barrier.await();
        assertEquals(Status.Code.OK, left.get().status().code());
        assertEquals(Status.Code.OK, right.get().status().code());
      } finally {
        executor.shutdownNow();
      }
      Set<Long> ordinals = new HashSet<Long>();
      for (LocalBackendTestHandle.StoredObservation observation :
          backend.testHandle().observations(METRIC_V1)) {
        ordinals.add(observation.acceptanceOrdinal());
      }
      assertEquals(4, ordinals.size());
    } finally {
      backend.close();
    }
  }

  @Test
  void mixedRejectionsAndBackendFailuresAreContainedWithoutPartialException() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      declare(backend, recordSchema(METRIC_V1));
      StampedObservation valid =
          stamped(METRIC_V1, dimensions(DimValue.of("t"), DimValue.of(1L)), NOW_MS);
      StampedObservation invalid =
          stamped(METRIC_V1, Collections.singletonMap("table", DimValue.of("t")), NOW_MS);

      WriteResult mixed = backend.record(Arrays.asList(valid, invalid));
      assertWrite(mixed, 0, 2, Status.Code.UNAVAILABLE);
      assertEquals(0, backend.testHandle().observations(METRIC_V1).size());

      LocalBackendTestHandle.CounterSnapshot snapshot =
          backend.testHandle().counters();
      assertEquals(0, snapshot.recordAccepted());
      assertEquals(2, snapshot.recordRejected());
      assertEquals(1, snapshot.recordOutcomeCount(Status.Code.UNAVAILABLE));
      assertEquals(1, snapshot.rejectionCount(
          LocalBackendTestHandle.RejectionReason.DIMENSION_MISMATCH));
      assertEquals(1, snapshot.rejectionCount(
          LocalBackendTestHandle.RejectionReason.BATCH_ABORTED));

      StampedObservation undeclared = stamped(
          METRIC_V2, dimensions(DimValue.of("t"), DimValue.of(1L)), NOW_MS);
      WriteResult multipleReasons = backend.record(Arrays.asList(invalid, undeclared));
      assertWrite(multipleReasons, 0, 2, Status.Code.UNAVAILABLE);
      assertEquals(2, snapshot.recordRejected());
    } finally {
      backend.close();
    }

    LocalHistoryMetricsBackend unavailable = backend(fixedClock());
    declare(unavailable, recordSchema(METRIC_V1));
    unavailable.close();
    StampedObservation valid =
        stamped(METRIC_V1, dimensions(DimValue.of("t"), DimValue.of(1L)), NOW_MS);
    WriteResult rejected = unavailable.record(Collections.singletonList(valid));
    assertWrite(rejected, 0, 1, Status.Code.UNAVAILABLE);
    assertEquals(0, unavailable.testHandle().observations(METRIC_V1).size());
    assertEquals(1, unavailable.testHandle().rejectionCount(
        LocalBackendTestHandle.RejectionReason.BACKEND_FAILURE));
  }

  @Test
  void undeclaredSummaryReturnsExplicitNotDeclaredStatus() {
    LocalHistoryMetricsBackend backend = backend(fixedClock());
    try {
      SummaryRequest request =
          SummaryRequest.builder(METRIC_V1).window(1L, 2L).build();
      List<SummaryResponse> responses =
          backend.summarize(Collections.singletonList(request), TIMEOUT);
      assertEquals(1, responses.size());
      assertEquals(Status.Code.NOT_DECLARED, responses.get(0).status().code());
    } finally {
      backend.close();
    }
  }

  private static LocalHistoryMetricsBackend backend(Clock clock) {
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(17, "test.metric"));
    return LocalHistoryMetricsBackend.create(catalog, clock, MAX_PLANNING_AGE);
  }

  private static SchemaStatus declare(LocalHistoryMetricsBackend backend, MetricSchema schema) {
    return backend.declare(Collections.singletonList(schema), TIMEOUT).get(0);
  }

  private static SchemaStatus declareAfterBarrier(
      LocalHistoryMetricsBackend backend, MetricSchema schema, CyclicBarrier barrier)
      throws Exception {
    await(barrier);
    return declare(backend, schema);
  }

  private static WriteResult recordAfterBarrier(
      LocalHistoryMetricsBackend backend, StampedObservation observation, CyclicBarrier barrier)
      throws Exception {
    await(barrier);
    return backend.record(Collections.singletonList(observation));
  }

  private static void await(CyclicBarrier barrier)
      throws InterruptedException, BrokenBarrierException {
    barrier.await();
  }

  private static MetricSchema recordSchema(MetricVersionId metric) {
    return schema(metric, Arrays.asList(
        new DimensionSpec("table", DimValue.Kind.STRING),
        new DimensionSpec("bucket", DimValue.Kind.LONG)), Duration.ofDays(3));
  }

  private static MetricSchema byteSchema(MetricVersionId metric) {
    return schema(metric, Collections.singletonList(
        new DimensionSpec("fingerprint", DimValue.Kind.BYTES)), Duration.ofDays(3));
  }

  private static MetricSchema schema(
      MetricVersionId metric, List<DimensionSpec> dimensions, Duration planningMaxAge) {
    return schema(metric, dimensions, planningMaxAge, Duration.ofDays(30));
  }

  private static MetricSchema schema(
      MetricVersionId metric,
      List<DimensionSpec> dimensions,
      Duration planningMaxAge,
      Duration storageRetention) {
    return new MetricSchema(
        metric, dimensions, new Retention(planningMaxAge, storageRetention));
  }

  private static List<DimensionSpec> dimensions(String first, String second) {
    return Arrays.asList(
        new DimensionSpec(first, DimValue.Kind.STRING),
        new DimensionSpec(second, DimValue.Kind.STRING));
  }

  private static List<DimensionSpec> dimensions(String only) {
    return Collections.singletonList(new DimensionSpec(only, DimValue.Kind.STRING));
  }

  private static Map<String, DimValue> dimensions(DimValue table, DimValue bucket) {
    Map<String, DimValue> dimensions = new HashMap<String, DimValue>();
    dimensions.put("table", table);
    dimensions.put("bucket", bucket);
    return dimensions;
  }

  private static StampedObservation stamped(
      MetricVersionId metric, Map<String, DimValue> dimensions, long timestampMs) {
    return new StampedObservation(
        new Observation(metric, dimensions, 1.25, timestampMs),
        new Provenance("app-1", "attempt-1", "26.10.0", NOW_MS));
  }

  private static void assertWrite(
      WriteResult result, int accepted, int rejected, Status.Code code) {
    assertEquals(accepted, result.accepted());
    assertEquals(rejected, result.rejected());
    assertEquals(code, result.status().code());
  }

  private static Clock fixedClock() {
    return Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
  }

}
