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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import com.nvidia.spark.history.Summary;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Compiled integration examples for the abstention-first local history metrics workflow. */
public final class HistoryMetricsIntegrationExampleTest {
  // These IDs, values, durations, and policy bounds are illustrative test inputs, not allocations,
  // defaults, or recommendations.
  private static final MetricVersionId EXAMPLE_METRIC = new MetricVersionId(61001, 1);
  private static final Clock DRIVER_CLOCK =
      Clock.fixed(Instant.ofEpochMilli(10_000L), ZoneOffset.UTC);
  private static final Duration OPERATION_BUDGET = Duration.ofSeconds(7);
  private static final Duration MAXIMUM_PLANNING_AGE = Duration.ofHours(2);
  private static final LocalQueuePolicy QUEUE_POLICY = LocalQueuePolicy.of(19, 7);
  private static final LocalExecutionPolicy EXECUTION_POLICY = LocalExecutionPolicy.of(2, 11);
  private static final LocalCircuitBreakerPolicy BREAKER_POLICY =
      LocalCircuitBreakerPolicy.of(
          9, 5, 0.75, Duration.ofMillis(850), 0.60, Duration.ofSeconds(3));

  @Test
  void localOwnerRunsDeclareRecordDrainAndExactAndWildcardSummaries() throws Exception {
    MetricStore previous = MetricStores.current();
    LocalHistoryMetrics owner = openEmpty();
    try {
      AutoCloseable registration = MetricStores.install(owner.store());
      try {
        assertSame(owner.store(), MetricStores.current());

        assertEquals(
            SchemaStatus.Code.ACCEPTED,
            owner.store().declare(
                Collections.singletonList(exampleSchema()), OPERATION_BUDGET).get(0).code());

        owner.store().record(Arrays.asList(
            observation("orders", "parquet", 2.0, 9_000L),
            observation("orders", "orc", 4.0, 9_500L)));
        assertTrue(owner.drain(OPERATION_BUDGET));

        SummaryResponse exact = summarize(owner.store(), exactRequest());
        assertEquals(Status.Code.OK, exact.status().code());
        assertEquals(Coverage.COMPLETE, exact.coverage());
        assertEquals(1L, exact.summary().count());
        assertEquals(2.0, exact.summary().mean());

        SummaryResponse wildcard = summarize(owner.store(), wildcardRequest());
        assertEquals(Status.Code.OK, wildcard.status().code());
        assertEquals(Coverage.COMPLETE, wildcard.coverage());
        assertEquals(2L, wildcard.summary().count());
        assertEquals(3.0, wildcard.summary().mean());

        assertEquals(
            DecisionPath.HISTORY_ELIGIBLE,
            chooseWholeDecision(Collections.singletonList(wildcard), 1));
        assertEquals(
            2L,
            owner.testHandle().counters().value(LocalMetricCounter.RECORD_ENQUEUED));
        assertEquals(
            2L,
            owner.testHandle().counters().value(LocalMetricCounter.BACKEND_ACCEPTED));
        assertEquals(
            2L,
            owner.testHandle().counters().value(LocalMetricCounter.SUMMARY_STATUS_OK));
      } finally {
        registration.close();
      }

      assertSame(previous, MetricStores.current());
      assertEquals(Status.Code.OK, summarize(owner.store(), exactRequest()).status().code());
    } finally {
      assertTrue(owner.shutdown(OPERATION_BUDGET));
    }
  }

  @Test
  void realLocalStoreNonOkSummaryUsesStaticFallback() {
    LocalHistoryMetrics owner = openEmpty();
    try {
      SummaryResponse response = summarize(owner.store(), exactRequest());
      assertEquals(Status.Code.NOT_DECLARED, response.status().code());
      assertEquals(
          DecisionPath.STATIC_FALLBACK,
          chooseWholeDecision(Collections.singletonList(response), 1));
    } finally {
      assertTrue(owner.shutdown(OPERATION_BUDGET));
    }
  }

  @Test
  void wholeDecisionUsesStaticFallbackForAbsenceErrorsAndMalformedBatches() {
    assertEquals(
        DecisionPath.STATIC_FALLBACK,
        chooseWholeDecision(
            Collections.singletonList(SummaryResponse.ok(null, Coverage.COMPLETE)), 1));

    for (Status.Code code : Arrays.asList(
        Status.Code.NOT_DECLARED,
        Status.Code.INVALID_REQUEST,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNAVAILABLE,
        Status.Code.DENIED)) {
      SummaryResponse response =
          SummaryResponse.error(Status.of(code, "bounded example reason"));
      assertEquals(
          DecisionPath.STATIC_FALLBACK,
          chooseWholeDecision(Collections.singletonList(response), 1));
    }

    assertEquals(DecisionPath.STATIC_FALLBACK, chooseWholeDecision(null, 1));
    assertEquals(
        DecisionPath.STATIC_FALLBACK,
        chooseWholeDecision(Collections.singletonList(null), 1));
    assertEquals(
        DecisionPath.STATIC_FALLBACK,
        chooseWholeDecision(Collections.<SummaryResponse>emptyList(), 1));
    assertEquals(
        DecisionPath.STATIC_FALLBACK,
        chooseWholeDecision(
            Arrays.asList(evidence(Coverage.COMPLETE), evidence(Coverage.COMPLETE)), 1));

    // WINDOW_CLIPPED reports coverage. This structural gate does not invent an evidence threshold.
    assertEquals(
        DecisionPath.HISTORY_ELIGIBLE,
        chooseWholeDecision(
            Collections.singletonList(evidence(Coverage.WINDOW_CLIPPED)), 1));
  }

  @Test
  void explicitSnapshotRoundTripRestoresQueryableState(@TempDir Path temporaryDirectory)
      throws Exception {
    Path snapshot = temporaryDirectory.resolve("history-metrics.snapshot");
    HistoryMetricCatalog catalog = exampleCatalog();

    LocalHistoryMetrics writer = open(catalog);
    try {
      assertEquals(
          SchemaStatus.Code.ACCEPTED,
          writer.store().declare(
              Collections.singletonList(exampleSchema()), OPERATION_BUDGET).get(0).code());
      writer.store().record(Collections.singletonList(
          observation("orders", "parquet", 6.0, 9_250L)));
      assertTrue(writer.drain(OPERATION_BUDGET));
      writer.save(snapshot, OPERATION_BUDGET);
    } finally {
      assertTrue(writer.shutdown(OPERATION_BUDGET));
    }

    LocalHistoryMetrics restored = LocalHistoryMetricsFactory.openSnapshot(
        snapshot,
        catalog,
        DRIVER_CLOCK,
        HistoryMetricsIntegrationExampleTest::exampleProvenance,
        MAXIMUM_PLANNING_AGE,
        QUEUE_POLICY,
        EXECUTION_POLICY,
        BREAKER_POLICY,
        OPERATION_BUDGET);
    try {
      SummaryResponse response = summarize(restored.store(), exactRequest());
      assertEquals(Status.Code.OK, response.status().code());
      assertEquals(1L, response.summary().count());
      assertEquals(6.0, response.summary().mean());
    } finally {
      assertTrue(restored.shutdown(OPERATION_BUDGET));
    }
  }

  private static DecisionPath chooseWholeDecision(
      List<SummaryResponse> responses, int expectedResponses) {
    if (responses == null || responses.size() != expectedResponses) {
      return DecisionPath.STATIC_FALLBACK;
    }
    for (SummaryResponse response : responses) {
      if (response == null ||
          response.status() == null ||
          response.status().code() != Status.Code.OK ||
          response.summary() == null ||
          response.coverage() == null) {
        return DecisionPath.STATIC_FALLBACK;
      }
    }
    return DecisionPath.HISTORY_ELIGIBLE;
  }

  private static SummaryResponse evidence(Coverage coverage) {
    return SummaryResponse.ok(Summary.of(1L, 3.0, 3.0, 3.0, 9_000L, 9_000L), coverage);
  }

  private static SummaryResponse summarize(MetricStore store, SummaryRequest request) {
    return store.summarize(Collections.singletonList(request), OPERATION_BUDGET).get(0);
  }

  private static LocalHistoryMetrics openEmpty() {
    return open(exampleCatalog());
  }

  private static LocalHistoryMetrics open(HistoryMetricCatalog catalog) {
    return LocalHistoryMetricsFactory.open(
        catalog,
        DRIVER_CLOCK,
        HistoryMetricsIntegrationExampleTest::exampleProvenance,
        MAXIMUM_PLANNING_AGE,
        QUEUE_POLICY,
        EXECUTION_POLICY,
        BREAKER_POLICY);
  }

  private static LocalProvenanceIdentity exampleProvenance() {
    return LocalProvenanceIdentity.of("redacted-example-app", "attempt-1", "example-build");
  }

  private static HistoryMetricCatalog exampleCatalog() {
    return LocalTestCatalog.builder()
        .addLive(EXAMPLE_METRIC.metricId(), "example.scan.expansion")
        .build();
  }

  private static MetricSchema exampleSchema() {
    return new MetricSchema(
        EXAMPLE_METRIC,
        Arrays.asList(
            new DimensionSpec("relation", DimValue.Kind.STRING),
            new DimensionSpec("format", DimValue.Kind.STRING)),
        new Retention(Duration.ofMinutes(37), Duration.ofHours(13)));
  }

  private static Observation observation(
      String relation, String format, double value, long timestampMs) {
    java.util.Map<String, DimValue> dimensions =
        new java.util.LinkedHashMap<String, DimValue>();
    dimensions.put("relation", DimValue.of(relation));
    dimensions.put("format", DimValue.of(format));
    return new Observation(EXAMPLE_METRIC, dimensions, value, timestampMs);
  }

  private static SummaryRequest exactRequest() {
    return SummaryRequest.builder(EXAMPLE_METRIC)
        .bind("relation", DimValue.of("orders"))
        .bind("format", DimValue.of("parquet"))
        .window(8_000L, 11_000L)
        .build();
  }

  private static SummaryRequest wildcardRequest() {
    return SummaryRequest.builder(EXAMPLE_METRIC)
        .bind("relation", DimValue.of("orders"))
        .window(8_000L, 11_000L)
        .build();
  }

  private enum DecisionPath {
    HISTORY_ELIGIBLE,
    STATIC_FALLBACK
  }
}
