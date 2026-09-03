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
package com.nvidia.spark.history.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.nvidia.spark.history.Coverage;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.Summary;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.TestHistoryMetricCatalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reusable black-box provider-neutral conformance tests for the observable MVP
 * {@link MetricStore} contract.
 *
 * <p>Provider modules inherit this suite and supply only a lifecycle adapter. API value-object tests
 * remain separate because passing those tests alone does not establish provider conformance. Stored
 * provenance inspection is intentionally outside this core suite and belongs in provider-specific
 * acceptance tooling.
 */
public abstract class MetricStoreProviderContract {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final Duration MAXIMUM_PLANNING_AGE = Duration.ofSeconds(10);
  private static final Duration STORAGE_RETENTION = Duration.ofDays(1);
  private static final long INITIAL_NOW_MS = 10_000L;
  private static final MetricVersionId METRIC_V1 = new MetricVersionId(101, 1);
  private static final MetricVersionId METRIC_V2 = new MetricVersionId(101, 2);
  private static final MetricVersionId UNDECLARED_METRIC = new MetricVersionId(101, 99);

  private HistoryMetricsProviderFixture fixture;

  /**
   * Supplies the provider-specific lifecycle adapter used to open each isolated fixture.
   *
   * @return a non-null factory for this provider
   */
  protected abstract HistoryMetricsProviderFactory providerFactory();

  @BeforeEach
  public final void openProviderFixture() throws Exception {
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(101, "tck.metric"));
    fixture = providerFactory().open(catalog, INITIAL_NOW_MS, MAXIMUM_PLANNING_AGE);
    assertNotNull(fixture);
    assertNotNull(fixture.catalog());
    assertNotNull(fixture.store());
  }

  @AfterEach
  public final void closeProviderFixture() throws Exception {
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  public final void declarationIsIdempotentRejectsConflictsAndKeepsVersionsIndependent() {
    MetricSchema v1 = schema(METRIC_V1, standardDimensions(), MAXIMUM_PLANNING_AGE);
    MetricSchema sameContractDifferentRecommendation = new MetricSchema(
        METRIC_V1,
        standardDimensions(),
        new Retention(Duration.ofSeconds(5), STORAGE_RETENTION));
    MetricSchema conflictingV1 =
        schema(METRIC_V1, reversedDimensions(), MAXIMUM_PLANNING_AGE);
    MetricSchema v2 = schema(METRIC_V2, reversedDimensions(), MAXIMUM_PLANNING_AGE);

    assertDeclaration(SchemaStatus.Code.ACCEPTED, v1);
    assertDeclaration(SchemaStatus.Code.ACCEPTED, sameContractDifferentRecommendation);
    assertDeclaration(SchemaStatus.Code.INCOMPATIBLE, conflictingV1);
    assertDeclaration(SchemaStatus.Code.ACCEPTED, v2);

    record(
        observation(METRIC_V1, dimensions("a", 1L), 1.0, 8_000L),
        observation(METRIC_V2, dimensions("a", 1L), 9.0, 8_000L));
    assertEquals(1.0, onlySummary(request(METRIC_V1, dimensions("a", 1L), 0L,
        INITIAL_NOW_MS + 1L, 0)).mean(), 0.0);
    assertEquals(9.0, onlySummary(request(METRIC_V2, dimensions("a", 1L), 0L,
        INITIAL_NOW_MS + 1L, 0)).mean(), 0.0);
  }

  @Test
  public final void exactAndWildcardEqualityProduceUnweightedSummaries() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());
    record(
        observation(METRIC_V1, dimensions("a", 1L), 2.0, 7_000L),
        observation(METRIC_V1, dimensions("a", 2L), 4.0, 8_000L),
        observation(METRIC_V1, dimensions("b", 1L), 100.0, 8_000L));

    Summary exact = onlySummary(request(
        METRIC_V1, dimensions("a", 1L), 0L, INITIAL_NOW_MS + 1L, 0));
    assertSummary(exact, 1L, 2.0, 2.0, 2.0, 7_000L, 7_000L);

    Summary wildcard = onlySummary(request(
        METRIC_V1, Collections.singletonMap("table", DimValue.of("a")),
        0L, INITIAL_NOW_MS + 1L, 0));
    assertSummary(wildcard, 2L, 3.0, 2.0, 4.0, 7_000L, 8_000L);
  }

  @Test
  public final void observationWindowIsInclusiveFromAndExclusiveTo() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());
    record(
        observation(METRIC_V1, dimensions("a", 1L), 1.0, 7_000L),
        observation(METRIC_V1, dimensions("a", 1L), 2.0, 8_000L),
        observation(METRIC_V1, dimensions("a", 1L), 3.0, 9_000L));

    Summary selected = onlySummary(request(
        METRIC_V1, dimensions("a", 1L), 7_000L, 9_000L, 0));
    assertSummary(selected, 2L, 1.5, 1.0, 2.0, 7_000L, 8_000L);
  }

  @Test
  public final void retentionUsesTheControllableProviderClockAndReportsClipping() {
    MetricSchema shortWindow =
        schema(METRIC_V1, standardDimensions(), Duration.ofSeconds(1));
    assertDeclaration(SchemaStatus.Code.ACCEPTED, shortWindow);
    record(
        observation(METRIC_V1, dimensions("a", 1L), 1.0, 9_999L),
        observation(METRIC_V1, dimensions("a", 1L), 2.0, 10_000L));

    fixture.setProviderTime(11_000L);
    SummaryResponse clipped = onlyResponse(request(
        METRIC_V1, dimensions("a", 1L), 0L, 12_000L, 0));
    assertEquals(Status.Code.OK, clipped.status().code());
    assertEquals(Coverage.WINDOW_CLIPPED, clipped.coverage());
    assertSummary(clipped.summary(), 1L, 2.0, 2.0, 2.0, 10_000L, 10_000L);

    SummaryResponse complete = onlyResponse(request(
        METRIC_V1, dimensions("a", 1L), 10_000L, 12_000L, 0));
    assertEquals(Coverage.COMPLETE, complete.coverage());
    assertEquals(1L, complete.summary().count());
  }

  @Test
  public final void fullyBoundLimitUsesTimestampThenProviderAcceptanceOrder() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());
    record(
        observation(METRIC_V1, dimensions("a", 1L), 10.0, 8_000L),
        observation(METRIC_V1, dimensions("a", 1L), 20.0, 8_000L),
        observation(METRIC_V1, dimensions("a", 1L), 30.0, 8_001L));

    Summary newestTwo = onlySummary(request(
        METRIC_V1, dimensions("a", 1L), 0L, INITIAL_NOW_MS + 1L, 2));
    assertSummary(newestTwo, 2L, 25.0, 20.0, 30.0, 8_000L, 8_001L);
  }

  @Test
  public final void emptyUnknownAndInvalidRequestsHaveExplicitStatuses() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());

    SummaryResponse empty = onlyResponse(request(
        METRIC_V1, dimensions("missing", 1L), 0L, INITIAL_NOW_MS + 1L, 0));
    assertEquals(Status.Code.OK, empty.status().code());
    assertEquals(Coverage.COMPLETE, empty.coverage());
    assertNull(empty.summary());

    SummaryResponse unknown = onlyResponse(request(
        UNDECLARED_METRIC, Collections.<String, DimValue>emptyMap(),
        0L, INITIAL_NOW_MS + 1L, 0));
    assertEquals(Status.Code.NOT_DECLARED, unknown.status().code());
    assertNull(unknown.summary());
    assertNull(unknown.coverage());

    SummaryRequest valid = request(
        METRIC_V1, dimensions("missing", 1L), 0L, INITIAL_NOW_MS + 1L, 0);
    SummaryRequest unknownDimension = request(
        METRIC_V1, Collections.singletonMap("other", DimValue.of("x")),
        0L, INITIAL_NOW_MS + 1L, 0);
    SummaryRequest wrongKind = request(
        METRIC_V1, Collections.singletonMap("bucket", DimValue.of("not-a-long")),
        0L, INITIAL_NOW_MS + 1L, 0);
    SummaryRequest wildcardLimit = request(
        METRIC_V1, Collections.singletonMap("table", DimValue.of("a")),
        0L, INITIAL_NOW_MS + 1L, 1);

    List<SummaryResponse> positional = fixture.store().summarize(
        Arrays.asList(valid, unknownDimension, wrongKind, wildcardLimit), TIMEOUT);
    assertEquals(4, positional.size());
    assertEquals(Status.Code.OK, positional.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, positional.get(1).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, positional.get(2).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, positional.get(3).status().code());

    List<SummaryResponse> nullElement =
        fixture.store().summarize(Arrays.asList(valid, null), TIMEOUT);
    assertEquals(2, nullElement.size());
    assertEquals(Status.Code.INVALID_REQUEST, nullElement.get(0).status().code());
    assertEquals(Status.Code.INVALID_REQUEST, nullElement.get(1).status().code());

    assertEquals(
        Status.Code.DEADLINE_EXCEEDED,
        fixture.store().summarize(Collections.singletonList(valid), Duration.ZERO)
            .get(0).status().code());
  }

  @Test
  public final void validSummaryBatchPreservesPositionAndCardinality() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());
    record(
        observation(METRIC_V1, dimensions("a", 1L), 3.0, 7_000L),
        observation(METRIC_V1, dimensions("b", 1L), 7.0, 8_000L));

    List<SummaryRequest> requests = Arrays.asList(
        request(METRIC_V1, dimensions("a", 1L), 0L, INITIAL_NOW_MS + 1L, 0),
        request(METRIC_V1, dimensions("b", 1L), 0L, INITIAL_NOW_MS + 1L, 0),
        request(METRIC_V1, dimensions("missing", 1L), 0L, INITIAL_NOW_MS + 1L, 0));
    List<SummaryResponse> responses = fixture.store().summarize(requests, TIMEOUT);

    assertEquals(requests.size(), responses.size());
    assertEquals(3.0, responses.get(0).summary().mean(), 0.0);
    assertEquals(7.0, responses.get(1).summary().mean(), 0.0);
    assertNull(responses.get(2).summary());
    assertEquals(Status.Code.OK, responses.get(2).status().code());
  }

  @Test
  public final void compensatedMeanIsFiniteAccurateAndOverflowSafe() {
    assertDeclaration(SchemaStatus.Code.ACCEPTED, standardSchema());
    record(
        observation(METRIC_V1, dimensions("cancel", 1L), 1.0e16, 7_000L),
        observation(METRIC_V1, dimensions("cancel", 1L), 1.0, 7_001L),
        observation(METRIC_V1, dimensions("cancel", 1L), -1.0e16, 7_002L),
        observation(METRIC_V1, dimensions("max", 1L), Double.MAX_VALUE, 7_000L),
        observation(METRIC_V1, dimensions("max", 1L), Double.MAX_VALUE, 7_001L),
        observation(METRIC_V1, dimensions("opposite", 1L), Double.MAX_VALUE, 7_000L),
        observation(METRIC_V1, dimensions("opposite", 1L), -Double.MAX_VALUE, 7_001L));

    assertClose(1.0 / 3.0, onlySummary(request(
        METRIC_V1, dimensions("cancel", 1L), 0L, INITIAL_NOW_MS + 1L, 0)).mean());
    assertEquals(Double.MAX_VALUE, onlySummary(request(
        METRIC_V1, dimensions("max", 1L), 0L, INITIAL_NOW_MS + 1L, 0)).mean());
    assertClose(0.0, onlySummary(request(
        METRIC_V1, dimensions("opposite", 1L), 0L, INITIAL_NOW_MS + 1L, 0)).mean());
  }

  private void assertDeclaration(SchemaStatus.Code expected, MetricSchema schema) {
    List<SchemaStatus> statuses =
        fixture.store().declare(Collections.singletonList(schema), TIMEOUT);
    assertEquals(1, statuses.size());
    assertEquals(schema.metric(), statuses.get(0).metric());
    assertEquals(expected, statuses.get(0).code());
  }

  private void record(Observation... observations) {
    fixture.store().record(Arrays.asList(observations));
    assertTrue(fixture.awaitWrites(TIMEOUT), "provider did not make accepted writes visible");
  }

  private Summary onlySummary(SummaryRequest request) {
    SummaryResponse response = onlyResponse(request);
    assertEquals(Status.Code.OK, response.status().code());
    assertNotNull(response.summary());
    return response.summary();
  }

  private SummaryResponse onlyResponse(SummaryRequest request) {
    List<SummaryResponse> responses =
        fixture.store().summarize(Collections.singletonList(request), TIMEOUT);
    assertEquals(1, responses.size());
    return responses.get(0);
  }

  private static MetricSchema standardSchema() {
    return schema(METRIC_V1, standardDimensions(), MAXIMUM_PLANNING_AGE);
  }

  private static MetricSchema schema(
      MetricVersionId metric, List<DimensionSpec> dimensions, Duration planningMaxAge) {
    return new MetricSchema(
        metric, dimensions, new Retention(planningMaxAge, STORAGE_RETENTION));
  }

  private static List<DimensionSpec> standardDimensions() {
    return Arrays.asList(
        new DimensionSpec("table", DimValue.Kind.STRING),
        new DimensionSpec("bucket", DimValue.Kind.LONG));
  }

  private static List<DimensionSpec> reversedDimensions() {
    return Arrays.asList(
        new DimensionSpec("bucket", DimValue.Kind.LONG),
        new DimensionSpec("table", DimValue.Kind.STRING));
  }

  private static Observation observation(
      MetricVersionId metric,
      Map<String, DimValue> dimensions,
      double value,
      long timestampMs) {
    return new Observation(metric, dimensions, value, timestampMs);
  }

  private static SummaryRequest request(
      MetricVersionId metric,
      Map<String, DimValue> bound,
      long fromMs,
      long toMs,
      int limit) {
    return SummaryRequest.builder(metric)
        .bound(bound)
        .window(fromMs, toMs)
        .limit(limit)
        .build();
  }

  private static Map<String, DimValue> dimensions(String table, long bucket) {
    Map<String, DimValue> values = new LinkedHashMap<String, DimValue>();
    values.put("table", DimValue.of(table));
    values.put("bucket", DimValue.of(bucket));
    return values;
  }

  private static void assertSummary(
      Summary summary,
      long count,
      double mean,
      double min,
      double max,
      long firstObservedMs,
      long lastObservedMs) {
    assertNotNull(summary);
    assertEquals(count, summary.count());
    assertClose(mean, summary.mean());
    assertEquals(min, summary.min(), 0.0);
    assertEquals(max, summary.max(), 0.0);
    assertEquals(firstObservedMs, summary.firstObservedMs());
    assertEquals(lastObservedMs, summary.lastObservedMs());
  }

  private static void assertClose(double expected, double actual) {
    double tolerance = 1.0e-12 + 1.0e-12 * Math.abs(expected);
    assertEquals(expected, actual, tolerance);
  }
}
