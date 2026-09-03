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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-04 and FR-09 through FR-14 immutable summary contracts. */
class SummaryContractTest {
  @Test
  void fr04RequestBuilderPreservesExplicitEqualityBindingsAndWindow() {
    Map<String, DimValue> bound = new HashMap<String, DimValue>();
    bound.put("table", DimValue.of("orders"));
    SummaryRequest request = SummaryRequest.builder(new MetricVersionId(5, 1))
        .bound(bound)
        .window(100L, 200L)
        .limit(3)
        .build();
    bound.clear();

    assertEquals(new MetricVersionId(5, 1), request.metric());
    assertEquals(DimValue.of("orders"), request.bound().get("table"));
    assertEquals(100L, request.fromMs());
    assertEquals(200L, request.toMs());
    assertEquals(3, request.limit());
    assertThrows(UnsupportedOperationException.class, () ->
        request.bound().put("other", DimValue.of(1L)));
    PublicApiSurface.assertConstructors(SummaryRequest.class);
    PublicApiSurface.assertMethods(
        SummaryRequest.class,
        "bound():java.util.Map",
        "builder(com.nvidia.spark.history.MetricVersionId):" +
            "com.nvidia.spark.history.SummaryRequest$Builder",
        "equals(java.lang.Object):boolean",
        "fromMs():long",
        "hashCode():int",
        "limit():int",
        "metric():com.nvidia.spark.history.MetricVersionId",
        "toMs():long",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(SummaryRequest.Builder.class);
    PublicApiSurface.assertMethods(
        SummaryRequest.Builder.class,
        "bind(java.lang.String,com.nvidia.spark.history.DimValue):" +
            "com.nvidia.spark.history.SummaryRequest$Builder",
        "bound(java.util.Map):com.nvidia.spark.history.SummaryRequest$Builder",
        "build():com.nvidia.spark.history.SummaryRequest",
        "limit(int):com.nvidia.spark.history.SummaryRequest$Builder",
        "window(long,long):com.nvidia.spark.history.SummaryRequest$Builder");
  }

  @Test
  void fr09RequestBuilderValidatesOnlyIntrinsicRequestInvariants() {
    assertThrows(NullPointerException.class, () -> SummaryRequest.builder(null));
    assertThrows(IllegalArgumentException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).window(10L, 10L).build());
    assertThrows(IllegalArgumentException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).window(11L, 10L).build());
    assertThrows(IllegalArgumentException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).window(1L, 2L).limit(-1));
    assertThrows(IllegalStateException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).build());

    Map<String, DimValue> nullValue = new HashMap<String, DimValue>();
    nullValue.put("table", null);
    assertThrows(NullPointerException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).bound(nullValue));
    assertThrows(IllegalArgumentException.class, () ->
        SummaryRequest.builder(new MetricVersionId(1, 1)).bind("\uD800", DimValue.of(1L)));
  }

  @Test
  void fr10SummaryValidatesFixedEvidenceShape() {
    Summary summary = Summary.of(3L, 2.0, 1.0, 3.0, 100L, 200L);

    assertEquals(3L, summary.count());
    assertEquals(2.0, summary.mean());
    assertEquals(1.0, summary.min());
    assertEquals(3.0, summary.max());
    assertEquals(100L, summary.firstObservedMs());
    assertEquals(200L, summary.lastObservedMs());

    assertThrows(IllegalArgumentException.class, () ->
        Summary.of(0L, 2.0, 1.0, 3.0, 100L, 200L));
    assertThrows(IllegalArgumentException.class, () ->
        Summary.of(1L, Double.NaN, 1.0, 3.0, 100L, 200L));
    assertThrows(IllegalArgumentException.class, () ->
        Summary.of(1L, 0.0, 1.0, 3.0, 100L, 200L));
    assertThrows(IllegalArgumentException.class, () ->
        Summary.of(1L, 4.0, 1.0, 3.0, 100L, 200L));
    assertThrows(IllegalArgumentException.class, () ->
        Summary.of(1L, 2.0, 1.0, 3.0, 201L, 200L));
  }

  @Test
  void fr12ResponseDistinguishesEvidenceAbsenceCoverageAndErrors() {
    Summary summary = Summary.of(1L, 2.0, 2.0, 2.0, 100L, 100L);
    SummaryResponse evidence = SummaryResponse.ok(summary, Coverage.COMPLETE);
    SummaryResponse absence = SummaryResponse.ok(null, Coverage.WINDOW_CLIPPED);
    SummaryResponse unavailable =
        SummaryResponse.error(Status.of(Status.Code.UNAVAILABLE, "backend unavailable"));

    assertEquals(summary, evidence.summary());
    assertEquals(Coverage.COMPLETE, evidence.coverage());
    assertEquals(Status.Code.OK, evidence.status().code());
    assertNull(absence.summary());
    assertEquals(Coverage.WINDOW_CLIPPED, absence.coverage());
    assertNull(unavailable.summary());
    assertNull(unavailable.coverage());
    assertEquals(Status.Code.UNAVAILABLE, unavailable.status().code());

    assertThrows(NullPointerException.class, () -> SummaryResponse.ok(summary, null));
    assertThrows(IllegalArgumentException.class, () ->
        SummaryResponse.error(Status.ok()));
    PublicApiSurface.assertConstructors(SummaryResponse.class);
    PublicApiSurface.assertMethods(
        SummaryResponse.class,
        "coverage():com.nvidia.spark.history.Coverage",
        "equals(java.lang.Object):boolean",
        "error(com.nvidia.spark.history.Status):" +
            "com.nvidia.spark.history.SummaryResponse",
        "hashCode():int",
        "ok(com.nvidia.spark.history.Summary," +
            "com.nvidia.spark.history.Coverage):" +
            "com.nvidia.spark.history.SummaryResponse",
        "status():com.nvidia.spark.history.Status",
        "summary():com.nvidia.spark.history.Summary",
        "toString():java.lang.String");
  }

  @Test
  void fr12StatusFamiliesEnforceExactReasonAndMetricRules() {
    assertNull(Status.ok().reason());
    assertEquals("late", Status.of(Status.Code.DEADLINE_EXCEEDED, "late").reason());
    assertThrows(IllegalArgumentException.class, () ->
        Status.of(Status.Code.OK, "not allowed"));
    assertThrows(IllegalArgumentException.class, () ->
        Status.of(Status.Code.UNAVAILABLE, ""));

    MetricVersionId metric = new MetricVersionId(8, 1);
    SchemaStatus accepted = SchemaStatus.accepted(metric, null);
    SchemaStatus warning = SchemaStatus.accepted(metric, "retention clamped");
    SchemaStatus invalid =
        SchemaStatus.of(null, SchemaStatus.Code.INVALID_REQUEST, "null schema");

    assertEquals(metric, accepted.metric());
    assertNull(accepted.reason());
    assertEquals("retention clamped", warning.reason());
    assertNull(invalid.metric());
    assertThrows(NullPointerException.class, () ->
        SchemaStatus.of(null, SchemaStatus.Code.UNAVAILABLE, "outage"));
    assertThrows(IllegalArgumentException.class, () ->
        SchemaStatus.of(metric, SchemaStatus.Code.ACCEPTED, "use accepted factory"));
    assertThrows(IllegalArgumentException.class, () ->
        SchemaStatus.of(metric, SchemaStatus.Code.DENIED, ""));

    PublicApiSurface.assertConstructors(SchemaStatus.class);
    PublicApiSurface.assertMethods(
        SchemaStatus.class,
        "accepted(com.nvidia.spark.history.MetricVersionId,java.lang.String):" +
            "com.nvidia.spark.history.SchemaStatus",
        "code():com.nvidia.spark.history.SchemaStatus$Code",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "metric():com.nvidia.spark.history.MetricVersionId",
        "of(com.nvidia.spark.history.MetricVersionId," +
            "com.nvidia.spark.history.SchemaStatus$Code,java.lang.String):" +
            "com.nvidia.spark.history.SchemaStatus",
        "reason():java.lang.String",
        "toString():java.lang.String");
  }

  @Test
  void fr14BackendInfoContainsOnlyVersionAndRedactedDescriptionContract() {
    BackendInfo info = new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "test backend");
    assertEquals(1, info.apiVersion());
    assertEquals("test backend", info.description());
    assertEquals(255, new BackendInfo(1, repeatAscii(255)).description().length());
    assertEquals(256, new BackendInfo(1, repeatAscii(256)).description().length());

    assertThrows(IllegalArgumentException.class, () -> new BackendInfo(0, "test"));
    assertThrows(IllegalArgumentException.class, () -> new BackendInfo(1, ""));
    assertThrows(IllegalArgumentException.class, () ->
        new BackendInfo(1, repeatAscii(257)));
    assertThrows(IllegalArgumentException.class, () ->
        new BackendInfo(1, "\uD800"));

    PublicApiSurface.assertConstructors(
        BackendInfo.class, "BackendInfo(int,java.lang.String)");
    PublicApiSurface.assertMethods(
        BackendInfo.class,
        "apiVersion():int",
        "description():java.lang.String",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
  }

  private static String repeatAscii(int count) {
    StringBuilder value = new StringBuilder(count);
    for (int index = 0; index < count; index++) {
      value.append('a');
    }
    return value.toString();
  }
}
