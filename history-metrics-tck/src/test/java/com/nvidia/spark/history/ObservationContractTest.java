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

/** Behavioral coverage for FR-05 and FR-07 observation and provenance boundaries. */
class ObservationContractTest {
  @Test
  void fr05ObservationIsStrictAndDefensivelyCopiesDimensions() {
    Map<String, DimValue> dimensions = new HashMap<String, DimValue>();
    dimensions.put("table", DimValue.of("orders"));
    Observation observation =
        new Observation(new MetricVersionId(4, 2), dimensions, 1.5, 1234L);
    dimensions.clear();

    assertEquals(new MetricVersionId(4, 2), observation.metric());
    assertEquals(DimValue.of("orders"), observation.dimensions().get("table"));
    assertEquals(1.5, observation.value());
    assertEquals(1234L, observation.timestampMs());
    assertThrows(UnsupportedOperationException.class, () ->
        observation.dimensions().put("other", DimValue.of(1L)));

    assertThrows(IllegalArgumentException.class, () ->
        new Observation(new MetricVersionId(4, 2), new HashMap<String, DimValue>(),
            Double.NaN, 1234L));
    assertThrows(IllegalArgumentException.class, () ->
        new Observation(new MetricVersionId(4, 2), new HashMap<String, DimValue>(),
            Double.POSITIVE_INFINITY, 1234L));
  }

  @Test
  void fr05ObservationRejectsNullOrIntrinsicallyInvalidDimensions() {
    Map<String, DimValue> nullName = new HashMap<String, DimValue>();
    nullName.put(null, DimValue.of(1L));
    Map<String, DimValue> nullValue = new HashMap<String, DimValue>();
    nullValue.put("table", null);
    Map<String, DimValue> invalidName = new HashMap<String, DimValue>();
    invalidName.put("\uD800", DimValue.of(1L));

    assertThrows(NullPointerException.class, () ->
        new Observation(null, new HashMap<String, DimValue>(), 1.0, 1L));
    assertThrows(NullPointerException.class, () ->
        new Observation(new MetricVersionId(1, 1), null, 1.0, 1L));
    assertThrows(NullPointerException.class, () ->
        new Observation(new MetricVersionId(1, 1), nullName, 1.0, 1L));
    assertThrows(NullPointerException.class, () ->
        new Observation(new MetricVersionId(1, 1), nullValue, 1.0, 1L));
    assertThrows(IllegalArgumentException.class, () ->
        new Observation(new MetricVersionId(1, 1), invalidName, 1.0, 1L));
  }

  @Test
  void fr07ProvenanceIsStrictBoundedAndOptionalAttemptRemainsAbsent() {
    Provenance provenance = new Provenance("application-1", null, "26.10.0", 5678L);

    assertEquals("application-1", provenance.app());
    assertNull(provenance.attempt());
    assertEquals("26.10.0", provenance.pluginVersion());
    assertEquals(5678L, provenance.writtenAtMs());
    assertEquals(255,
        new Provenance(repeatAscii(255), repeatAscii(64), repeatAscii(64), 1L)
            .app().length());

    assertThrows(IllegalArgumentException.class, () ->
        new Provenance("", null, "26.10.0", 1L));
    assertThrows(IllegalArgumentException.class, () ->
        new Provenance(repeatAscii(256), null, "26.10.0", 1L));
    assertThrows(IllegalArgumentException.class, () ->
        new Provenance("app", repeatAscii(65), "26.10.0", 1L));
    assertThrows(IllegalArgumentException.class, () ->
        new Provenance("app", null, repeatAscii(65), 1L));
    assertThrows(IllegalArgumentException.class, () ->
        new Provenance("app", null, "\uD800", 1L));
  }

  @Test
  void fr07OnlyStampedObservationCarriesFrameworkProvenance() {
    Observation raw =
        new Observation(new MetricVersionId(1, 1), new HashMap<String, DimValue>(), 2.0, 10L);
    Provenance provenance = new Provenance("app", "attempt-1", "26.10.0", 20L);
    StampedObservation stamped = new StampedObservation(raw, provenance);

    assertEquals(raw, stamped.observation());
    assertEquals(provenance, stamped.provenance());

    PublicApiSurface.assertConstructors(
        Observation.class,
        "Observation(com.nvidia.spark.history.MetricVersionId,java.util.Map,double,long)");
    PublicApiSurface.assertMethods(
        Observation.class,
        "metric():com.nvidia.spark.history.MetricVersionId",
        "dimensions():java.util.Map",
        "value():double",
        "timestampMs():long",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(
        StampedObservation.class,
        "StampedObservation(com.nvidia.spark.history.Observation," +
            "com.nvidia.spark.history.Provenance)");
    PublicApiSurface.assertMethods(
        StampedObservation.class,
        "observation():com.nvidia.spark.history.Observation",
        "provenance():com.nvidia.spark.history.Provenance",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(
        Provenance.class,
        "Provenance(java.lang.String,java.lang.String,java.lang.String,long)");
    PublicApiSurface.assertMethods(
        Provenance.class,
        "app():java.lang.String",
        "attempt():java.lang.String",
        "pluginVersion():java.lang.String",
        "writtenAtMs():long",
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
