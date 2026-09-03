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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-02 ordered, immutable metric declaration identity. */
class MetricSchemaTest {
  @Test
  void fr02TreatsDimensionOrderAsDeclarationIdentity() {
    MetricVersionId metric = new MetricVersionId(7, 3);
    DimensionSpec table = new DimensionSpec("table", DimValue.Kind.STRING);
    DimensionSpec projection = new DimensionSpec("projection", DimValue.Kind.BYTES);

    MetricSchema first = schema(metric, Arrays.asList(table, projection));
    MetricSchema identical = schema(
        new MetricVersionId(7, 3),
        Arrays.asList(
            new DimensionSpec("table", DimValue.Kind.STRING),
            new DimensionSpec("projection", DimValue.Kind.BYTES)));
    MetricSchema reordered = schema(metric, Arrays.asList(projection, table));

    assertEquals(first, identical);
    assertEquals(first.hashCode(), identical.hashCode());
    assertNotEquals(first, reordered);
  }

  @Test
  void fr02DefensivelyCopiesTheOrderedDimensionDeclaration() {
    List<DimensionSpec> dimensions = new ArrayList<DimensionSpec>();
    dimensions.add(new DimensionSpec("table", DimValue.Kind.STRING));
    MetricSchema schema = schema(new MetricVersionId(1, 1), dimensions);
    dimensions.clear();

    assertEquals(1, schema.dimensions().size());
    assertThrows(UnsupportedOperationException.class, () ->
        schema.dimensions().add(new DimensionSpec("other", DimValue.Kind.LONG)));
  }

  @Test
  void fr02AcceptsEightDimensionsAndRejectsNineOrDuplicateNames() {
    List<DimensionSpec> eight = new ArrayList<DimensionSpec>();
    for (int index = 0; index < 8; index++) {
      eight.add(new DimensionSpec("d" + index, DimValue.Kind.LONG));
    }
    assertEquals(8, schema(new MetricVersionId(1, 1), eight).dimensions().size());

    List<DimensionSpec> nine = new ArrayList<DimensionSpec>(eight);
    nine.add(new DimensionSpec("d8", DimValue.Kind.LONG));
    assertThrows(IllegalArgumentException.class, () ->
        schema(new MetricVersionId(1, 1), nine));
    assertThrows(IllegalArgumentException.class, () -> schema(
        new MetricVersionId(1, 1),
        Arrays.asList(
            new DimensionSpec("same", DimValue.Kind.STRING),
            new DimensionSpec("same", DimValue.Kind.BYTES))));
  }

  @Test
  void fr02DimensionNamesUseStrictUtf8WithA128ByteCap() {
    assertEquals(repeatAscii(127),
        new DimensionSpec(repeatAscii(127), DimValue.Kind.STRING).name());
    assertEquals(repeatAscii(128),
        new DimensionSpec(repeatAscii(128), DimValue.Kind.STRING).name());
    assertEquals(repeatUnicode(64),
        new DimensionSpec(repeatUnicode(64), DimValue.Kind.STRING).name());

    assertThrows(IllegalArgumentException.class, () ->
        new DimensionSpec(repeatAscii(129), DimValue.Kind.STRING));
    assertThrows(IllegalArgumentException.class, () ->
        new DimensionSpec(repeatUnicode(65), DimValue.Kind.STRING));
    assertThrows(IllegalArgumentException.class, () ->
        new DimensionSpec("\uD800", DimValue.Kind.STRING));
    assertThrows(IllegalArgumentException.class, () ->
        new DimensionSpec("\uDC00", DimValue.Kind.STRING));
  }

  @Test
  void fr02DimensionNamesRemainNonEmptyCaseSensitiveAndTyped() {
    assertThrows(IllegalArgumentException.class, () ->
        new DimensionSpec("", DimValue.Kind.STRING));
    assertThrows(NullPointerException.class, () ->
        new DimensionSpec("name", null));

    DimensionSpec upper = new DimensionSpec("Table", DimValue.Kind.STRING);
    DimensionSpec lower = new DimensionSpec("table", DimValue.Kind.STRING);
    assertNotEquals(upper, lower);
    assertEquals(2, schema(
        new MetricVersionId(1, 1), Arrays.asList(upper, lower)).dimensions().size());
  }

  private static MetricSchema schema(
      MetricVersionId metric, List<DimensionSpec> dimensions) {
    return new MetricSchema(
        metric,
        dimensions,
        new Retention(Duration.ofDays(7), Duration.ofDays(30)));
  }

  private static String repeatAscii(int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, 'a');
    return new String(chars);
  }

  private static String repeatUnicode(int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, '\u00e9');
    return new String(chars);
  }
}
