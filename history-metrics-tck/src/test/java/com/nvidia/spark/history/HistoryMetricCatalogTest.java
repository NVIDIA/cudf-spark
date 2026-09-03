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

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-01 catalog validation and test-catalog isolation. */
class HistoryMetricCatalogTest {
  @Test
  void fr01AllowsAnEmptyProductionCatalog() {
    assertTrue(HistoryMetricCatalog.production().entries().isEmpty());
  }

  @Test
  void fr01KeepsInjectedTestEntriesOutOfTheProductionCatalog() {
    HistoryMetricCatalog testCatalog =
        TestHistoryMetricCatalog.create(TestHistoryMetricCatalog.live(41, "test.metric"));

    assertEquals("test.metric", testCatalog.find(41).get().name());
    assertFalse(testCatalog.find(41).get().retired());
    assertFalse(HistoryMetricCatalog.production().find(41).isPresent());
  }

  @Test
  void fr01RejectsDuplicateIdsNamesAndInvalidIds() {
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(1, "first"),
        TestHistoryMetricCatalog.live(1, "second")));
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(1, "same"),
        TestHistoryMetricCatalog.live(2, "same")));
    assertThrows(IllegalArgumentException.class, () ->
        TestHistoryMetricCatalog.create(TestHistoryMetricCatalog.live(0, "zero")));
    assertThrows(IllegalArgumentException.class, () ->
        TestHistoryMetricCatalog.create(TestHistoryMetricCatalog.live(65536, "too-large")));
  }

  @Test
  void fr01MetricNamesUseExactStrictUtf8WithA128ByteLimit() {
    String accepted = repeat('a', 128);
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(7, accepted),
        TestHistoryMetricCatalog.live(8, "Case"),
        TestHistoryMetricCatalog.live(9, "case"),
        TestHistoryMetricCatalog.live(11, "\u00E9"),
        TestHistoryMetricCatalog.live(12, "e\u0301"));

    assertEquals(accepted, catalog.find(7).get().name());
    assertEquals("Case", catalog.find(8).get().name());
    assertEquals("case", catalog.find(9).get().name());
    assertEquals("\u00E9", catalog.find(11).get().name());
    assertEquals("e\u0301", catalog.find(12).get().name());
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(10, repeat('b', 129))));
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(10, "\uD83Dreviewer-probe")));
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(10, "reviewer-probe\uDE00")));
  }

  @Test
  void fr01MetricNameLimitCountsMultibyteUtf8BytesNotJavaCharacters() {
    String bytes127 = multibyteName(127);
    String bytes128 = multibyteName(128);
    String bytes129 = multibyteName(129);
    assertEquals(127, bytes127.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(128, bytes128.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(129, bytes129.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(65, bytes129.length());

    HistoryMetricCatalog accepted = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(21, bytes127),
        TestHistoryMetricCatalog.live(22, bytes128));
    assertEquals(bytes127, accepted.find(21).get().name());
    assertEquals(bytes128, accepted.find(22).get().name());
    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(23, bytes129)));
  }

  @Test
  void fr01CompleteCurrentCatalogRejectsConflictingLiveAndRetiredEntries() {
    HistoryMetricCatalog completeCurrentCatalog =
        TestHistoryMetricCatalog.create(TestHistoryMetricCatalog.retired(9, "old.metric"));
    assertEquals("old.metric", completeCurrentCatalog.find(9).get().name());
    assertTrue(completeCurrentCatalog.find(9).get().retired());

    assertThrows(IllegalArgumentException.class, () -> TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.retired(9, "old.metric"),
        TestHistoryMetricCatalog.live(9, "new.metric")));
  }

  private static String multibyteName(int utf8Bytes) {
    StringBuilder builder = new StringBuilder((utf8Bytes + 1) / 2);
    for (int index = 0; index < utf8Bytes / 2; index++) {
      builder.append('\u00E9');
    }
    if ((utf8Bytes & 1) != 0) {
      builder.append('a');
    }
    return builder.toString();
  }

  private static String repeat(char value, int count) {
    StringBuilder builder = new StringBuilder(count);
    for (int index = 0; index < count; index++) {
      builder.append(value);
    }
    return builder.toString();
  }
}
