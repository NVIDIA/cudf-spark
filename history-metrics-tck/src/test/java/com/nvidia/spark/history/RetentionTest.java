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
import java.util.Collections;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-03 and FR-20 recommended retention policy values. */
class RetentionTest {
  @Test
  void fr20RequiresNonnegativeOrderedRetentionDurations() {
    Retention retention = new Retention(Duration.ofDays(7), Duration.ofDays(30));
    assertEquals(Duration.ofDays(7), retention.planningMaxAge());
    assertEquals(Duration.ofDays(30), retention.storageRetention());

    assertEquals(Duration.ZERO, new Retention(Duration.ZERO, Duration.ZERO)
        .planningMaxAge());
    assertThrows(IllegalArgumentException.class, () ->
        new Retention(Duration.ofMillis(-1), Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () ->
        new Retention(Duration.ZERO, Duration.ofMillis(-1)));
    assertThrows(IllegalArgumentException.class, () ->
        new Retention(Duration.ofDays(31), Duration.ofDays(30)));
  }

  @Test
  void fr03RecommendationDoesNotChangeCanonicalSchemaIdentity() {
    MetricVersionId metric = new MetricVersionId(2, 1);
    MetricSchema shortRecommendation = new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec("table", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(1), Duration.ofDays(7)));
    MetricSchema longRecommendation = new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec("table", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(30), Duration.ofDays(90)));

    assertEquals(shortRecommendation, longRecommendation);
    assertEquals(shortRecommendation.hashCode(), longRecommendation.hashCode());
    assertNotEquals(
        shortRecommendation.recommendedRetention(),
        longRecommendation.recommendedRetention());
  }
}
