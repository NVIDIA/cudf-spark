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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-01 numeric metric identity and reserved key bits. */
class MetricVersionIdTest {
  @Test
  void fr01PacksMetricAndVersionIntoTheLowThirtyTwoBits() {
    assertEquals(0x0000000000010001L, new MetricVersionId(1, 1).packedKey());
    assertEquals(0x00000000FFFFFFFFL, new MetricVersionId(65535, 65535).packedKey());

    MetricVersionId unpacked = MetricVersionId.fromPackedKey(0x000000001234ABCDL);
    assertEquals(0x1234, unpacked.metricId());
    assertEquals(0xABCD, unpacked.version());
  }

  @Test
  void fr01RejectsReservedOrZeroIdentityBits() {
    assertThrows(IllegalArgumentException.class, () -> new MetricVersionId(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new MetricVersionId(1, 0));
    assertThrows(IllegalArgumentException.class, () -> new MetricVersionId(65536, 1));
    assertThrows(IllegalArgumentException.class, () -> new MetricVersionId(1, 65536));
    assertThrows(IllegalArgumentException.class, () -> MetricVersionId.fromPackedKey(1L << 32));
    assertThrows(IllegalArgumentException.class,
        () -> MetricVersionId.fromPackedKey(Long.MIN_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> MetricVersionId.fromPackedKey(0x0000000000010000L));
    assertThrows(IllegalArgumentException.class, () -> MetricVersionId.fromPackedKey(1L));
  }
}
