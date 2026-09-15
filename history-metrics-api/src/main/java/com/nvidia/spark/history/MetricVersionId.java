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

/**
 * Numeric identity for one family-scoped contract version of a historical metric.
 *
 * <p>The packed metric key reserves bits 63-32, stores the governed metric-family/catalog ID in
 * bits 31-16, and stores the family-scoped contract version in bits 15-0.
 */
public final class MetricVersionId {
  private static final int MIN_COMPONENT = 1;
  private static final int MAX_COMPONENT = 0xFFFF;
  private static final long RESERVED_BITS_MASK = 0xFFFFFFFF00000000L;

  private final int metricId;
  private final int version;

  /**
   * Creates an identity from its two positive unsigned-16-bit components.
   *
   * @param metricId permanent governed family/catalog ID, from 1 through 65535
   * @param version positive family-scoped contract version, from 1 through 65535
   * @throws IllegalArgumentException if either component is outside its permitted range
   */
  public MetricVersionId(int metricId, int version) {
    validateComponent("metricId", metricId);
    validateComponent("version", version);
    this.metricId = metricId;
    this.version = version;
  }

  /** @return the permanent governed metric-family/catalog ID */
  public int metricId() {
    return metricId;
  }

  /** @return the positive family-scoped metric-contract version */
  public int version() {
    return version;
  }

  /** @return the canonical packed metric key with reserved high bits clear */
  public long packedKey() {
    return ((long) metricId << 16) | version;
  }

  /**
   * Decodes a canonical packed metric key.
   *
   * @param packedKey packed key with reserved high bits clear
   * @return the decoded metric identity
   * @throws IllegalArgumentException if reserved bits are set or either component is zero
   */
  public static MetricVersionId fromPackedKey(long packedKey) {
    if ((packedKey & RESERVED_BITS_MASK) != 0) {
      throw new IllegalArgumentException("reserved metric key bits must be zero");
    }
    int metricId = (int) ((packedKey >>> 16) & MAX_COMPONENT);
    int version = (int) (packedKey & MAX_COMPONENT);
    return new MetricVersionId(metricId, version);
  }

  private static void validateComponent(String field, int value) {
    if (value < MIN_COMPONENT || value > MAX_COMPONENT) {
      throw new IllegalArgumentException(field + " must be in the range 1 through 65535");
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MetricVersionId)) {
      return false;
    }
    MetricVersionId that = (MetricVersionId) other;
    return metricId == that.metricId && version == that.version;
  }

  @Override
  public int hashCode() {
    return 31 * metricId + version;
  }

  @Override
  public String toString() {
    return "MetricVersionId{" + "metricId=" + metricId + ", version=" + version + '}';
  }
}
