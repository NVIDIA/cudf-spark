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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

/**
 * Immutable CPU-stage measurements attached to a completed read subtask.
 *
 * <p>Times are elapsed nanoseconds measured around work, not wall-clock timestamps. The object is
 * safely published together with its sealed output through the completion queue.</p>
 */
public final class SubtaskStats {
  private final long ioNanos;
  private final long combineNanos;
  private final boolean diskBacked;
  private final long cacheHitCount;
  private final long cacheHitBytes;
  private final long cacheMissCount;
  private final long cacheMissBytes;
  private final long cacheReadNanos;

  /**
   * Construct immutable measurements for a completed subtask.
   *
   * @param ioNanos elapsed I/O-stage time in nanoseconds
   * @param combineNanos elapsed combine-stage time in nanoseconds
   * @param diskBacked whether the sealed result uses an executor-local file
   * @param cacheHitCount column-chunk data-cache hits
   * @param cacheHitBytes bytes copied from data-cache hits
   * @param cacheMissCount column-chunk data-cache misses
   * @param cacheMissBytes bytes fetched for data-cache misses
   * @param cacheReadNanos elapsed time copying cached ranges
   */
  public SubtaskStats(
      long ioNanos,
      long combineNanos,
      boolean diskBacked,
      long cacheHitCount,
      long cacheHitBytes,
      long cacheMissCount,
      long cacheMissBytes,
      long cacheReadNanos) {
    if (ioNanos < 0) {
      throw new IllegalArgumentException("ioNanos must be non-negative: " + ioNanos);
    }
    if (combineNanos < 0) {
      throw new IllegalArgumentException(
          "combineNanos must be non-negative: " + combineNanos);
    }
    if (cacheHitCount < 0 || cacheHitBytes < 0 || cacheMissCount < 0 ||
        cacheMissBytes < 0 || cacheReadNanos < 0) {
      throw new IllegalArgumentException("cache measurements must be non-negative");
    }
    this.ioNanos = ioNanos;
    this.combineNanos = combineNanos;
    this.diskBacked = diskBacked;
    this.cacheHitCount = cacheHitCount;
    this.cacheHitBytes = cacheHitBytes;
    this.cacheMissCount = cacheMissCount;
    this.cacheMissBytes = cacheMissBytes;
    this.cacheReadNanos = cacheReadNanos;
  }

  public long getIoNanos() {
    return ioNanos;
  }

  public long getCombineNanos() {
    return combineNanos;
  }

  public boolean isDiskBacked() {
    return diskBacked;
  }

  public long getCacheHitCount() {
    return cacheHitCount;
  }

  public long getCacheHitBytes() {
    return cacheHitBytes;
  }

  public long getCacheMissCount() {
    return cacheMissCount;
  }

  public long getCacheMissBytes() {
    return cacheMissBytes;
  }

  public long getCacheReadNanos() {
    return cacheReadNanos;
  }
}
