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
 * safely published together with its completed assembly buffer.</p>
 */
public final class SubtaskStats {
  private final long ioNanos;
  private final long ioAllocNanos;
  private final long ioReadWaitNanos;
  private final long ioRouteNanos;
  private final long ioFinalizeNanos;
  private final long ioRequestCount;
  private final long ioRequestedBytes;
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
   * @param ioNanos remote-to-cache file-pipeline elapsed time in nanoseconds
   * @param ioAllocNanos time allocating or growing the reusable assembly host buffer
   * @param ioReadWaitNanos time blocked waiting for leader-owned S3 requests
   * @param ioRouteNanos time copying all selected cache ranges into the assembly buffer
   * @param ioFinalizeNanos time committing direct cache writers and acquiring read leases
   * @param ioRequestCount coalesced S3 requests issued
   * @param ioRequestedBytes total bytes requested from S3
   * @param combineNanos elapsed combine-stage time in nanoseconds
   * @param diskBacked whether encoded source data was staged through executor-local cache files
   * @param cacheHitCount column-chunk data-cache hits
   * @param cacheHitBytes bytes copied from data-cache hits
   * @param cacheMissCount column-chunk data-cache misses
   * @param cacheMissBytes bytes fetched for data-cache misses
   * @param cacheReadNanos elapsed time copying ranges that were cache hits before this scan
   */
  public SubtaskStats(
      long ioNanos,
      long ioAllocNanos,
      long ioReadWaitNanos,
      long ioRouteNanos,
      long ioFinalizeNanos,
      long ioRequestCount,
      long ioRequestedBytes,
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
    if (ioAllocNanos < 0 || ioReadWaitNanos < 0 || ioRouteNanos < 0 ||
        ioFinalizeNanos < 0 || ioRequestCount < 0 || ioRequestedBytes < 0) {
      throw new IllegalArgumentException("I/O phase measurements must be non-negative");
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
    this.ioAllocNanos = ioAllocNanos;
    this.ioReadWaitNanos = ioReadWaitNanos;
    this.ioRouteNanos = ioRouteNanos;
    this.ioFinalizeNanos = ioFinalizeNanos;
    this.ioRequestCount = ioRequestCount;
    this.ioRequestedBytes = ioRequestedBytes;
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

  public long getIoAllocNanos() {
    return ioAllocNanos;
  }

  public long getIoReadWaitNanos() {
    return ioReadWaitNanos;
  }

  public long getIoRouteNanos() {
    return ioRouteNanos;
  }

  public long getIoFinalizeNanos() {
    return ioFinalizeNanos;
  }

  public long getIoRequestCount() {
    return ioRequestCount;
  }

  public long getIoRequestedBytes() {
    return ioRequestedBytes;
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
