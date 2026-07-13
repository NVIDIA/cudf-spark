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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.nvidia.spark.rapids.filecache.FileCacheDataRangeLease;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * One filtered Iceberg file whose selected column chunks are committed to the local file cache.
 *
 * <p>This object owns one eviction-pinning cache lease per original Parquet column chunk and no
 * encoded host allocation. The synchronized planner transfers disjoint row-group ranges to
 * assembly plans. A lease is closed as soon as its bytes have been copied into an assembly
 * buffer, so cache eviction is blocked only while the staged pipeline still needs the range.</p>
 */
final class DiskReadyFile implements AutoCloseable {
  private final FooterResult footer;
  private final List<List<CachedRange>> rangesByBlock;
  private final DownloadStats stats;
  private boolean statsClaimed;
  private boolean closed;

  DiskReadyFile(
      FooterResult footer,
      List<List<CachedRange>> rangesByBlock,
      DownloadStats stats) {
    this.footer = Objects.requireNonNull(footer, "footer");
    this.stats = Objects.requireNonNull(stats, "stats");
    Objects.requireNonNull(rangesByBlock, "rangesByBlock");
    if (rangesByBlock.size() != footer.getBlocks().size()) {
      throw new IllegalArgumentException(
          "one cache-range list is required for every filtered row group");
    }

    ArrayList<List<CachedRange>> blockCopy = new ArrayList<>(rangesByBlock.size());
    for (int blockIndex = 0; blockIndex < rangesByBlock.size(); blockIndex++) {
      List<CachedRange> sourceRanges = Objects.requireNonNull(
          rangesByBlock.get(blockIndex), "block ranges");
      List<ColumnChunkMetaData> columns = footer.getBlocks().get(blockIndex).getColumns();
      if (sourceRanges.size() != columns.size()) {
        throw new IllegalArgumentException(
            "one cache lease is required for every selected column chunk");
      }
      ArrayList<CachedRange> ranges = new ArrayList<>(sourceRanges);
      for (int columnIndex = 0; columnIndex < ranges.size(); columnIndex++) {
        CachedRange range = Objects.requireNonNull(ranges.get(columnIndex), "cached range");
        if (range.length() != columns.get(columnIndex).getTotalSize()) {
          throw new IllegalArgumentException(
              "cache range length does not match its Parquet column chunk");
        }
      }
      blockCopy.add(ranges);
    }
    this.rangesByBlock = blockCopy;
  }

  FooterResult footer() {
    return footer;
  }

  /**
   * Transfer the leases for a consecutive row-group interval into one assembly plan.
   * Each filtered row group is planned exactly once, so taking an already-transferred range is
   * a planner bug and fails immediately instead of allowing two workers to share one channel.
   */
  synchronized List<CachedRange> takeRanges(int firstBlock, int blockCount) {
    if (closed) {
      throw new IllegalStateException("disk-ready file is closed");
    }
    if (firstBlock < 0 || blockCount <= 0 ||
        firstBlock > rangesByBlock.size() - blockCount) {
      throw new IllegalArgumentException("invalid row-group range");
    }
    ArrayList<CachedRange> claimed = new ArrayList<>();
    for (int blockIndex = firstBlock; blockIndex < firstBlock + blockCount; blockIndex++) {
      List<CachedRange> ranges = rangesByBlock.get(blockIndex);
      for (int columnIndex = 0; columnIndex < ranges.size(); columnIndex++) {
        CachedRange range = ranges.set(columnIndex, null);
        if (range == null) {
          closeRanges(claimed);
          throw new IllegalStateException("cache range was already transferred to assembly");
        }
        claimed.add(range);
      }
    }
    return claimed;
  }

  /** Attribute overlapping per-file download measurements exactly once. */
  synchronized DownloadStats claimStats() {
    if (statsClaimed) {
      return DownloadStats.EMPTY;
    }
    statsClaimed = true;
    return stats;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (List<CachedRange> ranges : rangesByBlock) {
      for (CachedRange range : ranges) {
        if (range != null) {
          range.close();
        }
      }
    }
  }

  private static void closeRanges(List<CachedRange> ranges) {
    for (CachedRange range : ranges) {
      range.close();
    }
  }

  /** One original column chunk pinned in the file cache until assembly consumes it. */
  static final class CachedRange implements AutoCloseable {
    private final long length;
    private final boolean initialCacheHit;
    private FileCacheDataRangeLease lease;

    CachedRange(long length, boolean initialCacheHit, FileCacheDataRangeLease lease) {
      if (length < 0) {
        throw new IllegalArgumentException("range length must be non-negative: " + length);
      }
      this.length = length;
      this.initialCacheHit = initialCacheHit;
      this.lease = Objects.requireNonNull(lease, "lease");
    }

    long length() {
      return length;
    }

    boolean initialCacheHit() {
      return initialCacheHit;
    }

    synchronized SeekableByteChannel channel() {
      if (lease == null) {
        throw new IllegalStateException("cache range is closed");
      }
      return lease.getChannel();
    }

    @Override
    public synchronized void close() {
      if (lease != null) {
        try {
          lease.close();
        } catch (IOException error) {
          throw new UncheckedIOException("failed to close file-cache range lease", error);
        } finally {
          lease = null;
        }
      }
    }
  }

  /** Immutable remote-to-cache measurements for one file pipeline. */
  static final class DownloadStats {
    static final DownloadStats EMPTY = new DownloadStats(
        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    final long ioNanos;
    final long remoteReadNanos;
    final long cacheCommitNanos;
    final long requestCount;
    final long requestedBytes;
    final long cacheHitCount;
    final long cacheHitBytes;
    final long cacheMissCount;
    final long cacheMissBytes;

    DownloadStats(
        long ioNanos,
        long remoteReadNanos,
        long cacheCommitNanos,
        long requestCount,
        long requestedBytes,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes) {
      this.ioNanos = requireNonNegative("ioNanos", ioNanos);
      this.remoteReadNanos = requireNonNegative("remoteReadNanos", remoteReadNanos);
      this.cacheCommitNanos = requireNonNegative("cacheCommitNanos", cacheCommitNanos);
      this.requestCount = requireNonNegative("requestCount", requestCount);
      this.requestedBytes = requireNonNegative("requestedBytes", requestedBytes);
      this.cacheHitCount = requireNonNegative("cacheHitCount", cacheHitCount);
      this.cacheHitBytes = requireNonNegative("cacheHitBytes", cacheHitBytes);
      this.cacheMissCount = requireNonNegative("cacheMissCount", cacheMissCount);
      this.cacheMissBytes = requireNonNegative("cacheMissBytes", cacheMissBytes);
    }

    private static long requireNonNegative(String name, long value) {
      if (value < 0) {
        throw new IllegalArgumentException(name + " must be non-negative: " + value);
      }
      return value;
    }
  }
}
