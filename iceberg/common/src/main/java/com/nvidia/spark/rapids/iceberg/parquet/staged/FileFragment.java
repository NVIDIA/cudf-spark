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

import java.util.List;
import java.util.Objects;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * One file's filtered column chunks packed contiguously, downloaded independently of planning.
 *
 * <p>The fragment layout is computed from the file's own footer alone: filtered row groups keep
 * their order and each row group's column chunks are packed back to back — the same
 * (block, column) order {@link ReadSubtask} uses for its synthetic layout. Any consecutive
 * row-group interval of this file therefore occupies one contiguous fragment region. A
 * {@link ReadSubtask.FileSlice} can borrow that region as one reference-counted HMB slice,
 * regardless of how the planner grouped or split the file; combination does not need another
 * data-sized copy.</p>
 *
 * <p>The fragment owns its sealed output until {@link #close()}. A fragment may be referenced by
 * several subtasks when the combine threshold splits its file; the reader closes it after the
 * last referencing subtask has been assembled.</p>
 */
final class FileFragment implements AutoCloseable {
  private final FooterResult footer;
  private final long[] blockStartOffsets;
  private final long totalBytes;
  private final StagedParquetOutput data;
  private final DownloadStats stats;

  FileFragment(
      FooterResult footer,
      long[] blockStartOffsets,
      StagedParquetOutput data,
      DownloadStats stats) {
    this.footer = Objects.requireNonNull(footer, "footer");
    this.blockStartOffsets = Objects.requireNonNull(blockStartOffsets, "blockStartOffsets");
    this.totalBytes = blockStartOffsets[blockStartOffsets.length - 1];
    this.data = data;
    this.stats = Objects.requireNonNull(stats, "stats");
    if (totalBytes > 0 && data == null) {
      throw new IllegalArgumentException("a non-empty fragment requires a data output");
    }
  }

  /**
   * Compute the fragment start offset of every filtered block, plus one trailing total-size
   * entry. Index {@code i} is the packed offset of {@code blocks.get(i)}; empty blocks occupy
   * zero bytes.
   */
  static long[] computeBlockOffsets(List<BlockMetaData> blocks) {
    long[] offsets = new long[blocks.size() + 1];
    long offset = 0L;
    for (int index = 0; index < blocks.size(); index++) {
      offsets[index] = offset;
      for (ColumnChunkMetaData column : blocks.get(index).getColumns()) {
        offset = Math.addExact(offset, column.getTotalSize());
      }
    }
    offsets[blocks.size()] = offset;
    return offsets;
  }

  FooterResult getFooter() {
    return footer;
  }

  long getTotalBytes() {
    return totalBytes;
  }

  DownloadStats getStats() {
    return stats;
  }

  /** Fragment offset of the first byte of {@code blockIndex}'s packed chunks. */
  long blockStartOffset(int blockIndex) {
    return blockStartOffsets[blockIndex];
  }

  /** Packed byte length of {@code blockCount} consecutive blocks starting at {@code first}. */
  long sliceBytes(int firstBlock, int blockCount) {
    return blockStartOffsets[firstBlock + blockCount] - blockStartOffsets[firstBlock];
  }

  /** The sealed fragment data; null only for a fully filtered-out (empty) fragment. */
  StagedParquetOutput getData() {
    return data;
  }

  @Override
  public void close() {
    if (data != null) {
      data.close();
    }
  }

  /**
   * Immutable per-fragment download measurements, attributed once to a consuming subtask.
   *
   * <p>The download span ({@code ioNanos}) is additionally split into its phases so the
   * per-file elapsed time can be attributed: fragment allocation, the blocked wait on coalesced
   * ranged reads, cache-to-HMB routing, and cache publication plus seal. {@code requestCount}
   * counts the contiguous ranged reads actually issued and {@code requestedBytes} counts the
   * exact owner-miss bytes; the copier never downloads gaps.
   * {@code diskBacked} records that the bounded allocation failed and the worker completed the
   * mandatory cache-file download before performing its blocking HMB allocation.</p>
   */
  static final class DownloadStats {
    final long ioNanos;
    final long allocNanos;
    final long readWaitNanos;
    final long routeNanos;
    final long finalizeNanos;
    final long requestCount;
    final long requestedBytes;
    final boolean diskBacked;
    final long cacheHitCount;
    final long cacheHitBytes;
    final long cacheMissCount;
    final long cacheMissBytes;
    final long cacheReadNanos;

    DownloadStats(
        long ioNanos,
        long allocNanos,
        long readWaitNanos,
        long routeNanos,
        long finalizeNanos,
        long requestCount,
        long requestedBytes,
        boolean diskBacked,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes,
        long cacheReadNanos) {
      this.ioNanos = ioNanos;
      this.allocNanos = allocNanos;
      this.readWaitNanos = readWaitNanos;
      this.routeNanos = routeNanos;
      this.finalizeNanos = finalizeNanos;
      this.requestCount = requestCount;
      this.requestedBytes = requestedBytes;
      this.diskBacked = diskBacked;
      this.cacheHitCount = cacheHitCount;
      this.cacheHitBytes = cacheHitBytes;
      this.cacheMissCount = cacheMissCount;
      this.cacheMissBytes = cacheMissBytes;
      this.cacheReadNanos = cacheReadNanos;
    }
  }
}
