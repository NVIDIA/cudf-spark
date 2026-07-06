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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * Consecutive filtered row groups from one physical file within a read subtask.
 *
 * <p>The segment borrows its {@link FooterResult} and format context. Its block and first-row
 * index lists are immutable snapshots in identical order. {@code firstBlockIndex} is an index
 * into the footer result's filtered-block list, not necessarily the original unfiltered footer.
 * Row counts are rows, while data and GPU estimates are bytes.</p>
 *
 * @param <C> format-specific footer context
 */
public final class ReadSegment<C> {
  private final FooterResult<C> footer;
  private final int firstBlockIndex;
  private final List<BlockMetaData> blocks;
  private final List<Long> blockFirstRowIndices;
  private final long rowCount;
  private final long dataSizeBytes;
  private final long estimatedGpuBytes;

  /**
   * Creates a segment from a consecutive slice of one footer result.
   *
   * @param footer borrowed footer result
   * @param firstBlockIndex index of the first block in {@code footer.getBlocks()}
   * @param blocks consecutive source blocks in physical read order
   * @param blockFirstRowIndices file-global first-row index for each supplied block
   * @param estimatedGpuBytes estimated decoded GPU bytes for the segment
   */
  public ReadSegment(
      FooterResult<C> footer,
      int firstBlockIndex,
      List<BlockMetaData> blocks,
      List<Long> blockFirstRowIndices,
      long estimatedGpuBytes) {
    this.footer = Objects.requireNonNull(footer, "footer");
    if (firstBlockIndex < 0) {
      throw new IllegalArgumentException("firstBlockIndex must be non-negative");
    }
    if (estimatedGpuBytes < 0) {
      throw new IllegalArgumentException("estimatedGpuBytes must be non-negative");
    }
    this.firstBlockIndex = firstBlockIndex;
    this.blocks = immutableCopy(blocks, "blocks");
    this.blockFirstRowIndices = immutableCopy(
        blockFirstRowIndices, "blockFirstRowIndices");
    this.estimatedGpuBytes = estimatedGpuBytes;

    if (this.blocks.isEmpty()) {
      throw new IllegalArgumentException("a read segment must contain at least one block");
    }
    if (this.blocks.size() != this.blockFirstRowIndices.size()) {
      throw new IllegalArgumentException(
          "blocks and blockFirstRowIndices must have the same size");
    }
    if (firstBlockIndex + this.blocks.size() > footer.getBlocks().size()) {
      throw new IllegalArgumentException("segment exceeds the footer block list");
    }

    long rows = 0L;
    long bytes = 0L;
    for (int index = 0; index < this.blocks.size(); index++) {
      BlockMetaData block = this.blocks.get(index);
      if (block != footer.getBlocks().get(firstBlockIndex + index)) {
        throw new IllegalArgumentException(
            "segment blocks must be a consecutive slice of the footer result");
      }
      if (!this.blockFirstRowIndices.get(index).equals(
          footer.getBlockFirstRowIndices().get(firstBlockIndex + index))) {
        throw new IllegalArgumentException(
            "segment first-row indices must match the footer result");
      }
      rows = Math.addExact(rows, block.getRowCount());
      for (ColumnChunkMetaData column : block.getColumns()) {
        bytes = Math.addExact(bytes, column.getTotalSize());
      }
    }
    this.rowCount = rows;
    this.dataSizeBytes = bytes;
  }

  private static <T> List<T> immutableCopy(List<T> values, String name) {
    Objects.requireNonNull(values, name);
    ArrayList<T> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException(name + " must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public FooterResult<C> getFooter() {
    return footer;
  }

  public int getFirstBlockIndex() {
    return firstBlockIndex;
  }

  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  public List<Long> getBlockFirstRowIndices() {
    return blockFirstRowIndices;
  }

  public long getRowCount() {
    return rowCount;
  }

  public long getDataSizeBytes() {
    return dataSizeBytes;
  }

  public long getEstimatedGpuBytes() {
    return estimatedGpuBytes;
  }
}
