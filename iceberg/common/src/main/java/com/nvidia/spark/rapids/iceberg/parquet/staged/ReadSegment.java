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
 * <p>The segment borrows its {@link FooterResult}; it does not own the footer or its Iceberg
 * post-processor. Blocks are a consecutive immutable slice of the filtered footer in physical
 * read order. Row counts are rows and data sizes are encoded Parquet bytes.</p>
 */
public final class ReadSegment {
  private final FooterResult footer;
  private final List<BlockMetaData> blocks;
  private final long rowCount;
  private final long dataSizeBytes;

  /**
   * Creates a segment from a consecutive slice of one footer result.
   *
   * @param footer borrowed footer result
   * @param blocks consecutive source blocks in physical read order
   */
  public ReadSegment(
      FooterResult footer,
      List<BlockMetaData> blocks) {
    this.footer = Objects.requireNonNull(footer, "footer");
    this.blocks = immutableCopy(blocks, "blocks");

    if (this.blocks.isEmpty()) {
      throw new IllegalArgumentException("a read segment must contain at least one block");
    }
    validateConsecutiveFooterSlice(footer, this.blocks);

    long rows = 0L;
    long bytes = 0L;
    for (BlockMetaData block : this.blocks) {
      rows = Math.addExact(rows, block.getRowCount());
      for (ColumnChunkMetaData column : block.getColumns()) {
        bytes = Math.addExact(bytes, column.getTotalSize());
      }
    }
    this.rowCount = rows;
    this.dataSizeBytes = bytes;
  }

  private static void validateConsecutiveFooterSlice(
      FooterResult footer,
      List<BlockMetaData> blocks) {
    List<BlockMetaData> footerBlocks = footer.getBlocks();
    int firstIndex = -1;
    for (int index = 0; index < footerBlocks.size(); index++) {
      if (footerBlocks.get(index) == blocks.get(0)) {
        firstIndex = index;
        break;
      }
    }
    if (firstIndex < 0 || firstIndex + blocks.size() > footerBlocks.size()) {
      throw new IllegalArgumentException("segment exceeds the footer block list");
    }
    for (int index = 0; index < blocks.size(); index++) {
      if (blocks.get(index) != footerBlocks.get(firstIndex + index)) {
        throw new IllegalArgumentException(
            "segment blocks must be a consecutive slice of the footer result");
      }
    }
  }

  private static <T> List<T> immutableCopy(List<T> values, String name) {
    Objects.requireNonNull(values, name);
    ArrayList<T> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException(name + " must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public FooterResult getFooter() {
    return footer;
  }

  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  public long getRowCount() {
    return rowCount;
  }

  public long getDataSizeBytes() {
    return dataSizeBytes;
  }
}
