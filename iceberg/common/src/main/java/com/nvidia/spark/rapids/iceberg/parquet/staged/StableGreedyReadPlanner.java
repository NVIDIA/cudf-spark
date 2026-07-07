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
import java.util.function.BiPredicate;

import com.nvidia.spark.rapids.GpuBatchUtils$;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * Stable greedy planner for filtered Iceberg Parquet row groups.
 *
 * <p>The planner runs only on the Spark task thread after the complete footer barrier. It walks
 * footer results and row groups in caller-provided order. A subtask is closed before adding a row
 * group that would exceed a hard row/GPU-byte limit or violate Iceberg compatibility. An
 * individual row group that exceeds a hard limit is retained as a standalone subtask, matching
 * the existing soft-limit behavior.</p>
 *
 * <p>The copied-data target is checked only when crossing to another source. Once a source has
 * been admitted to a subtask, its row groups remain together unless a row/GPU-byte limit requires
 * a split. A non-positive target disables cross-source combination while retaining row-group
 * batching within each source.</p>
 */
public final class StableGreedyReadPlanner {
  /** Estimates final GPU bytes after Iceberg schema evolution and constant materialization. */
  @FunctionalInterface
  public interface GpuSizeEstimator {
    long estimate(FooterResult footer, long rowCount);
  }

  private final int maxRows;
  private final long maxEstimatedGpuBytes;
  private final long targetParquetBytes;
  private final BiPredicate<FooterResult, FooterResult> compatibility;
  private final GpuSizeEstimator gpuSizeEstimator;
  private final SyntheticParquetLayoutBuilder layoutBuilder;

  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      BiPredicate<FooterResult, FooterResult> compatibility) {
    this(maxRows, maxEstimatedGpuBytes, targetParquetBytes,
        compatibility,
        (footer, rows) -> GpuBatchUtils$.MODULE$.estimateGpuMemory(
            footer.getReadSchema(), rows));
  }

  /** Creates a planner with an Iceberg-aware final-output GPU estimator. */
  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      BiPredicate<FooterResult, FooterResult> compatibility,
      GpuSizeEstimator gpuSizeEstimator) {
    if (maxRows <= 0) {
      throw new IllegalArgumentException("maxRows must be positive");
    }
    if (maxEstimatedGpuBytes <= 0) {
      throw new IllegalArgumentException("maxEstimatedGpuBytes must be positive");
    }
    this.maxRows = maxRows;
    this.maxEstimatedGpuBytes = maxEstimatedGpuBytes;
    this.targetParquetBytes = targetParquetBytes;
    this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
    this.gpuSizeEstimator = Objects.requireNonNull(gpuSizeEstimator, "gpuSizeEstimator");
    this.layoutBuilder = new SyntheticParquetLayoutBuilder();
  }

  /**
   * Plans every non-empty filtered row group in deterministic input order.
   *
   * @param footers filtered Iceberg footers in partition traversal order
   * @return immutable ordered subtasks
   */
  public List<ReadSubtask> plan(List<FooterResult> footers) {
    Objects.requireNonNull(footers, "footers");
    if (footers.contains(null)) {
      throw new IllegalArgumentException("footers must not contain null values");
    }

    ArrayList<ReadSubtask> subtasks = new ArrayList<>();
    ArrayList<SelectedBlock> selected = new ArrayList<>();
    long selectedRows = 0L;
    long selectedDataBytes = 0L;
    long nextSubtaskId = 0L;

    for (FooterResult footer : footers) {
      List<BlockMetaData> blocks = footer.getBlocks();
      for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
        BlockMetaData block = blocks.get(blockIndex);
        if (block.getRowCount() == 0) {
          continue;
        }
        if (block.getRowCount() > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("too many rows in one Parquet row group");
        }
        long blockDataBytes = encodedDataBytes(block);
        long candidateRows = Math.addExact(selectedRows, block.getRowCount());
        long candidateGpuBytes = estimateGpuBytes(
            selected.isEmpty() ? footer : selected.get(0).footer,
            candidateRows);
        boolean crossesSourceBoundary = !selected.isEmpty()
            && selected.get(selected.size() - 1).footer.getSource() != footer.getSource();

        boolean shouldFlush = !selected.isEmpty()
            && ((crossesSourceBoundary && hasReachedTarget(selectedDataBytes))
                || candidateRows > maxRows
                || candidateGpuBytes > maxEstimatedGpuBytes
                || !isCompatibleWithAll(selected, footer));
        if (shouldFlush) {
          subtasks.add(buildSubtask(nextSubtaskId++, selected));
          selected.clear();
          selectedRows = 0L;
          selectedDataBytes = 0L;
        }

        selected.add(new SelectedBlock(footer, blockIndex, block));
        selectedRows = Math.addExact(selectedRows, block.getRowCount());
        selectedDataBytes = Math.addExact(selectedDataBytes, blockDataBytes);
      }
    }

    if (!selected.isEmpty()) {
      subtasks.add(buildSubtask(nextSubtaskId, selected));
    }
    return Collections.unmodifiableList(new ArrayList<>(subtasks));
  }

  private boolean hasReachedTarget(long selectedDataBytes) {
    return targetParquetBytes > 0 && selectedDataBytes >= targetParquetBytes;
  }

  private boolean isCompatibleWithAll(
      List<SelectedBlock> selected,
      FooterResult candidate) {
    FooterResult previous = null;
    for (SelectedBlock item : selected) {
      FooterResult existing = item.footer;
      if (existing == previous || existing == candidate) {
        previous = existing;
        continue;
      }
      if (targetParquetBytes <= 0 || !compatibility.test(existing, candidate)) {
        return false;
      }
      previous = existing;
    }
    return true;
  }

  private ReadSubtask buildSubtask(
      long subtaskId,
      List<SelectedBlock> selected) {
    ArrayList<ReadSegment> segments = new ArrayList<>();
    int start = 0;
    while (start < selected.size()) {
      FooterResult footer = selected.get(start).footer;
      int end = start + 1;
      while (end < selected.size()
          && selected.get(end).footer == footer
          && selected.get(end).blockIndex == selected.get(end - 1).blockIndex + 1) {
        end++;
      }

      ArrayList<BlockMetaData> blocks = new ArrayList<>(end - start);
      for (int index = start; index < end; index++) {
        blocks.add(selected.get(index).block);
      }
      segments.add(new ReadSegment(footer, blocks));
      start = end;
    }

    long rows = 0L;
    for (ReadSegment segment : segments) {
      rows = Math.addExact(rows, segment.getRowCount());
    }
    SyntheticParquetLayout layout = layoutBuilder.build(segments);
    return new ReadSubtask(subtaskId, segments, layout, rows);
  }

  private static long encodedDataBytes(BlockMetaData block) {
    long bytes = 0L;
    for (ColumnChunkMetaData column : block.getColumns()) {
      bytes = Math.addExact(bytes, column.getTotalSize());
    }
    return bytes;
  }

  private long estimateGpuBytes(FooterResult footer, long rows) {
    long estimate = gpuSizeEstimator.estimate(footer, rows);
    if (estimate < 0) {
      throw new IllegalStateException("GPU memory estimate must not be negative");
    }
    return estimate;
  }

  /** Task-thread-only tuple used while preserving stable row-group order. */
  private static final class SelectedBlock {
    private final FooterResult footer;
    private final int blockIndex;
    private final BlockMetaData block;

    private SelectedBlock(
        FooterResult footer,
        int blockIndex,
        BlockMetaData block) {
      this.footer = footer;
      this.blockIndex = blockIndex;
      this.block = block;
    }
  }
}
