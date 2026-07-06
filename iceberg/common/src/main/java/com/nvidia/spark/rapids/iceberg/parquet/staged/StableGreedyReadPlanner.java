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

import com.nvidia.spark.rapids.GpuBatchUtils$;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * Stable greedy planner for filtered Parquet row groups.
 *
 * <p>The planner runs only on the Spark task thread after the complete footer barrier. It walks
 * footer results and row groups in caller-provided order. A subtask is closed before adding a row
 * group that would exceed a hard row/GPU-byte limit or violate format compatibility. The copied
 * data target is checked only when crossing to another source: once a source has been admitted to
 * a subtask, all of its row groups remain together unless a row/GPU-byte limit requires a split.
 * An individual row group that exceeds a hard limit is retained as a standalone subtask, matching
 * the existing soft-limit behavior.</p>
 *
 * <p>{@code targetParquetBytes} is compared with encoded column-chunk bytes, excluding the small
 * synthetic header/footer overhead, before admitting the first row group of the next source. A
 * non-positive target disables cross-source combination while retaining row-group batching within
 * each source. No execution work is submitted by this class.</p>
 *
 * @param <C> format-specific footer context
 */
public final class StableGreedyReadPlanner<C> {
  private final int maxRows;
  private final long maxEstimatedGpuBytes;
  private final long targetParquetBytes;
  private final FooterCompatibility<C> compatibility;
  private final FooterGpuSizeEstimator<C> gpuSizeEstimator;
  private final SyntheticParquetLayoutBuilder layoutBuilder;

  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      FooterCompatibility<C> compatibility) {
    this(maxRows, maxEstimatedGpuBytes, targetParquetBytes,
        compatibility,
        (footer, rows) -> GpuBatchUtils$.MODULE$.estimateGpuMemory(
            footer.getReadSchema(), rows),
        new SyntheticParquetLayoutBuilder());
  }

  /**
   * Creates a planner with an injectable layout builder for focused tests.
   */
  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      FooterCompatibility<C> compatibility,
      SyntheticParquetLayoutBuilder layoutBuilder) {
    this(maxRows, maxEstimatedGpuBytes, targetParquetBytes,
        compatibility,
        (footer, rows) -> GpuBatchUtils$.MODULE$.estimateGpuMemory(
            footer.getReadSchema(), rows),
        layoutBuilder);
  }

  /** Creates a planner with a format-aware final-output GPU estimator. */
  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      FooterCompatibility<C> compatibility,
      FooterGpuSizeEstimator<C> gpuSizeEstimator) {
    this(maxRows, maxEstimatedGpuBytes, targetParquetBytes,
        compatibility, gpuSizeEstimator, new SyntheticParquetLayoutBuilder());
  }

  /** Creates a fully injectable planner for focused tests. */
  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      FooterCompatibility<C> compatibility,
      FooterGpuSizeEstimator<C> gpuSizeEstimator,
      SyntheticParquetLayoutBuilder layoutBuilder) {
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
    this.layoutBuilder = Objects.requireNonNull(layoutBuilder, "layoutBuilder");
  }

  /**
   * Plans all non-empty filtered row groups.
   *
   * @param footers immutable footer results in input traversal order
   * @return deterministic partition plan
   */
  public PartitionReadPlan<C> plan(List<FooterResult<C>> footers) {
    Objects.requireNonNull(footers, "footers");
    if (footers.contains(null)) {
      throw new IllegalArgumentException("footers must not contain null values");
    }

    ArrayList<ReadSubtask<C>> subtasks = new ArrayList<>();
    ArrayList<SelectedBlock<C>> selected = new ArrayList<>();
    long selectedRows = 0L;
    long selectedDataBytes = 0L;
    long nextSubtaskId = 0L;

    for (FooterResult<C> footer : footers) {
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

        selected.add(new SelectedBlock<>(footer, blockIndex, block));
        selectedRows = Math.addExact(selectedRows, block.getRowCount());
        selectedDataBytes = Math.addExact(selectedDataBytes, blockDataBytes);
      }
    }

    if (!selected.isEmpty()) {
      subtasks.add(buildSubtask(nextSubtaskId, selected));
    }

    long totalRows = 0L;
    long totalGpuBytes = 0L;
    long totalParquetBytes = 0L;
    for (ReadSubtask<C> subtask : subtasks) {
      totalRows = Math.addExact(totalRows, subtask.getRowCount());
      totalGpuBytes = Math.addExact(totalGpuBytes, subtask.getEstimatedGpuBytes());
      totalParquetBytes = Math.addExact(
          totalParquetBytes, subtask.getLayout().getTotalSizeBytes());
    }
    return new PartitionReadPlan<>(
        subtasks, totalRows, totalGpuBytes, totalParquetBytes);
  }

  private boolean hasReachedTarget(long selectedDataBytes) {
    return targetParquetBytes > 0 && selectedDataBytes >= targetParquetBytes;
  }

  private boolean isCompatibleWithAll(
      List<SelectedBlock<C>> selected,
      FooterResult<C> candidate) {
    FooterResult<C> previous = null;
    for (SelectedBlock<C> item : selected) {
      FooterResult<C> existing = item.footer;
      if (existing == previous || existing == candidate) {
        previous = existing;
        continue;
      }
      if (targetParquetBytes <= 0 || !compatibility.canCombine(existing, candidate)) {
        return false;
      }
      previous = existing;
    }
    return true;
  }

  private ReadSubtask<C> buildSubtask(
      long subtaskId,
      List<SelectedBlock<C>> selected) {
    ArrayList<ReadSegment<C>> segments = new ArrayList<>();
    int start = 0;
    while (start < selected.size()) {
      FooterResult<C> footer = selected.get(start).footer;
      int firstBlockIndex = selected.get(start).blockIndex;
      int end = start + 1;
      while (end < selected.size()
          && selected.get(end).footer == footer
          && selected.get(end).blockIndex == selected.get(end - 1).blockIndex + 1) {
        end++;
      }

      ArrayList<BlockMetaData> blocks = new ArrayList<>(end - start);
      ArrayList<Long> firstRowIndices = new ArrayList<>(end - start);
      long segmentRows = 0L;
      for (int index = start; index < end; index++) {
        SelectedBlock<C> item = selected.get(index);
        blocks.add(item.block);
        firstRowIndices.add(footer.getBlockFirstRowIndices().get(item.blockIndex));
        segmentRows = Math.addExact(segmentRows, item.block.getRowCount());
      }
      segments.add(new ReadSegment<>(
          footer,
          firstBlockIndex,
          blocks,
          firstRowIndices,
          estimateGpuBytes(footer, segmentRows)));
      start = end;
    }

    long rows = 0L;
    for (ReadSegment<C> segment : segments) {
      rows = Math.addExact(rows, segment.getRowCount());
    }
    long gpuBytes = estimateGpuBytes(segments.get(0).getFooter(), rows);
    SyntheticParquetLayout layout = layoutBuilder.build(segments);
    return new ReadSubtask<>(subtaskId, segments, layout, rows, gpuBytes);
  }

  private static long encodedDataBytes(BlockMetaData block) {
    long bytes = 0L;
    for (ColumnChunkMetaData column : block.getColumns()) {
      bytes = Math.addExact(bytes, column.getTotalSize());
    }
    return bytes;
  }

  private long estimateGpuBytes(FooterResult<C> footer, long rows) {
    long estimate = gpuSizeEstimator.estimate(footer, rows);
    if (estimate < 0) {
      throw new IllegalStateException("GPU memory estimate must not be negative");
    }
    return estimate;
  }

  /** Task-thread-only tuple used while preserving stable traversal order. */
  private static final class SelectedBlock<C> {
    private final FooterResult<C> footer;
    private final int blockIndex;
    private final BlockMetaData block;

    private SelectedBlock(
        FooterResult<C> footer,
        int blockIndex,
        BlockMetaData block) {
      this.footer = footer;
      this.blockIndex = blockIndex;
      this.block = block;
    }
  }
}
