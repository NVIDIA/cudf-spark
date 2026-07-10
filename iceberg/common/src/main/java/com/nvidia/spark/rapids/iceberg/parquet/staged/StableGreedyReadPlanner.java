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
import org.apache.spark.sql.types.StructType;

/**
 * Stable greedy planner for filtered Iceberg Parquet row groups.
 *
 * <p>The planner runs only on the Spark task thread after the complete footer barrier. It walks
 * footer results and row groups in caller-provided order. A subtask is closed before adding a row
 * group that would exceed a hard row/GPU-byte limit or violate Iceberg compatibility. An
 * individual row group that exceeds a hard limit is retained as a standalone subtask, matching
 * the existing soft-limit behavior.</p>
 *
 * <p>The copied-data target is a subtask size target: once the accumulated encoded bytes reach
 * it, the next row group starts a new subtask whether or not it comes from the same source. Small
 * sources combine up to the target and a large source splits into multiple roughly target-sized
 * subtasks, so one oversized file cannot monopolize a worker slot with an arbitrarily large
 * buffer. A non-positive target disables cross-source combination while retaining row-group
 * batching within each source.</p>
 */
public final class StableGreedyReadPlanner {
  private final int maxRows;
  private final long maxEstimatedGpuBytes;
  private final long targetParquetBytes;
  private final StructType expectedSparkSchema;

  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      StructType expectedSparkSchema) {
    if (maxRows <= 0) {
      throw new IllegalArgumentException("maxRows must be positive");
    }
    if (maxEstimatedGpuBytes <= 0) {
      throw new IllegalArgumentException("maxEstimatedGpuBytes must be positive");
    }
    this.maxRows = maxRows;
    this.maxEstimatedGpuBytes = maxEstimatedGpuBytes;
    this.targetParquetBytes = targetParquetBytes;
    this.expectedSparkSchema = Objects.requireNonNull(
        expectedSparkSchema, "expectedSparkSchema");
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

    Session session = newSession();
    ArrayList<ReadSubtask> subtasks = new ArrayList<>();
    for (FooterResult footer : footers) {
      subtasks.addAll(session.add(footer));
    }
    subtasks.addAll(session.finish());
    return Collections.unmodifiableList(subtasks);
  }

  /** Create an incremental planning session that consumes footers in file-list order. */
  public Session newSession() {
    return new Session();
  }

  /**
   * Incremental planning state fed one footer at a time.
   *
   * <p>The greedy walk only ever inspects row groups seen so far, so a subtask can be published
   * to a worker as soon as its closing decision is made instead of after the complete footer
   * barrier. Any feed order is correct: row groups within one footer always keep their file
   * order, and the compatibility rules hold for every pairing. The feed order determines which
   * sources combine, so callers that need a reproducible plan (such as {@link #plan}) must feed
   * a deterministic order, while the staged reader deliberately feeds completion order to avoid
   * footer head-of-line blocking.</p>
   */
  public final class Session {
    private final ArrayList<SelectedBlock> selected = new ArrayList<>();
    private long selectedRows;
    private long selectedDataBytes;
    private long nextSubtaskId;
    private boolean finished;

    private Session() {
    }

    /** Feed the next footer in file-list order and return the subtasks its row groups closed. */
    public List<ReadSubtask> add(FooterResult footer) {
      Objects.requireNonNull(footer, "footer");
      ensureActive();
      ArrayList<ReadSubtask> closed = new ArrayList<>();
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
        long candidateGpuBytes = estimateGpuBytes(candidateRows);

        boolean shouldFlush = !selected.isEmpty()
            && (hasReachedTarget(selectedDataBytes)
                || candidateRows > maxRows
                || candidateGpuBytes > maxEstimatedGpuBytes
                || !isCompatibleWithAll(selected, footer));
        if (shouldFlush) {
          closed.add(buildSubtask(nextSubtaskId++, selected));
          selected.clear();
          selectedRows = 0L;
          selectedDataBytes = 0L;
        }

        selected.add(new SelectedBlock(footer, blockIndex));
        selectedRows = Math.addExact(selectedRows, block.getRowCount());
        selectedDataBytes = Math.addExact(selectedDataBytes, blockDataBytes);
      }
      return closed;
    }

    /** Flush the final open subtask after the last footer has been fed. */
    public List<ReadSubtask> finish() {
      ensureActive();
      finished = true;
      if (selected.isEmpty()) {
        return Collections.emptyList();
      }
      List<ReadSubtask> last =
          Collections.singletonList(buildSubtask(nextSubtaskId, selected));
      selected.clear();
      return last;
    }

    private void ensureActive() {
      if (finished) {
        throw new IllegalStateException("planning session is finished");
      }
    }
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
      if (targetParquetBytes <= 0 || !compatible(existing, candidate)) {
        return false;
      }
      previous = existing;
    }
    return true;
  }

  private ReadSubtask buildSubtask(
      long subtaskId,
      List<SelectedBlock> selected) {
    ArrayList<ReadSubtask.FileSlice> fileSlices = new ArrayList<>();
    int start = 0;
    while (start < selected.size()) {
      FooterResult footer = selected.get(start).footer;
      int end = start + 1;
      while (end < selected.size()
          && selected.get(end).footer == footer
          && selected.get(end).blockIndex == selected.get(end - 1).blockIndex + 1) {
        end++;
      }

      fileSlices.add(new ReadSubtask.FileSlice(
          footer, selected.get(start).blockIndex, end - start));
      start = end;
    }

    return new ReadSubtask(subtaskId, fileSlices);
  }

  private static long encodedDataBytes(BlockMetaData block) {
    long bytes = 0L;
    for (ColumnChunkMetaData column : block.getColumns()) {
      bytes = Math.addExact(bytes, column.getTotalSize());
    }
    return bytes;
  }

  private long estimateGpuBytes(long rows) {
    long estimate = GpuBatchUtils$.MODULE$.estimateGpuMemory(expectedSparkSchema, rows);
    if (estimate < 0) {
      throw new IllegalStateException("GPU memory estimate must not be negative");
    }
    return estimate;
  }

  /** Iceberg files may share one synthetic Parquet file only when decode behavior is identical. */
  private static boolean compatible(FooterResult left, FooterResult right) {
    return left.getClippedSchema().equals(right.getClippedSchema())
        && left.getReadSchema().equals(right.getReadSchema())
        && left.getDateRebaseMode().equals(right.getDateRebaseMode())
        && left.getTimestampRebaseMode().equals(right.getTimestampRebaseMode())
        && left.hasInt96Timestamps() == right.hasInt96Timestamps()
        && left.getPostProcessor().compatibleForCombining(right.getPostProcessor());
  }

  /** Task-thread-only tuple used while preserving stable row-group order. */
  private static final class SelectedBlock {
    private final FooterResult footer;
    private final int blockIndex;

    private SelectedBlock(
        FooterResult footer,
        int blockIndex) {
      this.footer = footer;
      this.blockIndex = blockIndex;
    }
  }
}
