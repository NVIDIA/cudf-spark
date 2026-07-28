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

package com.nvidia.spark.rapids.iceberg.parquet.async;

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
 * <p>The planner runs only on the Spark task thread, consuming footers incrementally in
 * caller-provided order and walking row groups in file order. A subtask is closed before adding a
 * row group that would exceed a hard row/GPU-byte limit or violate Iceberg compatibility. An
 * individual row group that exceeds a hard limit is retained as a standalone subtask, matching
 * the existing soft-limit behavior.</p>
 *
 * <p>The copied-data target controls combination between complete file results, matching the
 * base multithreaded reader. Row groups from one file are split only by the row/GPU-byte limits;
 * the combine target is checked after the complete footer has been admitted. Consequently a
 * file may take a subtask beyond the target, but the target never creates additional decode
 * batches within that file. A non-positive target disables cross-source combination while
 * retaining row-group batching within each source.</p>
 */
public final class StableGreedyReadPlanner {
  private final int maxRows;
  private final long maxEstimatedGpuBytes;
  private final long combineThreshold;

  public StableGreedyReadPlanner(
      int maxRows,
      long maxEstimatedGpuBytes,
      long combineThreshold) {
    if (maxRows <= 0) {
      throw new IllegalArgumentException("maxRows must be positive");
    }
    if (maxEstimatedGpuBytes <= 0) {
      throw new IllegalArgumentException("maxEstimatedGpuBytes must be positive");
    }
    this.maxRows = maxRows;
    this.maxEstimatedGpuBytes = maxEstimatedGpuBytes;
    this.combineThreshold = combineThreshold;
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

  /** Create an incremental planning session that consumes footers in caller-provided order. */
  public Session newSession() {
    return new Session();
  }

  /**
   * Incremental planning state fed one footer at a time.
   *
   * <p>The greedy walk only ever inspects row groups seen so far, so a subtask can be consumed
   * as soon as its closing decision is made instead of after the complete footer barrier. Any
   * feed order is correct: row groups within one footer always keep their file order, and the
   * compatibility rules hold for every pairing. The feed order determines which sources combine:
   * the asynchronous reader uses file-list order when combining is disabled, and worker-completion
   * order when it is enabled, matching the base multithreaded reader.</p>
   */
  public final class Session {
    private final ArrayList<SelectedBlock> selected = new ArrayList<>();
    private long selectedRows;
    private long selectedDataBytes;
    private long selectedGpuBytes;
    private long nextSubtaskId;
    private boolean finished;

    private Session() {
    }

    /** Feed the next footer in admission order and return the subtasks its row groups closed. */
    public List<ReadSubtask> add(FooterResult footer) {
      Objects.requireNonNull(footer, "footer");
      ensureActive();
      ArrayList<ReadSubtask> closed = new ArrayList<>();
      List<BlockMetaData> blocks = footer.getBlocks();
      // The base reader decides whether to admit the next complete file result by inspecting the
      // bytes/rows already selected. Once admitted, that file is atomic for cross-file combine:
      // it may soft-overshoot a limit, but is not split from the files preceding it.
      boolean combineWholeFooter = !selected.isEmpty()
          && combineThreshold > 0
          && isCompatibleWithAll(selected, footer);
      boolean splitCurrentFooter = false;
      for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
        BlockMetaData block = blocks.get(blockIndex);
        if (block.getRowCount() == 0) {
          continue;
        }
        if (block.getRowCount() > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("too many rows in one Parquet row group");
        }
        long blockDataBytes = encodedDataBytes(block);
        long blockGpuBytes = estimateGpuBytes(footer.getReadSchema(), block.getRowCount());
        long candidateRows = Math.addExact(selectedRows, block.getRowCount());
        long candidateGpuBytes = Math.addExact(selectedGpuBytes, blockGpuBytes);

        boolean selectedHasCurrentFooter = !selected.isEmpty()
            && selected.get(selected.size() - 1).footer == footer;
        boolean shouldFlush = !selected.isEmpty() && !combineWholeFooter
            && (candidateRows > maxRows
                || candidateGpuBytes > maxEstimatedGpuBytes
                || !isCompatibleWithAll(selected, footer));
        if (shouldFlush) {
          splitCurrentFooter |= selectedHasCurrentFooter;
          closed.add(buildSubtask(nextSubtaskId++, selected));
          selected.clear();
          selectedRows = 0L;
          selectedDataBytes = 0L;
          selectedGpuBytes = 0L;
        }

        selected.add(new SelectedBlock(footer, blockIndex));
        selectedRows = Math.addExact(selectedRows, block.getRowCount());
        selectedDataBytes = Math.addExact(selectedDataBytes, blockDataBytes);
        selectedGpuBytes = Math.addExact(selectedGpuBytes, blockGpuBytes);
      }
      // The base reader's combination unit is a completed file result. Checking limits only at
      // this boundary prevents the combine target from splitting one file, while closing an
      // atomically admitted file after any soft overshoot. A file that needed internal row/GPU
      // splitting remains by itself, just as the base reader's multi-buffer file result does.
      // At this outer boundary the reader-byte limit compares encoded host bytes; GPU estimates
      // apply only to the within-file row-group split above.
      if (!selected.isEmpty()
          && (combineThreshold <= 0
              || splitCurrentFooter
              || hasReachedCombineThreshold(selectedDataBytes)
              || selectedRows >= maxRows
              || selectedDataBytes >= maxEstimatedGpuBytes)) {
        closed.add(buildSubtask(nextSubtaskId++, selected));
        selected.clear();
        selectedRows = 0L;
        selectedDataBytes = 0L;
        selectedGpuBytes = 0L;
      }
      return closed;
    }

    /** Return whether an admitted file result is waiting for more compatible results. */
    public boolean hasOpenBlocks() {
      return !selected.isEmpty();
    }

    /** Close the currently admitted result group after the combine wait expires. */
    public List<ReadSubtask> flush() {
      ensureActive();
      if (selected.isEmpty()) {
        return Collections.emptyList();
      }
      List<ReadSubtask> closed =
          Collections.singletonList(buildSubtask(nextSubtaskId++, selected));
      selected.clear();
      selectedRows = 0L;
      selectedDataBytes = 0L;
      selectedGpuBytes = 0L;
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
      selectedRows = 0L;
      selectedDataBytes = 0L;
      selectedGpuBytes = 0L;
      return last;
    }

    private void ensureActive() {
      if (finished) {
        throw new IllegalStateException("planning session is finished");
      }
    }
  }

  private boolean hasReachedCombineThreshold(long selectedDataBytes) {
    return combineThreshold > 0 && selectedDataBytes >= combineThreshold;
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
      if (combineThreshold <= 0 || !compatible(existing, candidate)) {
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

  private static long estimateGpuBytes(StructType readSchema, long rows) {
    long estimate = GpuBatchUtils$.MODULE$.estimateGpuMemory(readSchema, rows);
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
