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

import java.util.Iterator;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.GpuBatchUtils$;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Supplies format-specific operations around the format-neutral staged Parquet pipeline.
 *
 * <p>The first implementation is backed by Iceberg, but the two context types deliberately
 * keep Iceberg details out of the planner, I/O, storage, and scheduling classes. Implementations
 * may be called from the Spark task thread and from footer worker threads. They must therefore
 * treat input objects as immutable after publication.</p>
 *
 * @param <F> format-specific input-file context
 * @param <C> format-specific context created while filtering a footer
 */
public interface StagedScanAdapter<F, C> extends AutoCloseable {
  /**
   * Fetches and filters the footer for one file or split.
   *
   * <p>The returned context is owned by the partition reader. It remains alive until the reader
   * is closed and is only borrowed by read subtasks.</p>
   */
  FooterResult<C> readAndFilterFooter(StagedScanFile<F> file) throws Exception;

  /** Reports footer/filter worker time on the Spark task thread after the footer barrier. */
  default void onFooterCompleted(FooterResult<C> footer, long footerNanos) {
  }

  /**
   * Returns whether row groups from two footer results may share one synthetic Parquet file.
   * This method executes on the Spark task thread during planning.
   */
  boolean canCombine(FooterResult<C> left, FooterResult<C> right);

  /**
   * Estimates the final decoded bytes used for planning GPU-sized subtasks. Formats that add
   * columns after Parquet decode should override this physical-read-schema default.
   */
  default long estimateGpuBytes(FooterResult<C> footer, long rowCount) {
    return GpuBatchUtils$.MODULE$.estimateGpuMemory(footer.getReadSchema(), rowCount);
  }

  /**
   * Decodes and post-processes one materialized synthetic Parquet file.
   *
   * <p>Ownership of {@code parquetData} transfers to this method. The returned iterator must
   * close it either eagerly or when the iterator itself is exhausted or closed through the
   * normal RAPIDS iterator lifecycle.</p>
   */
  Iterator<ColumnarBatch> decodeAndPostProcess(
      ReadSubtask<C> subtask,
      HostMemoryBuffer parquetData) throws Exception;

  /**
   * Reports a sealed subtask immediately before the Spark task thread materializes and decodes
   * it. Implementations can update task metrics here without making worker-thread metric updates.
   */
  default void onSubtaskCompleted(ReadSubtask<C> subtask, SubtaskStats stats) {
  }

  /** Reports successful staged-output materialization time on the Spark task thread. */
  default void onMaterializationCompleted(
      ReadSubtask<C> subtask,
      long materializationNanos) {
  }

  /** Reports time for which the Spark task thread blocked waiting for staged worker output. */
  default void onTaskWait(long waitNanos) {
  }

  /** Reports the task-thread portion of {@link #onTaskWait(long)} spent at the footer barrier. */
  default void onFooterWait(long waitNanos) {
    onTaskWait(waitNanos);
  }

  /** Reports the task-thread portion of {@link #onTaskWait(long)} awaiting a completed subtask. */
  default void onResultWait(long waitNanos) {
    onTaskWait(waitNanos);
  }

  /** Closes one adapter context exactly once when the partition reader is closed. */
  void closeContext(C context) throws Exception;

  /** Closes adapter-wide state. Implementations must be idempotent. */
  @Override
  void close() throws Exception;
}
