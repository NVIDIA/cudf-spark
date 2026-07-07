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
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Scala callbacks used by the Java Iceberg staged reader.
 *
 * <p>This is the only abstraction between the Java pipeline and the existing Scala Iceberg
 * footer/decode code. Footer and input-opening callbacks run on shared workers. Planning,
 * metrics, and decode callbacks run on the Spark task thread.</p>
 */
public interface StagedScanAdapter {
  /**
   * Fetches and filters the footer for one file or split.
   *
   * <p>The returned footer contains the Iceberg post-processor needed when its subtask is decoded.
   * It owns no closeable resource.</p>
   */
  FooterResult readAndFilterFooter(StagedFileSource file) throws Exception;

  /**
   * Opens the physical input immediately before an admitted source job reads its column chunks.
   * Keeping this Iceberg callback lazy avoids constructing S3 clients for files removed by footer
   * filtering or projections with no physical columns.
   */
  RapidsInputFile openInputFile(StagedFileSource file) throws Exception;

  /** Reports footer/filter worker time on the Spark task thread after the footer barrier. */
  default void onFooterCompleted(FooterResult footer, long footerNanos) {
  }

  /**
   * Returns whether row groups from two footer results may share one synthetic Parquet file.
   * This method executes on the Spark task thread during planning.
   */
  boolean canCombine(FooterResult left, FooterResult right);

  /**
   * Estimates the final decoded bytes used for planning GPU-sized subtasks. Formats that add
   * columns after Parquet decode should override this physical-read-schema default.
   */
  default long estimateGpuBytes(FooterResult footer, long rowCount) {
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
      ReadSubtask subtask,
      HostMemoryBuffer parquetData) throws Exception;

  /**
   * Reports a sealed subtask immediately before the Spark task thread materializes and decodes
   * it. Implementations can update task metrics here without making worker-thread metric updates.
   */
  default void onSubtaskCompleted(ReadSubtask subtask, SubtaskStats stats) {
  }

  /** Reports successful staged-output materialization time on the Spark task thread. */
  default void onMaterializationCompleted(
      ReadSubtask subtask,
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

}
