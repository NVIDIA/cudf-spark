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

import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Scala callbacks used by the Java Iceberg staged reader.
 *
 * <p>This is the only abstraction between the Java pipeline and the existing Scala Iceberg
 * footer/decode code. Footer and input-opening callbacks run on shared workers; the Java reader's
 * synchronized event callbacks own planning; metrics and decode callbacks run on the Spark task
 * thread.</p>
 */
public interface StagedScanAdapter {
  /**
   * Fetches and filters the footer for one file or split.
   *
   * <p>The returned footer contains the Iceberg post-processor needed when its subtask is decoded.
   * It owns no closeable resource.</p>
   */
  FooterResult readAndFilterFooter(IcebergPartitionedFile file) throws Exception;

  /**
   * Opens the physical input immediately before an admitted source job reads its column chunks.
   * Keeping this Iceberg callback lazy avoids constructing S3 clients for files removed by footer
   * filtering or projections with no physical columns.
   */
  RapidsInputFile openInputFile(IcebergPartitionedFile file) throws Exception;

  /** Reports accumulated footer/filter worker time on the Spark task thread after a wait. */
  default void onFooterCompleted(long footerNanos) {
  }

  /**
   * Decodes and post-processes one logical synthetic Parquet file.
   *
   * <p>{@code parquetInput} is borrowed and remains valid only for this call. The implementation
   * may materialize a fresh owning array for every RMM retry attempt; each array must be closed
   * or transferred to the cuDF Parquet reader before this method returns.</p>
   */
  Iterator<ColumnarBatch> decodeAndPostProcess(
      ReadSubtask subtask,
      StagedParquetInput parquetInput) throws Exception;

  /**
   * Reports a prepared subtask immediately before the Spark task thread materializes and decodes
   * it. Implementations can update task metrics here without making worker-thread metric updates.
   */
  default void onSubtaskCompleted(ReadSubtask subtask, SubtaskStats stats) {
  }

  /** Reports successful staged-output materialization time on the Spark task thread. */
  default void onMaterializationCompleted(long materializationNanos) {
  }

  /** Reports task-thread time spent waiting for a completed subtask. */
  default void onResultWait(long waitNanos) {
  }

}
