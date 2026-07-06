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

/**
 * Immutable result of the partition-wide planning barrier on the Spark task thread.
 *
 * <p>Subtasks are retained in deterministic planning order. Execution is allowed to complete in
 * a different order. The aggregate fields let metrics and invariant checks avoid rescanning the
 * plan; rows and bytes are exact sums of their respective subtask values.</p>
 *
 * @param <C> format-specific footer context
 */
public final class PartitionReadPlan<C> {
  private final List<ReadSubtask<C>> subtasks;
  private final long totalRows;
  private final long totalEstimatedGpuBytes;
  private final long totalParquetBytes;

  public PartitionReadPlan(
      List<ReadSubtask<C>> subtasks,
      long totalRows,
      long totalEstimatedGpuBytes,
      long totalParquetBytes) {
    this.subtasks = immutableCopy(subtasks);
    if (totalRows < 0 || totalEstimatedGpuBytes < 0 || totalParquetBytes < 0) {
      throw new IllegalArgumentException("plan aggregate values must be non-negative");
    }
    this.totalRows = totalRows;
    this.totalEstimatedGpuBytes = totalEstimatedGpuBytes;
    this.totalParquetBytes = totalParquetBytes;

    long expectedRows = 0L;
    long expectedGpuBytes = 0L;
    long expectedParquetBytes = 0L;
    for (ReadSubtask<C> subtask : this.subtasks) {
      expectedRows = Math.addExact(expectedRows, subtask.getRowCount());
      expectedGpuBytes = Math.addExact(
          expectedGpuBytes, subtask.getEstimatedGpuBytes());
      expectedParquetBytes = Math.addExact(
          expectedParquetBytes, subtask.getLayout().getTotalSizeBytes());
    }
    if (expectedRows != totalRows
        || expectedGpuBytes != totalEstimatedGpuBytes
        || expectedParquetBytes != totalParquetBytes) {
      throw new IllegalArgumentException("plan aggregates do not match the subtasks");
    }
  }

  private static <C> List<ReadSubtask<C>> immutableCopy(List<ReadSubtask<C>> values) {
    Objects.requireNonNull(values, "subtasks");
    ArrayList<ReadSubtask<C>> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException("subtasks must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public List<ReadSubtask<C>> getSubtasks() {
    return subtasks;
  }

  public long getTotalRows() {
    return totalRows;
  }

  public long getTotalEstimatedGpuBytes() {
    return totalEstimatedGpuBytes;
  }

  public long getTotalParquetBytes() {
    return totalParquetBytes;
  }
}
