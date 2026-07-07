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

import java.util.Objects;

/**
 * A sealed asynchronous subtask result published to the Spark task thread.
 *
 * <p>The combine worker constructs this object only after {@link StagedParquetOutput#seal(long)}
 * succeeds. Construction transfers exclusive ownership of {@code output} to this result. The
 * completion queue transfers the result to the task thread, which must close it after GPU decode.
 * Reader cancellation closes queued or in-flight results instead. Close is idempotent so those
 * paths may converge safely.</p>
 */
public final class StagedReadResult implements AutoCloseable {
  private final ReadSubtask subtask;
  private final StagedParquetOutput output;
  private final SubtaskStats stats;
  private boolean closed;

  /**
   * Construct a completed result and take ownership of its sealed output.
   *
   * @param subtask immutable plan that produced the bytes
   * @param output sealed output whose ownership is transferred to this result
   * @param stats immutable I/O and combine measurements
   */
  public StagedReadResult(
      ReadSubtask subtask,
      StagedParquetOutput output,
      SubtaskStats stats) {
    this.subtask = Objects.requireNonNull(subtask, "subtask");
    this.output = Objects.requireNonNull(output, "output");
    this.stats = Objects.requireNonNull(stats, "stats");
    if (!output.isSealed()) {
      throw new IllegalArgumentException("a staged read result requires a sealed output");
    }
    if (stats.getStagedBytes() != output.sizeBytes()) {
      throw new IllegalArgumentException(
          "subtask stats byte count does not match the sealed output size");
    }
    if (stats.getBackingStore() != output.backingStore()) {
      throw new IllegalArgumentException(
          "subtask stats backing store does not match the sealed output");
    }
  }

  public ReadSubtask getSubtask() {
    return subtask;
  }

  public StagedParquetOutput getOutput() {
    return output;
  }

  public SubtaskStats getStats() {
    return stats;
  }

  /** Release the staged output or local file. */
  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      output.close();
    }
  }
}
