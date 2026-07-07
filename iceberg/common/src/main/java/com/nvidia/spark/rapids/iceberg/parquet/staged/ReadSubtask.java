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
 * Immutable unit of asynchronous I/O, finalization, and subsequent GPU decode.
 *
 * <p>Segments retain stable input order and borrow their Iceberg footer state. The layout is
 * exact and can be safely published with this object to I/O and combine workers.
 * {@code subtaskId} is unique only within one partition read plan. Row counts are rows.</p>
 */
public final class ReadSubtask {
  private final long subtaskId;
  private final List<ReadSegment> segments;
  private final SyntheticParquetLayout layout;
  private final long rowCount;

  public ReadSubtask(
      long subtaskId,
      List<ReadSegment> segments,
      SyntheticParquetLayout layout,
      long rowCount) {
    if (subtaskId < 0) {
      throw new IllegalArgumentException("subtaskId must be non-negative");
    }
    if (rowCount < 0) {
      throw new IllegalArgumentException("rowCount must be non-negative");
    }
    this.subtaskId = subtaskId;
    this.segments = immutableCopy(segments);
    this.layout = Objects.requireNonNull(layout, "layout");
    this.rowCount = rowCount;
    if (this.segments.isEmpty()) {
      throw new IllegalArgumentException("a read subtask must contain at least one segment");
    }

    long segmentRows = 0L;
    long segmentDataBytes = 0L;
    for (ReadSegment segment : this.segments) {
      segmentRows = Math.addExact(segmentRows, segment.getRowCount());
      segmentDataBytes = Math.addExact(segmentDataBytes, segment.getDataSizeBytes());
    }
    if (segmentRows != rowCount) {
      throw new IllegalArgumentException("rowCount does not match the segment row counts");
    }
    if (segmentDataBytes != layout.getDataSizeBytes()) {
      throw new IllegalArgumentException(
          "layout data size does not match the segment column chunks");
    }
  }

  private static List<ReadSegment> immutableCopy(List<ReadSegment> values) {
    Objects.requireNonNull(values, "segments");
    ArrayList<ReadSegment> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException("segments must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public long getSubtaskId() {
    return subtaskId;
  }

  public List<ReadSegment> getSegments() {
    return segments;
  }

  public SyntheticParquetLayout getLayout() {
    return layout;
  }

  public long getRowCount() {
    return rowCount;
  }
}
