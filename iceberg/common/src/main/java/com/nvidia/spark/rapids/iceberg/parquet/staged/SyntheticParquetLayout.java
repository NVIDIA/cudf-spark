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
 * Exact immutable layout of one synthetic Parquet file.
 *
 * <p>The Spark task thread calculates this object before asynchronous I/O. The header begins at
 * synthetic offset zero, planned source ranges immediately follow it, and
 * {@code footerAndTrailerBytes} contains the serialized Parquet metadata, its four-byte
 * little-endian length, and the final {@code PAR1} magic. Consequently
 * {@code totalSizeBytes} is an exact allocation size rather than an estimate.</p>
 *
 * <p>The builder serializes adjusted row-group metadata directly into the footer; the runtime
 * layout only retains bytes and copy instructions needed by I/O workers. Byte arrays are
 * defensively copied on construction and access. Sizes and offsets are bytes.</p>
 */
public final class SyntheticParquetLayout {
  private final List<PlannedReadRange> ranges;
  private final byte[] headerBytes;
  private final byte[] footerAndTrailerBytes;
  private final long dataSizeBytes;
  private final long totalSizeBytes;

  public SyntheticParquetLayout(
      List<PlannedReadRange> ranges,
      byte[] headerBytes,
      byte[] footerAndTrailerBytes,
      long dataSizeBytes,
      long totalSizeBytes) {
    this.ranges = immutableCopy(ranges, "ranges");
    this.headerBytes = Objects.requireNonNull(headerBytes, "headerBytes").clone();
    this.footerAndTrailerBytes = Objects.requireNonNull(
        footerAndTrailerBytes, "footerAndTrailerBytes").clone();
    if (dataSizeBytes < 0 || totalSizeBytes < 0) {
      throw new IllegalArgumentException("layout sizes must be non-negative");
    }
    this.dataSizeBytes = dataSizeBytes;
    this.totalSizeBytes = totalSizeBytes;

    long expectedOffset = this.headerBytes.length;
    long rangeBytes = 0L;
    for (PlannedReadRange range : this.ranges) {
      if (range.getOutputOffset() != expectedOffset) {
        throw new IllegalArgumentException(
            "planned output ranges must be contiguous and follow the header");
      }
      rangeBytes = Math.addExact(rangeBytes, range.getLength());
      expectedOffset = Math.addExact(expectedOffset, range.getLength());
    }
    if (rangeBytes != dataSizeBytes) {
      throw new IllegalArgumentException("dataSizeBytes does not match planned ranges");
    }
    long expectedTotal = Math.addExact(
        expectedOffset, this.footerAndTrailerBytes.length);
    if (expectedTotal != totalSizeBytes) {
      throw new IllegalArgumentException(
          "totalSizeBytes does not match header, ranges, and footer");
    }
  }

  private static <T> List<T> immutableCopy(List<T> values, String name) {
    Objects.requireNonNull(values, name);
    ArrayList<T> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException(name + " must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public List<PlannedReadRange> getRanges() {
    return ranges;
  }

  public byte[] getHeaderBytes() {
    return headerBytes.clone();
  }

  public byte[] getFooterAndTrailerBytes() {
    return footerAndTrailerBytes.clone();
  }

  public long getDataSizeBytes() {
    return dataSizeBytes;
  }

  /** Returns the first synthetic byte offset occupied by the serialized footer. */
  public long getFooterOffset() {
    return headerBytes.length + dataSizeBytes;
  }

  public long getTotalSizeBytes() {
    return totalSizeBytes;
  }
}
