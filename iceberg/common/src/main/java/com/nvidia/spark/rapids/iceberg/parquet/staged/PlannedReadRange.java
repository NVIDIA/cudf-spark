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
 * Immutable copy instruction for one source column-chunk byte range.
 *
 * <p>{@code inputOffset} is an absolute byte offset in the original source file.
 * {@code outputOffset} is an absolute byte offset in the synthetic Parquet file, including its
 * four-byte header. {@code length} is measured in bytes. Planned ranges never overlap in the
 * output and are ordered by increasing output offset within a layout.</p>
 */
public final class PlannedReadRange {
  private final StagedFileSource source;
  private final long inputOffset;
  private final long length;
  private final long outputOffset;

  public PlannedReadRange(
      StagedFileSource source,
      long inputOffset,
      long length,
      long outputOffset) {
    this.source = Objects.requireNonNull(source, "source");
    if (inputOffset < 0 || length < 0 || outputOffset < 0) {
      throw new IllegalArgumentException("range offsets and length must be non-negative");
    }
    this.inputOffset = inputOffset;
    this.length = length;
    this.outputOffset = outputOffset;
  }

  public StagedFileSource getSource() {
    return source;
  }

  public long getInputOffset() {
    return inputOffset;
  }

  public long getLength() {
    return length;
  }

  public long getOutputOffset() {
    return outputOffset;
  }
}
