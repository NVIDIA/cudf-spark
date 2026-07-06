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
 * Immutable input to the staged footer operation.
 *
 * <p>The type parameter carries format-specific metadata without making the staged data model
 * depend on Iceberg. The first implementation uses an Iceberg partitioned file; a future raw
 * Parquet or Delta adapter can use a different type without changing planning or layout types.
 * This wrapper owns neither {@code source} nor {@code formatFile}.</p>
 *
 * @param <F> format-specific file metadata type
 */
public final class StagedScanFile<F> {
  private final StagedFileSource source;
  private final F formatFile;

  public StagedScanFile(StagedFileSource source, F formatFile) {
    this.source = Objects.requireNonNull(source, "source");
    this.formatFile = Objects.requireNonNull(formatFile, "formatFile");
  }

  public int getOrdinal() {
    return source.getOrdinal();
  }

  public StagedFileSource getSource() {
    return source;
  }

  public F getFormatFile() {
    return formatFile;
  }
}
