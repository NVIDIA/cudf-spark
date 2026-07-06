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

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * Immutable, format-neutral identity and opener for one physical file or split.
 *
 * <p>{@code ordinal} records the input ordering selected by the Spark task. It is an ordering
 * key only and is not a row-group number. Offsets associated with this source elsewhere in the
 * staged pipeline are byte offsets in the original physical file. This object owns neither the
 * {@link PartitionedFile} nor the opener's backing file-system resources.</p>
 *
 * <p>The object is safe to publish to footer and I/O worker threads because all of its fields are
 * final. Thread safety of the supplied {@link InputFileOpener} is part of that opener's contract.</p>
 */
public final class StagedFileSource {
  private final int ordinal;
  private final Path path;
  private final PartitionedFile partitionedFile;
  private final InputFileOpener opener;

  /**
   * Creates a staged source.
   *
   * @param ordinal zero-based input ordinal within the Spark partition
   * @param path physical Hadoop path used for source identity
   * @param partitionedFile Spark file/split metadata; ownership remains with the caller
   * @param opener worker-safe input-file opener
   */
  public StagedFileSource(
      int ordinal,
      Path path,
      PartitionedFile partitionedFile,
      InputFileOpener opener) {
    if (ordinal < 0) {
      throw new IllegalArgumentException("ordinal must be non-negative");
    }
    this.ordinal = ordinal;
    this.path = Objects.requireNonNull(path, "path");
    this.partitionedFile = Objects.requireNonNull(partitionedFile, "partitionedFile");
    this.opener = Objects.requireNonNull(opener, "opener");
  }

  public int getOrdinal() {
    return ordinal;
  }

  public Path getPath() {
    return path;
  }

  public PartitionedFile getPartitionedFile() {
    return partitionedFile;
  }

  public InputFileOpener getOpener() {
    return opener;
  }
}
