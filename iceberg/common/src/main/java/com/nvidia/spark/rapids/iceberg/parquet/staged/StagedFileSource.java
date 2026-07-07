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

import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;

/**
 * Immutable Iceberg input used by footer planning and staged data reads.
 *
 * <p>The Iceberg file carries the location, split, and partition metadata needed by footer
 * filtering, data I/O, and GPU post-processing. The physical {@code RapidsInputFile} is created
 * only if filtered row groups from this source are actually admitted for reading.</p>
 */
public final class StagedFileSource {
  private final IcebergPartitionedFile icebergFile;

  /**
   * Creates a staged source.
   *
   * @param icebergFile Iceberg file and split metadata; ownership remains with the caller
   */
  public StagedFileSource(IcebergPartitionedFile icebergFile) {
    this.icebergFile = Objects.requireNonNull(icebergFile, "icebergFile");
  }

  public IcebergPartitionedFile getIcebergFile() {
    return icebergFile;
  }

}
