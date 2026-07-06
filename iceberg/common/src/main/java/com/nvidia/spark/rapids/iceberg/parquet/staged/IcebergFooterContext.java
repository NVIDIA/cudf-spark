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

import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;

/**
 * Iceberg-specific state produced while filtering one Parquet footer.
 *
 * <p>The generic staged planning and layout classes treat this object as an opaque context. It
 * intentionally concentrates Iceberg dependencies at the adapter boundary so those lower-level
 * classes can later be extracted for raw Parquet or Delta scans.</p>
 *
 * <p>This object does not own the partitioned file or Parquet metadata. The post-processor is
 * task-confined during GPU result consumption: it is not thread-safe and must never be invoked
 * concurrently. In particular, its row-position counters make completion-order consumption
 * unsafe for {@code _pos}; the first staged-reader version must route such scans to the existing
 * reader.</p>
 */
public final class IcebergFooterContext {
  private final IcebergPartitionedFile file;
  private final ParquetFileInfoWithBlockMeta parquetInfo;
  private final MessageType shadedFileReadSchema;
  private final GpuParquetReaderPostProcessor postProcessor;

  public IcebergFooterContext(
      IcebergPartitionedFile file,
      ParquetFileInfoWithBlockMeta parquetInfo,
      MessageType shadedFileReadSchema,
      GpuParquetReaderPostProcessor postProcessor) {
    this.file = Objects.requireNonNull(file, "file");
    this.parquetInfo = Objects.requireNonNull(parquetInfo, "parquetInfo");
    this.shadedFileReadSchema = Objects.requireNonNull(
        shadedFileReadSchema, "shadedFileReadSchema");
    this.postProcessor = Objects.requireNonNull(postProcessor, "postProcessor");
  }

  public IcebergPartitionedFile getFile() {
    return file;
  }

  public ParquetFileInfoWithBlockMeta getParquetInfo() {
    return parquetInfo;
  }

  public MessageType getShadedFileReadSchema() {
    return shadedFileReadSchema;
  }

  public GpuParquetReaderPostProcessor getPostProcessor() {
    return postProcessor;
  }
}
