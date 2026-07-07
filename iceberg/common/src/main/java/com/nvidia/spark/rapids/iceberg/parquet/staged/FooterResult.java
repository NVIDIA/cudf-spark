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

import com.nvidia.spark.rapids.DateTimeRebaseMode;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.types.StructType;

/**
 * Immutable result of fetching a footer and filtering its row groups.
 *
 * <p>A footer worker constructs this object and publishes it to the Spark task thread. Lists are
 * defensively copied and exposed as unmodifiable views. The referenced Parquet metadata objects
 * are treated as immutable by this pipeline; the synthetic-layout builder creates new metadata
 * rather than mutating them.</p>
 *
 * <p>This first implementation is deliberately Iceberg-specific. The post-processor contains the
 * schema-evolution and partition-constant work that must run after cuDF decodes a subtask. It is
 * borrowed by planned segments and remains task-confined during decode.</p>
 */
public final class FooterResult {
  private final StagedFileSource source;
  private final List<BlockMetaData> blocks;
  private final MessageType clippedSchema;
  private final StructType readSchema;
  private final DateTimeRebaseMode dateRebaseMode;
  private final DateTimeRebaseMode timestampRebaseMode;
  private final boolean hasInt96Timestamps;
  private final GpuParquetReaderPostProcessor postProcessor;

  /**
   * Creates a fully filtered footer result.
   *
   * @param source source file or split
   * @param blocks filtered blocks in original file order
   * @param clippedSchema unshaded physical Parquet schema for the copied columns
   * @param readSchema Spark schema materialized by the Parquet decoder
   * @param dateRebaseMode date rebase behavior used during decode
   * @param timestampRebaseMode timestamp rebase behavior used during decode
   * @param hasInt96Timestamps whether the source can contain INT96 timestamps
   * @param postProcessor Iceberg post-processing state borrowed by planned subtasks
   */
  public FooterResult(
      StagedFileSource source,
      List<BlockMetaData> blocks,
      MessageType clippedSchema,
      StructType readSchema,
      DateTimeRebaseMode dateRebaseMode,
      DateTimeRebaseMode timestampRebaseMode,
      boolean hasInt96Timestamps,
      GpuParquetReaderPostProcessor postProcessor) {
    this.source = Objects.requireNonNull(source, "source");
    this.blocks = immutableCopy(blocks, "blocks");
    this.clippedSchema = Objects.requireNonNull(clippedSchema, "clippedSchema");
    this.readSchema = Objects.requireNonNull(readSchema, "readSchema");
    this.dateRebaseMode = Objects.requireNonNull(dateRebaseMode, "dateRebaseMode");
    this.timestampRebaseMode = Objects.requireNonNull(
        timestampRebaseMode, "timestampRebaseMode");
    this.hasInt96Timestamps = hasInt96Timestamps;
    this.postProcessor = Objects.requireNonNull(postProcessor, "postProcessor");
  }

  private static <T> List<T> immutableCopy(List<T> values, String name) {
    Objects.requireNonNull(values, name);
    ArrayList<T> copy = new ArrayList<>(values);
    if (copy.contains(null)) {
      throw new IllegalArgumentException(name + " must not contain null values");
    }
    return Collections.unmodifiableList(copy);
  }

  public StagedFileSource getSource() {
    return source;
  }

  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  public MessageType getClippedSchema() {
    return clippedSchema;
  }

  public StructType getReadSchema() {
    return readSchema;
  }

  public DateTimeRebaseMode getDateRebaseMode() {
    return dateRebaseMode;
  }

  public DateTimeRebaseMode getTimestampRebaseMode() {
    return timestampRebaseMode;
  }

  public boolean hasInt96Timestamps() {
    return hasInt96Timestamps;
  }

  public GpuParquetReaderPostProcessor getPostProcessor() {
    return postProcessor;
  }
}
