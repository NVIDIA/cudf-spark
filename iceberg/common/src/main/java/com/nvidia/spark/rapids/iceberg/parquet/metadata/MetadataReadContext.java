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

package com.nvidia.spark.rapids.iceberg.parquet.metadata;

import java.util.Collections;

import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

/** Schema and batch limits shared by all files in a reader. */
final class MetadataReadContext {
  final GpuIcebergParquetReaderConf conf;
  final DataType[] sparkTypes;
  final MessageType emptyParquetSchema;
  final org.apache.parquet.schema.MessageType unshadedParquetSchema;
  final int maxRows;
  final long maxBytes;
  final long columnLimit;

  MetadataReadContext(GpuIcebergParquetReaderConf conf) {
    this.conf = conf;
    StructField[] fields = SparkSchemaUtil.convert(conf.expectedSchema()).fields();
    sparkTypes = new DataType[fields.length];
    for (int i = 0; i < fields.length; i++) {
      sparkTypes[i] = fields[i].dataType();
    }
    emptyParquetSchema = new MessageType("metadata", Collections.emptyList());
    unshadedParquetSchema = ShimUtils.unshadeParquetSchema(emptyParquetSchema);
    maxRows = Math.max(1, conf.maxBatchSizeRows());
    // The total byte bound also keeps string/binary offsets below cuDF's row limit.
    maxBytes = Math.max(1L, Math.min(Integer.MAX_VALUE,
        Math.min(conf.maxBatchSizeBytes(), conf.targetBatchSizeBytes())));
    columnLimit = Math.min(conf.maxGpuColumnSizeBytes(), Integer.MAX_VALUE);
  }
}
