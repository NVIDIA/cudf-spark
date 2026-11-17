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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.collection.JavaConverters;

import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor;
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Projected constants, their sizes, and a lazily created materializer for one file. */
final class MetadataFileState {
  final FileMetadata file;
  final long rowBytes;
  final long[] leafSizes;
  private final MetadataReadContext context;
  private final Object[] values;
  private GpuParquetReaderPostProcessor processor;

  MetadataFileState(FileMetadata file, MetadataReadContext context) {
    this.file = file;
    this.context = context;
    List<Types.NestedField> fields = context.conf.expectedSchema().columns();
    values = new Object[fields.size()];
    List<Long> leaves = new ArrayList<>();
    long bytes = 0;
    for (int i = 0; i < fields.size(); i++) {
      int id = fields.get(i).fieldId();
      values[i] = file.constants().containsKey(id) ? file.constants().get(id) :
          UTF8String.fromString(new Path(file.path()).toString());
      bytes += rowSize(values[i], context.sparkTypes[i], leaves);
    }
    rowBytes = bytes;
    leafSizes = new long[leaves.size()];
    for (int i = 0; i < leafSizes.length; i++) {
      leafSizes[i] = leaves.get(i);
    }
  }

  boolean sameOutput(MetadataFileState other) {
    return Arrays.deepEquals(values, other.values);
  }

  GpuParquetReaderPostProcessor processor() {
    if (processor == null) {
      ParquetFileInfoWithBlockMeta info = new ParquetFileInfoWithBlockMeta(
          new Path(file.path()),
          JavaConverters.asScalaBufferConverter(Collections
              .<org.apache.parquet.hadoop.metadata.BlockMetaData>emptyList()).asScala().toList(),
          InternalRow.empty(), context.unshadedParquetSchema, new StructType(),
          DateTimeRebaseCorrected$.MODULE$, DateTimeRebaseCorrected$.MODULE$, false,
          JavaConverters.asScalaBufferConverter(Collections.emptyList()).asScala().toList());
      processor = new GpuParquetReaderPostProcessor(info, file.constants(),
          context.conf.expectedSchema(), context.emptyParquetSchema, context.conf.metrics());
    }
    return processor;
  }

  // A conservative batch-size estimate. Keep variable-width leaves separate so a short value
  // in one column cannot hide an oversized sibling, including inside a struct.
  private static long rowSize(Object value, DataType type, List<Long> leaves) {
    if (type.equals(DataTypes.StringType) || type.equals(DataTypes.BinaryType)) {
      long bytes = value == null ? 0 : type.equals(DataTypes.StringType) ?
          ((UTF8String) value).numBytes() : ((byte[]) value).length;
      leaves.add(bytes);
      return bytes + 5; // offsets and validity
    }
    if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      long bytes = 1;
      for (int i = 0; i < fields.length; i++) {
        DataType childType = fields[i].dataType();
        Object child = value == null ? null : ((InternalRow) value).get(i, childType);
        bytes += rowSize(child, childType, leaves);
      }
      return bytes;
    }
    return (long) type.defaultSize() + 1;
  }
}
