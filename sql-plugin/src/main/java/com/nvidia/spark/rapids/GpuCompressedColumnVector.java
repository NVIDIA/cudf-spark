/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.DeviceMemoryBuffer;
import com.nvidia.spark.rapids.format.TableMeta;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import static org.apache.spark.sql.types.DataTypes.NullType;

/**
 * A column vector that tracks a compressed table. Unlike a normal GPU column vector, the
 * columnar data within cannot be accessed directly. This class primarily serves the role
 * of tracking the compressed data and table metadata so it can be decompressed later.
 */
public final class GpuCompressedColumnVector extends GpuColumnVectorBase
    implements WithTableBuffer {
  private static final String BAD_ACCESS_MSG = "Column is compressed";

  private final DeviceMemoryBuffer buffer;
  private final TableMeta tableMeta;

  /**
   * Build a columnar batch from a compressed table.
   * NOTE: The data remains compressed and cannot be accessed directly from the columnar batch.
   */
  public static ColumnarBatch from(CompressedTable compressedTable) {
    return from(compressedTable.buffer(), compressedTable.meta());
  }

  public static boolean isBatchCompressed(ColumnarBatch batch) {
    return batch.numCols() == 1 && batch.column(0) instanceof GpuCompressedColumnVector;
  }

  public static ColumnarBatch incRefCounts(ColumnarBatch batch) {
    if (!isBatchCompressed(batch)) {
      throw new IllegalStateException(
              "Attempted to incRefCount for a compressed batch, but the batch was not compressed.");
    }
    ((GpuCompressedColumnVector)batch.column(0)).buffer.incRefCount();
    return batch;
  }

  /**
   * Build a columnar batch from a compressed data buffer and specified table metadata
   * NOTE: The data remains compressed and cannot be accessed directly from the columnar batch.
   */
  public static ColumnarBatch from(DeviceMemoryBuffer compressedBuffer, TableMeta tableMeta) {
    long rows = tableMeta.rowCount();
    int batchRows = (int) rows;
    if (rows != batchRows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }

    ColumnVector column = new GpuCompressedColumnVector(compressedBuffer, tableMeta);
    return new ColumnarBatch(new ColumnVector[] { column }, batchRows);
  }

  private GpuCompressedColumnVector(DeviceMemoryBuffer buffer, TableMeta tableMeta) {
    super(NullType);
    this.buffer = buffer;
    this.tableMeta = tableMeta;
    // reference the buffer so it remains valid for the duration of this column
    this.buffer.incRefCount();
  }

  @Override
  public DeviceMemoryBuffer getTableBuffer() {
    return buffer;
  }

  public TableMeta getTableMeta() {
    return tableMeta;
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public boolean hasNull() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public int numNulls() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }
}
