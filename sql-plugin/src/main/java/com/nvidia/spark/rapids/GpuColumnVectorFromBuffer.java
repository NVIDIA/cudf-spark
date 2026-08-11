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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.format.TableMeta;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Arrays;

/** GPU column vector carved from a single buffer, like those from cudf's contiguousSplit. */
public final class GpuColumnVectorFromBuffer extends GpuColumnVector {
  private final DeviceMemoryBuffer buffer;
  private final TableMeta tableMeta;

  /**
   * Get a ColumnarBatch from a set of columns in a contiguous table. This differs from the
   * GpuColumnVector version in that it produces GpuColumnVectorFromBuffer instances that are
   * useful for determining the original buffer from which the column was derived.
   * This will increment the reference count for all columns converted so you will need to close
   * both the table that is passed in and the batch returned to be sure that there are no leaks.
   *
   * @param contigTable contiguous table
   * @param colTypes the types of the columns to get back out.
   * @return batch of GpuColumnVectorFromBuffer instances derived from the table
   */
  public static ColumnarBatch from(ContiguousTable contigTable, DataType[] colTypes) {
    DeviceMemoryBuffer buffer = contigTable.getBuffer();
    Table table = contigTable.getTable();
    TableMeta meta = MetaUtils.buildTableMeta(0, contigTable);
    return from(table, buffer, meta, colTypes);
  }

  /**
   * Get a ColumnarBatch from a set of columns in a table, and the corresponding device buffer,
   * which backs such columns. The resulting batch is composed of columns which are instances of
   * GpuColumnVectorFromBuffer. This will increment the reference count for all columns
   * converted so you will need to close both the table that is passed in and the batch
   * returned to be sure that there are no leaks.
   *
   * @param table a table with columns at offsets of `buffer`
   * @param buffer a device buffer that packs data for columns in `table`
   * @param meta metadata describing the table layout and schema
   * @param colTypes the types the columns should have.
   * @return batch of GpuColumnVectorFromBuffer instances derived from the table and buffer
   */
  public static ColumnarBatch from(Table table, DeviceMemoryBuffer buffer,
      TableMeta meta, DataType[] colTypes) {
    assert table != null : "Table cannot be null";
    assert GpuColumnVector.typeConversionAllowed(table, colTypes) :
        "Type conversion is not allowed from " + table + " to " + Arrays.toString(colTypes);
    long rows = table.getRowCount();
    if (rows != (int) rows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }
    int numColumns = table.getNumberOfColumns();
    GpuColumnVector[] columns = new GpuColumnVector[numColumns];
    try {
      for (int i = 0; i < numColumns; ++i) {
        ColumnVector v = table.getColumn(i);
        DataType type = colTypes[i];
        columns[i] = new GpuColumnVectorFromBuffer(type, v.incRefCount(), buffer, meta);
      }
      return new ColumnarBatch(columns, (int) rows);
    } catch (Exception e) {
      for (GpuColumnVector v : columns) {
        if (v != null) {
          v.close();
        }
      }
      throw e;
    }
  }

  /**
   * Note that this type of [[GpuColumnVector]] takes a single buffer. The buffer is
   * shared between the various components of the vector (data, validity, offsets), and held
   * in one contiguous chunk.
   *
   * This class does not need to override close, because [[columnVector]]
   * already holds references to the buffer, and will be released once
   * [[ColumnVector]] is closed.
   *
   * See [[from]] in this class for more info on its lifecycle.
   *
   * @param type the spark data type for this column
   * @param cudfColumn a ColumnVector instance
   * @param buffer the buffer to hold
   * @param meta the metadata describing the buffer layout
   */
  public GpuColumnVectorFromBuffer(DataType type, ColumnVector cudfColumn,
      DeviceMemoryBuffer buffer, TableMeta meta) {
    super(type, cudfColumn);
    this.buffer = buffer;
    this.tableMeta = meta;
  }

  public static boolean isFromBuffer(ColumnarBatch cb) {
    if (cb.numCols() > 0) {
      long bufferAddr = 0L;
      boolean isSet = false;
      for (int i = 0; i < cb.numCols(); ++i) {
        GpuColumnVectorFromBuffer gcvfb = null;
        if (!(cb.column(i) instanceof GpuColumnVectorFromBuffer)) {
          return false;
        } else {
          gcvfb = (GpuColumnVectorFromBuffer) cb.column(i);
          if (!isSet) {
            bufferAddr = gcvfb.buffer.getAddress();
            isSet = true;
          } else if (bufferAddr != gcvfb.buffer.getAddress()) {
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Get the underlying contiguous buffer, shared between columns of the original
   * `ContiguousTable`
   * @return contiguous (data, validity and offsets) device memory buffer
   */
  public DeviceMemoryBuffer getBuffer() {
    return buffer;
  }

  /**
   * Get the metadata describing the data layout in the buffer,
   * shared between columns of the original `ContiguousTable`
   * @return opaque metadata
   */
  public TableMeta getTableMeta() {
    return tableMeta;
  }
}
