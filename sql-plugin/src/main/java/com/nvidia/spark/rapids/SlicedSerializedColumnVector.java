/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostMemoryBuffer;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.NullType;

/**
 * Wraps a GpuColumnVector but only points to a slice of it.  This is intended to only be used
 * during shuffle after the data is partitioned and before it is serialized.
 */
public class SlicedSerializedColumnVector extends ColumnVector {
  private final HostMemoryBuffer wrap;

  private static final String BAD_ACCESS_MSG = "Column is serialized";

  /**
   * Sets up the data type of this column vector.
   * Visible for testing
   */
  public SlicedSerializedColumnVector(HostMemoryBuffer w, int start, int end) {
    super(NullType);
    this.wrap = w.slice(start, end - start);
    assert start >= 0;
    assert end > start; // we don't support empty slices, it should be a null
    assert end <= w.getLength();
  }

  @Override
  public void close() {
    wrap.close();
  }

  public static ColumnarBatch incRefCount(ColumnarBatch batch) {
    for (int i = 0; i < batch.numCols(); i++) {
      if (batch.column(i) instanceof SlicedSerializedColumnVector) {
        ((SlicedSerializedColumnVector)batch.column(i)).getWrap().incRefCount();
      }
    }
    return batch;
  }

  @Override
  public boolean hasNull() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public int numNulls() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public boolean isNullAt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public byte getByte(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public short getShort(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public int getInt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public long getLong(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public float getFloat(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public double getDouble(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Children for a slice are not currently supported...");
  }

  public HostMemoryBuffer getWrap() {
    return wrap;
  }

  public long getLength() { return this.wrap.getLength(); }

  public static long getTotalHostMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() > 0) {
      for (int i = 0; i < batch.numCols(); i++) {
        ColumnVector tmp = batch.column(i);
        if (tmp instanceof SlicedSerializedColumnVector) {
          SlicedSerializedColumnVector scv = (SlicedSerializedColumnVector) tmp;
          sum += scv.getLength();
        } else {
          throw new RuntimeException(tmp + " is not supported for this");
        }
      }
    }
    return sum;
  }
}
