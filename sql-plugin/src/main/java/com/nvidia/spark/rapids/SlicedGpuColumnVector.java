/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Wraps a GpuColumnVector but only points to a slice of it.  This is intended to only be used
 * during shuffle after the data is partitioned and before it is serialized.
 */
public class SlicedGpuColumnVector extends ColumnVector {
  private final RapidsHostColumnVector wrap;
  private final int start;
  private final int end;

  /**
   * Sets up the data type of this column vector.
   */
  protected SlicedGpuColumnVector(RapidsHostColumnVector w, int start, int end) {
    super(w.dataType());
    this.wrap = w;
    this.start = start;
    this.end = end;
    assert start >= 0;
    assert end > start; // we don't support empty slices, it should be a null
    assert end <= w.getBase().getRowCount();
    w.incRefCount();
  }

  @Override
  public void close() {
    wrap.close();
  }

  public static ColumnarBatch incRefCount(ColumnarBatch batch) {
    for (int i = 0; i < batch.numCols(); i++) {
      if (batch.column(i) instanceof SlicedGpuColumnVector) {
        ((SlicedGpuColumnVector)batch.column(i)).getBase().incRefCount();
      }
    }
    return batch;
  }

  @Override
  public boolean hasNull() {
    // This is a hack, we don't really know...
    return wrap.hasNull();
  }

  @Override
  public int numNulls() {
    // This is a hack, we don't really know...
    return wrap.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    assert rowId + start < end;
    return wrap.isNullAt(rowId + start);
  }

  @Override
  public boolean getBoolean(int rowId) {
    assert rowId + start < end;
    return wrap.getBoolean(rowId + start);
  }

  @Override
  public byte getByte(int rowId) {
    assert rowId + start < end;
    return wrap.getByte(rowId + start);
  }

  @Override
  public short getShort(int rowId) {
    assert rowId + start < end;
    return wrap.getShort(rowId + start);
  }

  @Override
  public int getInt(int rowId) {
    assert rowId + start < end;
    return wrap.getInt(rowId + start);
  }

  @Override
  public long getLong(int rowId) {
    assert rowId + start < end;
    return wrap.getLong(rowId + start);
  }

  @Override
  public float getFloat(int rowId) {
    assert rowId + start < end;
    return wrap.getFloat(rowId + start);
  }

  @Override
  public double getDouble(int rowId) {
    assert rowId + start < end;
    return wrap.getDouble(rowId + start);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    assert rowId + start < end;
    return wrap.getArray(rowId + start);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    assert rowId + start < end;
    return wrap.getMap(rowId + start);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    assert rowId + start < end;
    return wrap.getDecimal(rowId + start, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    assert rowId + start < end;
    return wrap.getUTF8String(rowId + start);
  }

  @Override
  public byte[] getBinary(int rowId) {
    assert rowId + start < end;
    return wrap.getBinary(rowId + start);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Children for a slice are not currently supported...");
  }

  public ai.rapids.cudf.HostColumnVector getBase() {
    return wrap.getBase();
  }

  public RapidsHostColumnVector getWrap() {
    return wrap;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  private static long getSizeOf(HostColumnVectorCore cv, int start, int end) {
    long total = 0;
    if (end > start) {
      ai.rapids.cudf.HostMemoryBuffer validity = cv.getValidity();
      if (validity != null) {
        // This is the same as ColumnView.getValidityBufferSize
        // number of bytes required = Math.ceil(number of bits / 8)
        long actualBytes = ((long) (end - start) + 7) >> 3;
        // padding to the multiplies of the padding boundary(64 bytes)
        total += ((actualBytes + 63) >> 6) << 6;
      }
      ai.rapids.cudf.HostMemoryBuffer off = cv.getOffsets();
      if (off != null) {
        total += (end - start + 1) * 4L;
        int newStart = (int) cv.getStartListOffset(start);
        int newEnd = (int) cv.getEndListOffset(end - 1);

        ai.rapids.cudf.HostMemoryBuffer data = cv.getData();
        if ((data != null) && (newEnd > newStart)) {
          if (DType.STRING.equals(cv.getType())) {
            total += newEnd - newStart;
          } else {
            throw new IllegalStateException("HOW CAN A " + cv.getType() +
                " HAVE DATA AND OFFSETS? " + cv);
          }
        }

        for (int i = 0; i < cv.getNumChildren(); i++) {
          total += getSizeOf(cv.getChildColumnView(i), newStart, newEnd);
        }
      } else {
        ai.rapids.cudf.HostMemoryBuffer data = cv.getData();
        if (data != null) {
          total += (long) (cv.getType().getSizeInBytes()) * (end - start);
        }

        for (int i = 0; i < cv.getNumChildren(); i++) {
          total += getSizeOf(cv.getChildColumnView(i), start, end);
        }
      }
    }
    return total;
  }

  public static long getTotalHostMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() > 0) {
      for (int i = 0; i < batch.numCols(); i++) {
        ColumnVector tmp = batch.column(i);
        if (tmp instanceof SlicedGpuColumnVector) {
          SlicedGpuColumnVector scv = (SlicedGpuColumnVector) tmp;
          sum += getSizeOf(scv.getBase(), scv.getStart(), scv.getEnd());
        } else {
          throw new RuntimeException(tmp + " is not supported for this");
        }
      }
    }
    return sum;
  }
}
