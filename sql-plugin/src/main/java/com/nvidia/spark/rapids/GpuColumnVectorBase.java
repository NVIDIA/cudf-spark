/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/** Base class for all GPU column vectors. */
abstract class GpuColumnVectorBase extends ColumnVector {
  private final static String BAD_ACCESS = "DATA ACCESS MUST BE ON A HOST VECTOR";

  private boolean isFinalBatch = false;

  /**
   * Set if this is a part of the final batch for this partition or not.
   * @param isFinal true if this is part of the final batch or false if unknown.
   */
  public final void setFinalBatch(boolean isFinal) {
    isFinalBatch = isFinal;
  }

  /**
   * Is this the final batch or is it unknown.
   * @return true if it is known to be a part of the final batch, else false if it is unknown
   */
  public boolean isKnownFinalBatch() {
    return isFinalBatch;
  }

  protected GpuColumnVectorBase(DataType type) {
    super(type);
  }

  @Override
  public final boolean isNullAt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final boolean getBoolean(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final byte getByte(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final short getShort(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final int getInt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final long getLong(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final float getFloat(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final double getDouble(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final ColumnarArray getArray(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final ColumnarMap getMap(int ordinal) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final Decimal getDecimal(int rowId, int precision, int scale) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final UTF8String getUTF8String(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final byte[] getBinary(int rowId) {
    throw new IllegalStateException(BAD_ACCESS);
  }

  @Override
  public final ColumnVector getChild(int ordinal) {
    throw new IllegalStateException(BAD_ACCESS);
  }
}
