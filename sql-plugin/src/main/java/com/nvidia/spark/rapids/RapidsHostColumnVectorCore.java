
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

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public class RapidsHostColumnVectorCore extends ColumnVector {

  private final HostColumnVectorCore cudfCv;
  private final RapidsHostColumnVectorCore[] cachedChildren;

  /**
   * Sets up the data type of this column vector.
   */
  RapidsHostColumnVectorCore(DataType type, HostColumnVectorCore cudfCv) {
    super(type);
    this.cudfCv = cudfCv;
    if (type instanceof MapType) {
      // Map is a special case where we cache 2 children because it really ends up being
      // a list of structs in CUDF so the list only has one child, not the key/value of
      // stored in the struct
      cachedChildren = new RapidsHostColumnVectorCore[2];
    } else {
      cachedChildren = new RapidsHostColumnVectorCore[cudfCv.getNumChildren()];
    }
  }

  @Override
  public final void close() {
    for (int i = 0; i < cachedChildren.length; i++) {
      RapidsHostColumnVectorCore cv = cachedChildren[i];
      if (cv != null) {
        cv.close();
        // avoid double closing this
        cachedChildren[i] = null;
      }
    }
    cudfCv.close();
  }

  @Override
  public final boolean hasNull() {
    return cudfCv.hasNulls();
  }

  @Override
  public final int numNulls() {
    return (int) cudfCv.getNullCount();
  }

  @Override
  public final boolean isNullAt(int rowId) {
    return cudfCv.isNull(rowId);
  }

  @Override
  public final boolean getBoolean(int rowId) {
    return cudfCv.getBoolean(rowId);
  }

  @Override
  public final byte getByte(int rowId) {
    return cudfCv.getByte(rowId);
  }

  @Override
  public final short getShort(int rowId) {
    return cudfCv.getShort(rowId);
  }

  @Override
  public final int getInt(int rowId) {
    return cudfCv.getInt(rowId);
  }

  @Override
  public final long getLong(int rowId) {
    return cudfCv.getLong(rowId);
  }

  @Override
  public final float getFloat(int rowId) {
    return cudfCv.getFloat(rowId);
  }

  @Override
  public final double getDouble(int rowId) {
    return cudfCv.getDouble(rowId);
  }

  @Override
  public final ColumnarArray getArray(int rowId) {
    if (cachedChildren[0] == null) {
      // cache the child data
      ArrayType at = (ArrayType) dataType();
      HostColumnVectorCore data = cudfCv.getChildColumnView(0);
      cachedChildren[0] = new RapidsHostColumnVectorCore(at.elementType(), data);
    }
    RapidsHostColumnVectorCore data = cachedChildren[0];
    int startOffset = (int) cudfCv.getStartListOffset(rowId);
    int endOffset = (int) cudfCv.getEndListOffset(rowId);
    return new ColumnarArray(data, startOffset, endOffset - startOffset);
  }

  @Override
  public final ColumnarMap getMap(int ordinal) {
    if (cachedChildren[0] == null) {
      // Cache the key/value
      MapType mt = (MapType) dataType();
      HostColumnVectorCore structHcv = cudfCv.getChildColumnView(0);
      // keys
      HostColumnVectorCore firstHcvCore = structHcv.getChildColumnView(0);
      // values
      HostColumnVectorCore secondHcvCore = structHcv.getChildColumnView(1);

      cachedChildren[0] = new RapidsHostColumnVectorCore(mt.keyType(), firstHcvCore);
      cachedChildren[1] = new RapidsHostColumnVectorCore(mt.valueType(), secondHcvCore);
    }
    RapidsHostColumnVectorCore keys = cachedChildren[0];
    RapidsHostColumnVectorCore values = cachedChildren[1];

    int startOffset = (int) cudfCv.getStartListOffset(ordinal);
    int endOffset = (int) cudfCv.getEndListOffset(ordinal);
    return new ColumnarMap(keys, values, startOffset,endOffset - startOffset);
  }

  @Override
  public final Decimal getDecimal(int rowId, int precision, int scale) {
    assert precision <= DType.DECIMAL128_MAX_PRECISION : "Assert " + precision + " <= DECIMAL128_MAX_PRECISION(" + DType.DECIMAL128_MAX_PRECISION + ")";
    assert scale == -cudfCv.getType().getScale() :
        "Assert fetch decimal with its original scale " + scale + " expected " + (-cudfCv.getType().getScale());
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      assert cudfCv.getType().getTypeId() == DType.DTypeEnum.DECIMAL32 : "type should be DECIMAL32";
      return Decimal.createUnsafe(cudfCv.getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      assert cudfCv.getType().getTypeId() == DType.DTypeEnum.DECIMAL64 : "type should be DECIMAL64";
      return Decimal.createUnsafe(cudfCv.getLong(rowId), precision, scale);
    } else {
      assert cudfCv.getType().getTypeId() == DType.DTypeEnum.DECIMAL128 : "type should be DECIMAL128";
      return Decimal.fromDecimal(new BigDecimal(cudfCv.getBigDecimal(rowId).unscaledValue(), scale));
    }

  }

  @Override
  public final UTF8String getUTF8String(int rowId) {
    return UTF8String.fromBytes(cudfCv.getUTF8(rowId));
  }

  @Override
  public final byte[] getBinary(int rowId) {
    if (cachedChildren[0] == null) {
      // cache the child data
      HostColumnVectorCore data = cudfCv.getChildColumnView(0);
      cachedChildren[0] = new RapidsHostColumnVectorCore(DataTypes.ByteType, data);
    }
    RapidsHostColumnVectorCore data = cachedChildren[0];
    int startOffset = (int) cudfCv.getStartListOffset(rowId);
    int endOffset = (int) cudfCv.getEndListOffset(rowId);
    return new ColumnarArray(data, startOffset, endOffset - startOffset).toByteArray();
  }

  @Override
  public final ColumnVector getChild(int ordinal) {
    if (cachedChildren[ordinal] == null) {
      StructType st = (StructType) dataType();
      StructField[] fields = st.fields();
      for (int i = 0; i < fields.length; i++) {
        HostColumnVectorCore tmp = cudfCv.getChildColumnView(i);
        cachedChildren[i] = new RapidsHostColumnVectorCore(fields[i].dataType(), tmp);
      }
    }
    return cachedChildren[ordinal];
  }

  public HostColumnVectorCore getBase() {
    return cudfCv;
  }

  public final long getRowCount() { return cudfCv.getRowCount(); }
}
