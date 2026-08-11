/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostColumnVectorCore;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Wrapper of a RapidsHostColumnVectorCore, which will check nulls in each "getXXX" call and
 * return the default value of a type when trying to read a null.
 * The performance may not be good enough, so use it only when there is no other way.
 */
public class RapidsNullSafeHostColumnVectorCore extends ColumnVector {
  private final RapidsHostColumnVectorCore rapidsHcvc;
  private final RapidsNullSafeHostColumnVectorCore[] cachedChildren;

  public RapidsNullSafeHostColumnVectorCore(RapidsHostColumnVectorCore hcvc) {
    super(hcvc.dataType());
    this.rapidsHcvc = hcvc;
    if (type instanceof MapType) {
      // Map is a special case where we cache 2 children because it really ends up being
      // a list of structs in cuDF so the list only has one child, not the key/value of
      // stored in the struct
      cachedChildren = new RapidsNullSafeHostColumnVectorCore[2];
    } else {
      cachedChildren = new RapidsNullSafeHostColumnVectorCore[hcvc.getBase().getNumChildren()];
    }
  }

  public ai.rapids.cudf.HostColumnVectorCore getBase() {
    return rapidsHcvc.getBase();
  }

  @Override
  public void close() {
    for (int i = 0; i < cachedChildren.length; i++) {
      RapidsNullSafeHostColumnVectorCore cv = cachedChildren[i];
      if (cv != null) {
        cv.close();
        // avoid double closing this
        cachedChildren[i] = null;
      }
    }
    rapidsHcvc.close();
  }

  @Override
  public boolean hasNull() {
    return rapidsHcvc.hasNull();
  }

  @Override
  public int numNulls() {
    return rapidsHcvc.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return rapidsHcvc.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return isNullAt(rowId) ? false : rapidsHcvc.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return isNullAt(rowId) ? 0 : rapidsHcvc.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return isNullAt(rowId) ? 0 : rapidsHcvc.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return isNullAt(rowId) ? 0 : rapidsHcvc.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return isNullAt(rowId) ? 0L : rapidsHcvc.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return isNullAt(rowId) ? 0.0F : rapidsHcvc.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return isNullAt(rowId) ? 0.0 : rapidsHcvc.getDouble(rowId);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return isNullAt(rowId) ? null : rapidsHcvc.getUTF8String(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return isNullAt(rowId) ? null : rapidsHcvc.getDecimal(rowId, precision, scale);
  }

  /** We also need to wrap up the children for nested types: array, map, struct, etc ... */

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    // Not null
    if (cachedChildren[0] == null) {
      // Cache the child data
      ArrayType at = (ArrayType) type;
      HostColumnVectorCore data = getBase().getChildColumnView(0);
      cachedChildren[0] = new RapidsNullSafeHostColumnVectorCore(
          new RapidsHostColumnVectorCore(at.elementType(), data));
    }
    RapidsNullSafeHostColumnVectorCore data = cachedChildren[0];
    int startOffset = (int) getBase().getStartListOffset(rowId);
    int endOffset = (int) getBase().getEndListOffset(rowId);
    return new ColumnarArray(data, startOffset, endOffset - startOffset);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    if (isNullAt(ordinal)) return null;
    // Not null
    if (cachedChildren[0] == null) {
      // Cache the key/value, map is stored as list of struct (two children)
      MapType mt = (MapType) type;
      HostColumnVectorCore structHcv = getBase().getChildColumnView(0);
      // keys and values
      HostColumnVectorCore keyHcvCore = structHcv.getChildColumnView(0);
      HostColumnVectorCore valueHcvCore = structHcv.getChildColumnView(1);

      cachedChildren[0] = new RapidsNullSafeHostColumnVectorCore(
          new RapidsHostColumnVectorCore(mt.keyType(), keyHcvCore));
      cachedChildren[1] = new RapidsNullSafeHostColumnVectorCore(
          new RapidsHostColumnVectorCore(mt.valueType(), valueHcvCore));
    }
    RapidsNullSafeHostColumnVectorCore keys = cachedChildren[0];
    RapidsNullSafeHostColumnVectorCore values = cachedChildren[1];

    int startOffset = (int) getBase().getStartListOffset(ordinal);
    int endOffset = (int) getBase().getEndListOffset(ordinal);
    return new ColumnarMap(keys, values, startOffset,endOffset - startOffset);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (cachedChildren[ordinal] == null) {
      StructType st = (StructType) type;
      StructField[] fields = st.fields();
      for (int i = 0; i < fields.length; i++) {
        HostColumnVectorCore tmp = getBase().getChildColumnView(i);
        cachedChildren[i] = new RapidsNullSafeHostColumnVectorCore(
            new RapidsHostColumnVectorCore(fields[i].dataType(), tmp));
      }
    }
    return cachedChildren[ordinal];
  }

  @Override
  public byte[] getBinary(int rowId) {
    if(isNullAt(rowId)) return null;
    // Not null
    if (cachedChildren[0] == null) {
      // cache the child data
      HostColumnVectorCore data = getBase().getChildColumnView(0);
      cachedChildren[0] = new RapidsNullSafeHostColumnVectorCore(
          new RapidsHostColumnVectorCore(DataTypes.ByteType, data));
    }
    RapidsNullSafeHostColumnVectorCore data = cachedChildren[0];
    int startOffset = (int) getBase().getStartListOffset(rowId);
    int endOffset = (int) getBase().getEndListOffset(rowId);
    return new ColumnarArray(data, startOffset, endOffset - startOffset).toByteArray();
  }
}
