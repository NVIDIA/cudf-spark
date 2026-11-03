/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A helper class which efficiently transfers different types of host columnar data into cuDF.
 * It is written in Java for two reasons:
 * 1. Scala for-loop is slower (Scala while-loop is identical to Java loop)
 * 2. Both ColumnBuilder and ColumnVector are Java classes
 */
public class ColumnarCopyHelper {

  public static long nullCopy(RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    for (int i = 0; i < rows; i++) {
      bytesCopied += b.appendNull();
    }
    return bytesCopied;
  }

  public static long booleanCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getBoolean(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getBoolean(i));
      }
    }
    return bytesCopied;
  }

  public static long byteCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getByte(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getByte(i));
      }
    }
    return bytesCopied;
  }

  public static long shortCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getShort(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getShort(i));
      }
    }
    return bytesCopied;
  }

  public static long intCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getInt(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getInt(i));
      }
    }
    return bytesCopied;
  }

  public static long longCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getLong(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getLong(i));
      }
    }
    return bytesCopied;
  }

  public static long floatCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getFloat(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getFloat(i));
      }
    }
    return bytesCopied;
  }

  public static long doubleCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getDouble(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getDouble(i));
      }
    }
    return bytesCopied;
  }

  public static long stringCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.appendUTF8String(cv.getUTF8String(i).getBytes());
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.appendUTF8String(cv.getUTF8String(i).getBytes());
      }
    }
    return bytesCopied;
  }

  public static long binaryCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        byte[] value = cv.getBinary(i);
        b.appendByteList(value);
        bytesCopied += value.length + Integer.BYTES;
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        byte[] value = cv.getBinary(i);
        b.appendByteList(value);
        bytesCopied += value.length + Integer.BYTES;
      }
    }
    return bytesCopied;
  }

  public static long decimal32Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    return intCopy(cv, b, rows);
  }

  public static long decimal64Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    return longCopy(cv, b, rows);
  }

  public static long decimal128Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.appendDecimal128(cv.getBinary(i));
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.appendDecimal128(cv.getBinary(i));
      }
    }
    return bytesCopied;
  }

  public static long decimal32Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append((int) cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append((int) cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
    }
    return bytesCopied;
  }

  public static long decimal64Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
    }
    return bytesCopied;
  }

  public static long decimal128Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    long bytesCopied = 0L;
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        bytesCopied += b.append(cv.getDecimal(i, precision, scale).toJavaBigDecimal());
      }
      return bytesCopied;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        bytesCopied += b.appendNull();
      } else {
        bytesCopied += b.append(cv.getDecimal(i, precision, scale).toJavaBigDecimal());
      }
    }
    return bytesCopied;
  }
}
