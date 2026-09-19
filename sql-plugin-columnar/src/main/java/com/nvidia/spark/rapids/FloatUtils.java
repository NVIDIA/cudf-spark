/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;

public final class FloatUtils {
  private FloatUtils() {}

  public static ColumnVector nanToZero(ColumnView cv) {
    if (cv.getType() != DType.FLOAT32 && cv.getType() != DType.FLOAT64) {
      throw new IllegalArgumentException("Only Floats and Doubles allowed");
    }

    try (ColumnVector isNan = cv.isNan();
         Scalar zero = cv.getType() == DType.FLOAT64
             ? Scalar.fromDouble(0.0d)
             : Scalar.fromFloat(0.0f)) {
      return isNan.ifElse(zero, cv);
    }
  }

  public static Scalar getNanScalar(DType dType) {
    if (dType == DType.FLOAT64) {
      return Scalar.fromDouble(Double.NaN);
    } else if (dType == DType.FLOAT32) {
      return Scalar.fromFloat(Float.NaN);
    } else {
      throw new IllegalArgumentException("NaNs are only supported for Float types");
    }
  }

  public static Scalar getPositiveInfinityScalar(DType dType) {
    if (dType == DType.FLOAT64) {
      return Scalar.fromDouble(Double.POSITIVE_INFINITY);
    } else {
      return Scalar.fromFloat(Float.POSITIVE_INFINITY);
    }
  }

  public static Scalar getNegativeInfinityScalar(DType dType) {
    if (dType == DType.FLOAT64) {
      return Scalar.fromDouble(Double.NEGATIVE_INFINITY);
    } else {
      return Scalar.fromFloat(Float.NEGATIVE_INFINITY);
    }
  }

  public static ColumnVector getInfinityVector(DType dtype) {
    if (dtype == DType.FLOAT64) {
      return ColumnVector.fromDoubles(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return ColumnVector.fromFloats(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }
  }

  public static ColumnVector infinityToNulls(ColumnVector vec) {
    try (ColumnVector infinityVector = getInfinityVector(vec.getType());
         ColumnVector nullVector = getNullVector(vec.getType())) {
      return vec.findAndReplaceAll(infinityVector, nullVector);
    }
  }

  private static ColumnVector getNullVector(DType dtype) {
    if (dtype == DType.FLOAT64) {
      return ColumnVector.fromBoxedDoubles((Double) null, (Double) null);
    } else {
      return ColumnVector.fromBoxedFloats((Float) null, (Float) null);
    }
  }
}
