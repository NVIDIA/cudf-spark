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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import org.apache.commons.lang3.Validate;

public final class BoolUtils {
  private BoolUtils() {}

  /**
   * Whether all the valid rows in {@code col} are true. An empty column will get true.
   * Null rows are skipped.
   *
   * @throws IllegalArgumentException if {@code col} is not a boolean column
   */
  public static boolean isAllValidTrue(ColumnVector col) {
    Validate.isTrue(DType.BOOL8 == col.getType(), "input column type is not bool");
    if (col.getRowCount() == 0) {
      return true;
    }

    if (col.getRowCount() == col.getNullCount()) {
      // all is null, equal to empty, since nulls should be skipped.
      return true;
    }
    try (Scalar allTrue = col.all()) {
      // Guaranteed there is at least one row and not all of the rows are null,
      // so result scalar must be valid.
      return allTrue.getBoolean();
    }
  }

  /**
   * Whether there is any valid row in {@code col} and it is true. An empty column will get false.
   * Null rows are skipped.
   *
   * @throws IllegalArgumentException if {@code col} is not a boolean column
   */
  public static boolean isAnyValidTrue(ColumnVector col) {
    Validate.isTrue(DType.BOOL8 == col.getType(), "input column type is not bool");

    if (col.getRowCount() == col.getNullCount()) {
      // all is null, return false since nulls should be skipped.
      return false;
    }
    try (Scalar anyTrue = col.any()) {
      // Guaranteed there is at least one row and not all of the rows are null,
      // so result scalar must be valid.
      return anyTrue.getBoolean();
    }
  }
}
