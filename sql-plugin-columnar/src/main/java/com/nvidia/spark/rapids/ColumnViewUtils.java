/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.Scalar;

public final class ColumnViewUtils {
  private ColumnViewUtils() {}

  /**
   * Get the {@code toString} on the scalar element at the specified row index in a column view.
   * E.g., returns: Scalar{type=INT32 value=-1250858453} (ID: 143 7149580cdd60)
   */
  public static String getElementStringFromColumnView(ColumnView cv, int rowIndex) {
    try (Scalar scalar = cv.getScalarElement(rowIndex)) {
      if (scalar.isValid()) {
        return scalar.toString();
      } else {
        return "null";
      }
    }
  }
}
