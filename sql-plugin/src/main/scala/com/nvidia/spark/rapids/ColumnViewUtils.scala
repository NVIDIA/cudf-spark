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
package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnView
import com.nvidia.spark.rapids.Arm.withResource

object ColumnViewUtils {
  /**
   * Get the `toString` on the scalar element at the specified row index in a column view.
   * E.g., returns: Scalar{type=INT32 value=-1250858453} (ID: 143 7149580cdd60)
   */
  def getElementStringFromColumnView(cv: ColumnView, rowIndex: Int): String = {
    withResource(cv.getScalarElement(rowIndex)) { scalar =>
      if (scalar.isValid) {
        scalar.toString
      } else {
        "null"
      }
    }
  }
}
