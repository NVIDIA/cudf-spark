/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, ColumnView, Table}
import com.nvidia.spark.rapids.Arm.withResource

object ArrayIndexUtils {

  /**
   * Return the first int value (should be valid) in 'indices' and 'numElements' as a pair
   * where the corresponding row in 'mask' is true. Null rows in mask are skipped.
   *
   * Both 'indices' and 'numElements' should be column of int, and have the same row number.
   * 'mask' should be a boolean column, and have the same row number with 'indices'.
   * Otherwise, the behavior is undefined.
   *
   * This is made for outputting more details for invalid index error in GpuElementAt and
   * GpuGetArrayItem. So the caller should take care of the limitations.
   */
  def firstIndexAndNumElementUnchecked(mask: ColumnView, indices: ColumnVector,
      numElements: ColumnVector): (Int, Int) = {
    val filteredTable = withResource(new Table(indices, numElements)) { indexTable =>
      indexTable.filter(mask)
    }
    withResource(filteredTable) { _ =>
      assert(filteredTable.getRowCount > 0)
      val index = withResource(filteredTable.getColumn(0).copyToHost()) { indicesH =>
        assert(!indicesH.isNull(0))
        indicesH.getInt(0)
      }
      val numElement = withResource(filteredTable.getColumn(1).copyToHost()) { numElemsH =>
        assert(!numElemsH.isNull(0))
        numElemsH.getInt(0)
      }
      (index, numElement)
    }
  }
}
