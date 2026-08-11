/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import java.util.Optional

import ai.rapids.cudf.{ColumnView, DType}
import com.nvidia.spark.rapids.Arm.withResource

/**
 * Provide a set of APIs to manipulate array/list columns in common ways.
 */
object GpuListUtils {
  /**
   * Replace the data column in a LIST column. This will keep the same offsets and validity
   * of the listColumn.  This returns a view so it is the responsibility of the caller to keep
   * both listCol and newDataCol alive longer than the returned ColumnView.
   * @param listCol the list column to use as a template
   * @param newDataCol the new data column.
   * @return a new ColumnView.
   * @throws scala.IllegalArgumentException if data column does not match the original data column
   *                                        in size.
   */
  def replaceListDataColumnAsView(
      listCol: ColumnView,
      newDataCol: ColumnView): ColumnView = {
    assert(DType.LIST.equals(listCol.getType))
    withResource(listCol.getChildColumnView(0)) { dataCol =>
      if (dataCol.getRowCount != newDataCol.getRowCount) {
        throw new scala.IllegalArgumentException(
          "Mismatch in the number of rows in the data columns")
      }
    }
    withResource(listCol.getOffsets) { offsets =>
      withResource(listCol.getValid) { validity =>
        new ColumnView(DType.LIST, listCol.getRowCount,
          Optional.of[java.lang.Long](listCol.getNullCount), validity, offsets,
          Array[ColumnView](newDataCol))
      }
    }
  }
}
