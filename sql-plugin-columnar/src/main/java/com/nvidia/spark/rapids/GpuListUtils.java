/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import java.util.Optional;

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import org.apache.commons.lang3.Validate;

/** Provides APIs to manipulate array/list columns in common ways. */
public final class GpuListUtils {
  private GpuListUtils() {}

  /**
   * Replace the data column in a LIST column. This keeps the same offsets and validity
   * of the list column. This returns a view, so the caller is responsible for keeping
   * both {@code listCol} and {@code newDataCol} alive longer than the returned view.
   *
   * @param listCol the list column to use as a template
   * @param newDataCol the new data column
   * @return a new ColumnView
   * @throws IllegalArgumentException if {@code listCol} is not a list column, or if the data
   *     column does not match the original data column in size
   */
  public static ColumnView replaceListDataColumnAsView(
      ColumnView listCol, ColumnView newDataCol) {
    Validate.isTrue(DType.LIST.equals(listCol.getType()), "input column type is not list");
    try (ColumnView dataCol = listCol.getChildColumnView(0)) {
      if (dataCol.getRowCount() != newDataCol.getRowCount()) {
        throw new IllegalArgumentException("Mismatch in the number of rows in the data columns");
      }
    }
    try (BaseDeviceMemoryBuffer offsets = listCol.getOffsets();
         BaseDeviceMemoryBuffer validity = listCol.getValid()) {
      return new ColumnView(
          DType.LIST,
          listCol.getRowCount(),
          Optional.of(listCol.getNullCount()),
          validity,
          offsets,
          new ColumnView[] { newDataCol });
    }
  }
}
