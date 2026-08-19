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

package com.nvidia.spark;

import ai.rapids.cudf.GroupByAggregationOnColumn;

/**
 * Standard CUDF-based aggregation step that uses built-in CUDF aggregation
 * operations. This handles the most common aggregation patterns and provides
 * the best performance.
 */
public interface RapidsSimpleGroupByAggregation extends RapidsUDAFGroupByAggregation {
  /**
   * The main aggregation step that uses built-in CUDF GroupBy operations.
   *
   * @param inputIndices An array of ints, which are the indices of the input
   *                     columns.
   * @return An array of CUDF `GroupByAggregationOnColumn` instances.
   */
  GroupByAggregationOnColumn[] aggregate(int[] inputIndices);
}
