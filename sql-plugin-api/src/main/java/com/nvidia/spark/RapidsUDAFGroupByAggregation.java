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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;

/**
 * Base interface for GPU-accelerated UDAF aggregation implementations. This provides
 * the contract for different aggregation strategies.
 * <p/>
 * Please do not try and extend from this interface directly.
 * `RapidsSimpleGroupByAggregation` is currently supported as interfaces to directly
 * implement. More may be added in the future.
 */
public interface RapidsUDAFGroupByAggregation {
  /**
   * An optional pre-step for the aggregation. By default, this is a no-op
   * and will just return the arguments passed in.
   * <br/>
   * Users should close the input columns to avoid GPU memory leak, but the
   * returned columns will be closed automatically.
   * <br/>
   * @param numRows The number of rows.
   * @param args An array of input ColumnVectors.
   * @return An array of ColumnVectors.
   */
  default ColumnVector[] preStep(int numRows, ColumnVector[] args) {
    return args;
  }

  /**
   * Performs a reduction on the pre-step output (no keys). The
   * output of this will be turned into a ColumnVector and possibly
   * combined with other rows before being processed more.
   * <br/>
   * Both the input columns and returned Scalars will be closed automatically.
   *
   * @param numRows The number of rows to process.
   * @param preStepData The output from the preStep method.
   * @return An array of cudf Scalars representing the reduced data.
   */
  Scalar[] reduce(int numRows, ColumnVector[] preStepData);

  /**
   * A post-process step for the aggregation. It takes the output of the
   * aggregations and performs any processing needed to make it match the
   * input to the merge aggregation.
   * <br/>
   * Users should close the input columns to avoid GPU memory leak, but the
   * returned columns will be closed automatically.
   *
   * @param numRows The number of rows in the aggregated data.
   * @param aggregatedData The output from the aggregation step.
   * @return An array of ColumnVectors compatible with the merge step.
   */
  default ColumnVector[] postStep(int numRows, ColumnVector[] aggregatedData) {
    return aggregatedData;
  }
}
