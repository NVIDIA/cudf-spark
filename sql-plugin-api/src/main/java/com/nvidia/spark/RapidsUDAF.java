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
import org.apache.spark.sql.types.DataType;

/**
 * An interface for a GPU-accelerated User Defined Aggregate Function (UDAF).
 * This provides the necessary methods to perform distributed group-by and
 * reduction aggregations using CUDF.
 */
public interface RapidsUDAF {
  /**
   * Provides an array of default values for the aggregation result. This is
   * used when a reduction aggregation does not have any rows to aggregate.
   * <br/>
   * The returned Scalars will be closed automatically.
   * <br/>
   * @return An array of cudf Scalar representing the output of the
   *         updateAggregation stage of processing. The output of this
   *         may still be merged with other tasks.
   */
  Scalar[] getDefaultValue();

  /**
   * This method returns a RapidsUDAFGroupByAggregation that defines the
   * logic for the initial aggregation.
   * <br/>
   * @return A RapidsUDAFGroupByAggregation that defines the aggregation logic.
   */
  RapidsUDAFGroupByAggregation updateAggregation();

  /**
   * This method returns a RapidsUDAFGroupByAggregation that defines how to
   * merge two sets of aggregation results. This is used in distributed
   * aggregation scenarios where intermediate results from different
   * partitions are combined.
   * <br/>
   * @return A RapidsUDAFGroupByAggregation that defines the merge logic.
   */
  RapidsUDAFGroupByAggregation mergeAggregation();

  /**
   * A last step that takes the result of the merged aggregation
   * and performs any necessary transformations before returning the final
   * result. This method returns a single ColumnVector, which is the final
   * result of the aggregation.
   * <br/>
   * Users should close the input columns to avoid GPU memory leak. But the
   * returned column will be closed automatically.
   * <br/>
   * @param numRows The number of rows in the aggregated data.
   * @param args An array of ColumnVector arguments from the final aggregation step.
   * @param outType The final data type of this UDAF
   * @return A single ColumnVector representing the final UDAF result.
   */
  ColumnVector getResult(int numRows, ColumnVector[] args, DataType outType);

  /**
   * Data types of the aggregate buffer.
   * <br/>
   * It is better to align with the "bufferSchema" of "UserDefinedAggregateFunction", or
   * data corruption is likely to happen when some operations of this aggregation fall
   * back to CPU. E.g. Partial aggregates runs on CPU but final aggregates runs on GPU,
   * or vice-versa. This is rare but just in case.
   */
  DataType[] bufferTypes();
}
