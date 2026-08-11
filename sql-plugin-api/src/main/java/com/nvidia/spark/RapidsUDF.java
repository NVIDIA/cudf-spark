/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

/** A RAPIDS accelerated version of a user-defined function (UDF). */
public interface RapidsUDF {
  /**
   * Evaluate a user-defined function with RAPIDS cuDF columnar inputs
   * producing a cuDF column as output. The method must return a column of
   * the appropriate type that corresponds to the type returned by the CPU
   * implementation of the UDF (e.g.: INT32 for int, FLOAT64 for double,
   * STRING for String, etc) or a runtime exception will occur when the
   * results are marshalled into the expected Spark result type for the UDF.
   * The number of rows of output must match the number of rows for the
   * input arguments, which will all have the same row count.
   * <p/>
   * Note that the inputs should NOT be closed by this method, as they will
   * be closed by the caller. This method must close any intermediate cuDF
   * results produced during the computation (e.g.: `Table`, `ColumnVector`
   * or `Scalar` instances).
   * @param args columnar inputs to the UDF that will be closed by the caller
   *             and should not be closed within this method.
   * @return columnar output from the user-defined function
   * @deprecated Use {@link #evaluateColumnar(int, ColumnVector...)}
   */
  @Deprecated
  default ColumnVector evaluateColumnar(ColumnVector... args) {
    throw new UnsupportedOperationException(
        "Must override evaluateColumnar(int numRows, ColumnVector... args");
  }

  /**
   * Evaluate a user-defined function with RAPIDS cuDF columnar inputs
   * producing a cuDF column as output. The method must return a column of
   * the appropriate type that corresponds to the type returned by the CPU
   * implementation of the UDF (e.g.: INT32 for int, FLOAT64 for double,
   * STRING for String, etc) or a runtime exception will occur when the
   * results are marshalled into the expected Spark result type for the UDF.
   * The number of rows of output must match the number of rows specified,
   * and all input columns must have that same number of rows.
   * <p/>
   * Note that the inputs should NOT be closed by this method, as they will
   * be closed by the caller. This method must close any intermediate cuDF
   * results produced during the computation (e.g.: `Table`, `ColumnVector`
   * or `Scalar` instances).
   * @param numRows number of rows of output to return
   * @param args columnar inputs to the UDF that will be closed by the caller
   *             and should not be closed within this method.
   * @return columnar output from the user-defined function
   */
  default ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
    return evaluateColumnar(args);
  }
}
