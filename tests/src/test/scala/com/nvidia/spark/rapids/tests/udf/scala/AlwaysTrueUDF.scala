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

package com.nvidia.spark.rapids.tests.udf.scala

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.Arm.withResource

/**
 * A Scala user-defined function (UDF) that always returns true.
 * Used for testing RAPIDS accelerated UDFs with no inputs.
 */
class AlwaysTrueUDF extends (() => Boolean) with RapidsUDF with Serializable {
  /** Row-by-row implementation that executes on the CPU */
  override def apply(): Boolean = true

  /** Columnar implementation that runs on the GPU */
  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.isEmpty)
    withResource(Scalar.fromBool(true)) { s =>
      ColumnVector.fromScalar(s, numRows);
    }
  }
}
