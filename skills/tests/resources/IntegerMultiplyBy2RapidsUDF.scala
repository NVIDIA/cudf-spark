/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import ai.rapids.cudf._
import com.nvidia.spark.RapidsUDF
import Arm.withResource

class IntegerMultiplyBy2RapidsUDF extends Function1[Integer, Integer] with Serializable with RapidsUDF {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    withResource(Scalar.fromInt(2)) { two =>
      args.head.mul(two)
    }
  }
}
