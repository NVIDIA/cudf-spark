/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import org.apache.spark.sql.api.java.UDF1;
import ai.rapids.cudf.*;
import com.nvidia.spark.RapidsUDF;

public class IntegerMultiplyBy2RapidsUDF implements UDF1<Integer, Integer>, RapidsUDF {
  @Override
  public Integer call(Integer value) {
    return value == null ? null : value * 2;
  }

  @Override
  public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
    try (Scalar two = Scalar.fromInt(2)) {
      return args[0].mul(two);
    }
  }
}
