/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

class IntegerMultiplyBy2UDF extends Function1[Integer, Integer] with Serializable {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }
}
