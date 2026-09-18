/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import java.lang.{Long => JLong}

/**
 * Sum a contiguous segment of a numeric array.
 */
class ArraySegmentSumUDF
    extends Function3[Seq[JLong], Integer, Integer, JLong] with Serializable {

  override def apply(values: Seq[JLong], start: Integer, length: Integer): JLong = {
    if (values == null || start == null || length == null) return null

    val n = values.size
    if (n == 0) return JLong.valueOf(0L)

    val from = start.intValue()
    val len = length.intValue()
    if (from < 0 || from >= n || len <= 0) return JLong.valueOf(0L)

    val until = if (len > n - from) n else from + len

    var acc = 0L
    var i = from
    while (i < until) {
      val v = values(i)
      if (v != null) acc += v.longValue() // skip null elements
      i += 1
    }
    JLong.valueOf(acc)
  }
}
