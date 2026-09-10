/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class RapidsHostColumnBuilderSuite extends AnyFunSuite {
  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }

  test("appendLists walks appendChildOrNull for typed and null list elements") {
    // appendLists -> append(List) -> appendChildOrNull: one arm per element type, plus the null arm
    def buildList(childType: DType, elems: AnyRef*): Unit = {
      val lt = new ListType(true, new BasicType(true, childType))
      val b = new RapidsHostColumnBuilder(lt, 1)
      try {
        b.appendLists(java.util.Arrays.asList(elems: _*))
        val v = b.build()
        try {
          assertResult(1L)(v.getRowCount)
        } finally {
          v.close()
        }
      } finally {
        b.close()
      }
    }
    buildList(DType.INT32, Integer.valueOf(1), null, Integer.valueOf(3))
    buildList(DType.INT64, java.lang.Long.valueOf(1L), null)
    buildList(DType.FLOAT64, java.lang.Double.valueOf(1.0d), null)
    buildList(DType.FLOAT32, java.lang.Float.valueOf(1.0f), null)
    buildList(DType.BOOL8, java.lang.Boolean.TRUE, null)
    buildList(DType.STRING, "a", null)
  }

  test("captureState then restoreState rolls back appended struct rows including children") {
    val st = new StructType(true,
      new BasicType(true, DType.INT32),
      new BasicType(true, DType.INT32))
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).append(1)
      b.getChild(1).append(10)
      b.endStruct()
      val snapshot = b.captureState()
      b.getChild(0).append(2)
      b.getChild(1).append(20)
      b.endStruct()
      b.restoreState(snapshot)
      val v = b.build()
      try {
        assertResult(1L)(v.getRowCount)
      } finally {
        v.close()
      }
    } finally {
      b.close()
    }
  }
}
