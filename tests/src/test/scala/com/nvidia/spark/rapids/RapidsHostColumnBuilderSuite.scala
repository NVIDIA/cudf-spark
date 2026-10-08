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
import com.nvidia.spark.rapids.Arm.withResource
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

  test("restoreState handles non-null rows beyond the allocated validity bitmap") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 4)) { b =>
      b.appendNull()
      (1 to 4096).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.append(4097)
      b.restoreState(snapshot)
      b.append(4098)
      withResource(b.build()) { column =>
        assertResult(4098L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 4096).foreach(i => assertResult(i)(column.getInt(i)))
        assertResult(4098)(column.getInt(4097))
      }
    }
  }

  test("restoreState preserves earlier nulls and is idempotent across validity bytes") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 16)) { b =>
      b.appendNull()
      (1 to 6).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.appendNull()
      b.append(8)
      b.appendNull()
      b.restoreState(snapshot)
      b.restoreState(snapshot)
      (7 to 9).foreach(i => b.append(i))
      withResource(b.build()) { column =>
        assertResult(10L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 9).foreach { i =>
          assert(!column.isNull(i))
          assertResult(i)(column.getInt(i))
        }
      }
    }
  }

  test("restoreState rolls back a partial struct child before non-null replay") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    withResource(new RapidsHostColumnBuilder(new StructType(true, byteList, byteList), 4)) {
      b =>
        val snapshot = b.captureState()
        b.getChild(0).appendNull()
        b.restoreState(snapshot)
        b.getChild(0).appendByteList(Array[Byte](1, 2))
        b.getChild(1).appendByteList(Array[Byte](3))
        b.endStruct()
        withResource(b.build()) { column =>
          assertResult(1L)(column.getRowCount)
          assertResult(0L)(column.getNullCount)
          (0 until column.getNumChildren).foreach { index =>
            withResource(column.getChildColumnView(index)) { child =>
              assertResult(0L)(child.getNullCount)
              assert(!child.isNull(0))
              withResource(child.getChildColumnView(0)) { values =>
                val expected = if (index == 0) Array[Byte](1, 2) else Array[Byte](3)
                assertResult(expected.length.toLong)(values.getRowCount)
                expected.indices.foreach(i => assertResult(expected(i))(values.getByte(i)))
              }
            }
          }
        }
    }
  }

  test("restoreState rolls back null counts and validity recursively") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    val st = new StructType(true, byteList, byteList)
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).appendByteList(Array[Byte](1, 2))
      b.getChild(1).appendByteList(Array[Byte](3))
      b.endStruct()

      val snapshot = b.captureState()
      b.appendNull()
      b.restoreState(snapshot)

      val partial = b.build()
      try {
        assertResult(1L)(partial.getRowCount)
        assertResult(0L)(partial.getNullCount)
        (0 until partial.getNumChildren).foreach { index =>
          withResource(partial.getChildColumnView(index)) { child =>
            assertResult(0L)(child.getNullCount)
          }
        }
      } finally {
        partial.close()
      }

      b.appendNull()
      val replayed = b.build()
      try {
        assertResult(2L)(replayed.getRowCount)
        assertResult(1L)(replayed.getNullCount)
        (0 until replayed.getNumChildren).foreach { index =>
          withResource(replayed.getChildColumnView(index)) { child =>
            assertResult(1L)(child.getNullCount)
          }
        }
      } finally {
        replayed.close()
      }
    } finally {
      b.close()
    }
  }
}
