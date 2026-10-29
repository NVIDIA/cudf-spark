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

/*** spark-rapids-shim-json-lines
{"spark":["334","342"],"carryForward":"sameMinor"}
{"spark":["351","352","353","354","355","356","357"],"carryForward":"none"}
{"spark":["358","400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.GetSequenceSize

class GetSequenceSizeSuite extends RmmSparkRetrySuiteBase {
  test("sequence sizes preserve descending ranges, long boundaries and nulls") {
    val starts = Array[java.lang.Long](0L, 4L, Long.MinValue, null)
    val stops = Array[java.lang.Long](4L, -4L, Long.MinValue, 4L)
    val steps = Array[java.lang.Long](2L, -2L, 1L, 1L)
    val expected = starts.zip(stops).zip(steps).map { case ((start, stop), step) =>
      if (start == null || stop == null || step == null) null else java.lang.Long.valueOf(
        Math.addExact(1L, Math.subtractExact(stop, start) / step))
    }
    withResource(ColumnVector.fromBoxedLongs(starts: _*)) { start =>
      withResource(ColumnVector.fromBoxedLongs(stops: _*)) { stop =>
        withResource(ColumnVector.fromBoxedLongs(steps: _*)) { step =>
          withResource(ColumnVector.fromBoxedLongs(expected: _*)) { cpu =>
            withResource(GetSequenceSize(start, stop, step)) { actual =>
              CudfTestHelper.assertColumnsAreEqual(cpu, actual)
            }
          }
        }
      }
    }
  }

  test("sequence overflow releases temporary device allocations") {
    val cases = Seq(
      (0L, Long.MinValue, -1L),
      (0L, Long.MaxValue, 1L),
      (Long.MinValue, Long.MaxValue, 1L))
    for ((first, last, increment) <- cases) {
      withResource(ColumnVector.fromLongs(first)) { start =>
        withResource(ColumnVector.fromLongs(last)) { stop =>
          withResource(ColumnVector.fromLongs(increment)) { step =>
            val before = Rmm.getTotalBytesAllocated
            intercept[ArithmeticException] {
              withResource(GetSequenceSize(start, stop, step)) { _ =>
                fail("sequence size should overflow")
              }
            }
            assert(Rmm.getTotalBytesAllocated == before)
          }
        }
      }
    }
  }
}
