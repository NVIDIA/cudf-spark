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
{"spark":["405"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.GetSequenceSize

import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

class GetSequenceSizeSuite extends RmmSparkRetrySuiteBase {
  test("sequence sizes preserve descending ranges, long boundaries and nulls") {
    val starts = Array[java.lang.Long](0L, 4L, Long.MinValue, null)
    val stops = Array[java.lang.Long](4L, -4L, Long.MinValue, 4L)
    val steps = Array[java.lang.Long](2L, -2L, 1L, 1L)
    val expected = starts.zip(stops).zip(steps).map { case ((start, stop), step) =>
      if (start == null || stop == null || step == null) {
        null
      } else {
        java.lang.Long.valueOf(Math.addExact(1L, Math.subtractExact(stop, start) / step))
      }
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

  test("sequence sizes use exact arithmetic after long subtraction overflow") {
    val starts = Array[java.lang.Long](Long.MinValue, Long.MinValue, Long.MaxValue, null)
    val stops = Array[java.lang.Long](Long.MaxValue, 0L, Long.MinValue, 0L)
    val steps = Array[java.lang.Long](
      Long.MaxValue, 4611686018427387904L, -Long.MaxValue, 1L)
    val expected = Array[java.lang.Long](3L, 3L, 3L, null)

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

  test("oversized exact sequence lengths remain above the array limit without leaking") {
    val cases = Seq(
      (0L, Long.MinValue, -1L),
      (0L, Long.MaxValue, 1L),
      (Long.MinValue, Long.MaxValue, 1L))
    for ((first, last, increment) <- cases) {
      withResource(ColumnVector.fromLongs(first)) { start =>
        withResource(ColumnVector.fromLongs(last)) { stop =>
          withResource(ColumnVector.fromLongs(increment)) { step =>
            val before = Rmm.getTotalBytesAllocated
            withResource(GetSequenceSize(start, stop, step)) { actual =>
              withResource(
                  ColumnVector.fromLongs(MAX_ROUNDED_ARRAY_LENGTH.toLong + 1L)) { expected =>
                CudfTestHelper.assertColumnsAreEqual(expected, actual)
              }
            }
            assert(Rmm.getTotalBytesAllocated == before)
          }
        }
      }
    }
  }
}
