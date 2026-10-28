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

import java.math.RoundingMode

import ai.rapids.cudf.{ColumnVector, DType, Rmm}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.rapids.shims.IntervalUtils

class GpuIntervalResourceSuite extends RmmSparkRetrySuiteBase {
  test("rounding preserves half-up and long boundaries with nulls") {
    val values = Array[java.lang.Double](
      -1.5d, -0.5d, 0.0d, 0.5d, 1.5d, Long.MinValue.toDouble,
      Math.nextDown(Long.MaxValue.toDouble), null)
    val expected = Array[java.lang.Long](
      -2L, -1L, 0L, 1L, 2L, Long.MinValue, 9223372036854774784L, null)
    withResource(ColumnVector.fromBoxedDoubles(values: _*)) { input =>
      withResource(ColumnVector.fromBoxedLongs(expected: _*)) { cpu =>
        withResource(IntervalUtils.roundToLongWithCheck(input)) { actual =>
          CudfTestHelper.assertColumnsAreEqual(cpu, actual)
        }
      }
    }
  }

  test("rounding overflow releases temporary device allocations") {
    for (value <- Seq(Math.nextDown(Long.MinValue.toDouble), Long.MaxValue.toDouble)) {
      intercept[ArithmeticException] {
        java.math.BigDecimal.valueOf(value).setScale(0, RoundingMode.HALF_UP).longValueExact()
      }
      withResource(ColumnVector.fromDoubles(value)) { input =>
        val before = Rmm.getTotalBytesAllocated
        intercept[ArithmeticException] {
          withResource(IntervalUtils.roundToLongWithCheck(input)) { _ =>
            fail("rounding should overflow")
          }
        }
        assert(Rmm.getTotalBytesAllocated == before)
      }
    }
  }

  test("integral division preserves signed half-up results and nulls") {
    val numerators = Array[java.lang.Long](5L, -5L, 5L, -5L, Long.MinValue, null)
    val divisors = Array[java.lang.Long](2L, 2L, -2L, -2L, 2L, 2L)
    val expected = numerators.zip(divisors).map { case (p, q) =>
      if (p == null || q == null) null else java.lang.Long.valueOf(
        java.math.BigDecimal.valueOf(p).divide(java.math.BigDecimal.valueOf(q),
          0, RoundingMode.HALF_UP).longValueExact())
    }
    withResource(ColumnVector.fromBoxedLongs(numerators: _*)) { left =>
      withResource(ColumnVector.fromBoxedLongs(divisors: _*)) { right =>
        withResource(ColumnVector.fromBoxedLongs(expected: _*)) { cpu =>
          withResource(IntervalUtils.divWithHalfUpModeWithOverflowCheck(left, right)) { result =>
            withResource(result.castTo(DType.INT64)) { actual =>
              CudfTestHelper.assertColumnsAreEqual(cpu, actual)
            }
          }
        }
      }
    }
  }

  test("integral division overflow releases temporary device allocations") {
    for (divisor <- Seq(-1L, 0L)) {
      withResource(ColumnVector.fromLongs(Long.MinValue)) { left =>
        withResource(ColumnVector.fromLongs(divisor)) { right =>
          val before = Rmm.getTotalBytesAllocated
          intercept[ArithmeticException] {
            withResource(IntervalUtils.divWithHalfUpModeWithOverflowCheck(left, right)) { _ =>
              fail("division should overflow")
            }
          }
          assert(Rmm.getTotalBytesAllocated == before)
        }
      }
    }
  }
}
