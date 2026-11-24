/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm._

import org.apache.spark.sql.rapids.{AddOverflowChecks, SubtractOverflowChecks}
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

object GetSequenceSize {
  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) contains at least one null element will produce
   * a null in the output.
   *
   * The returned column should be closed.
   */
  def apply(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {

    // Spark's algorithm to get the length (aka size)
    // ``` Scala
    //    val delta = Math.subtractExact(stop, start)
    //    if (delta == Long.MinValue && step == -1L) {
    //      // We must special-case division of Long.MinValue by -1 to catch potential unchecked
    //      // overflow in next operation. Division does not have a builtin overflow check. We
    //      // previously special-case div-by-zero.
    //      throw new ArithmeticException("Long overflow (Long.MinValue / -1)")
    //    }
    //    val len = if (stop == start) 1L else Math.addExact(1L, (delta / step))
    //    if (len > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
    //      throw QueryExecutionErrors.createArrayWithElementsExceedLimitError(len)
    //    }
    //    len.toInt
    // ```
    val delta = withResource(stop.castTo(DType.INT64)) { stopAsLong =>
      withResource(start.castTo(DType.INT64)) { startAsLong =>
        closeOnExcept(stopAsLong.sub(startAsLong)) { ret =>
          // Throw an exception if stop - start overflows
          SubtractOverflowChecks.basicOpOverflowCheck(stopAsLong, startAsLong, ret)
          ret
        }
      }
    }
    val quotient = withResource(delta) { _ =>
      val hasLongMin = withResource(Scalar.fromLong(Long.MinValue)) { longMin =>
        delta.equalTo(longMin)
      }
      val hasLongMinAndStepMinusOne = withResource(hasLongMin) { _ =>
        withResource(Scalar.fromInt(-1)) { minusOne =>
          withResource(step.equalTo(minusOne)) { stepEqualsMinusOne =>
            hasLongMin.and(stepEqualsMinusOne)
          }
        }
      }
      withResource(hasLongMinAndStepMinusOne) { _ =>
        withResource(hasLongMinAndStepMinusOne.any()) { result =>
          if (result.isValid && result.getBoolean) {
            // Overflow, throw an exception
            throw new ArithmeticException("Unsuccessful try to create array with " +
              s"elements exceeding the array size limit $MAX_ROUNDED_ARRAY_LENGTH")
          }
        }
      }
      withResource(step.castTo(DType.INT64)) { stepAsLong =>
        delta.div(stepAsLong)
      }
    }
    // delta = (stop.toLong - start.toLong) / estimatedStep.toLong
    // actualSize = 1L + delta
    val actualSize = withResource(quotient) { _ =>
      withResource(Scalar.fromLong(1L)) { one =>
        closeOnExcept(quotient.add(one, DType.INT64)) { ret =>
          AddOverflowChecks.basicOpOverflowCheck(quotient, one, ret)
          ret
        }
      }
    }
    actualSize
  }
}
