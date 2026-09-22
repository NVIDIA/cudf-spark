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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm._

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
    //   size = if (start == stop) 1L else 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
    //   require(size <= MAX_ROUNDED_ARRAY_LENGTH,
    //       s"Too long sequence: $len. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
    //   size.toInt
    // ```
    val diff = withResource(stop.castTo(DType.INT64)) { stopAsLong =>
      withResource(start.castTo(DType.INT64)) { startAsLong =>
        stopAsLong.sub(startAsLong)
      }
    }
    val quotient = withResource(diff) { _ =>
      withResource(step.castTo(DType.INT64)) { stepAsLong =>
        diff.div(stepAsLong)
      }
    }
    // actualSize = 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
    val actualSize = withResource(Scalar.fromLong(1L)) { one =>
      withResource(quotient) { quotient =>
        quotient.add(one, DType.INT64)
      }
    }
    actualSize
  }
}