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
{"spark":["405","414","421"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.math.BigInteger

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm._

import org.apache.spark.sql.rapids.{AddOverflowChecks, SubtractOverflowChecks}
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

object GetSequenceSize {
  private val decimal128ScaleZero = DType.create(DType.DTypeEnum.DECIMAL128, 0)

  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) containing a null element produces a null output.
   *
   * The returned column should be closed.
   */
  def apply(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    try {
      computeWithLongs(start, stop, step)
    } catch {
      case _: ArithmeticException => computeExactly(start, stop, step)
    }
  }

  private def computeWithLongs(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    val delta = withResource(stop.castTo(DType.INT64)) { stopAsLong =>
      withResource(start.castTo(DType.INT64)) { startAsLong =>
        closeOnExcept(stopAsLong.sub(startAsLong)) { ret =>
          SubtractOverflowChecks.basicOpOverflowCheck(stopAsLong, startAsLong, ret)
          ret
        }
      }
    }
    closeOnExcept(delta) { _ =>
      if (hasLongMinAndStepMinusOne(delta, step)) {
        throw new ArithmeticException("Long overflow (Long.MinValue / -1)")
      }
    }
    val quotient = withResource(delta) { _ =>
      withResource(step.castTo(DType.INT64)) { stepAsLong =>
        delta.div(stepAsLong)
      }
    }
    withResource(Scalar.fromLong(1L)) { one =>
      withResource(quotient) { _ =>
        closeOnExcept(quotient.add(one, DType.INT64)) { ret =>
          AddOverflowChecks.basicOpOverflowCheck(quotient, one, ret)
          ret
        }
      }
    }
  }

  private def hasLongMinAndStepMinusOne(
      delta: ColumnVector,
      step: ColumnVector): Boolean = {
    val hasLongMin = withResource(Scalar.fromLong(Long.MinValue)) { longMin =>
      delta.equalTo(longMin)
    }
    val overflow = withResource(hasLongMin) { hasLongMin =>
      andStepEqualsMinusOne(hasLongMin, step)
    }
    withResource(overflow) { overflow =>
      withResource(overflow.any()) { result =>
        result.isValid && result.getBoolean
      }
    }
  }

  private def andStepEqualsMinusOne(
      hasLongMin: ColumnVector,
      step: ColumnVector): ColumnVector = {
    val stepEqualsMinusOne = withResource(Scalar.fromInt(-1)) { minusOne =>
      step.equalTo(minusOne)
    }
    withResource(stepEqualsMinusOne) { stepEqualsMinusOne =>
      hasLongMin.and(stepEqualsMinusOne)
    }
  }

  private def computeExactly(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    val delta = withResource(stop.castTo(decimal128ScaleZero)) { stopWide =>
      withResource(start.castTo(decimal128ScaleZero)) { startWide =>
        stopWide.sub(startWide)
      }
    }
    val quotient = withResource(delta) { _ =>
      withResource(step.castTo(decimal128ScaleZero)) { stepWide =>
        delta.div(stepWide, decimal128ScaleZero)
      }
    }
    val exactSize = withResource(quotient) { _ =>
      withResource(Scalar.fromDecimal(0, BigInteger.ONE)) { one =>
        quotient.add(one, decimal128ScaleZero)
      }
    }

    // Values beyond Spark's maximum array length only need to remain recognizably too large.
    // Clamp them before casting back to INT64 so lengths above Long.MaxValue cannot wrap.
    val boundedSize = withResource(exactSize) { exactSize =>
      clampToArrayLimit(exactSize)
    }
    withResource(boundedSize) { boundedSize =>
      boundedSize.castTo(DType.INT64)
    }
  }

  private def clampToArrayLimit(exactSize: ColumnVector): ColumnVector = {
    val tooLong = withResource(Scalar.fromDecimal(
        0, BigInteger.valueOf(MAX_ROUNDED_ARRAY_LENGTH.toLong))) { maxLength =>
      exactSize.greaterThan(maxLength)
    }
    withResource(tooLong) { tooLong =>
      withResource(Scalar.fromDecimal(
          0, BigInteger.valueOf(MAX_ROUNDED_ARRAY_LENGTH.toLong + 1L))) { overflowLength =>
        tooLong.ifElse(overflowLength, exactSize)
      }
    }
  }
}
