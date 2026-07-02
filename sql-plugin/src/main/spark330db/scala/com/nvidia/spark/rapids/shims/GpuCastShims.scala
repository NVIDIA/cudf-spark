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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.math.BigInteger

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.CastStrings

object GpuCastShims {
  def makeZeroScalar(t: DType): Scalar = t.getTypeId match {
    case DType.DTypeEnum.DECIMAL32 => Scalar.fromDecimal(t.getScale, 0)
    case DType.DTypeEnum.DECIMAL64 => Scalar.fromDecimal(t.getScale, 0L)
    case DType.DTypeEnum.DECIMAL128 => Scalar.fromDecimal(t.getScale, BigInteger.ZERO)
    case _ => throw new IllegalArgumentException(s"Unsupported type in cast $t")
  }

  def CastDecimalToString(decimalInput: ColumnView, usePlainString: Boolean): ColumnVector = {
    if (usePlainString) {
      // This is equivalent to
      // https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html#toPlainString--
      // except there are a few corner cases, but they are really rare
      val t = decimalInput.getType
      if (t.getScale > 0) {
        val isZero = withResource(makeZeroScalar(t)) { zero =>
          zero.equalTo(decimalInput)
        }
        withResource(isZero) { isZero =>
          withResource(decimalInput.castTo(DType.STRING)) { decStr =>
            withResource(Scalar.fromString("0")) { zeroStr =>
              isZero.ifElse(zeroStr, decStr)
            }
          }
        }
      } else {
        decimalInput.castTo(DType.STRING)
      }
    } else {
      // This is equivalent to
      // https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html#toString--
      CastStrings.fromDecimal(decimalInput)
    }
  }
}
