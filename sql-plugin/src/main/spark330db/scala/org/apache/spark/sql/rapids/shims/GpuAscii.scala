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
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
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
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class GpuAscii(child: Expression) extends GpuUnaryExpression with ImplicitCastInputTypes 
    with NullIntolerantShim {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  private def utf8CodePointsToAscii(codePoints: ColumnVector): ColumnVector = {
    // Currently we only support ASCII characters, so we need to convert the UTF8 code points
    // to ASCII code points. Results for code points outside range [0, 255] are undefined.
    // seg A: 0 <= codePoints < 128, already ASCII
    // seg B: 49792 <= codePoints < 49856, ASCII = codePoints - 49664
    // seg C: 50048 <= codePoints < 50112, ASCII = codePoints - 49856
    //
    // To reduce cuDF API calling, following algorithm will be performed:
    // 1. For anything above 49792, we subtract 49664, now seg A and B are correct.
    // 2. seg C: 50048 <= current + 49664 < 50112 => 384 <= current < 448, ASCII = current - 192
    // So for anything above 384, we subtract 192, now seg C is correct too.
    val greaterThan49792 = withResource(Scalar.fromInt(49792)) { segBLeftEnd =>
      codePoints.greaterOrEqualTo(segBLeftEnd)
    }
    val segAB = withResource(greaterThan49792) { _ =>
      val sub1 = withResource(Scalar.fromInt(49664)) { segBValue =>
        codePoints.sub(segBValue)
      }
      withResource(sub1) { _ =>
        greaterThan49792.ifElse(sub1, codePoints)
      }
    }
    withResource(segAB) { _ =>
      val geraterThan384 = withResource(Scalar.fromInt(384)) { segCLeftEnd =>
        segAB.greaterOrEqualTo(segCLeftEnd)
      }
      withResource(geraterThan384) { _ =>
        val sub2 = withResource(Scalar.fromInt(192)) { segCValue =>
          segAB.sub(segCValue)
        }
        withResource(sub2) { _ =>
          geraterThan384.ifElse(sub2, segAB)
        }
      }
    }
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // replace empty strings with 'NUL' (which will convert to ascii 0)
    val emptyMask = withResource(Scalar.fromString("")) { emptyScalar =>
      input.getBase.equalTo(emptyScalar)
    }
    val emptyReplaced = withResource(emptyMask) { _ =>
      // replace empty strings with 'NUL' (which will convert to ascii 0)
      withResource(Scalar.fromString('\u0000'.toString)) { zeroScalar =>
        emptyMask.ifElse(zeroScalar, input.getBase)
      }
    }
    // replace nulls with 'n' and save the null mask
    val nullMask = closeOnExcept(emptyReplaced) { _ =>
      input.getBase.isNull
    }
    withResource(nullMask) { _ =>
      val nullsReplaced = withResource(emptyReplaced) { _ =>
        withResource(Scalar.fromString("n")) { nullScalar =>
          nullMask.ifElse(nullScalar, emptyReplaced)
        }
      }
      val substr = withResource(nullsReplaced) { _ =>
        nullsReplaced.substring(0, 1)
      }
      val codePoints = withResource(substr) { _ =>
        substr.codePoints()
      }
      val segABC = withResource(codePoints) { _ =>
        utf8CodePointsToAscii(codePoints)
      }
      // replace nulls with null
      withResource(segABC) { _ =>
        withResource(Scalar.fromNull(DType.INT32)) { nullScalar =>
          nullMask.ifElse(nullScalar, segABC)
        }
      }
    }
  }
}
