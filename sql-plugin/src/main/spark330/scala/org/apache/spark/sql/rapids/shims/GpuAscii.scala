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

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val emptyMask = withResource(Scalar.fromString("")) { emptyScalar =>
      input.getBase.equalTo(emptyScalar)
    }
    val emptyReplaced = withResource(emptyMask) { _ =>
      // replace empty strings with 'NUL' (which will convert to ascii 0)
      withResource(Scalar.fromString('\u0000'.toString)) { zeroScalar =>
        emptyMask.ifElse(zeroScalar, input.getBase)
      }
    }
    // convert to byte lists
    val byteLists = withResource(emptyReplaced) { _ =>
      emptyReplaced.asByteList()
    }
    val firstBytes = withResource(byteLists) { bytes =>
      bytes.extractListElement(0)
    }
    val firstBytesInt = withResource(firstBytes) { _ =>
      firstBytes.castTo(DType.INT32)
    }
    withResource(firstBytesInt) { _ =>
      val greaterThan127 = withResource(Scalar.fromInt(127)) { scalar =>
        firstBytesInt.greaterThan(scalar)
      }
      withResource(greaterThan127) { _ =>
        val sub256 = withResource(Scalar.fromInt(256)) { scalar =>
          firstBytesInt.sub(scalar)
        }
        withResource(sub256) { _ =>
          greaterThan127.ifElse(sub256, firstBytesInt)
        }
      }
    }
  }
}
