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

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.BinaryType

class HostColumnarToGpuSuite extends AnyFunSuite {
  test("binary columnar copy counts list offsets for row-limit estimates") {
    val values = Seq(
      Array[Byte](1, 2, 3),
      Array.emptyByteArray,
      Array[Byte](4, 5))

    val bytesCopied = copyBinaryColumn(values, nullable = false) { actual =>
      assertBinaryValues(values, actual)
    }

    assertResult(values.map(_.length + Integer.BYTES).sum)(bytesCopied)
  }

  test("binary columnar copy handles null and empty values") {
    val values = Seq(
      null,
      Array.emptyByteArray,
      Array[Byte](10, 11, 12),
      null,
      Array[Byte](13))

    copyBinaryColumn(values, nullable = true) { actual =>
      assertBinaryValues(values, actual)
    }
  }

  private def copyBinaryColumn(
      values: Seq[Array[Byte]],
      nullable: Boolean)(verify: RapidsHostColumnVector => Unit): Long = {
    withResource(new OnHeapColumnVector(values.length, BinaryType)) { cv =>
      values.zipWithIndex.foreach {
        case (null, index) => cv.putNull(index)
        case (value, index) => cv.putByteArray(index, value)
      }

      withResource(new RapidsHostColumnBuilder(
          GpuColumnVector.convertFrom(BinaryType, nullable), values.length)) { builder =>
        val bytesCopied = HostColumnarToGpu.columnarCopy(cv, builder, BinaryType, values.length)
        withResource(new RapidsHostColumnVector(BinaryType, builder.build())) { actual =>
          verify(actual)
        }
        bytesCopied
      }
    }
  }

  private def assertBinaryValues(
      expected: Seq[Array[Byte]],
      actual: RapidsHostColumnVector): Unit = {
    assertResult(expected.length)(actual.getBase.getRowCount)
    expected.zipWithIndex.foreach {
      case (null, index) =>
        assert(actual.isNullAt(index))
      case (value, index) =>
        assert(!actual.isNullAt(index))
        assert(actual.getBinary(index).sameElements(value))
    }
  }
}
