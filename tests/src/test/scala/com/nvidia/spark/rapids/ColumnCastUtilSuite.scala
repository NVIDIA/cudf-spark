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

import java.util.Arrays

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StructField, StructType}

class ColumnCastUtilSuite extends AnyFunSuite {
  private val normalizeDoubles:
      PartialFunction[(ColumnView, Option[DataType]), ColumnView] = {
    case (cv, Some(DoubleType)) => cv.normalizeNANsAndZeros()
  }

  private def assertWrapperViewIsTracked(input: ColumnVector, dataType: DataType): Unit = {
    val (updated, needsClosing) =
      ColumnCastUtil.deepTransformView(input, Some(dataType))(normalizeDoubles)
    assert(updated.nonEmpty)

    val wrapper = updated.get
    withResource(needsClosing.filterNot(_ eq wrapper)) { _ =>
      withResource(wrapper) { _ =>
        assert(needsClosing.exists(_ eq wrapper))
      }
    }
  }

  test("deepTransformView tracks rebuilt list wrapper") {
    val hostType = new HostColumnVector.ListType(
      false, new HostColumnVector.BasicType(false, DType.FLOAT64))
    val values = Arrays.asList(Double.box(-0.0), Double.box(1.0))
    withResource(ColumnVector.fromLists(hostType, values)) { input =>
      assertWrapperViewIsTracked(input, ArrayType(DoubleType, containsNull = false))
    }
  }

  test("deepTransformView tracks rebuilt struct wrapper") {
    val hostType = new HostColumnVector.StructType(
      false, new HostColumnVector.BasicType(false, DType.FLOAT64))
    val values = new HostColumnVector.StructData(Double.box(-0.0))
    withResource(ColumnVector.fromStructs(hostType, values)) { input =>
      assertWrapperViewIsTracked(
        input, StructType(Seq(StructField("value", DoubleType, nullable = false))))
    }
  }
}
