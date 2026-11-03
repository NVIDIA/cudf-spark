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
{"spark": "404"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{FQSuiteName, GpuLiteral}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{IntegerType, StringType}

class GpuCreateNamedStructSuite extends AnyFunSuite with FQSuiteName {
  test("dataType is null-safe for a null field name") {
    val struct = GpuCreateNamedStruct(Seq(
      GpuLiteral.create(null, StringType),
      GpuLiteral.create(1, IntegerType)))
    val dataType = struct.dataType

    assert(dataType.length === 1)
    assert(dataType.head.name === null)
    assert(dataType.head.dataType === IntegerType)
    assert(dataType.head.nullable === false)
    assert(struct.checkInputDataTypes().isFailure)
  }
}
