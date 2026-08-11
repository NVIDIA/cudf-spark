/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import com.nvidia.spark.rapids.shuffle.ucx.UCXConnection._
import org.scalatest.funsuite.AnyFunSuite

class UCXConnectionSuite extends AnyFunSuite {
  test("generate active message id") {
    Seq(0, 1, 2, 100000, Int.MaxValue).foreach { eId =>
      assertResult(eId)(
        extractExecutorId(composeRequestHeader(eId, 123L)))
    }
  }

  test("negative executor ids are invalid") {
    Seq(-1, -1 * Int.MaxValue).foreach { eId =>
      assertThrows[IllegalArgumentException](
        extractExecutorId(composeRequestHeader(eId, 123L)))
    }
  }

  test("executor id longer that doesn't fit in an int is invalid") {
    assertThrows[IllegalArgumentException](composeRequestHeader(Long.MaxValue, 123L))
  }

  test("transaction ids can rollover") {
    assertResult(0)(composeRequestHeader(0,  0x0000000100000000L))
    assertResult(10)(composeRequestHeader(0, 0x000000010000000AL))
  }
}
