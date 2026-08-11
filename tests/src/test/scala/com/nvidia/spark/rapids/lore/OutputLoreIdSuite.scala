/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import org.scalatest.funsuite.AnyFunSuite

class OutputLoreIdSuite extends AnyFunSuite {
  test("Parse one output lore id") {
    val expectedLoreIds = Map(1 -> OutputLoreId(1, Set(1, 2, 4, 8)))
    val loreIds = OutputLoreId.parse("1[1 2 4 8]")

    assert(loreIds == expectedLoreIds)
  }

  test("Parse multi output lore id") {
    val expectedLoreIds = Map(
      1 -> OutputLoreId(1, Set(1, 2, 4, 8)),
      2 -> OutputLoreId(2, Set(1, 4, 5, 6, 7, 8, 100))
    )
    val loreIds = OutputLoreId.parse("1[1 2 4 8], 2[1 4-9 100]")

    assert(loreIds == expectedLoreIds)
  }

  test("Parse empty output lore id should fail") {
    assertThrows[IllegalArgumentException] {
      OutputLoreId.parse(" 1, 2 ")
    }
  }

  test("Parse mixed") {
    val expectedLoreIds = Map(
      1 -> OutputLoreId(1),
      2 -> OutputLoreId(2, Set(4, 5, 8)),
      3 -> OutputLoreId(3, Set(1, 2, 4, 8))
    )
    val loreIds = OutputLoreId.parse("1[*], 2[4-6 8] , 3[1 2 4 8]")

    assert(loreIds == expectedLoreIds)
  }
}
