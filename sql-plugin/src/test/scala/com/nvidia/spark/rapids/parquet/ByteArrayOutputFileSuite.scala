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

package com.nvidia.spark.rapids.parquet

import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.funsuite.AnyFunSuite

class ByteArrayOutputFileSuite extends AnyFunSuite {
  test("single-byte writes advance the position by one") {
    val stream = new ByteArrayOutputStream()
    val output = new ByteArrayOutputFile(stream).create(0L)

    output.write(42)

    assertResult(1L)(output.getPos)
  }
}
