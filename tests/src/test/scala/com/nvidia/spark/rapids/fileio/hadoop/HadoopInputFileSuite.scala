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

package com.nvidia.spark.rapids.fileio.hadoop

import java.util.{Arrays, Collections}

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.scalatest.funsuite.AnyFunSuite

class HadoopInputFileSuite extends AnyFunSuite {
  test("copy buffer allocation is capped by the largest range") {
    val allocationSize = HadoopInputFile.getCopyBufferAllocationSize(
      Arrays.asList(new CopyRange(0, 1024, 0), new CopyRange(4096, 2048, 1024)),
      8 * 1024 * 1024)

    assertResult(2048)(allocationSize)
  }

  test("configured copy buffer size still limits large ranges") {
    val allocationSize = HadoopInputFile.getCopyBufferAllocationSize(
      Collections.singletonList(new CopyRange(0, 16 * 1024 * 1024, 0)),
      8 * 1024 * 1024)

    assertResult(8 * 1024 * 1024)(allocationSize)
  }
}
