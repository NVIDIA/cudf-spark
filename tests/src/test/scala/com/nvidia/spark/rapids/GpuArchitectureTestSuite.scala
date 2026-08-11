/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsPluginUtils.validateGpuArchitectureInternal
import org.scalatest.funsuite.AnyFunSuite

class GpuArchitectureTestSuite extends AnyFunSuite {
  test("test supported architecture") {
    val jniSupportedGpuArchs = Set(50, 60, 70)
    val cudfSupportedGpuArchs = Set(50, 60, 65, 70)
    val gpuArch = 60
    validateGpuArchitectureInternal(gpuArch, jniSupportedGpuArchs, cudfSupportedGpuArchs)
  }

  test("test unsupported architecture") {
    val jniSupportedGpuArchs = Set(50, 60, 70)
    val cudfSupportedGpuArchs = Set(50, 60, 65, 70)
    val gpuArch = 40
    val exception = intercept[RuntimeException] {
      validateGpuArchitectureInternal(gpuArch, jniSupportedGpuArchs, cudfSupportedGpuArchs)
    }
    assert(exception.getMessage.contains(s"Device architecture $gpuArch is unsupported"))
  }

  test("test supported major architecture with higher minor version") {
    val jniSupportedGpuArchs = Set(50, 60, 65, 70)
    val cudfSupportedGpuArchs = Set(50, 60, 65, 70)
    val gpuArch = 67
    validateGpuArchitectureInternal(gpuArch, jniSupportedGpuArchs, cudfSupportedGpuArchs)
  }

  test("test supported major architecture with lower minor version") {
    val jniSupportedGpuArchs = Set(50, 60, 65, 70)
    val cudfSupportedGpuArchs = Set(50, 60, 65, 70)
    val gpuArch = 63
    validateGpuArchitectureInternal(gpuArch, jniSupportedGpuArchs, cudfSupportedGpuArchs)
  }

  test("test empty supported architecture set") {
    val jniSupportedGpuArchs = Set(50, 60)
    val cudfSupportedGpuArchs = Set(70, 80)
    val gpuArch = 60
    val exception = intercept[IllegalStateException] {
      validateGpuArchitectureInternal(gpuArch, jniSupportedGpuArchs, cudfSupportedGpuArchs)
    }
    assert(exception.getMessage.contains(
      s"Compatibility check failed for GPU architecture $gpuArch"))
  }
}
