/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.hybrid

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmSparkRetrySuiteBase
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.jni.RmmSpark.OomInjectionType

class HostRetryAllocatorTest extends RmmSparkRetrySuiteBase {

  private val sizesInBytes = Seq(100L, 200L, 300L)
  private val allocator = new HostRetryAllocator {}
  private val tryPinned = false

  private def testAlloc(numOOMs: Int, start: Int = 1): Unit = {
    if (numOOMs > 0) {
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, numOOMs,
        OomInjectionType.CPU.ordinal(), start - 1)
    }
    withResource(allocator.allocWithRetry(sizesInBytes, tryPinned)) { bufs =>
      assertResult(sizesInBytes.length)(bufs.length)
      sizesInBytes.zip(bufs).foreach { case (expSize, bufInfo) =>
        assertResult(expSize)(bufInfo.buffer.getLength)
      }
    }
  }

  test("allocate with no OOM") {
    testAlloc(numOOMs = 0)
  }

  test("allocate when second one retry OOM") {
    testAlloc(numOOMs = 1, start = 2)
  }

  test("allocate when first and second ones retry OOM") {
    testAlloc(numOOMs = 2)
  }

  test("allocate when all retry OOM") {
    testAlloc(numOOMs = 3)
  }

}
