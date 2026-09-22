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

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

class HostByteBufferIteratorSuite extends AnyFunSuite {

  test("HostByteBufferIterator emits a ByteBuffer covering the whole host buffer") {
    withResource(HostMemoryBuffer.allocate(16)) { hmb =>
      val it = new HostByteBufferIterator(hmb)
      assert(it.hasNext)
      val bb = it.next()
      assertResult(16)(bb.remaining())
      assert(!it.hasNext)
    }
  }

  test("HostByteBufferIterator treats a null buffer as empty") {
    val it = new HostByteBufferIterator(null)
    assertResult(0L)(it.totalLength)
    assert(!it.hasNext)
  }
}
