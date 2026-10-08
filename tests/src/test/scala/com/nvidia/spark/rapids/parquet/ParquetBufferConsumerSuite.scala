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

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import org.mockito.Mockito.{mock, verify}
import org.scalatest.funsuite.AnyFunSuite

class ParquetBufferConsumerSuite extends AnyFunSuite {

  test("closes every received buffer when their total is too large for one array") {
    // The lengths are only reported, so the mocked buffers hold no memory.
    val buffers = Seq.fill(3)(mock(classOf[HostMemoryBuffer]))
    withResource(new ParquetBufferConsumer(numRows = 1)) { consumer =>
      buffers.foreach(consumer.handleBuffer(_, 1L << 30))
      intercept[AssertionError](consumer.getBuffer)
    }
    buffers.foreach(buffer => verify(buffer).close())
  }
}
