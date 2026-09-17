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

package org.apache.spark.sql.rapids.execution.python

import java.io.{ByteArrayInputStream, DataInputStream}

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

class GpuArrowReaderSuite extends AnyFunSuite {

  test("StreamToBufferProvider tracks bytes read across requests") {
    val inputBytes = Array.tabulate[Byte](16)(_.toByte)
    val stream = new DataInputStream(new ByteArrayInputStream(inputBytes))
    val provider = new StreamToBufferProvider(stream)

    withResource(HostMemoryBuffer.allocate(20)) { hostBuffer =>
      assertResult(7L)(provider.readInto(hostBuffer, 7))
      assertResult(7L)(provider.totalBytesRead)

      assertResult(9L)(provider.readInto(hostBuffer, 20))
      assertResult(16L)(provider.totalBytesRead)

      assertResult(0L)(provider.readInto(hostBuffer, 1))
      assertResult(16L)(provider.totalBytesRead)
    }
  }
}
