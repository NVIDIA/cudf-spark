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

import java.io.IOException

import scala.collection.mutable

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.jni.fileio.RapidsHostBufferConsumer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ColumnarOutputWriterSuite extends AnyFunSuite with Matchers {
  private class ClosingConsumer(failAt: Int = -1) extends RapidsHostBufferConsumer {
    val lengths = mutable.ArrayBuffer[Long]()

    override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
      lengths += len
      buffer.close()
      if (lengths.size == failAt) {
        throw new IOException("injected consumer failure")
      }
    }

    override def close(): Unit = {}
    override def abort(): Unit = {}
  }

  test("direct host-buffer drain transfers every buffer in order") {
    val first = HostMemoryBuffer.allocate(3)
    val second = HostMemoryBuffer.allocate(5)
    val buffers = mutable.Queue((first, 3L), (second, 5L))
    val consumer = new ClosingConsumer()

    ColumnarOutputWriter.writeBufferedData(buffers, consumer)

    consumer.lengths.toSeq shouldEqual Seq(3L, 5L)
    buffers shouldBe empty
    first.getRefCount shouldEqual 0
    second.getRefCount shouldEqual 0
  }

  test("direct host-buffer drain closes unsubmitted buffers after failure") {
    val first = HostMemoryBuffer.allocate(3)
    val second = HostMemoryBuffer.allocate(5)
    val buffers = mutable.Queue((first, 3L), (second, 5L))
    val consumer = new ClosingConsumer(failAt = 1)

    intercept[IOException](ColumnarOutputWriter.writeBufferedData(buffers, consumer))

    consumer.lengths.toSeq shouldEqual Seq(3L)
    buffers shouldBe empty
    first.getRefCount shouldEqual 0
    second.getRefCount shouldEqual 0
  }
}
