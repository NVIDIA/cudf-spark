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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class MultiSegmentFileRegionSuite extends AnyFunSuite {

  test("transferTo returns zero when the target channel is not writable") {
    val data = "shuffle-data".getBytes("UTF-8")
    val handle = mock[SpillablePartialFileHandle]
    when(handle.readAt(anyLong(), any[Array[Byte]](), anyInt(), anyInt()))
      .thenAnswer(invocation => {
        val position = invocation.getArgument[Long](0).toInt
        val target = invocation.getArgument[Array[Byte]](1)
        val offset = invocation.getArgument[Int](2)
        val length = invocation.getArgument[Int](3)
        val toCopy = math.min(length, data.length - position)
        if (toCopy > 0) {
          System.arraycopy(data, position, target, offset, toCopy)
          toCopy
        } else {
          -1
        }
      })

    val region = new MultiSegmentFileRegion(
      Seq(PartitionSegment(handle, 0, data.length)), Seq(handle))
    val channel = new ZeroThenWriteChannel

    try {
      assert(region.transferTo(channel, 0) === 0)
      assert(region.transferred() === 0)
      assert(channel.writeCalls === 1)

      assert(region.transferTo(channel, 0) === data.length)
      assert(region.transferred() === data.length)
      assert(channel.bytes === data)
    } finally {
      assert(region.release())
    }

    verify(handle).acquireRead()
    verify(handle).releaseRead()
  }

  private final class ZeroThenWriteChannel extends WritableByteChannel {
    private val output = new ByteArrayOutputStream()
    private var open = true
    var writeCalls: Int = 0

    override def write(src: ByteBuffer): Int = {
      writeCalls += 1
      if (writeCalls == 1) {
        0
      } else {
        val bytes = new Array[Byte](src.remaining())
        src.get(bytes)
        output.write(bytes)
        bytes.length
      }
    }

    override def isOpen: Boolean = open

    override def close(): Unit = {
      open = false
    }

    def bytes: Array[Byte] = output.toByteArray
  }
}
