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

package com.nvidia.spark.rapids.spill

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, WritableByteChannel}
import java.util.Objects

import com.nvidia.spark.rapids.ByteBufferInputStream
import org.apache.commons.io.IOUtils

// Closing the codec from a task interruption callback can re-enter a stream whose write is active.
private[spill] final class SpillOutputChannel(out: OutputStream) extends WritableByteChannel {
  private val operationLock = new Object
  private lazy val stagingBuffer = new Array[Byte](IOUtils.DEFAULT_BUFFER_SIZE)
  @volatile private var open = true

  override def write(src: ByteBuffer): Int = operationLock.synchronized {
    if (!open) {
      throw new ClosedChannelException
    }
    IOUtils.copyLarge(new ByteBufferInputStream(Objects.requireNonNull(src, "src")),
      out, stagingBuffer).toInt
  }

  override def isOpen: Boolean = open

  override def close(): Unit = operationLock.synchronized {
    if (open) {
      open = false
      out.close()
    }
  }
}
