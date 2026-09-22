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

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * An input stream that reads bytes from a byte buffer.
 *
 * Migrated from Spark org.apache.spark.util.ByteBufferInputStream and adjusted.
 */
class ByteBufferInputStream(private var buffer: ByteBuffer)
    extends InputStream {

  override def read(): Int = {
    if (buffer == null || !buffer.hasRemaining()) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (dest == null) {
      throw new NullPointerException("dest")
    } else if (offset < 0 || length < 0 || length > dest.length - offset) {
      throw new IndexOutOfBoundsException
    } else if (length == 0) {
      0
    } else if (buffer == null || !buffer.hasRemaining()) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null && bytes > 0) {
      val amountToSkip = math.min(bytes, buffer.remaining().toLong).toInt
      buffer.position(buffer.position() + amountToSkip)
      if (!buffer.hasRemaining()) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * Clean up the buffer.
   */
  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}
