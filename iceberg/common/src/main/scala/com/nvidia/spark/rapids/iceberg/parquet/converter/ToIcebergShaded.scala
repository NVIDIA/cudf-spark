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

package com.nvidia.spark.rapids.iceberg.parquet.converter

import java.nio.ByteBuffer

import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile => ShadedInputFile, SeekableInputStream => ShadedSeekableInputStream}
import org.apache.parquet.io.{InputFile, SeekableInputStream}

/**
 * Adapters from the non-shaded parquet `InputFile` / `SeekableInputStream` to their shaded
 * equivalents under `org.apache.iceberg.shaded.org.apache.parquet.io`.
 */
object ToIcebergShaded {
  def shade(inputFile: InputFile): ShadedInputFile = {
    if (inputFile == null) {
      return null
    }
    new ShadedInputFile {
      override def getLength: Long = inputFile.getLength
      override def newStream(): ShadedSeekableInputStream = shade(inputFile.newStream())
      override def toString: String = inputFile.toString
    }
  }

  def shade(inputStream: SeekableInputStream): ShadedSeekableInputStream = {
    if (inputStream == null) {
      return null
    }
    new ShadedSeekableInputStream {
      override def getPos: Long = inputStream.getPos
      override def seek(newPos: Long): Unit = inputStream.seek(newPos)
      override def read(): Int = inputStream.read()
      override def read(bytes: Array[Byte], start: Int, len: Int): Int =
        inputStream.read(bytes, start, len)
      override def readFully(bytes: Array[Byte]): Unit = inputStream.readFully(bytes)
      override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit =
        inputStream.readFully(bytes, start, len)
      override def read(buf: ByteBuffer): Int = inputStream.read(buf)
      override def readFully(buf: ByteBuffer): Unit = inputStream.readFully(buf)
      override def close(): Unit = inputStream.close()
    }
  }
}
