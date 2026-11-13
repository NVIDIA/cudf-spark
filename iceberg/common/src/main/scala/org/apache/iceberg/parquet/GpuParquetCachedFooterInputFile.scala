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

package org.apache.iceberg.parquet

import java.io.IOException
import java.util.Objects

import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile, SeekableInputStream => ShadedInputStream}

/** Presents a cached mini Parquet footer at its original offsets in the full file. */
private[parquet] class GpuParquetCachedFooterInputFile(
    inputFile: RapidsInputFile,
    footer: Array[Byte]) extends InputFile {
  private val fileLength = inputFile.getLength
  // The cached buffer starts with a synthetic PAR1 header that is not part of the file's tail.
  private val footerStart = fileLength - footer.length + MAGIC.length
  require(footer.length >= 2 * MAGIC.length + 4 && footerStart >= MAGIC.length,
    s"Invalid cached footer for ${inputFile.path()}")

  override def getLength: Long = fileLength
  override def toString: String = inputFile.path()

  override def newStream(): ShadedInputStream = GpuParquetIO.stream(new SeekableInputStream {
    private var position = 0L
    private var body: SeekableInputStream = _
    private var closed = false
    private val singleByte = new Array[Byte](1)

    private def ensureOpen(): Unit = {
      if (closed) {
        throw new IOException("Stream is closed")
      }
    }

    override def getPos: Long = {
      ensureOpen()
      position
    }

    override def seek(newPos: Long): Unit = {
      ensureOpen()
      if (newPos < 0) {
        throw new IOException(s"Cannot seek to a negative position: $newPos")
      }
      position = newPos
    }

    override def read(): Int = {
      if (read(singleByte, 0, 1) < 0) -1 else singleByte(0) & 0xff
    }

    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
      Objects.requireNonNull(bytes, "bytes")
      if (offset < 0 || length < 0 || offset > bytes.length - length) {
        throw new IndexOutOfBoundsException()
      }
      ensureOpen()
      if (length == 0) {
        return 0
      }
      if (position >= fileLength) {
        return -1
      }
      val count = if (position >= footerStart) {
        val size = math.min(length, fileLength - position).toInt
        System.arraycopy(footer, (position - footerStart + MAGIC.length).toInt,
          bytes, offset, size)
        size
      } else {
        if (body == null) {
          body = inputFile.open()
        }
        body.seek(position)
        body.read(bytes, offset, math.min(length, footerStart - position).toInt)
      }
      if (count > 0) {
        position += count
      }
      count
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        if (body != null) {
          body.close()
        }
      }
    }
  })
}
