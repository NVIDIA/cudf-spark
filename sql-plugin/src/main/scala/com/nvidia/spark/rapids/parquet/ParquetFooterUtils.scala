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

import java.nio.charset.StandardCharsets
import java.util.Arrays

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, NoopMetric, NvtxRegistry, RapidsConf}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC

/**
 * Shared helpers for reading and caching Parquet footer bytes.
 *
 * The buffer produced here is framed as `MAGIC + footer + footerLen + MAGIC`, i.e. a self-contained
 * mini Parquet tail that can be fed directly to `ParquetFileReader.readFooter` via an
 * `HMBInputFile`.
 */
object ParquetFooterUtils {
  val FooterLengthSize: Int = java.lang.Integer.BYTES
  private val ParquetMagicEncrypted = "PARE".getBytes(StandardCharsets.US_ASCII)

  def verifyParquetMagic(filePath: Path, magic: Array[Byte]): Unit = {
    if (!Arrays.equals(MAGIC, magic)) {
      if (Arrays.equals(ParquetMagicEncrypted, magic)) {
        throw new RuntimeException("The GPU does not support reading encrypted Parquet files. " +
          s"To read encrypted or columnar encrypted files, disable the GPU Parquet reader via " +
          s"${RapidsConf.ENABLE_PARQUET_READ.key}.")
      } else {
        throw new RuntimeException(s"$filePath is not a Parquet file. Expected magic number " +
          s"at tail ${Arrays.toString(MAGIC)} but found ${Arrays.toString(magic)}")
      }
    }
  }

  def readBytesFromBuffer(buffer: HostMemoryBuffer, offset: Long, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    buffer.getBytes(bytes, 0, offset, length)
    bytes
  }

  def footerIndex(
      filePath: Path,
      fileLen: Long,
      footerLength: Int,
      footerLengthSize: Int): Long = {
    val footerLengthIndex = fileLen - footerLengthSize - MAGIC.length
    val idx = footerLengthIndex - footerLength
    if (idx < MAGIC.length || idx >= footerLengthIndex) {
      throw new RuntimeException(s"corrupted file $filePath: the footer index is not within " +
        s"the file: $idx")
    }
    idx
  }

  /**
   * Read the Parquet footer bytes directly from a `RapidsInputFile` into a framed
   * `MAGIC + footer + footerLen + MAGIC` `HostMemoryBuffer`.
   */
  def readFooterBufferFromInputFile(
      inputFile: RapidsInputFile,
      filePath: Path): HostMemoryBuffer = {
    val fileLen = inputFile.getLength
    if (fileLen < MAGIC.length + FooterLengthSize + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    val footerLengthIndex = fileLen - FooterLengthSize - MAGIC.length
    withResource(inputFile.open()) { inputStream =>
      NvtxRegistry.PARQUET_READ_FOOTER_BYTES {
        inputStream.seek(footerLengthIndex)
        val footerLength = readIntLittleEndian(inputStream)
        val magic = new Array[Byte](MAGIC.length)
        IOUtils.readFully(inputStream, magic, 0, magic.length)
        verifyParquetMagic(filePath, magic)
        val fIdx = footerIndex(filePath, fileLen, footerLength, FooterLengthSize)
        val tailBytes = Math.toIntExact(fileLen - fIdx)
        closeOnExcept(HostMemoryBuffer.allocate(tailBytes + MAGIC.length, false)) { outBuffer =>
          outBuffer.setBytes(0, MAGIC, 0, MAGIC.length)
          inputStream.seek(fIdx)
          val tmp = new Array[Byte](4096)
          var written = MAGIC.length.toLong
          var bytesLeft = tailBytes
          while (bytesLeft > 0) {
            val toRead = math.min(bytesLeft, tmp.length)
            IOUtils.readFully(inputStream, tmp, 0, toRead)
            outBuffer.setBytes(written, tmp, 0, toRead)
            written += toRead
            bytesLeft -= toRead
          }
          outBuffer
        }
      }
    }
  }

  /**
   * Return a framed footer buffer (`MAGIC + footer + footerLen + MAGIC`) for `inputFile`,
   * serving from `FileCache` when present and populating the cache on miss. The `readFooterBuffer`
   * thunk is only evaluated on a miss and must return a buffer with the same framing.
   */
  def getFooterBuffer(
      inputFile: RapidsInputFile,
      metrics: Map[String, GpuMetric],
      readFooterBuffer: => HostMemoryBuffer): HostMemoryBuffer = {
    FileCache.get.getFooter(inputFile).map { footer =>
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS, NoopMetric) += 1
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS_SIZE, NoopMetric) += footer.getLength
      footer
    }.getOrElse {
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
      val cacheToken = FileCache.get.startFooterCache(inputFile)
      cacheToken.map { token =>
        var needTokenCancel = true
        try {
          closeOnExcept(readFooterBuffer) { footer =>
            metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES_SIZE, NoopMetric) +=
              footer.getLength
            token.complete(footer.slice(0, footer.getLength))
            needTokenCancel = false
            footer
          }
        } finally {
          if (needTokenCancel) {
            token.cancel()
          }
        }
      }.getOrElse {
        closeOnExcept(readFooterBuffer) { footer =>
          metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES_SIZE, NoopMetric) +=
            footer.getLength
          footer
        }
      }
    }
  }
}
