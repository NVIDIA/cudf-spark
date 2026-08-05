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

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.Arrays

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, HostAlloc, NoopMetric, NvtxRegistry, RapidsConf}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.apache.hadoop.fs.Path
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
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen)")
    }
    NvtxRegistry.PARQUET_READ_FOOTER_BYTES {
      val trailerLen = FooterLengthSize + MAGIC.length
      val (footerLength, trailerMagic) = withResource(
          HostAlloc.alloc(trailerLen, preferPinned = false)) { trailerBuf =>
        inputFile.readTail(trailerLen, trailerBuf)
        val view = trailerBuf.asByteBuffer(0, trailerLen).order(ByteOrder.LITTLE_ENDIAN)
        val len = view.getInt()
        val magic = new Array[Byte](MAGIC.length)
        view.get(magic)
        (len, magic)
      }
      verifyParquetMagic(filePath, trailerMagic)
      val footerLengthIndex = fileLen - trailerLen
      val footerIndex = footerLengthIndex - footerLength
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException(s"corrupted file $filePath: the footer index is not within " +
          s"the file: $footerIndex")
      }
      val hmbLength = (fileLen - footerIndex).toInt
      closeOnExcept(HostAlloc.alloc(hmbLength + MAGIC.length, preferPinned = false)) { outBuffer =>
        outBuffer.asByteBuffer(0, MAGIC.length).put(MAGIC)
        val ranges = new java.util.ArrayList[CopyRange](1)
        ranges.add(new CopyRange(footerIndex, hmbLength, MAGIC.length))
        inputFile.readVectored(outBuffer, ranges)
        outBuffer
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
