/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ContiguousTable, Cuda, DeviceMemoryBuffer}
import ai.rapids.cudf.nvcomp.{BatchedZstdCompressor, BatchedZstdDecompressor}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec that uses nvcomp's ZSTD-GPU codec */
class NvcompZSTDCompressionCodec(codecConfigs: TableCompressionCodecConfig)
    extends TableCompressionCodec {
  override val name: String = "nvcomp-ZSTD"
  override val codecId: Byte = CodecType.NVCOMP_ZSTD

  override def createBatchCompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedTableCompressor = {
    new BatchedNvcompZSTDCompressor(maxBatchMemoryBytes, codecConfigs, stream)
  }

  override def createBatchDecompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor = {
    new BatchedNvcompZSTDDecompressor(maxBatchMemoryBytes, codecConfigs, stream)
  }
}

class BatchedNvcompZSTDCompressor(maxBatchMemorySize: Long,
    codecConfigs: TableCompressionCodecConfig,
    stream: Cuda.Stream) extends BatchedTableCompressor(maxBatchMemorySize, stream) {

  private val batchCompressor =
    new BatchedZstdCompressor(codecConfigs.zstdChunkSize, maxBatchMemorySize)

  override protected def compress(tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable] = {
    // Increase ref count to keep inputs alive since cudf compressor will close input buffers.
    val inputBufs = DeviceBuffersUtils.incRefCount(tables.map(_.getBuffer))
    closeOnExcept(batchCompressor.compress(inputBufs, stream)) { compressedBufs =>
      NvtxRegistry.ZSTD_POST_PROCESS {
        require(compressedBufs.length == tables.length,
          s"expected ${tables.length} buffers, but compress() returned ${compressedBufs.length}")
        compressedBufs.zip(tables).map { case (buffer, table) =>
          val compressedLen = buffer.getLength
          val meta = MetaUtils.buildTableMeta(None, table, CodecType.NVCOMP_ZSTD, compressedLen)
          CompressedTable(compressedLen, meta, buffer)
        }.toArray
      }
    }
  }
}

class BatchedNvcompZSTDDecompressor(maxBatchMemory: Long,
    codecConfigs: TableCompressionCodecConfig,
    stream: Cuda.Stream) extends BatchedBufferDecompressor(maxBatchMemory, stream) {
  private val batchDecompressor = new BatchedZstdDecompressor(codecConfigs.zstdChunkSize)

  override val codecId: Byte = CodecType.NVCOMP_ZSTD

  override def decompressAsync(inputBufs: Array[BaseDeviceMemoryBuffer],
      bufMetas: Array[BufferMeta], stream: Cuda.Stream): Array[DeviceMemoryBuffer] = {
    require(inputBufs.length == bufMetas.length,
      s"Got ${inputBufs.length} input buffers but ${bufMetas.length} metadata buffers")

    // Increase ref count to keep inputs alive since cudf decompressor will close the inputs.
    val compressedBufs = DeviceBuffersUtils.incRefCount(inputBufs)
    val outputBufs = closeOnExcept(compressedBufs) { _ =>
      NvtxRegistry.ALLOC_OUTPUT_BUFS {
        DeviceBuffersUtils.allocateBuffers(bufMetas.map(_.uncompressedSize()))
      }
    }
    batchDecompressor.decompressAsync(compressedBufs,
      outputBufs.asInstanceOf[Array[BaseDeviceMemoryBuffer]], stream)
    outputBufs
  }
}

object DeviceBuffersUtils {
  def incRefCount(bufs: Array[BaseDeviceMemoryBuffer]): Array[BaseDeviceMemoryBuffer] = {
    bufs.safeMap { b =>
      b.incRefCount()
      b
    }
  }

  def allocateBuffers(bufSizes: Array[Long]): Array[DeviceMemoryBuffer] = {
    var curPos = 0L
    withResource(DeviceMemoryBuffer.allocate(bufSizes.sum)) { singleBuf =>
      bufSizes.safeMap { len =>
        val ret = singleBuf.slice(curPos, len)
        curPos += len
        ret
      }
    }
  }
}