/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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
import ai.rapids.cudf.nvcomp.{BatchedLZ4Compressor, BatchedLZ4Decompressor}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec that uses nvcomp's LZ4-GPU codec */
class NvcompLZ4CompressionCodec(codecConfigs: TableCompressionCodecConfig)
    extends TableCompressionCodec {
  override val name: String = "nvcomp-LZ4"
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def createBatchCompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedTableCompressor = {
    new BatchedNvcompLZ4Compressor(maxBatchMemoryBytes, codecConfigs, stream)
  }

  override def createBatchDecompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor = {
    new BatchedNvcompLZ4Decompressor(maxBatchMemoryBytes, codecConfigs, stream)
  }
}

class BatchedNvcompLZ4Compressor(maxBatchMemorySize: Long,
    codecConfigs: TableCompressionCodecConfig, stream: Cuda.Stream)
    extends BatchedTableCompressor(maxBatchMemorySize, stream) {
  override protected def compress(
      tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable] = {
    val batchCompressor = new BatchedLZ4Compressor(codecConfigs.lz4ChunkSize,
      maxBatchMemorySize)
    val inputBuffers: Array[BaseDeviceMemoryBuffer] = tables.map { table =>
      val buffer = table.getBuffer
      // cudf compressor guarantees that close will be called for 'inputBuffers' and will not throw
      // before doing so, but this interface does not close inputs so we need to increment the ref
      // count.
      buffer.incRefCount()
      buffer
    }
    closeOnExcept(batchCompressor.compress(inputBuffers, stream)) { compressedBuffers =>
      NvtxRegistry.LZ4_POST_PROCESS {
        require(compressedBuffers.length == tables.length,
          s"expected ${tables.length} buffers, but compress() returned ${compressedBuffers.length}")
        compressedBuffers.zip(tables).map { case (buffer, table) =>
          val compressedSize = buffer.getLength
          val meta = MetaUtils.buildTableMeta(
            None,
            table,
            CodecType.NVCOMP_LZ4,
            compressedSize)
          CompressedTable(compressedSize, meta, buffer)
        }.toArray
      }
    }
  }
}

class BatchedNvcompLZ4Decompressor(maxBatchMemory: Long,
    codecConfigs: TableCompressionCodecConfig, stream: Cuda.Stream)
    extends BatchedBufferDecompressor(maxBatchMemory, stream) {
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def decompressAsync(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta],
      stream: Cuda.Stream): Array[DeviceMemoryBuffer] = {
    require(inputBuffers.length == bufferMetas.length,
      s"number of input buffers (${inputBuffers.length}) does not equal number of metadata " +
          s"buffers (${bufferMetas.length}")
    val outputBuffers = allocateOutputBuffers(inputBuffers, bufferMetas)
    BatchedLZ4Decompressor.decompressAsync(
      codecConfigs.lz4ChunkSize,
      inputBuffers,
      outputBuffers.asInstanceOf[Array[BaseDeviceMemoryBuffer]],
      stream)
    outputBuffers
  }

  private def allocateOutputBuffers(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta]): Array[DeviceMemoryBuffer] = {
    NvtxRegistry.ALLOC_OUTPUT_BUFS {
      bufferMetas.zip(inputBuffers).safeMap { case (meta, input) =>
        // cudf decompressor guarantees that close will be called for 'inputBuffers' and will not
        // throw before doing so, but this interface does not close inputs so we need to increment
        // the ref count.
        input.incRefCount()
        DeviceMemoryBuffer.allocate(meta.uncompressedSize())
      }
    }
  }
}
