/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec used only for testing that copies the data. */
class CopyCompressionCodec extends TableCompressionCodec {
  override val name: String = "COPY"
  override val codecId: Byte = CodecType.COPY

  override def createBatchCompressor(
      maxBatchMemorySize: Long,
      stream: Cuda.Stream): BatchedTableCompressor =
    new BatchedCopyCompressor(maxBatchMemorySize, stream)

  override def createBatchDecompressor(
      maxBatchMemorySize: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor =
    new BatchedCopyDecompressor(maxBatchMemorySize, stream)
}

class BatchedCopyCompressor(maxBatchMemory: Long, stream: Cuda.Stream)
    extends BatchedTableCompressor(maxBatchMemory, stream) {
  override protected def compress(
      tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable] = {
    val result = tables.safeMap { ct =>
      val inBuffer = ct.getBuffer
      closeOnExcept(DeviceMemoryBuffer.allocate(inBuffer.getLength)) { outBuffer =>
        outBuffer.copyFromDeviceBufferAsync(0, inBuffer, 0, inBuffer.getLength, stream)
        val meta = MetaUtils.buildTableMeta(
          None,
          ct,
          CodecType.COPY,
          outBuffer.getLength)
        CompressedTable(outBuffer.getLength, meta, outBuffer)
      }
    }
    closeOnExcept(result) { _ => stream.sync() }
    result
  }
}

class BatchedCopyDecompressor(maxBatchMemory: Long, stream: Cuda.Stream)
    extends BatchedBufferDecompressor(maxBatchMemory, stream) {
  override val codecId: Byte = CodecType.COPY

  override def decompressAsync(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta],
      stream: Cuda.Stream): Array[DeviceMemoryBuffer] = {
    inputBuffers.safeMap { inBuffer =>
      closeOnExcept(DeviceMemoryBuffer.allocate(inBuffer.getLength)) { buffer =>
        buffer.copyFromDeviceBufferAsync(0, inBuffer, 0, inBuffer.getLength, stream)
        buffer
      }
    }
  }
}
