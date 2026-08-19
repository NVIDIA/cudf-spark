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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ContiguousTable, Cuda, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType, TableMeta}

import org.apache.spark.internal.Logging

/**
 * Compressed table descriptor
 * @param compressedSize size of the compressed data in bytes
 * @param meta metadata describing the table layout when uncompressed
 * @param buffer buffer containing the compressed data
 */
case class CompressedTable(
    compressedSize: Long,
    meta: TableMeta,
    buffer: DeviceMemoryBuffer) extends AutoCloseable {
  override def close(): Unit = buffer.close()
}

/** An interface to a compression codec that can compress a contiguous Table on the GPU */
trait TableCompressionCodec {
  /** The name of the codec, used for logging. */
  val name: String

  /** The ID used for this codec.  See the definitions in `CodecType`. */
  val codecId: Byte

  /**
   * Create a batched compressor instance
   * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
   *                           which a batch should be compressed. A single table that requires
   *                           temporary and output memory above this limit is allowed but will
   *                           be compressed individually.
   * @param stream CUDA stream to use for compression
   * @return batched compressor instance
   */
  def createBatchCompressor(maxBatchMemorySize: Long, stream: Cuda.Stream): BatchedTableCompressor

  /**
   * Create a batched decompressor instance
   * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
   *                           which a batch should be decompressed. A single buffer that requires
   *                           temporary and output memory above this limit is allowed but will
   *                           be decompressed individually.
   * @param stream CUDA stream to use for decompression
   * @return batched decompressor instance
   */
  def createBatchDecompressor(
      maxBatchMemorySize: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor
}

/**
 * A small case class used to carry codec-specific settings.
 */
case class TableCompressionCodecConfig(lz4ChunkSize: Long, zstdChunkSize: Long)

object TableCompressionCodec extends Logging {
  private val codecNameToId = Map(
    "copy" -> CodecType.COPY,
    "zstd" -> CodecType.NVCOMP_ZSTD,
    "lz4" -> CodecType.NVCOMP_LZ4)

  /** Make a codec configuration object which can be serialized (can be used in tasks) */
  def makeCodecConfig(rapidsConf: RapidsConf): TableCompressionCodecConfig =
    TableCompressionCodecConfig(
      rapidsConf.shuffleCompressionLz4ChunkSize,
      rapidsConf.shuffleCompressionZstdChunkSize)

  /** Get a compression codec by short name or fully qualified class name */
  def getCodec(name: String, codecConfigs: TableCompressionCodecConfig): TableCompressionCodec = {
    val codecId = codecNameToId.getOrElse(name,
      throw new IllegalArgumentException(s"Unknown table codec: $name"))
    getCodec(codecId, codecConfigs)
  }

  /** Get a compression codec by ID, using a cache. */
  def getCodec(codecId: Byte, codecConfig: TableCompressionCodecConfig): TableCompressionCodec = {
    val ret = codecId match {
      case CodecType.NVCOMP_ZSTD => new NvcompZSTDCompressionCodec(codecConfig)
      case CodecType.NVCOMP_LZ4 => new NvcompLZ4CompressionCodec(codecConfig)
      case CodecType.COPY => new CopyCompressionCodec
      case _ => throw new IllegalArgumentException(s"Unknown codec ID: $codecId")
    }
    logDebug(s"Using codec: ${ret.name}")
    ret
  }
}

/**
 * Base class for batched compressors
 * @param maxBatchMemorySize The upper limit in bytes of estimated output memory usage at
 *                           which a batch should be compressed. A single table that requires
 *                           estimated output memory above this limit is allowed but will
 *                           be compressed individually.
 * @param stream CUDA stream to use
 */
abstract class BatchedTableCompressor(maxBatchMemorySize: Long, stream: Cuda.Stream)
    extends AutoCloseable with Logging {
  // The tables that need to be compressed in the next batch
  private[this] val tables = new ArrayBuffer[ContiguousTable]

  // The compressed outputs of all tables across all batches
  private[this] val results = new ArrayBuffer[CompressedTable]

  // temporary and output memory being used as part of the current batch
  private[this] var batchMemUsed: Long = 0

  /**
   * Add a contiguous table to be batch-compressed. Ownership of the table is transferred to the
   * batch compressor which is responsible for closing the table.
   * @param contigTable the contiguous table to be compressed
   */
  def addTableToCompress(contigTable: ContiguousTable): Unit = {
    closeOnExcept(contigTable) { contigTable =>
      // use original input size as a conservative estimate of compressed output size
      val memNeededToCompressThisBuffer = contigTable.getBuffer.getLength
      if (batchMemUsed + memNeededToCompressThisBuffer > maxBatchMemorySize) {
        compressBatch()
      }
      tables += contigTable
      batchMemUsed += memNeededToCompressThisBuffer
    }
  }

  /**
   * Add an array of contiguous tables to be compressed. The tables will be closed by the
   * batch compressor.
   * @param contigTable contiguous tables to compress
   */
  def addTables(contigTable: Array[ContiguousTable]): Unit = {
    var i = 0
    try {
      contigTable.foreach { ct =>
        addTableToCompress(ct)
        i += 1
      }
    } catch {
      case t: Throwable =>
        contigTable.drop(i).foreach(_.safeClose())
        throw t
    }
  }

  /**
   * This must be called after all tables to be compressed have been added to retrieve the
   * compression results.
   * @note the table IDs in the TableMeta of all tables will be set to zero
   * @return compressed tables
   */
  def finish(): Array[CompressedTable] = {
    // compress the last batch
    compressBatch()

    val compressedTables = results.toArray
    results.clear()

    // Ensure we synchronize on the CUDA stream, because `CompressedTable` instances
    // could be copied to host during a spill before we are done.
    // TODO: A better way to do this would be via CUDA events, synchronizing on the event
    //  instead of the whole stream
    stream.sync()
    compressedTables
  }

  /** Must be closed to release the resources owned by the batch compressor */
  override def close(): Unit = {
    tables.safeClose()
    tables.clear()
    results.safeClose()
    results.clear()
  }

  private def compressBatch(): Unit = if (tables.nonEmpty) {
    NvtxRegistry.BATCH_COMPRESS {
      val startTime = System.nanoTime()
      val compressedTables = compress(tables.toArray, stream)
      results ++= compressedTables
      require(compressedTables.length == tables.length)

      if (log.isDebugEnabled) {
        val duration = (System.nanoTime() - startTime).toFloat
        val inputSize = tables.map(_.getBuffer.getLength).sum
        val outputSize = compressedTables.map(_.compressedSize).sum
        logDebug(s"Compressed ${tables.length} tables from $inputSize to $outputSize " +
            s"in ${duration / 1000000} msec rate=${inputSize / duration} GB/s " +
            s"ratio=${outputSize.toFloat/inputSize}")
      }

      // free the inputs to this batch
      tables.safeClose()
      tables.clear()
      batchMemUsed = 0
    }
  }

  /**
   * Reallocates and copies data for oversized compressed data buffers due to inaccurate estimates
   * of the compressed output size. If the buffer is already the appropriate size then no copy
   * is performed.
   * @note This method takes ownership of the tables and is responsible for closing them.
   * @param tables compressed tables to resize
   * @return right-sized compressed tables
   */
  protected def resizeOversizedOutputs(tables: Array[CompressedTable]): Array[CompressedTable] = {
    NvtxRegistry.COPY_COMPRESSED_BUFFERS {
      withResource(tables) { _ =>
        tables.safeMap { ct =>
          val newBuffer = if (ct.buffer.getLength > ct.compressedSize) {
            closeOnExcept(DeviceMemoryBuffer.allocate(ct.compressedSize)) { buffer =>
              buffer.copyFromDeviceBufferAsync(
                0, ct.buffer, 0, ct.compressedSize, stream)
              buffer
            }
          } else {
            ct.buffer.incRefCount()
            ct.buffer
          }
          CompressedTable(ct.compressedSize, ct.meta, newBuffer)
        }
      }
    }
  }

  /**
   * Batch-compress contiguous tables
   * @param tables contiguous tables to compress
   * @param stream CUDA stream to use
   * @return compressed tables. Table IDs in the `TableMeta` should be set to 0.
   */
  protected def compress(
      tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable]
}

/**
 * Base class for batched decompressors
 * @param maxBatchMemorySize The upper limit in bytes of output memory usage at which a batch
 *                           should be decompressed. A single table that requires output memory
 *                           above this limit is allowed but will be decompressed individually.
 * @param stream CUDA stream to use
 */
abstract class BatchedBufferDecompressor(maxBatchMemorySize: Long, stream: Cuda.Stream)
    extends AutoCloseable with Logging {
  // The buffers of compressed data that will be decompressed in the next batch
  private[this] val inputBuffers = new ArrayBuffer[BaseDeviceMemoryBuffer]

  // The output buffers that will contain the decompressed data in the next batch
  private[this] val bufferMetas = new ArrayBuffer[BufferMeta]

  // The decompressed data results for all input buffers across all batches
  private[this] val results = new ArrayBuffer[DeviceMemoryBuffer]

  // temporary and output memory being used as part of the current batch
  private[this] var batchMemUsed: Long = 0

  /** The codec ID corresponding to this decompressor */
  val codecId: Byte

  def addBufferToDecompress(buffer: BaseDeviceMemoryBuffer, meta: BufferMeta): Unit = {
    closeOnExcept(buffer) { buffer =>
      // Only supports a single codec per buffer for now.
      require(meta.codecBufferDescrsLength == 1)
      val descr = meta.codecBufferDescrs(0)
      require(descr.codec == codecId)

      // Only support codec that consumes entire input buffer for now.
      require(descr.compressedOffset == 0)
      require(descr.compressedSize == buffer.getLength)

      val outputNeeded = descr.uncompressedSize
      if (batchMemUsed + outputNeeded > maxBatchMemorySize) {
        decompressBatch()
      }

      batchMemUsed += outputNeeded
      bufferMetas += meta
      inputBuffers += buffer
    }
  }

  /**
   * This must be called after all buffers to be decompressed have been added to retrieve the
   * decompression results. Note that the decompression may still be occurring asynchronously
   * using the CUDA stream specified when the decompressor was instantiated.
   * @return decompressed tables
   */
  def finishAsync(): Array[DeviceMemoryBuffer] = {
    // decompress the last batch
    decompressBatch()
    val resultsArray = results.toArray
    results.clear()
    resultsArray
  }

  override def close(): Unit = {
    inputBuffers.safeClose()
    inputBuffers.clear()
    bufferMetas.clear()
    results.safeClose()
    results.clear()
  }

  protected def decompressBatch(): Unit = {
    if (inputBuffers.nonEmpty) {
      NvtxRegistry.BATCH_DECOMPRESS {
        val startTime = System.nanoTime()
        val uncompressedBuffers = decompressAsync(inputBuffers.toArray, bufferMetas.toArray, stream)
        results ++= uncompressedBuffers
        require(uncompressedBuffers.length == inputBuffers.length)
        if (log.isDebugEnabled) {
          val duration = (System.nanoTime - startTime).toFloat
          val inputSize = inputBuffers.map(_.getLength).sum
          val outputSize = uncompressedBuffers.map(_.getLength).sum
          logDebug(s"Decompressed ${inputBuffers.length} buffers from $inputSize " +
              s"to $outputSize in ${duration / 1000000} msec rate=${outputSize / duration} GB/s")
        }

        // free all the inputs to this batch
        inputBuffers.safeClose()
        inputBuffers.clear()
        bufferMetas.clear()
        batchMemUsed = 0
      }
    }
  }

  /**
   * Decompress a batch of compressed buffers
   * @param inputBuffers buffers that contain the compressed input
   * @param bufferMetas corresponding metadata for each compressed input buffer
   * @param stream CUDA stream to use
   * @return buffers that contain the uncompressed output
   */
  protected def decompressAsync(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta],
      stream: Cuda.Stream): Array[DeviceMemoryBuffer]
}
