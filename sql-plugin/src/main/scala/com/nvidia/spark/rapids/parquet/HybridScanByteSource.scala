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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ByteRange, HostMemoryBuffer}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsInputFile}
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import com.nvidia.spark.rapids.parquet.HybridScanParquetPartitionReader.HostBufferLayout
import com.nvidia.spark.rapids.parquet.ParquetPartitionReader.LocalCopy
import org.apache.hadoop.fs.Path

/**
 * Where a hybrid scan read's bytes come from.
 *
 * The reader plans a read as byte ranges of a Parquet file and asks for their contents.
 */
private[parquet] trait HybridScanByteSource {
  /**
   * Fetch `ranges` into host memory, returning one buffer per range in the order requested. An
   * empty range yields an empty buffer rather than being dropped, so the result stays aligned
   * with the request. The caller owns the buffers that come back.
   */
  def fetch(ranges: Array[ByteRange]): Array[HostMemoryBuffer]
}

/**
 * Ranges of a Parquet file, read from that file. Ranges already in the local file cache are
 * copied from there and the rest are read from the file, coalesced into as few requests as the
 * layout allows.
 */
private[parquet] class HybridScanFileByteSource(
    fileIO: RapidsFileIO,
    filePath: Path,
    execMetrics: Map[String, GpuMetric]) extends HybridScanByteSource {

  override def fetch(ranges: Array[ByteRange]): Array[HostMemoryBuffer] = {
    if (ranges.isEmpty) {
      Array.empty
    } else {
      val layout = HybridScanParquetPartitionReader.hostBufferLayout(ranges)
      val buffer = withRetryNoSplit[HostMemoryBuffer] {
        HostMemoryBuffer.allocate(layout.totalSize)
      }
      // Each slice holds a reference to the buffer, so closing it here leaves the slices valid
      // and releases the memory once the last one is closed.
      withResource(buffer) { _ =>
        val inputFile = fileIO.newInputFile(filePath)
        fillRanges(inputFile, buffer, ranges, layout)
        ranges.indices.toArray.safeMap { i =>
          buffer.slice(layout.offsets(i), ranges(i).size())
        }
      }
    }
  }

  private def fillRanges(
      inputFile: RapidsInputFile,
      buffer: HostMemoryBuffer,
      ranges: Array[ByteRange],
      layout: HostBufferLayout): Unit = {
    val remoteRanges = new ArrayBuffer[CopyRange](ranges.length)
    withResource(new ArrayBuffer[LocalCopy](ranges.length)) { cachedRanges =>
      layout.fileOrder.foreach { i =>
        val offset = ranges(i).offset()
        val size = ranges(i).size()
        // An empty range has nothing to read but still owns a slot in the result, so that the
        // caller can keep passing the buffers to the reader in the order it asked for them.
        if (size > 0) {
          FileCache.get.getDataRangeChannel(inputFile, offset, size) match {
            case Some(channel) => cachedRanges += LocalCopy(channel, size, layout.offsets(i))
            case None => remoteRanges += new CopyRange(offset, size, layout.offsets(i))
          }
        }
      }
      val out = new HostMemoryOutputStream(buffer)
      cachedRanges.foreach { cached =>
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_HITS, NoopMetric) += 1
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_HITS_SIZE, NoopMetric) += cached.length
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_READ_TIME, NoopMetric).ns {
          out.seek(cached.outputOffset)
          out.copyFromChannel(cached.channel, cached.length)
        }
      }
    }
    if (remoteRanges.nonEmpty) {
      val coalesced = GpuParquetScan.coalesceReads(remoteRanges.toSeq)
      GpuParquetScan.readRangesToHostMemory(inputFile, buffer, coalesced, execMetrics)
      remoteRanges.foreach { range =>
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_MISSES, NoopMetric) += 1
        execMetrics.getOrElse(FILECACHE_DATA_RANGE_MISSES_SIZE, NoopMetric) += range.getLength
        FileCache.get.startDataRangeCache(inputFile, range.getInputOffset, range.getLength)
            .foreach(_.complete(buffer.slice(range.getOutputOffset, range.getLength)))
      }
    }
  }
}
