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

import ai.rapids.cudf.{ByteRange, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import com.nvidia.spark.rapids.parquet.HybridScanPipeline.{deviceBufferLayout, GDS_ALIGNMENT}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * [[HybridScanFileByteSource.fetchToDevice]] with a range reader that copies a file held in memory
 * to the device, so the fetch is tested without KvikIO or GPUDirect Storage.
 */
class HybridScanDeviceFetchSuite extends RmmSparkRetrySuiteBase {
  // Not a multiple of GDS_ALIGNMENT, so the last block of the file is partial.
  private val FILE_SIZE = 3 * GDS_ALIGNMENT + 1000

  private def fileByte(position: Long): Byte = (position % 251).toByte

  /** Reads a file of `size` bytes to the device, checking each read against what cuFile needs. */
  private class InMemoryDeviceReader(size: Long) extends HybridScanRangeReader {
    val reads = new ArrayBuffer[(Long, Long, Long)]()

    override def readToHost(
        inputFile: RapidsInputFile,
        buffer: HostMemoryBuffer,
        ranges: Seq[CopyRange]): Unit = fail("a device fetch read to host memory")

    override def isDeviceReadDirect(ranges: Array[ByteRange]): Boolean = true

    override def readToDevice(buffer: DeviceMemoryBuffer, ranges: Seq[CopyRange]): Unit = {
      assert(buffer.getAddress % GDS_ALIGNMENT == 0, "the device buffer is not aligned")
      ranges.foreach { r =>
        assert(r.getInputOffset + r.getLength <= size, "a read runs past the end of the file")
        assert(r.getOutputOffset + r.getLength <= buffer.getLength, "a read overruns the buffer")
        val bytes = Array.tabulate[Byte](r.getLength.toInt)(i => fileByte(r.getInputOffset + i))
        withResource(HostMemoryBuffer.allocate(r.getLength)) { host =>
          host.setBytes(0, bytes, 0, bytes.length)
          withResource(buffer.slice(r.getOutputOffset, r.getLength))(_.copyFromHostBuffer(host))
        }
        reads += ((r.getInputOffset, r.getLength, r.getOutputOffset))
      }
    }

    override def fileSize: Long = size
  }

  private def newMetrics(): Map[String, GpuMetric] =
    Map(GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ -> new LocalGpuMetric)

  private def source(
      reader: InMemoryDeviceReader,
      metrics: Map[String, GpuMetric]): HybridScanFileByteSource =
    new HybridScanFileByteSource(new HadoopFileIO(new Configuration()),
      new Path("file:/unused"), metrics, reader)

  /** Fetch `ranges` to the device and check each buffer holds its range's bytes. */
  private def assertDeviceFetchHoldsRanges(
      reader: InMemoryDeviceReader,
      metrics: Map[String, GpuMetric],
      ranges: Array[ByteRange]): Unit = {
    withResource(source(reader, metrics)) { src =>
      withResource(src.fetchToDevice(ranges)) { device =>
        assert(device.length == ranges.length)
        ranges.zip(device).foreach { case (range, buffer) =>
          assert(buffer.getLength == range.size(), s"$range")
          val actual = new Array[Byte](range.size().toInt)
          if (actual.nonEmpty) {
            withResource(HostMemoryBuffer.allocate(range.size())) { host =>
              host.copyFromDeviceBuffer(buffer)
              host.getBytes(actual, 0, 0, actual.length)
            }
          }
          val expected = Array.tabulate[Byte](actual.length)(i => fileByte(range.offset() + i))
          assert(actual.sameElements(expected), s"$range")
        }
      }
    }
  }

  test("a device fetch puts each range's bytes in its own buffer and counts the aligned reads") {
    // Out of file order, adjacent ranges, a block crossing, an empty range and a far range.
    val ranges = Array(
      new ByteRange(2 * GDS_ALIGNMENT + 100, 500),
      new ByteRange(4, 96),
      new ByteRange(100, 50),
      new ByteRange(4000, 200),
      new ByteRange(300, 0))
    val reader = new InMemoryDeviceReader(FILE_SIZE)
    val metrics = newMetrics()
    assertDeviceFetchHoldsRanges(reader, metrics, ranges)
    val layout = deviceBufferLayout(ranges, FILE_SIZE)
    assert(reader.reads ==
      layout.reads.map(r => (r.getInputOffset, r.getLength, r.getOutputOffset)))
    assert(metrics(GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ).value ==
      layout.reads.map(_.getLength).sum)
  }

  test("a device fetch stops its aligned reads at the end of the file") {
    val ranges = Array(new ByteRange(FILE_SIZE - 10, 10), new ByteRange(100, 50),
      new ByteRange(GDS_ALIGNMENT + 10, 10))
    val reader = new InMemoryDeviceReader(FILE_SIZE)
    val metrics = newMetrics()
    assertDeviceFetchHoldsRanges(reader, metrics, ranges)
    assert(reader.reads.map { case (offset, length, _) => offset + length }.max == FILE_SIZE)
    assert(metrics(GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ).value ==
      reader.reads.map { case (_, length, _) => length }.sum)
  }

  test("an empty device fetch reads nothing") {
    val reader = new InMemoryDeviceReader(FILE_SIZE)
    val metrics = newMetrics()
    withResource(source(reader, metrics)) { src =>
      assert(src.fetchToDevice(Array.empty).isEmpty)
    }
    assert(reader.reads.isEmpty)
    assert(metrics(GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ).value == 0)
  }
}
