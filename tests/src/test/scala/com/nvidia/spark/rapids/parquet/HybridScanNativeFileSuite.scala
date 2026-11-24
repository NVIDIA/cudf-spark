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

import java.io.FileNotFoundException
import java.nio.file.Files
import java.util.concurrent.Executors

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import ai.rapids.cudf.{ByteRange, HostMemoryBuffer}
import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/** The hybrid scan's reads of local files to host memory through KvikIO. */
class HybridScanNativeFileSuite extends RmmSparkRetrySuiteBase {
  private val FILE_SIZE: Int = 10000

  private def fileByte(position: Long): Byte = (position % 251).toByte

  private def withLocalFile[V](block: Path => V): V = {
    val dir = Files.createTempDirectory("hybrid-scan-native")
    val file = dir.resolve("data.bin")
    try {
      Files.write(file, Array.tabulate[Byte](FILE_SIZE)(i => fileByte(i)))
      block(new Path(file.toUri))
    } finally {
      Files.deleteIfExists(file)
      Files.delete(dir)
    }
  }

  private val missingFile = new Path("file:/nonexistent/hybrid-scan-native/data.bin")

  private def newMetrics(): Map[String, GpuMetric] =
    Map(GpuMetric.HYBRID_SCAN_NATIVE_HOST_BYTES_READ -> new LocalGpuMetric)

  private def nativeSource(
      path: Path,
      metrics: Map[String, GpuMetric]): HybridScanFileByteSource =
    new HybridScanFileByteSource(new HadoopFileIO(new Configuration()), path, metrics,
      new NativeFileRangeReader(path, metrics))

  // Out of file order, adjacent ranges to coalesce, a gap, an empty range and a range ending at
  // the end of the file. None starts at 0, which a Parquet file's magic occupies and
  // GpuParquetScan.coalesceReads does not handle.
  private val ranges = Array(
    new ByteRange(9000L, 500L),
    new ByteRange(4L, 96L),
    new ByteRange(100L, 50L),
    new ByteRange(4000L, 200L),
    new ByteRange(300L, 0L),
    new ByteRange(FILE_SIZE - 300L, 300L))
  private val rangesSize = ranges.map(_.size()).sum

  private def assertHoldsRange(buffer: HostMemoryBuffer, range: ByteRange): Unit = {
    assert(buffer.getLength == range.size())
    val actual = new Array[Byte](range.size().toInt)
    buffer.getBytes(actual, 0, 0, actual.length)
    val expected = Array.tabulate[Byte](actual.length)(i => fileByte(range.offset() + i))
    assert(actual.sameElements(expected), s"$range")
  }

  test("only a local file: path is read natively") {
    assert(NativeFileRangeReader.canRead(new Path("file:/data/t.parquet")))
    assert(NativeFileRangeReader.canRead(new Path("file:///data/t.parquet")))
    assert(!NativeFileRangeReader.canRead(new Path("file://host/data/t.parquet")))
    assert(!NativeFileRangeReader.canRead(new Path("/data/t.parquet")))
    assert(!NativeFileRangeReader.canRead(new Path("hdfs://nn:8020/data/t.parquet")))
    assert(!NativeFileRangeReader.canRead(new Path("s3a://bucket/data/t.parquet")))
  }

  test("a temp file is read natively, and any other path through Hadoop") {
    withLocalFile { path =>
      withResource(HybridScanFileByteSource.createRangeReader(path, Map.empty)) { reader =>
        assert(reader.isInstanceOf[NativeFileRangeReader])
      }
    }
    val hdfsPath = new Path("hdfs://nn:8020/data/t.parquet")
    withResource(HybridScanFileByteSource.createRangeReader(hdfsPath, Map.empty)) { reader =>
      assert(reader.isInstanceOf[RapidsFileIORangeReader])
    }
  }

  test("a local file with a Hadoop checksum file is read through Hadoop") {
    withLocalFile { path =>
      val crc = NativeFileRangeReader.checksumFile(path.toUri.getPath).toPath
      Files.write(crc, Array[Byte](0))
      try {
        withResource(HybridScanFileByteSource.createRangeReader(path, Map.empty)) { reader =>
          assert(reader.isInstanceOf[RapidsFileIORangeReader])
        }
      } finally {
        Files.delete(crc)
      }
    }
  }

  test("KvikIO reads each range to host memory") {
    withLocalFile { path =>
      val metrics = newMetrics()
      withResource(nativeSource(path, metrics)) { source =>
        withResource(source.fetch(ranges)) { buffers =>
          assert(buffers.length == ranges.length)
          ranges.zip(buffers).foreach { case (range, buffer) => assertHoldsRange(buffer, range) }
        }
      }
      assert(metrics(GpuMetric.HYBRID_SCAN_NATIVE_HOST_BYTES_READ).value == rangesSize)
    }
  }

  // The MULTITHREADED reader's pool threads each open a source, so KvikIO serves several files
  // at once.
  test("pool threads read local files natively, to host memory, at the same time") {
    withLocalFile { path =>
      val pool = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
      try {
        val reads = Seq.fill(8)(Future(readOnPoolThread(path))(pool))
        reads.foreach(Await.result(_, Duration.Inf))
      } finally {
        pool.shutdown()
      }
    }
  }

  private def readOnPoolThread(path: Path): Unit = {
    RmmSpark.poolThreadWorkingOnTask(1)
    try {
      val metrics = newMetrics()
      withResource(nativeSource(path, metrics)) { source =>
        withResource(source.fetch(ranges)) { buffers =>
          ranges.zip(buffers).foreach { case (range, buffer) => assertHoldsRange(buffer, range) }
        }
      }
      assert(metrics(GpuMetric.HYBRID_SCAN_NATIVE_HOST_BYTES_READ).value == rangesSize)
    } finally {
      RmmSpark.poolThreadFinishedForTask(1)
    }
  }

  test("KvikIO and RapidsFileIO read the same bytes to host memory") {
    withLocalFile { path =>
      val fileIO = new HadoopFileIO(new Configuration())
      val hadoopMetrics = newMetrics()
      withResource(nativeSource(path, Map.empty)) { native =>
        withResource(new HybridScanFileByteSource(fileIO, path, hadoopMetrics,
            new RapidsFileIORangeReader(hadoopMetrics))) { hadoop =>
          withResource(native.fetch(ranges)) { nativeBuffers =>
            withResource(hadoop.fetch(ranges)) { hadoopBuffers =>
              nativeBuffers.zip(hadoopBuffers).zip(ranges).foreach { case ((n, h), range) =>
                assert(n.getLength == h.getLength)
                (0L until range.size()).foreach { i =>
                  assert(n.getByte(i) == h.getByte(i), s"$range byte $i")
                }
              }
            }
          }
        }
      }
      assert(hadoopMetrics(GpuMetric.HYBRID_SCAN_NATIVE_HOST_BYTES_READ).value == 0)
    }
  }

  test("a missing local file fails the fetch as not found and counts no bytes") {
    val metrics = newMetrics()
    withResource(nativeSource(missingFile, metrics)) { source =>
      intercept[FileNotFoundException](source.fetch(ranges))
    }
    assert(metrics(GpuMetric.HYBRID_SCAN_NATIVE_HOST_BYTES_READ).value == 0)
  }

  test("closing a native reader that never read, or closing it twice, is a no-op") {
    withResource(new NativeFileRangeReader(missingFile, Map.empty)) {
      _.close()
    }
  }
}
