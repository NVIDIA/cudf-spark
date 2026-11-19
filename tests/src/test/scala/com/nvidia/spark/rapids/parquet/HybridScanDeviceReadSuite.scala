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

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, ByteOrder}

import ai.rapids.cudf.{ByteRange, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric, NoopMetric, RapidsConf, SparkQueryCompareTestSuite, TestUtils}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsConf.GpuDirectStorageMode
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.sources.{EqualTo, Filter, LessThan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The hybrid reader's device branch, end to end, with a range reader that copies the file to the
 * device itself, so neither KvikIO nor GPUDirect Storage is needed.
 */
class HybridScanDeviceReadSuite extends SparkQueryCompareTestSuite {

  private val fileIO = new HadoopFileIO(new Configuration())

  private def newMetrics(): Map[String, GpuMetric] = Map[String, GpuMetric](
    GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ -> new LocalGpuMetric)
    .withDefault(_ => NoopMetric)

  /** Reports every read as `direct` or not, reads `path` to the device, and counts its reads. */
  private class CountingReader(path: Path, metrics: Map[String, GpuMetric], direct: Boolean)
    extends RapidsFileIORangeReader(metrics) {
    var deviceReads = 0
    var hostReads = 0

    override def isDeviceReadDirect(ranges: Array[ByteRange]): Boolean = direct

    override def readToHost(
        inputFile: RapidsInputFile,
        buffer: HostMemoryBuffer,
        ranges: Seq[CopyRange]): Unit = {
      hostReads += 1
      super.readToHost(inputFile, buffer, ranges)
    }

    override def readToDevice(buffer: DeviceMemoryBuffer, ranges: Seq[CopyRange]): Unit = {
      deviceReads += 1
      withResource(new RandomAccessFile(path.toUri.getPath, "r")) { file =>
        ranges.foreach { r =>
          val bytes = new Array[Byte](r.getLength.toInt)
          file.seek(r.getInputOffset)
          file.readFully(bytes)
          withResource(HostMemoryBuffer.allocate(r.getLength)) { host =>
            host.setBytes(0, bytes, 0, bytes.length)
            withResource(buffer.slice(r.getOutputOffset, r.getLength))(_.copyFromHostBuffer(host))
          }
        }
      }
    }

    override def fileSize: Long = new File(path.toUri.getPath).length()
  }

  private val readSchema = StructType(Seq(
    StructField("sorted_int", IntegerType),
    StructField("filter_int", IntegerType),
    StructField("filter_str", StringType),
    StructField("payload_long", LongType)))

  /**
   * A file of several row groups. `sorted_int` ascends, so statistics rule out most row groups on
   * a range filter; `filter_int` and `filter_str` have few distinct values, so they are dictionary
   * encoded and an equality filter is answered from the dictionary pages.
   */
  private def withParquetFile[T](body: Path => T): T = {
    withTempPath { dir =>
      withCpuSparkSession { spark =>
        spark.range(0, 20000, 1, 1).selectExpr(
          "CAST(id AS INT) AS sorted_int",
          "CAST(id % 100 AS INT) AS filter_int",
          "CONCAT('key_', CAST(id % 50 AS STRING)) AS filter_str",
          "id * 2 AS payload_long"
        ).write.option("parquet.block.size", 8192).parquet(dir.getPath)
      }
      val file = dir.listFiles().filter(_.getName.endsWith(".parquet")).head
      body(new Path(file.toURI))
    }
  }

  /** The file's thrift footer, without the magic and footer length around it. */
  private def thriftFooter(path: Path): HostMemoryBuffer = {
    withResource(new RandomAccessFile(path.toUri.getPath, "r")) { file =>
      val tail = new Array[Byte](8)
      file.seek(file.length() - tail.length)
      file.readFully(tail)
      val length = ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt
      val bytes = new Array[Byte](length)
      file.seek(file.length() - tail.length - length)
      file.readFully(bytes)
      closeOnExcept(HostMemoryBuffer.allocate(length)) { footer =>
        footer.setBytes(0, bytes, 0, length)
        footer
      }
    }
  }

  /** Read the whole file through `source` with `filter` pushed; None when no batch comes back. */
  private def read[T](
      source: HybridScanByteSource,
      path: Path,
      filter: Filter,
      gdsMode: GpuDirectStorageMode.Value)(
      body: Option[ColumnarBatch] => T): T = {
    val filters = new HybridScanFilters(readSchema, true, Array(filter))
    val context = HybridScanContext(readSchema, filters, 1.0,
      RapidsConf.PARQUET_HYBRID_SCAN_MAX_H2D_PACK_SIZE.defaultValue,
      RapidsConf.PARQUET_HYBRID_SCAN_MIN_H2D_PACK_SIZE.defaultValue, newMetrics(), gdsMode)
    val split = HybridScanSplit(source, 0, new File(path.toUri.getPath).length())
    val reader = closeOnExcept(source) { _ =>
      closeOnExcept(thriftFooter(path)) { footer =>
        new HybridScanPerFileParquetPartitionReader(split, footer,
          HybridScanPipeline.parquetOptions(readSchema.fieldNames), context)
      }
    }
    withResource(reader) { r =>
      if (r.next()) {
        withResource(r.get())(batch => body(Some(batch)))
      } else {
        body(None)
      }
    }
  }

  private def hostSource(path: Path): HybridScanFileByteSource = {
    val metrics = newMetrics()
    new HybridScanFileByteSource(fileIO, path, metrics, new RapidsFileIORangeReader(metrics))
  }

  /**
   * A pushed filter and the reads it takes when the file goes to the device: the dictionary pages,
   * if it prunes with them, are read to host memory, and the surviving column chunks, if any, to
   * the device.
   */
  private case class FilterCase(name: String, filter: Filter, deviceReads: Int, hostReads: Int)

  private val filterCases = Seq(
    FilterCase("a statistics filter", LessThan("sorted_int", 500), deviceReads = 1,
      hostReads = 0),
    FilterCase("a dictionary filter", EqualTo("filter_int", 7), deviceReads = 1, hostReads = 1),
    // 'key_500' sorts inside every row group's range of strings, so statistics keep them all,
    // but no dictionary holds it, so only the dictionary pages are read.
    FilterCase("a filter no dictionary matches", EqualTo("filter_str", "key_500"),
      deviceReads = 0, hostReads = 1))

  // With no task holding the semaphore, HOST_IF_GPU_BUSY reads the same as WAIT_IF_GPU_BUSY.
  for (filterCase <- filterCases; gdsMode <- GpuDirectStorageMode.values.toSeq;
       direct <- Seq(true, false)) {
    val file = if (direct) "a device-direct file" else "a file not readable to the device"
    test(s"${filterCase.name} on $file in $gdsMode decodes the same rows as a host read") {
      withParquetFile { path =>
        val metrics = newMetrics()
        val rangeReader = new CountingReader(path, metrics, direct)
        read(new HybridScanFileByteSource(fileIO, path, metrics, rangeReader), path,
            filterCase.filter, gdsMode) { batch =>
          read(hostSource(path), path, filterCase.filter, GpuDirectStorageMode.OFF) { hostBatch =>
            assert(batch.isDefined == hostBatch.isDefined)
            batch.foreach(TestUtils.compareBatches(hostBatch.get, _))
          }
        }
        val deviceBytes = metrics(GpuMetric.HYBRID_SCAN_DEVICE_BYTES_READ).value
        if (direct && gdsMode != GpuDirectStorageMode.OFF) {
          assert(rangeReader.deviceReads == filterCase.deviceReads)
          assert(rangeReader.hostReads == filterCase.hostReads)
          assert((deviceBytes > 0) == (filterCase.deviceReads > 0))
        } else {
          assert(rangeReader.deviceReads == 0)
          assert(rangeReader.hostReads > 0)
          assert(deviceBytes == 0)
        }
      }
    }
  }
}
