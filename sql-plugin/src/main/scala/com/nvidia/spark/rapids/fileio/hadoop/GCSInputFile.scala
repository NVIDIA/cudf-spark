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

package com.nvidia.spark.rapids.fileio.hadoop

import java.io.IOException
import java.net.URI
import java.util.OptionalLong

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{IntRangeWithOffset, PerfIO, RangeWithOffset, SuffixRangeWithOffset}
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.rapids.GpuTaskMetrics

/**
 * GCS-backed {@link RapidsInputFile} for Hadoop-conf-driven (non-iceberg) reads.
 * {@code readVectored} issues batched byte-range reads through the optimized
 * PerfIO path; the other operations delegate to the standard {@link HadoopInputFile}.
 */
class GCSInputFile private (
    delegate: HadoopInputFile,
    fileUri: URI,
    hadoopConf: Configuration)
  extends RapidsInputFile {

  override def path(): String = delegate.path()

  @throws[IOException]
  override def getLength(): Long = delegate.getLength()

  @throws[IOException]
  override def getLastModificationTime(): OptionalLong = delegate.getLastModificationTime()

  @throws[IOException]
  override def open(): SeekableInputStream = delegate.open()

  @throws[IOException]
  override def readVectored(
      output: HostMemoryBuffer,
      copyRanges: java.util.List[RapidsInputFile.CopyRange]): Unit = {
    val ranges = copyRanges.asScala.map { r =>
      IntRangeWithOffset(r.getInputOffset, r.getLength, r.getOutputOffset)
    }.toSeq
    readWithPerfIO(output, ranges)
  }

  /**
   * Issue a single suffix-range read for the last {@code length} bytes. Avoids
   * the {@code getLength()} round-trip the default {@link RapidsInputFile#readTail}
   * would make. PerfIO resolves the GCS suffix range internally.
   */
  @throws[IOException]
  override def readTail(length: Long, output: HostMemoryBuffer): Unit = {
    require(length >= 0, s"length must be non-negative, actual: $length")
    if (length > 0) {
      val ranges = Seq[RangeWithOffset](SuffixRangeWithOffset(length, /*destOffset*/ 0L))
      readWithPerfIO(output, ranges)
    }
  }

  @throws[IOException]
  private def readWithPerfIO(output: HostMemoryBuffer, ranges: Seq[RangeWithOffset]): Unit = {
    if (PerfIO.readToHostMemory(hadoopConf, output, fileUri, ranges).isEmpty) {
      throw new IOException(s"expected PerfIO to read GCS file ${fileUri.toString}")
    }
    GpuTaskMetrics.get.recordPerfioGCSBackendOnce()
  }
}

object GCSInputFile {
  @throws[IOException]
  def create(filePath: Path, conf: Configuration): GCSInputFile = {
    new GCSInputFile(HadoopInputFile.create(filePath, conf), filePath.toUri, conf)
  }
}
