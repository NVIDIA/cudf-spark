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

import java.net.URI

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.PerfIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.rapids.GpuTaskMetrics

/** Built-in cloud input files can prefetch a Parquet suffix without a file status request. */
private[rapids] abstract class PerfIOInputFile(
    protected val fileUri: URI,
    protected val hadoopConf: Configuration) extends RapidsInputFile {

  def readParquetFooter(verifyMagic: (Path, Array[Byte]) => Unit): Option[HostMemoryBuffer] =
    PerfIO.readParquetFooterBuffer(new Path(fileUri), hadoopConf, verifyMagic,
      GpuTaskMetrics.get.perfioS3RequestLimiterMetricsRecorder)
}
