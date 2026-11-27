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

package com.nvidia.spark.rapids.iceberg.iceberg19x

import com.nvidia.spark.rapids.{GpuMetric, NoopMetric}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.parquet.ParquetFooterUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader

/** Reads 1.9.x footers through PerfIO, while retaining native streams for dictionaries. */
object GpuParquetIOShim {
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
    val footer = withResource(
        ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath)) { buffer =>
      val bytes = new Array[Byte](Math.toIntExact(buffer.getLength))
      buffer.getBytes(bytes, 0, 0, bytes.length)
      bytes
    }
    // Unlike 1.10.x, the bundled Parquet reader cannot take an InputFile and parsed metadata.
    // Present the prefetched footer at its original offsets so its constructor can still read
    // dictionaries during filtering. The native stream is opened only for reads before the footer.
    ParquetFileReader.open(new FooterInputFile(inputFile, footer), options)
  }
}
