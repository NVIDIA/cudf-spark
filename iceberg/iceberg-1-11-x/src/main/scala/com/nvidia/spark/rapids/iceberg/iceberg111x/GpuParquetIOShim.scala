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

package com.nvidia.spark.rapids.iceberg.iceberg111x

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.parquet.converter.ToIcebergShaded
import com.nvidia.spark.rapids.parquet.{HMBInputFile, ParquetFooterUtils}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.parquet.GpuParquetIO
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader

/**
 * Iceberg 1.11.x shim: reads the footer via `FileCache` and injects it into `ParquetFileReader`
 * through the 4-arg `(InputFile, ParquetMetadata, ParquetReadOptions, SeekableInputStream)`
 * constructor that is available from iceberg 1.10.x onward. Lives in the versioned
 * `iceberg111x` package so each minor-version shim can evolve independently without the
 * shade-plugin class-name collision that the 1.10.x split introduced.
 */
object GpuParquetIOShim {
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    val metadata = withResource(ParquetFooterUtils.getFooterBuffer(
        inputFile, metrics,
        ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath))) { hmb =>
      val shadedHmbFile = ToIcebergShaded.shade(new HMBInputFile(hmb))
      withResource(shadedHmbFile.newStream()) { hmbStream =>
        ParquetFileReader.readFooter(shadedHmbFile, options, hmbStream)
      }
    }
    val realFile = GpuParquetIO.file(inputFile.getDelegate)
    closeOnExcept(realFile.newStream()) { stream =>
      new ParquetFileReader(realFile, metadata, options, stream)
    }
  }
}
