/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.parquet

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.ShimUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile => ShadedInputFile}

object GpuParquetIO {
  def file(file: InputFile): ShadedInputFile = {
    GpuParquetIOAccess.file(file)
  }

  /**
   * Open a shaded `ParquetFileReader`. Footer caching is version-dependent and resolved via
   * `IcebergShimUtils.openParquetReader`: 1.10.x overrides it to cache via `FileCache`, while
   * 1.6.x / 1.9.x inherit the no-cache default (their shaded parquet has no way to inject a
   * pre-parsed footer).
   */
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    ShimUtils.openParquetReader(inputFile, filePath, options, metrics)
  }
}
