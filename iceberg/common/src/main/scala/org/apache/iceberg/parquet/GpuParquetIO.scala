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

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.ShimUtils
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import com.nvidia.spark.rapids.parquet.ParquetFooterUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile => ShadedInputFile, SeekableInputStream => ShadedInputStream}

object GpuParquetIO {
  def file(file: InputFile): ShadedInputFile = {
    GpuParquetIOAccess.file(file)
  }

  def file(inputFile: RapidsInputFile): ShadedInputFile = new ShadedInputFile {
    override def getLength: Long = inputFile.getLength
    override def newStream(): ShadedInputStream = stream(inputFile.open())
    override def toString: String = inputFile.path()
  }

  private[parquet] def stream(input: SeekableInputStream): ShadedInputStream = {
    new DelegatingSeekableInputStream(input) {
      override def getPos: Long = input.getPos
      override def seek(position: Long): Unit = input.seek(position)
    }
  }

  /**
   * Older Parquet versions cannot accept parsed metadata. Supply the cached footer at its
   * original file offsets instead, preserving row-group offsets and split filtering. Copying
   * the small footer to the heap lets the native cache buffer be released before construction,
   * including when Parquet rejects the footer. The underlying stream is opened only on a body
   * read (for example, a dictionary page).
   */
  def openReaderWithCachedFooter(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    val footer = withResource(ParquetFooterUtils.getFooterBuffer(
        inputFile, metrics,
        ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath))) { buffer =>
      ParquetFooterUtils.readBytesFromBuffer(buffer, 0, Math.toIntExact(buffer.getLength))
    }
    ParquetFileReader.open(new GpuParquetCachedFooterInputFile(inputFile, footer), options)
  }

  /**
   * Open a shaded `ParquetFileReader` using the version-appropriate footer-cache adapter.
   */
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    ShimUtils.openParquetReader(inputFile, filePath, options, metrics)
  }
}
