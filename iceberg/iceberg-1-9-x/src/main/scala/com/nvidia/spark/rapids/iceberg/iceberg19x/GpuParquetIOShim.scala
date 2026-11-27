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

import java.lang.reflect.{Field, InvocationTargetException}
import java.util.{ArrayList, Collections, List => JList}

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.parquet.converter.ToIcebergShaded
import com.nvidia.spark.rapids.parquet.{HMBInputFile, ParquetFooterUtils}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.parquet.GpuParquetIO
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.filter2.compat.FilterCompat
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.BlockMetaData

/** Caches 1.9.x footers read through PerfIO, retaining native streams for dictionaries. */
object GpuParquetIOShim {
  // Parquet bundled with Iceberg 1.9 lacks 1.10's constructor accepting parsed metadata.
  // Initialize a reader from the cached footer, then attach the real file before filtering.
  private def readerField(name: String): Field = {
    val field = classOf[ParquetFileReader].getDeclaredField(name)
    field.setAccessible(true)
    field
  }

  private val fileField = readerField("file")
  private val streamField = readerField("f")
  private val optionsField = readerField("options")
  private val blocksField = readerField("blocks")
  private val blockCacheFields = Seq(readerField("blockIndexStores"), readerField("blockRowRanges"))
  private val filterRowGroups = {
    val method = classOf[ParquetFileReader].getDeclaredMethod("filterRowGroups", classOf[JList[_]])
    method.setAccessible(true)
    method
  }

  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    withResource(ParquetFooterUtils.getFooterBuffer(inputFile, metrics,
        ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath))) { buffer =>
      val footerFile = ToIcebergShaded.shade(new HMBInputFile(buffer))
      // Dictionary and bloom filtering need the real stream; split filtering still applies
      // while parsing the footer. Retain the original allocator, codecs and decryption options.
      val footerOptions = ParquetReadOptions.builder().copy(options)
        .withRecordFilter(FilterCompat.NOOP).build()
      withResource(footerFile.newStream()) { footerStream =>
        closeOnExcept(new ParquetFileReader(footerFile, footerOptions, footerStream)) { reader =>
          val realFile = GpuParquetIO.file(inputFile.getDelegate)
          closeOnExcept(realFile.newStream()) { stream =>
            fileField.set(reader, realFile)
            optionsField.set(reader, options)
            // Transfer ownership last. Subsequent failures close the stream through the reader.
            streamField.set(reader, stream)
          }
          val blocks = try {
            filterRowGroups.invoke(reader, reader.getFooter.getBlocks)
              .asInstanceOf[JList[BlockMetaData]]
          } catch {
            case e: InvocationTargetException => throw e.getCause
          }
          blocksField.set(reader, blocks)
          blockCacheFields.foreach { field =>
            field.set(reader, new ArrayList[AnyRef](Collections.nCopies(blocks.size(), null)))
          }
          reader
        }
      }
    }
  }
}
