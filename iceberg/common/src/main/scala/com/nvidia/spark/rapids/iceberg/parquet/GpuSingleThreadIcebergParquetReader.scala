/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}

import scala.annotation.tailrec

import com.nvidia.spark.rapids.{DateTimeRebaseCorrected, PartitionReaderWithBytesRead}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, ParquetPartitionReader}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSingleThreadIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val gpuDeleteProvider: IcebergPartitionedFile => Option[GpuDeleteFilter],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader  {

  private val taskIterator = files.iterator
  private var gpuDeleteFilter: Option[GpuDeleteFilter] = _
  private var parquetIterator: SingleFileReader = _
  private var dataIterator: Iterator[ColumnarBatch] = _

  override def hasNext: Boolean = {
    ensureParquetReader()
    if (parquetIterator == null) {
      return false
    }
    dataIterator.hasNext
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more elements")
    }
    dataIterator.next()
  }

  @tailrec
  private def ensureParquetReader(): Unit = {
    if (parquetIterator == null) {
      if (taskIterator.hasNext) {
        val file = taskIterator.next()

        gpuDeleteFilter = gpuDeleteProvider(file)
        parquetIterator = new SingleFileReader(rapidsFileIO, file, constantsProvider(file),
          gpuDeleteFilter, conf)
        dataIterator = gpuDeleteFilter
          .map(_.filterAndDelete(parquetIterator))
          .getOrElse(parquetIterator)
        // update the current file for Spark's filename() function
        InputFileUtils.setInputFileBlock(file.path.toString, file.start, file.length)
      }
    } else {
      if (!parquetIterator.hasNext) {
        withResource(parquetIterator) { _ =>
          parquetIterator = null
          withResource(gpuDeleteFilter) { _ =>
            gpuDeleteFilter = None
          }
        }
        ensureParquetReader()
      }
    }
  }

  override def close(): Unit = {
    if (parquetIterator != null) {
      withResource(parquetIterator) { _ =>
        parquetIterator = null
        withResource(gpuDeleteFilter) { _ =>
          gpuDeleteFilter = None
        }
      }
    }
  }
}

private class SingleFileReader(
    val rapidsFileIO: IcebergFileIO,
    val file: IcebergPartitionedFile,
    val idToConstant: JMap[Integer, _],
    val deleteFilter: Option[GpuDeleteFilter],
    override val conf: GpuIcebergParquetReaderConf)
  extends GpuIcebergParquetReader {

  private var inited = false
  private lazy val (reader, postProcessor) = open()

  override def close(): Unit = {
    if (inited) {
      withResource(reader) { _ => }
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = {
    postProcessor.process(reader.get())
  }

  private def open() = {
    val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

    val (filteredParquet, shadedFileReadSchema) = super.filterParquetBlocks(file, requiredSchema)

    val parquetPartReader = new ParquetPartitionReader(
      rapidsFileIO,
      conf.conf,
      file.sparkPartitionedFile,
      new Path(file.file.getDelegate.location()),
      filteredParquet.blocks,
      filteredParquet.schema,
      conf.caseSensitive,
      filteredParquet.readSchema,
      conf.parquetDebugDumpPrefix,
      conf.parquetDebugDumpAlways,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      conf.targetBatchSizeBytes,
      conf.useChunkedReader,
      conf.maxChunkedReaderMemoryUsageSizeBytes,
      CpuCompressionConfig.disabled(),
      conf.metrics,
      DateTimeRebaseCorrected, // dateRebaseMode
      DateTimeRebaseCorrected, // timestampRebaseMode
      true, // hasInt96Timestamps
      false) // useFieldId

    val parquetReader = new PartitionReaderWithBytesRead(parquetPartReader)
    val postProcessor = new GpuParquetReaderPostProcessor(filteredParquet,
      idToConstant,
      requiredSchema,
      shadedFileReadSchema,
      conf.metrics)

    inited = true
    (parquetReader, postProcessor)
  }
}
