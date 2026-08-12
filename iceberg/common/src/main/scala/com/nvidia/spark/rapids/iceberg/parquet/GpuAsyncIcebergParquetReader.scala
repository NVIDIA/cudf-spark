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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.parquet.async._
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, ParquetPartitionReaderBase}
import com.nvidia.spark.rapids.reader.UnifiedReader
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iceberg implementation of the asynchronous Parquet reader framework.
 *
 * Footer filtering, packed fragment reads, combining, and GPU decode are concrete components.
 * The format-neutral UnifiedReader only connects their futures and drives decoded batches from
 * the Spark task thread.
 */
class GpuAsyncIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf,
    workerThreads: Int) extends GpuIcebergParquetReader {

  private val closed = new AtomicBoolean()
  private val readerRef =
    new AtomicReference[
      UnifiedReader[IcebergPartitionedFile, FooterResult, FileFragment, ParquetCombinedResult]]()
  private lazy val asyncReader = {
    val reader = createReader()
    readerRef.set(reader)
    // A task-completion close can race lazy construction. Whichever side observes the reader owns
    // the one close; a close that happened before publication is honored here.
    if (closed.get() && readerRef.compareAndSet(reader, null)) {
      reader.close()
    }
    reader
  }

  override def hasNext: Boolean = asyncReader.hasNext

  override def next(): ColumnarBatch = asyncReader.next()

  override def close(): Unit = {
    closed.set(true)
    Option(readerRef.getAndSet(null)).foreach(_.close())
  }

  private def createReader()
  : UnifiedReader[IcebergPartitionedFile, FooterResult, FileFragment, ParquetCombinedResult] = {
    val executor = ParquetReaderThreadPool.getOrCreate(workerThreads).executor()
    val planner = new IcebergParquetPlanner(this, TaskContext.get(), executor, closed)
    new UnifiedReader(
      files.asJava,
      planner,
      executor)
  }

  /**
   * Build cuDF Parquet options through the shared plugin column-selection implementation.
   *
   * Package visibility lets the initialization-order regression test exercise the same lazy
   * helper used by the decode path without allocating a GPU buffer.
   */
  private[parquet] def asyncParquetOptions(
      readSchema: StructType,
      clippedSchema: MessageType) = {
    DecodeSupport.parquetOptions(readSchema, clippedSchema)
  }

  /** Reuses the plugin's Parquet column-name selection without duplicating field-ID logic. */
  private object DecodeSupport extends ParquetPartitionReaderBase {
    // These must be methods, not eager vals. ParquetPartitionReaderBase initializes
    // copyBufferSize in its trait constructor by calling conf; an override val would still have
    // its JVM default value (null) at that point in Scala 2.12.
    override def fileIO = rapidsFileIO
    override def conf = GpuAsyncIcebergParquetReader.this.conf.conf
    override def execMetrics = GpuAsyncIcebergParquetReader.this.conf.metrics
    override def isSchemaCaseSensitive = GpuAsyncIcebergParquetReader.this.conf.caseSensitive
    override def compressCfg = CpuCompressionConfig.disabled()

    def parquetOptions(readSchema: StructType, clippedSchema: MessageType) = {
      getParquetOptions(readSchema, clippedSchema, useFieldId = false)
    }
  }
}
