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

package org.apache.iceberg.io

import java.util.function.{BiFunction, Consumer, Supplier}

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, DeleteFile, PartitionSpec, StructLike}
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory
import org.apache.iceberg.util.CharSequenceSet

private[io] abstract class GpuFanoutWriter[R]
    extends PartitioningWriter[SpillableColumnarBatch, R] {
  protected def newWriter(partitionSpec: PartitionSpec, partition: StructLike):
      FileWriter[SpillableColumnarBatch, R]
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  private val delegate: PartitioningWriter[SpillableColumnarBatch, R] =
    new GpuFanoutWriterBridge[SpillableColumnarBatch, R](
      new BiFunction[PartitionSpec, StructLike, FileWriter[SpillableColumnarBatch, R]] {
        override def apply(
            partitionSpec: PartitionSpec,
            partition: StructLike): FileWriter[SpillableColumnarBatch, R] =
          newWriter(partitionSpec, partition)
      },
      new Consumer[R] {
        override def accept(result: R): Unit = addResult(result)
      },
      new Supplier[R] {
        override def get(): R = aggregatedResult()
      })

  override def write(
      batch: SpillableColumnarBatch,
      partitionSpec: PartitionSpec,
      partition: StructLike): Unit = {
    delegate.write(batch, partitionSpec, partition)
  }

  override def close(): Unit = {
    delegate.close()
  }

  override def result(): R = {
    delegate.result()
  }
}

class GpuFanoutDataWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long) extends GpuFanoutWriter[DataWriteResult] {
  private val dataFiles = Lists.newArrayList[DataFile]()

  override def newWriter(partitionSpec: PartitionSpec, structLike: StructLike):
  FileWriter[SpillableColumnarBatch, DataWriteResult] = {
    new GpuRollingDataWriter(
      writerFactory,
      fileFactory,
      io,
      targetFileSize,
      partitionSpec,
      structLike)
  }

  override def addResult(r: DataWriteResult): Unit = {
    dataFiles.addAll(r.dataFiles())
  }

  override def aggregatedResult(): DataWriteResult = {
    new DataWriteResult(dataFiles)
  }
}

class GpuFanoutPositionDeleteWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long) extends GpuFanoutWriter[DeleteWriteResult] {

  private val deleteFiles = Lists.newArrayList[DeleteFile]()
  private val referencedDataFiles = CharSequenceSet.empty()

  override def newWriter(partitionSpec: PartitionSpec, structLike: StructLike):
  FileWriter[SpillableColumnarBatch, DeleteWriteResult] = new GpuRollingPositionDeleteWriter(
    writerFactory, fileFactory, io, targetFileSize, partitionSpec, structLike)

  override def addResult(r: DeleteWriteResult): Unit = {
    deleteFiles.addAll(r.deleteFiles())
    referencedDataFiles.addAll(r.referencedDataFiles())
  }

  override def aggregatedResult(): DeleteWriteResult = {
    new DeleteWriteResult(deleteFiles, referencedDataFiles)
  }
}
