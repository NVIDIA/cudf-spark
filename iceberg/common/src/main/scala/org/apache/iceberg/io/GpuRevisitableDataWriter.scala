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

package org.apache.iceberg.io

import java.util.ArrayList

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, PartitionSpec, StructLike}
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory
import org.apache.iceberg.util.{StructLikeUtil, StructLikeWrapper}

/** Keeps one partition writer open and creates a new file when a partition is revisited. */
class GpuRevisitableDataWriter(
    writerFactory: GpuSparkFileWriterFactory,
    fileFactory: OutputFileFactory,
    io: FileIO,
    targetFileSize: Long)
  extends PartitioningWriter[SpillableColumnarBatch, DataWriteResult] {

  private val dataFiles = new ArrayList[DataFile]()
  private var currentSpecId = -1
  private var currentPartition: StructLikeWrapper = _
  private var currentWriter: FileWriter[SpillableColumnarBatch, DataWriteResult] = _
  private var closed = false

  protected def newWriter(spec: PartitionSpec, partition: StructLike):
      FileWriter[SpillableColumnarBatch, DataWriteResult] = {
    new GpuRollingDataWriter(writerFactory, fileFactory, io, targetFileSize, spec, partition)
  }

  override def write(
      batch: SpillableColumnarBatch,
      spec: PartitionSpec,
      partition: StructLike): Unit = {
    require(!closed, "Cannot write to a closed writer")
    if (currentWriter == null || currentSpecId != spec.specId() ||
        currentPartition != currentPartition.copyFor(partition)) {
      closeCurrentWriter()
      currentSpecId = spec.specId()
      // Keep a snapshot: callers may reuse their partition object for the next batch.
      currentPartition = StructLikeWrapper.forType(spec.partitionType())
        .set(StructLikeUtil.copy(partition))
      currentWriter = newWriter(spec, currentPartition.get())
    }
    currentWriter.write(batch)
  }

  private def closeCurrentWriter(): Unit = {
    if (currentWriter != null) {
      currentWriter.close()
      dataFiles.addAll(currentWriter.result().dataFiles())
      currentWriter = null
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closeCurrentWriter()
      closed = true
    }
  }

  override def result(): DataWriteResult = {
    require(closed, "Cannot get result from unclosed writer")
    new DataWriteResult(dataFiles)
  }
}
