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

package org.apache.iceberg.io

import java.nio.ByteBuffer
import java.util.{List => JList}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuColumnVector, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.iceberg._
import org.apache.iceberg.MetadataColumns.{DELETE_FILE_PATH, DELETE_FILE_POS}
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.io.GpuPositionDeleteFileWriter.FILE_AND_POS_FIELD_IDS
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory
import org.apache.iceberg.util.CharSequenceSet



trait GpuRollingFileWriter[W <: FileWriter[SpillableColumnarBatch, R], R] extends
  FileWriter[SpillableColumnarBatch, R] {

  val fileFactory: OutputFileFactory
  val io: FileIO
  val targetFileSize: Long
  val spec: PartitionSpec
  val partition: StructLike

  private var currentFile: Option[EncryptedOutputFile] = None
  private var currentFileRows: Long = 0L
  private var currentWriter: Option[W] = None

  private var closed: Boolean = false

  protected def newWriter(file: EncryptedOutputFile): W
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  override def length(): Long = {
    throw new UnsupportedOperationException("length is not supported" +
      " in GpuRollingFileWriter")
  }

  override def write(batch: SpillableColumnarBatch): Unit = {
    if (closed) {
      throw new IllegalStateException("Cannot write to a closed writer")
    }

    if (currentWriter.isEmpty) {
      openCurrentWriter()
    }

    currentWriter.get.write(batch)
    currentFileRows += batch.numRows()

    if (currentWriter.get.length() >= targetFileSize) {
      closeCurrentWriter()
      openCurrentWriter()
    }
  }

  protected def openCurrentWriter(): Unit = {
    require(currentWriter.isEmpty,
      "Current writer should be empty when opening a new writer")

    currentFile = Some(newFile())
    currentWriter = Some(newWriter(currentFile.get))
    currentFileRows = 0L
  }

  private def newFile(): EncryptedOutputFile = {
    if (spec.isUnpartitioned || partition == null) {
      fileFactory.newOutputFile
    } else {
      fileFactory.newOutputFile(spec, partition)
    }
  }

  private def closeCurrentWriter(): Unit = {
    currentWriter.foreach { w =>
      w.close()

      if (currentFileRows == 0L) {
        io.deleteFile(currentFile.get.encryptingOutputFile)
      } else {
        addResult(w.result())
      }

      this.currentFile = None
      this.currentFileRows = 0
      this.currentWriter = None
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closeCurrentWriter()
      this.closed = true
    }
  }

  override def result: R = {
    require(closed, "Cannot get result from unclosed writer")
    aggregatedResult()
  }
}

class GpuRollingDataWriter(
                            val gpuSparkFileWriterFactory: GpuSparkFileWriterFactory,
                            val fileFactory: OutputFileFactory,
                            val io: FileIO,
                            val targetFileSize: Long,
                            val spec: PartitionSpec,
                            val partition: StructLike) extends
  GpuRollingFileWriter[DataWriter[SpillableColumnarBatch], DataWriteResult] {

  private val dataFiles: JList[DataFile] = Lists.newArrayList[DataFile]()

  protected override def newWriter(file: EncryptedOutputFile): DataWriter[SpillableColumnarBatch] =
  {
    gpuSparkFileWriterFactory.newDataWriter(file, spec, partition)
  }

  protected override def addResult(result: DataWriteResult): Unit = {
    dataFiles.addAll(result.dataFiles())
  }

  protected override def aggregatedResult(): DataWriteResult = {
    new DataWriteResult(dataFiles)
  }
}

class GpuRollingPositionDeleteWriter(
                                      val gpuSparkFileWriterFactory: GpuSparkFileWriterFactory,
                                      val fileFactory: OutputFileFactory,
                                      val io: FileIO,
                                      val targetFileSize: Long,
                                      val spec: PartitionSpec,
                                      val partition: StructLike)
  extends GpuRollingFileWriter[GpuPositionDeleteFileWriter, DeleteWriteResult] {

  private val deleteFiles: JList[DeleteFile] = Lists.newArrayList[DeleteFile]()
  private val referenceDataFiles = CharSequenceSet.empty()


  override protected def newWriter(file: EncryptedOutputFile): GpuPositionDeleteFileWriter = {
    gpuSparkFileWriterFactory.newGpuPositionDeleteWriter(file, spec, partition)
  }

  override protected def addResult(result: DeleteWriteResult): Unit = {
    deleteFiles.addAll(result.deleteFiles())
    referenceDataFiles.addAll(result.referencedDataFiles())
  }

  override protected def aggregatedResult(): DeleteWriteResult = {
    new DeleteWriteResult(deleteFiles, referenceDataFiles)
  }
}

class GpuPositionDeleteFileWriter(
                                   val appender: FileAppender[SpillableColumnarBatch],
                                   val format: FileFormat,
                                   val location: String,
                                   val spec: PartitionSpec,
                                   val partition: StructLike,
                                   val keyMetadata: ByteBuffer)
  extends FileWriter[SpillableColumnarBatch, DeleteWriteResult] {

  val referencedDataFiles: CharSequenceSet = CharSequenceSet.empty()

  private var deleteFile: DeleteFile = _

  override def write(t: SpillableColumnarBatch): Unit = {
    withResource(t.getColumnarBatch()) { batch =>
      val pathCol = batch.column(0).asInstanceOf[GpuColumnVector]
        .copyToHost()
      withResource(pathCol) { _ =>
        for (row <- 0 until batch.numRows()) {
          referencedDataFiles.add(pathCol.getUTF8String(row).toString)
        }
      }
    }
    appender.add(t)
  }

  override def length(): Long = appender.length()

  override def result(): DeleteWriteResult = {
    require(deleteFile != null, "Cannot get result from unclosed writer")
    new DeleteWriteResult(deleteFile, referencedDataFiles)
  }

  override def close(): Unit = {
    if (deleteFile == null) {
      appender.close()
      this.deleteFile = FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withFormat(format)
        .withPath(location)
        .withPartition(partition)
        .withEncryptionKeyMetadata(keyMetadata)
        .withSplitOffsets(appender.splitOffsets())
        .withFileSizeInBytes(appender.length())
        .withMetrics(metrics())
        .build()
    }
  }

  private def metrics(): Metrics = {
    val metrics = appender.metrics()
    if (referencedDataFiles.size() > 1) {
      MetricsUtil.copyWithoutFieldCountsAndBounds(metrics, FILE_AND_POS_FIELD_IDS.asJava)
    } else {
      MetricsUtil.copyWithoutFieldCounts(metrics, FILE_AND_POS_FIELD_IDS.asJava)
    }
  }
}

object GpuPositionDeleteFileWriter {
  private[io] val FILE_AND_POS_FIELD_IDS: Set[Integer] = Set(
    DELETE_FILE_PATH.fieldId(), DELETE_FILE_POS.fieldId())
}

