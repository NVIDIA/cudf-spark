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
/*** spark-rapids-shim-json-lines
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuWrite
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{
  COPY_OPERATION, INSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GpuDelteWritingSparkTask.{
  firstOperation, splitIntoContiguousOperationRuns}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuReplaceDataExec(
    inner: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] = {
    // Spark 4.2 DataAndMetadataWritingSparkTask: UPDATE/COPY use write(metadata, data),
    // INSERT uses write(data).
    projections match {
      case ReplaceDataProjections(dataProj, Some(metadataProj)) =>
        GpuDataAndMetadataWritingSpark420Task(dataProj, metadataProj)
      case _ =>
        GpuReplaceDataWritingSparkTask(projections)
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuReplaceDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuReplaceDataExec = {
    copy(inner = newChild)
  }
}

case class GpuReplaceDataWritingSparkTask(
    projs: ReplaceDataProjections)
  extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(projs.rowProjection)
  override protected def write(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    withResource(rowProjection.project(batch)) { projected =>
      writer.write(projected)
    }
  }
}

/**
 * GPU counterpart of Spark 4.2's DataAndMetadataWritingSparkTask.
 * Preserves contiguous operation order and isolates each run in its own retry scope.
 */
case class GpuDataAndMetadataWritingSpark420Task(
    dataProj: ProjectingInternalRow,
    metadataProj: ProjectingInternalRow)
  extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(dataProj)
  private lazy val metadataProjection = GpuProjectingColumnarBatch(metadataProj)

  override protected def write(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    withResource(splitIntoContiguousOperationRuns(batch)) { runs =>
      runs.foreach { run =>
        withRetryNoSplit {
          withResource(run.getColumnarBatch()) { b =>
            firstOperation(b) match {
              case UPDATE_OPERATION | COPY_OPERATION => writeWithMetadata(writer, b)
              case INSERT_OPERATION => writeOrdinary(writer, b)
              case other =>
                throw new IllegalStateException(s"Unexpected replace-data operation: $other")
            }
          }
        }
      }
    }
  }

  private def writeWithMetadata(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val dataBatch = rowProjection.project(batch)
    closeOnExcept(dataBatch) { _ =>
      if (dataBatch.numRows() > 0) {
        val metadataBatch = metadataProjection.project(batch)
        writer.write(metadataBatch, dataBatch)
      } else {
        dataBatch.close()
      }
    }
  }

  private def writeOrdinary(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val dataBatch = rowProjection.project(batch)
    if (dataBatch.numRows() > 0) {
      writer.write(dataBatch)
    } else {
      dataBatch.close()
    }
  }
}
