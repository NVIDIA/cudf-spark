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
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.GpuDsv2WriteMetadata
import com.nvidia.spark.rapids.GpuWrite
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillableColumnarBatch
import com.nvidia.spark.rapids.SpillPriorities

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GpuDelteWritingSparkTask.filterByOperation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuReplaceDataExec(
    inner: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] = {
    // Match CPU ReplaceDataExec: use metadata-aware task when metadataProjection is present
    // (SPARK-50820 / DataAndMetadataWritingSparkTask).
    projections match {
      case ReplaceDataProjections(dataProj, Some(metadataProj)) =>
        GpuDataAndMetadataWritingSparkTask(dataProj, metadataProj)
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
 * GPU counterpart of Spark's DataAndMetadataWritingSparkTask.
 * Routes WRITE_WITH_METADATA_OPERATION through [[DataWriter.write(Object, Object)]] and
 * WRITE_OPERATION through [[DataWriter.write(Object)]].
 */
case class GpuDataAndMetadataWritingSparkTask(
    dataProj: ProjectingInternalRow,
    metadataProj: ProjectingInternalRow)
  extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(dataProj)
  private lazy val rowDataTypes = dataProj.schema.fields.map(_.dataType)
  private lazy val metadataProjection = GpuProjectingColumnarBatch(metadataProj)
  private lazy val metadataDataTypes = metadataProj.schema.fields.map(_.dataType)

  override protected def write(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    // Separate retry scopes per writer side-effect. A single withRetryNoSplit around both
    // phases would replay a completed write(metadata, data) if ordinary-row filtering OOMs.
    withResource(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)) { scb =>
      withRetryNoSplit {
        withResource(scb.getColumnarBatch()) { b =>
          writeMetadataRows(writer, b)
        }
      }
      withRetryNoSplit {
        withResource(scb.getColumnarBatch()) { b =>
          writeOrdinaryRows(writer, b)
        }
      }
    }
  }

  private def writeMetadataRows(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val withMetadataFilter = filterByOperation(batch, WRITE_WITH_METADATA_OPERATION)
    withResource(withMetadataFilter) { _ =>
      withResource(rowProjection.project(batch)) { rows =>
        val dataBatch = GpuColumnVector.filter(rows, rowDataTypes, withMetadataFilter)
        closeOnExcept(dataBatch) { _ =>
          if (dataBatch.numRows() > 0) {
            val metadataBatch = withResource(metadataProjection.project(batch)) { metadata =>
              GpuColumnVector.filter(metadata, metadataDataTypes, withMetadataFilter)
            }
            GpuDsv2WriteMetadata.withMetadataSchema(metadataProj.schema) {
              writer.write(metadataBatch, dataBatch)
            }
          } else {
            dataBatch.close()
          }
        }
      }
    }
  }

  private def writeOrdinaryRows(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val writeFilter = filterByOperation(batch, WRITE_OPERATION)
    withResource(writeFilter) { _ =>
      withResource(rowProjection.project(batch)) { rows =>
        val dataBatch = GpuColumnVector.filter(rows, rowDataTypes, writeFilter)
        if (dataBatch.numRows() > 0) {
          writer.write(dataBatch)
        } else {
          dataBatch.close()
        }
      }
    }
  }
}
