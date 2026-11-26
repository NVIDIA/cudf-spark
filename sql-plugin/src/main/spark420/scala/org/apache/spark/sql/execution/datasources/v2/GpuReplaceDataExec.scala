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
{"spark":["420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.{GpuColumnVector, GpuDataWriter, GpuDataWriterFactory, GpuMetric,
  GpuWrite}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.write.{DataWriterFactory, RowLevelOperation}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuReplaceDataExec(
    inner: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: GpuWrite,
    override val rowLevelCommand: Option[RowLevelOperation.Command] = None)
  extends GpuRowLevelWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] =
    GpuReplaceDataWritingSparkTask(projections, GpuMetric.unwrap(rowLevelMetrics))

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuReplaceDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuReplaceDataExec = {
    copy(inner = newChild)
  }
}

case class GpuReplaceDataWritingSparkTask(
    projs: ReplaceDataProjections,
    sparkMetrics: Map[String, SQLMetric] = Map.empty)
  extends GpuWritingSparkTask[GpuDataWriter] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(projs.rowProjection)
  private lazy val metadataProjection = projs.metadataProjection.map(GpuProjectingColumnarBatch(_))

  override protected def createWriter(
      writerFactory: DataWriterFactory,
      partitionId: Int,
      taskId: Long): GpuDataWriter = {
    writerFactory.asInstanceOf[GpuDataWriterFactory]
      .createWriter(partitionId, taskId, metadataProjection.map(_.schema).orNull)
      .asInstanceOf[GpuDataWriter]
  }

  // Copy only the 4-byte operation column to host once to avoid per-batch GPU reduction
  // allocations and multiple stream-synchronizing Scalar.getLong round trips.
  private def countOperations(
      batch: ColumnarBatch,
      trackUpdated: Boolean,
      trackCopied: Boolean): (Long, Long) = {
    val numRows = batch.numRows()
    val opCol = batch.column(0).asInstanceOf[GpuColumnVector].getBase
    if (opCol.getNullCount == numRows) {
      (0L, 0L)
    } else {
      withRetryNoSplit {
        withResource(opCol.copyToHost()) { hcv =>
          var updated = 0L
          var copied = 0L
          if (hcv.getNullCount == 0L) {
            val buf = hcv.getData
            var offset = 0L
            val end = numRows.toLong * 4L
            while (offset < end) {
              buf.getInt(offset) match {
                case UPDATE_OPERATION => updated += 1L
                case COPY_OPERATION => copied += 1L
                case _ =>
              }
              offset += 4L
            }
          } else {
            var i = 0
            while (i < numRows) {
              if (!hcv.isNull(i)) {
                hcv.getInt(i) match {
                  case UPDATE_OPERATION => updated += 1L
                  case COPY_OPERATION => copied += 1L
                  case _ =>
                }
              }
              i += 1
            }
          }
          (if (trackUpdated) updated else 0L, if (trackCopied) copied else 0L)
        }
      }
    }
  }

  override protected def write(
      writer: GpuDataWriter,
      batch: ColumnarBatch): Unit = {
    val trackUpdated = sparkMetrics.contains(GpuRowLevelWriteExec.NUM_UPDATED_ROWS)
    val trackCopied = sparkMetrics.contains(GpuRowLevelWriteExec.NUM_COPIED_ROWS)
    val (numUpdated, numCopied) = withResource(batch) { _ =>
      val counts = if (batch.numRows() > 0 && (trackUpdated || trackCopied)) {
        countOperations(batch, trackUpdated, trackCopied)
      } else {
        (0L, 0L)
      }

      // Preserve the full-batch write order; RewriteMergeIntoTable already nullifies INSERT
      // metadata columns so passing metadata uniformly matches row-lineage expectations.
      metadataProjection match {
        case Some(projection) =>
          withResource(rowProjection.project(batch)) { projected =>
            withResource(projection.project(batch)) { metadata =>
              writer.write(metadata, projected)
            }
          }
        case None =>
          // DataWriter.write(ColumnarBatch) takes ownership of the projected batch.
          writer.write(rowProjection.project(batch))
      }
      counts
    }

    if (trackUpdated) {
      sparkMetrics(GpuRowLevelWriteExec.NUM_UPDATED_ROWS).add(numUpdated)
    }
    if (trackCopied) {
      sparkMetrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS).add(numCopied)
    }
  }

  override protected def onTaskComplete(): Unit = {
    sparkMetrics.get(GpuRowLevelWriteExec.NUM_UPDATED_ROWS).foreach(_.add(0L))
    sparkMetrics.get(GpuRowLevelWriteExec.NUM_COPIED_ROWS).foreach(_.add(0L))
  }
}
