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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDataWriter, GpuDeltaWriter,
  RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, DELETE_OPERATION,
  INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRowLevelWritingSparkTaskSuite extends RmmSparkRetrySuiteBase {
  private val rowSchema = new StructType().add("partition", IntegerType).add("value", LongType)
  private val rowIdSchema = new StructType().add("value", LongType)
  private val metadataSchema = new StructType().add("_row_id", LongType)

  private def newMetric(): SQLMetric = new SQLMetric("sum")

  private class RecordingDataWriter extends GpuDataWriter {
    var writtenRows = 0L
    var metadataCalls = 0
    var plainCalls = 0

    override def write(record: ColumnarBatch): Unit = {
      plainCalls += 1
      writtenRows += record.numRows()
    }

    override def write(metadata: ColumnarBatch, record: ColumnarBatch): Unit = {
      metadataCalls += 1
      writtenRows += record.numRows()
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = ()
    override def close(): Unit = ()
  }

  private class RecordingDeltaWriter extends GpuDeltaWriter {
    var deletedRows = 0L
    var updatedRows = 0L
    var insertedOrReinsertedRows = 0L

    override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
      withResource(Seq(metadata, rowId)) { _ => deletedRows += rowId.numRows() }
    }

    override def update(
        metadata: ColumnarBatch,
        rowId: ColumnarBatch,
        row: ColumnarBatch): Unit = {
      withResource(Seq(metadata, rowId, row)) { _ => updatedRows += row.numRows() }
    }

    override def insert(row: ColumnarBatch): Unit = {
      withResource(row) { _ => insertedOrReinsertedRows += row.numRows() }
    }

    override def insertAndReinsert(
        metadata: ColumnarBatch,
        row: ColumnarBatch,
        reinsertMask: CudfColumnVector): Unit = {
      withResource(Seq(metadata, row, reinsertMask)) { _ =>
        insertedOrReinsertedRows += row.numRows()
      }
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = ()
    override def close(): Unit = ()
  }

  private class ReplaceTask(
      projs: ReplaceDataProjections,
      metrics: Map[String, SQLMetric])
    extends GpuReplaceDataWritingSparkTask(projs, metrics) {
    def writeBatch(writer: GpuDataWriter, batch: ColumnarBatch): Unit = write(writer, batch)
    def finishTask(): Unit = onTaskComplete()
  }

  private class DeltaPlainTask(
      projs: WriteDeltaProjections,
      metrics: Map[String, SQLMetric])
    extends GpuDeltaWritingSparkTask(projs, metrics) {
    def writeBatch(writer: GpuDeltaWriter, batch: ColumnarBatch): Unit = write(writer, batch)
    def finishTask(): Unit = onTaskComplete()
  }

  private class DeltaMetadataTask(
      projs: WriteDeltaProjections,
      metrics: Map[String, SQLMetric])
    extends GpuDeltaWithMetadataWritingSparkTask(projs, metrics) {
    def writeBatch(writer: GpuDeltaWriter, batch: ColumnarBatch): Unit = write(writer, batch)
    def finishTask(): Unit = onTaskComplete()
  }

  private def buildReplaceBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(Int.box(UPDATE_OPERATION), COPY_OPERATION, UPDATE_OPERATION,
        COPY_OPERATION, COPY_OPERATION, INSERT_OPERATION)
      .column(Int.box(0), 0, 1, 1, 1, 2)
      .column(Long.box(10L), 20L, 30L, 40L, 50L, 60L)
      .column(Long.box(100L), 200L, 300L, 400L, 500L, null.asInstanceOf[java.lang.Long])
      .build()) { table =>
      GpuColumnVector.from(table,
        Array[DataType](IntegerType, IntegerType, LongType, LongType))
    }
  }

  private def buildDeltaBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(Int.box(DELETE_OPERATION), DELETE_OPERATION, UPDATE_OPERATION,
        REINSERT_OPERATION, REINSERT_OPERATION, INSERT_OPERATION)
      .column(Int.box(0), 0, 1, 1, 2, 2)
      .column(Long.box(10L), 20L, 30L, 40L, 50L, 60L)
      .column(Long.box(100L), 200L, 300L, 400L, 500L, null.asInstanceOf[java.lang.Long])
      .build()) { table =>
      GpuColumnVector.from(table,
        Array[DataType](IntegerType, IntegerType, LongType, LongType))
    }
  }

  Seq(false, true).foreach { withMetadata =>
    test(s"GpuReplaceDataWritingSparkTask counts UPDATE and COPY rows (metadata=$withMetadata)") {
      val projs = ReplaceDataProjections(
        ProjectingInternalRow(rowSchema, Seq(1, 2)),
        if (withMetadata) Some(ProjectingInternalRow(metadataSchema, Seq(3))) else None)
      val updatedMetric = newMetric()
      val copiedMetric = newMetric()
      val metrics = Map(
        GpuRowLevelWriteExec.NUM_UPDATED_ROWS -> updatedMetric,
        GpuRowLevelWriteExec.NUM_COPIED_ROWS -> copiedMetric)
      val writer = new RecordingDataWriter
      val task = new ReplaceTask(projs, metrics)

      withResource(buildReplaceBatch()) { batch =>
        RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
        task.writeBatch(writer, batch)
      }
      task.finishTask()

      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
      assert(writer.writtenRows === 6L)
      assert(updatedMetric.value === 2L)
      assert(copiedMetric.value === 3L)
    }
  }

  test("GpuReplaceDataWritingSparkTask DELETE mode tracks only numCopiedRows") {
    val projs = ReplaceDataProjections(ProjectingInternalRow(rowSchema, Seq(1, 2)), None)
    val copiedMetric = newMetric()
    val metrics = Map(GpuRowLevelWriteExec.NUM_COPIED_ROWS -> copiedMetric)
    val writer = new RecordingDataWriter
    val task = new ReplaceTask(projs, metrics)

    withResource(buildReplaceBatch()) { batch =>
      task.writeBatch(writer, batch)
    }
    task.finishTask()

    assert(writer.writtenRows === 6L)
    assert(copiedMetric.value === 3L)
  }

  Seq(false, true).foreach { withMetadata =>
    test(s"GpuDeltaWritingSparkTask counts UPDATE+REINSERT and DELETE (metadata=$withMetadata)") {
      val projs = WriteDeltaProjections(
        Some(ProjectingInternalRow(rowSchema, Seq(1, 2))),
        ProjectingInternalRow(rowIdSchema, Seq(2)),
        if (withMetadata) Some(ProjectingInternalRow(metadataSchema, Seq(3))) else None)

      // UPDATE mode: count UPDATE_OPERATION (1) + REINSERT_OPERATION (2) = 3; exclude INSERT/DELETE
      val updatedMetric = newMetric()
      val updateMetrics = Map(GpuRowLevelWriteExec.NUM_UPDATED_ROWS -> updatedMetric)
      val updateWriter = new RecordingDeltaWriter
      val updateBatch = buildDeltaBatch()
      RmmSpark.forceRetryOOM(
        RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      if (withMetadata) {
        val task = new DeltaMetadataTask(projs, updateMetrics)
        task.writeBatch(updateWriter, updateBatch)
        task.finishTask()
      } else {
        val task = new DeltaPlainTask(projs, updateMetrics)
        task.writeBatch(updateWriter, updateBatch)
        task.finishTask()
      }
      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
      assert(updatedMetric.value === 3L)

      // DELETE mode: count DELETE_OPERATION (2)
      val deletedMetric = newMetric()
      val deleteMetrics = Map(GpuRowLevelWriteExec.NUM_DELETED_ROWS -> deletedMetric)
      val deleteWriter = new RecordingDeltaWriter
      val deleteBatch = buildDeltaBatch()
      if (withMetadata) {
        val task = new DeltaMetadataTask(projs, deleteMetrics)
        task.writeBatch(deleteWriter, deleteBatch)
        task.finishTask()
      } else {
        val task = new DeltaPlainTask(projs, deleteMetrics)
        task.writeBatch(deleteWriter, deleteBatch)
        task.finishTask()
      }
      assert(deletedMetric.value === 2L)
    }
  }
}
