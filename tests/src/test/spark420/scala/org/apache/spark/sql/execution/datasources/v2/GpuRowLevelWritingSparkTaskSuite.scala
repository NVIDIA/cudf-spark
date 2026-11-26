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
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDataWriter, GpuDataWriterFactory,
  GpuDeltaWrite, GpuDeltaWriter, GpuRowToColumnarExec, GpuWrite, RapidsConf,
  RmmSparkRetrySuiteBase, TargetSize}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, DELETE_OPERATION,
  INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory,
  DeleteSummary, DeltaBatchWrite, DeltaWriter, DeltaWriterFactory, PhysicalWriteInfo,
  RowLevelOperation, UpdateSummary, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRowLevelWritingSparkTaskSuite extends RmmSparkRetrySuiteBase {
  private val rowSchema = new StructType().add("partition", IntegerType).add("value", LongType)
  private val rowIdSchema = new StructType().add("value", LongType)
  private val metadataSchema = new StructType().add("_row_id", LongType)

  private def newMetric(): SQLMetric = new SQLMetric("sum")

  // Mirrors GpuUnpartitionedDataWriter / GpuDataWriterWithRowLineage ownership:
  // write(record) takes ownership and closes record; write(metadata, record) borrows both.
  private class RecordingDataWriter(failOnWrite: Boolean = false) extends GpuDataWriter {
    var writtenRows = 0L
    var metadataCalls = 0
    var plainCalls = 0

    override def write(record: ColumnarBatch): Unit = {
      withResource(record) { _ =>
        plainCalls += 1
        if (failOnWrite) {
          throw new RuntimeException("simulated write failure")
        }
        writtenRows += record.numRows()
      }
    }

    override def write(metadata: ColumnarBatch, record: ColumnarBatch): Unit = {
      metadataCalls += 1
      if (failOnWrite) {
        throw new RuntimeException("simulated write failure")
      }
      writtenRows += record.numRows()
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = ()
    override def close(): Unit = ()
  }

  private class RecordingDeltaWriter(
      injectOomAfterFirstDelete: Boolean = false,
      injectOomAfterFirstUpdate: Boolean = false) extends GpuDeltaWriter {
    var deleteCalls = 0
    var updateCalls = 0
    var deletedRows = 0L
    var updatedRows = 0L
    var insertedOrReinsertedRows = 0L

    override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
      deleteCalls += 1
      withResource(Seq(metadata, rowId)) { _ => deletedRows += rowId.numRows() }
      if (injectOomAfterFirstDelete && deleteCalls == 1) {
        RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
    }

    override def update(
        metadata: ColumnarBatch,
        rowId: ColumnarBatch,
        row: ColumnarBatch): Unit = {
      updateCalls += 1
      withResource(Seq(metadata, rowId, row)) { _ => updatedRows += row.numRows() }
      if (injectOomAfterFirstUpdate && updateCalls == 1) {
        RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
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

  private def buildEmptyReplaceBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(Array.empty[java.lang.Integer]: _*)
      .column(Array.empty[java.lang.Integer]: _*)
      .column(Array.empty[java.lang.Long]: _*)
      .column(Array.empty[java.lang.Long]: _*)
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

      val batch = buildReplaceBatch()
      // Inject CPU OOM on copyToHost() to verify host-side retry does not double-count or leak.
      RmmSpark.forceRetryOOM(
        RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.CPU.ordinal, 0)
      task.writeBatch(writer, batch)
      task.finishTask()

      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
      assert(writer.writtenRows === 6L)
      assert(writer.metadataCalls === (if (withMetadata) 1 else 0))
      assert(writer.plainCalls === (if (withMetadata) 0 else 1))
      assert(updatedMetric.value === 2L)
      assert(copiedMetric.value === 3L)
    }
  }

  test("GpuReplaceDataWritingSparkTask handles nullable operation column") {
    val projs = ReplaceDataProjections(ProjectingInternalRow(rowSchema, Seq(1, 2)), None)
    val updatedMetric = newMetric()
    val copiedMetric = newMetric()
    val task = new ReplaceTask(projs, Map(
      GpuRowLevelWriteExec.NUM_UPDATED_ROWS -> updatedMetric,
      GpuRowLevelWriteExec.NUM_COPIED_ROWS -> copiedMetric))
    val writer = new RecordingDataWriter
    val batchWithNullOp = withResource(new Table.TestBuilder()
      .column(Int.box(UPDATE_OPERATION), null.asInstanceOf[java.lang.Integer], COPY_OPERATION)
      .column(Int.box(0), 1, 2)
      .column(Long.box(10L), 20L, 30L)
      .column(Long.box(100L), 200L, 300L)
      .build()) { table =>
      GpuColumnVector.from(table,
        Array[DataType](IntegerType, IntegerType, LongType, LongType))
    }
    task.writeBatch(writer, batchWithNullOp)
    task.finishTask()
    assert(writer.writtenRows === 3L)
    assert(updatedMetric.value === 1L)
    assert(copiedMetric.value === 1L)
  }

  test("GpuReplaceDataWritingSparkTask handles DELETE, empty batch, MERGE, and write failures") {
    val projs = ReplaceDataProjections(ProjectingInternalRow(rowSchema, Seq(1, 2)), None)

    val copiedMetric = newMetric()
    val deleteTask = new ReplaceTask(
      projs, Map(GpuRowLevelWriteExec.NUM_COPIED_ROWS -> copiedMetric))
    val deleteWriter = new RecordingDataWriter
    deleteTask.writeBatch(deleteWriter, buildEmptyReplaceBatch())
    deleteTask.writeBatch(deleteWriter, buildReplaceBatch())
    deleteTask.finishTask()
    assert(deleteWriter.writtenRows === 6L)
    assert(deleteWriter.plainCalls === 2)
    assert(copiedMetric.value === 3L)

    // MERGE passes empty sparkMetrics and writes without error or leak.
    val mergeWriter = new RecordingDataWriter
    new ReplaceTask(projs, Map.empty).writeBatch(mergeWriter, buildReplaceBatch())
    assert(mergeWriter.writtenRows === 6L)

    // Write failure must leave metrics untouched and still release batch resources.
    val failUpdated = newMetric()
    val failCopied = newMetric()
    val failTask = new ReplaceTask(projs, Map(
      GpuRowLevelWriteExec.NUM_UPDATED_ROWS -> failUpdated,
      GpuRowLevelWriteExec.NUM_COPIED_ROWS -> failCopied))
    intercept[RuntimeException] {
      failTask.writeBatch(new RecordingDataWriter(failOnWrite = true), buildReplaceBatch())
    }
    assert(failUpdated.value === 0L)
    assert(failCopied.value === 0L)
  }

  Seq(false, true).foreach { withMetadata =>
    test(s"GpuDeltaWritingSparkTask counts UPDATE+REINSERT and DELETE across mid-batch retries " +
      s"(metadata=$withMetadata)") {
      val projs = WriteDeltaProjections(
        Some(ProjectingInternalRow(rowSchema, Seq(1, 2))),
        ProjectingInternalRow(rowIdSchema, Seq(2)),
        if (withMetadata) Some(ProjectingInternalRow(metadataSchema, Seq(3))) else None)

      // UPDATE mode: inject OOM after writer.update so retry re-executes update + writeInserts,
      // catching any premature metric increment inside withRetryNoSplit.
      val updatedMetric = newMetric()
      val updateMetrics = Map(GpuRowLevelWriteExec.NUM_UPDATED_ROWS -> updatedMetric)
      val updateWriter = new RecordingDeltaWriter(injectOomAfterFirstUpdate = true)
      if (withMetadata) {
        val task = new DeltaMetadataTask(projs, updateMetrics)
        task.writeBatch(updateWriter, buildDeltaBatch())
        task.finishTask()
      } else {
        val task = new DeltaPlainTask(projs, updateMetrics)
        task.writeBatch(updateWriter, buildDeltaBatch())
        task.finishTask()
      }
      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
      assert(updateWriter.updateCalls === 2)
      assert(updatedMetric.value === 3L)

      // DELETE mode: inject OOM after writer.delete so retry re-executes delete cleanly.
      val deletedMetric = newMetric()
      val deleteMetrics = Map(GpuRowLevelWriteExec.NUM_DELETED_ROWS -> deletedMetric)
      val deleteWriter = new RecordingDeltaWriter(injectOomAfterFirstDelete = true)
      if (withMetadata) {
        val task = new DeltaMetadataTask(projs, deleteMetrics)
        task.writeBatch(deleteWriter, buildDeltaBatch())
        task.finishTask()
      } else {
        val task = new DeltaPlainTask(projs, deleteMetrics)
        task.writeBatch(deleteWriter, buildDeltaBatch())
        task.finishTask()
      }
      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
      assert(deleteWriter.deleteCalls === 2)
      assert(deletedMetric.value === 2L)
    }
  }

  test("GpuReplaceDataExec and GpuWriteDeltaExec wire metrics and summaries through writeWithV2") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config(RapidsConf.METRICS_LEVEL.key, "ESSENTIAL")
      .getOrCreate()
    try {
      import spark.implicits._

      val replaceDf = Seq(
        (UPDATE_OPERATION, 0, 10L),
        (COPY_OPERATION, 0, 20L),
        (UPDATE_OPERATION, 1, 30L),
        (COPY_OPERATION, 1, 40L),
        (COPY_OPERATION, 1, 50L)
      ).toDF("__row_operation", "partition", "value")
      val replaceInput = GpuRowToColumnarExec(
        replaceDf.queryExecution.executedPlan, TargetSize(1L << 20))
      val replaceProjs = ReplaceDataProjections(
        ProjectingInternalRow(rowSchema, Seq(1, 2)), None)
      val replaceBatchWrite = new EndToEndDataBatchWrite
      val replaceExec = GpuReplaceDataExec(
        replaceInput, () => (), replaceProjs,
        new EndToEndGpuWrite(replaceBatchWrite),
        Some(RowLevelOperation.Command.UPDATE))
      replaceExec.executeCollect()

      assert(replaceBatchWrite.summaryCommitCount === 1)
      val replaceSummary = replaceBatchWrite.lastSummary.get.asInstanceOf[UpdateSummary]
      assert(replaceSummary.numUpdatedRows() === 2L)
      assert(replaceSummary.numCopiedRows() === 3L)

      val deltaDf = Seq(
        (DELETE_OPERATION, 0, 10L),
        (DELETE_OPERATION, 0, 20L),
        (UPDATE_OPERATION, 1, 30L),
        (REINSERT_OPERATION, 1, 40L),
        (REINSERT_OPERATION, 2, 50L),
        (INSERT_OPERATION, 2, 60L)
      ).toDF("__row_operation", "partition", "value")
      val deltaProjs = WriteDeltaProjections(
        Some(ProjectingInternalRow(rowSchema, Seq(1, 2))),
        ProjectingInternalRow(rowIdSchema, Seq(2)),
        None)

      val updateDeltaBatchWrite = new EndToEndDeltaBatchWrite
      val updateDeltaExec = GpuWriteDeltaExec(
        GpuRowToColumnarExec(deltaDf.queryExecution.executedPlan, TargetSize(1L << 20)),
        () => (), deltaProjs,
        new EndToEndGpuDeltaWrite(updateDeltaBatchWrite),
        Some(RowLevelOperation.Command.UPDATE))
      updateDeltaExec.executeCollect()
      val deltaUpdateSummary = updateDeltaBatchWrite.lastSummary.get.asInstanceOf[UpdateSummary]
      assert(deltaUpdateSummary.numUpdatedRows() === 3L)
      assert(deltaUpdateSummary.numCopiedRows() === 0L)

      val deleteDeltaBatchWrite = new EndToEndDeltaBatchWrite
      val deleteDeltaExec = GpuWriteDeltaExec(
        GpuRowToColumnarExec(deltaDf.queryExecution.executedPlan, TargetSize(1L << 20)),
        () => (), deltaProjs,
        new EndToEndGpuDeltaWrite(deleteDeltaBatchWrite),
        Some(RowLevelOperation.Command.DELETE))
      deleteDeltaExec.executeCollect()
      val deltaDeleteSummary = deleteDeltaBatchWrite.lastSummary.get.asInstanceOf[DeleteSummary]
      assert(deltaDeleteSummary.numDeletedRows() === 2L)
      assert(deltaDeleteSummary.numCopiedRows() === 0L)
    } finally {
      spark.stop()
    }
  }
}

private object NoopGpuDataWriterFactory extends GpuDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long): DataWriter[InternalRow] = createWriter(partitionId, taskId, null)

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      metadataSchema: StructType): DataWriter[InternalRow] = {
    new GpuDataWriter {
      override def write(record: ColumnarBatch): Unit = withResource(record) { _ => () }
      override def write(metadata: ColumnarBatch, record: ColumnarBatch): Unit = ()
      override def commit(): WriterCommitMessage = null
      override def abort(): Unit = ()
      override def close(): Unit = ()
    }.asInstanceOf[DataWriter[InternalRow]]
  }
}

private object NoopGpuDeltaWriterFactory extends DeltaWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long): DeltaWriter[InternalRow] = {
    new GpuDeltaWriter {
      override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit =
        withResource(Seq(metadata, rowId)) { _ => () }
      override def update(metadata: ColumnarBatch, rowId: ColumnarBatch, row: ColumnarBatch): Unit =
        withResource(Seq(metadata, rowId, row)) { _ => () }
      override def insert(row: ColumnarBatch): Unit = withResource(row) { _ => () }
      override def insertAndReinsert(
          metadata: ColumnarBatch,
          row: ColumnarBatch,
          reinsertMask: CudfColumnVector): Unit =
        withResource(Seq(metadata, row, reinsertMask)) { _ => () }
      override def commit(): WriterCommitMessage = null
      override def abort(): Unit = ()
      override def close(): Unit = ()
    }.asInstanceOf[DeltaWriter[InternalRow]]
  }
}

private class EndToEndDataBatchWrite extends BatchWrite {
  var summaryCommitCount = 0
  var lastSummary: Option[WriteSummary] = None

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    NoopGpuDataWriterFactory

  override def commit(messages: Array[WriterCommitMessage]): Unit = ()

  override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit = {
    summaryCommitCount += 1
    lastSummary = Some(summary)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = ()
}

private class EndToEndDeltaBatchWrite extends DeltaBatchWrite {
  var summaryCommitCount = 0
  var lastSummary: Option[WriteSummary] = None

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory =
    NoopGpuDeltaWriterFactory

  override def commit(messages: Array[WriterCommitMessage]): Unit = ()

  override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit = {
    summaryCommitCount += 1
    lastSummary = Some(summary)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = ()
}

private class EndToEndGpuWrite(batchWrite: BatchWrite) extends GpuWrite {
  override def toBatch: BatchWrite = batchWrite
}

private class EndToEndGpuDeltaWrite(batchWrite: DeltaBatchWrite) extends GpuDeltaWrite {
  override def toBatch: DeltaBatchWrite = batchWrite
}
