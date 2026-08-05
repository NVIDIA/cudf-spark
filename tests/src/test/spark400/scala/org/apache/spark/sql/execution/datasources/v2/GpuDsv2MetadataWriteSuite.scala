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

import java.io.IOException

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.shims.DeltaInsertFilter

import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{
  INSERT_OPERATION, REINSERT_OPERATION, WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION}
import org.apache.spark.sql.connector.write.{DataWriter, DeltaWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Lightweight coverage for SPARK-50820 GPU write-task routing (issue #15348).
 */
class GpuDsv2MetadataWriteSuite extends RmmSparkRetrySuiteBase {

  private val rowSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("v", LongType)))
  private val metaSchema = StructType(Seq(StructField("meta", LongType)))

  private def projectingRow(schema: StructType, ordinals: Seq[Int]): ProjectingInternalRow = {
    ProjectingInternalRow(schema, ordinals.toIndexedSeq)
  }

  private def buildOperationBatch(ops: Array[Int], ids: Array[Int],
      values: Array[Long], metadataValues: Array[Long]): ColumnarBatch = {
    require(ops.length == ids.length && ids.length == values.length &&
      values.length == metadataValues.length)
    new ColumnarBatch(
      Array(
        GpuColumnVector.from(ColumnVector.fromInts(ops: _*), IntegerType),
        GpuColumnVector.from(ColumnVector.fromInts(ids: _*), IntegerType),
        GpuColumnVector.from(ColumnVector.fromLongs(values: _*), LongType),
        GpuColumnVector.from(ColumnVector.fromLongs(metadataValues: _*), LongType)),
      ops.length)
  }

  test("GpuReplaceDataExec selects metadata-aware writing task when metadataProjection present") {
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val projections = ReplaceDataProjections(rowProj, Some(metaProj))

    // writingTask selection mirrors CPU ReplaceDataExec / DataAndMetadataWritingSparkTask
    val task = projections match {
      case ReplaceDataProjections(dataProj, Some(metadataProj)) =>
        GpuDataAndMetadataWritingSparkTask(dataProj, metadataProj)
      case _ =>
        fail("expected metadata projection")
    }
    assert(task.isInstanceOf[GpuDataAndMetadataWritingSparkTask])
    assert(task.dataProj === rowProj)
    assert(task.metadataProj === metaProj)
  }

  test("GpuReplaceDataExec keeps plain writing task when metadataProjection absent") {
    val rowProj = projectingRow(StructType(Seq(StructField("id", IntegerType))), Seq(1))
    val projections = ReplaceDataProjections(rowProj, None)
    val task = projections match {
      case ReplaceDataProjections(_, Some(_)) =>
        fail("did not expect metadata projection")
      case _ =>
        GpuReplaceDataWritingSparkTask(projections)
    }
    assert(task.isInstanceOf[GpuReplaceDataWritingSparkTask])
  }

  test("ordinary-row OOM does not replay a completed metadata write") {
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val task = GpuDataAndMetadataWritingSparkTask(rowProj, metaProj)
    val writer = new RecordingDataWriter(failOrdinaryOnce = true)
    withResource(buildOperationBatch(
      Array(WRITE_WITH_METADATA_OPERATION, WRITE_OPERATION),
      Array(1, 2),
      Array(10L, 20L),
      Array(100L, 200L))) { batch =>
      task.writeBatchForTest(writer, GpuColumnVector.incRefCounts(batch))
    }
    assert(writer.metadataWriteCount === 1)
    assert(writer.ordinaryWriteCount === 1)
  }

  test("operation splitting retries before writer side effects") {
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val task = GpuDataAndMetadataWritingSparkTask(rowProj, metaProj)
    val writer = new RecordingDataWriter()
    val batch = buildOperationBatch(
      Array(WRITE_WITH_METADATA_OPERATION, WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION),
      Array(1, 2, 3),
      Array(10L, 20L, 30L),
      Array(100L, 200L, 300L))
    RmmSpark.forceRetryOOM(
      RmmSpark.getCurrentThreadId,
      1,
      RmmSpark.OomInjectionType.GPU.ordinal,
      0)
    task.writeBatchForTest(writer, batch)
    assert(RmmSpark.getAndResetNumRetryThrow(/* taskId = */ 1) > 0)
    assert(writer.writeKinds === Seq("metadata", "ordinary", "metadata"))
  }

  test("contiguous operation runs preserve interleaved metadata and ordinary order") {
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val task = GpuDataAndMetadataWritingSparkTask(rowProj, metaProj)
    val writer = new RecordingDataWriter()
    withResource(buildOperationBatch(
      Array(WRITE_WITH_METADATA_OPERATION, WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION),
      Array(1, 2, 3),
      Array(10L, 20L, 30L),
      Array(100L, 200L, 300L))) { batch =>
      task.writeBatchForTest(writer, GpuColumnVector.incRefCounts(batch))
    }
    assert(writer.writeKinds === Seq("metadata", "ordinary", "metadata"))
  }

  test("DeltaInsertFilter routes insert and reinsert rows with metadata") {
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val rowProjection = GpuProjectingColumnarBatch(rowProj)
    val metadataProjection = GpuProjectingColumnarBatch(metaProj)
    val rowDataTypes: Array[DataType] = rowSchema.fields.map(_.dataType)
    val metadataDataTypes: Array[DataType] = metaSchema.fields.map(_.dataType)
    val writer = new RecordingDeltaWriter
    withResource(buildOperationBatch(
      Array(INSERT_OPERATION, REINSERT_OPERATION, INSERT_OPERATION),
      Array(1, 2, 3),
      Array(10L, 20L, 30L),
      Array(100L, 200L, 300L))) { batch =>
      DeltaInsertFilter.writeInserts(writer, batch, rowProjection, rowDataTypes)
      DeltaInsertFilter.writeReinserts(
        writer, batch, rowProjection, rowDataTypes, metadataProjection, metadataDataTypes)
    }
    assert(writer.insertedRows === 2)
    assert(writer.reinsertedRows === 1)
    assert(writer.reinsertMetadataRows === 1)
  }

  private class RecordingDataWriter(failOrdinaryOnce: Boolean = false)
      extends DataWriter[ColumnarBatch] {
    var metadataWriteCount = 0
    var ordinaryWriteCount = 0
    var writeKinds: Seq[String] = Seq.empty
    private var failOrdinary = failOrdinaryOnce

    override def write(batch: ColumnarBatch): Unit = {
      withResource(batch) { _ =>
        if (failOrdinary) {
          failOrdinary = false
          throw new GpuRetryOOM("inject ordinary-phase retry")
        }
        ordinaryWriteCount += 1
        writeKinds = writeKinds :+ "ordinary"
      }
    }

    override def write(metadata: ColumnarBatch, data: ColumnarBatch): Unit = {
      withResource(metadata) { _ =>
        withResource(data) { _ =>
          metadataWriteCount += 1
          writeKinds = writeKinds :+ "metadata"
        }
      }
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = {}
    override def close(): Unit = {}
  }

  private class RecordingDeltaWriter extends DeltaWriter[ColumnarBatch] {
    var insertedRows = 0
    var reinsertedRows = 0
    var reinsertMetadataRows = 0

    override def delete(metadata: ColumnarBatch, id: ColumnarBatch): Unit =
      throw new UnsupportedOperationException
    override def update(metadata: ColumnarBatch, id: ColumnarBatch, row: ColumnarBatch): Unit =
      throw new UnsupportedOperationException

    override def insert(row: ColumnarBatch): Unit = {
      withResource(row) { b =>
        insertedRows += b.numRows()
      }
    }

    override def reinsert(metadata: ColumnarBatch, row: ColumnarBatch): Unit = {
      withResource(row) { r =>
        reinsertedRows += r.numRows()
        if (metadata != null) {
          withResource(metadata) { m =>
            reinsertMetadataRows += m.numRows()
          }
        }
      }
    }

    override def write(row: ColumnarBatch): Unit = throw new UnsupportedOperationException
    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = {}
    @throws[IOException]
    override def close(): Unit = {}
  }
}
