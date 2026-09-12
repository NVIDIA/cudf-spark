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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDeltaBatchWriter, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{DELETE_OPERATION, INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.{DeltaWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuDeltaWritingSparkTaskSuite extends RmmSparkRetrySuiteBase {
  private val rowSchema = new StructType().add("value", LongType)
  private val metadataSchema = new StructType().add("_row_id", LongType)
    .add("_last_updated_sequence_number", LongType)

  private def longValues(batch: ColumnarBatch, ordinal: Int): Seq[Option[Long]] = {
    withResource(batch.column(ordinal).asInstanceOf[GpuColumnVector].copyToHost()) { column =>
      (0 until batch.numRows()).map { row =>
        if (column.isNullAt(row)) None else Some(column.getLong(row))
      }
    }
  }

  private class RecordingWriter extends DeltaWriter[ColumnarBatch] with GpuDeltaBatchWriter {
    var dataOrder = Seq.empty[Option[Long]]
    var inserted = Seq.empty[Option[Long]]
    var reinserted = Seq.empty[Option[Long]]
    var lineage = Seq.empty[Seq[Option[Long]]]
    var deletedRows = 0
    var updatedRows = 0

    override def insert(row: ColumnarBatch): Unit = withResource(row) { _ =>
      dataOrder ++= longValues(row, 0)
      inserted ++= longValues(row, 0)
    }

    override def reinsert(metadata: ColumnarBatch, row: ColumnarBatch): Unit = {
      withResource(Seq(metadata, row)) { _ =>
        dataOrder ++= longValues(row, 0)
        reinserted ++= longValues(row, 0)
        if (metadata != null) {
          lineage = Seq(longValues(metadata, 0), longValues(metadata, 1))
        }
      }
    }

    override def insertAndReinsert(
        metadata: ColumnarBatch,
        row: ColumnarBatch,
        reinsertMask: CudfColumnVector): Unit = {
      withResource(Seq(metadata, row, reinsertMask)) { _ =>
        val flags = withResource(reinsertMask.copyToHost()) { host =>
          (0 until row.numRows()).map(index => host.getBoolean(index))
        }
        val values = longValues(row, 0)
        dataOrder ++= values
        inserted ++= values.zip(flags).collect { case (value, false) => value }
        reinserted ++= values.zip(flags).collect { case (value, true) => value }
        if (metadata != null) {
          lineage = Seq(0, 1).map { ordinal =>
            longValues(metadata, ordinal).zip(flags).collect { case (value, true) => value }
          }
        }
      }
    }

    override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
      withResource(Seq(metadata, rowId)) { _ => deletedRows += rowId.numRows() }
    }

    override def update(
        metadata: ColumnarBatch,
        rowId: ColumnarBatch,
        row: ColumnarBatch): Unit = {
      withResource(Seq(metadata, rowId, row)) { _ => updatedRows += row.numRows() }
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = ()
    override def close(): Unit = ()
  }

  private class MetadataTask(projections: WriteDeltaProjections)
      extends GpuDeltaWithMetadataWritingSparkTask(projections) {
    def writeBatch(
        writer: DeltaWriter[ColumnarBatch] with GpuDeltaBatchWriter,
        batch: ColumnarBatch): Unit = write(writer, batch)
  }

  private class PlainTask(projections: WriteDeltaProjections)
      extends GpuDeltaWritingSparkTask(projections) {
    def writeBatch(
        writer: DeltaWriter[ColumnarBatch] with GpuDeltaBatchWriter,
        batch: ColumnarBatch): Unit = write(writer, batch)
  }

  Seq(false, true).foreach { withMetadata =>
    test(s"preserve insert and reinsert order with metadata=$withMetadata") {
      val projections = WriteDeltaProjections(
        Some(ProjectingInternalRow(rowSchema, Seq(1))),
        ProjectingInternalRow(rowSchema, Seq(1)),
        if (withMetadata) Some(ProjectingInternalRow(metadataSchema, Seq(2, 3))) else None)
      val writer = new RecordingWriter
      val batch = withResource(new Table.TestBuilder()
          .column(Int.box(INSERT_OPERATION), REINSERT_OPERATION, DELETE_OPERATION,
            INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION)
          .column(Long.box(10L), 20L, 30L, 40L, 50L, 60L)
          .column(Long.box(110L), 220L, 330L, 440L, 550L, 660L)
          .column(Long.box(5L), null.asInstanceOf[java.lang.Long], 7L, 8L, 9L, 11L)
          .build()) { table =>
        GpuColumnVector.from(table, Array[DataType](IntegerType, LongType, LongType, LongType))
      }
      // Writing tasks own and close the input batch.
      if (withMetadata) {
        new MetadataTask(projections).writeBatch(writer, batch)
      } else {
        new PlainTask(projections).writeBatch(writer, batch)
      }
      assert(writer.dataOrder == Seq(Some(10L), Some(20L), Some(40L), Some(50L)))
      assert(writer.inserted == Seq(Some(10L), Some(40L)))
      assert(writer.reinserted == Seq(Some(20L), Some(50L)))
      assert(writer.deletedRows == 1)
      assert(writer.updatedRows == 1)
      if (withMetadata) {
        assert(writer.lineage == Seq(Seq(Some(220L), Some(550L)), Seq(None, Some(9L))))
      } else {
        assert(writer.lineage.isEmpty)
      }
    }
  }
}
