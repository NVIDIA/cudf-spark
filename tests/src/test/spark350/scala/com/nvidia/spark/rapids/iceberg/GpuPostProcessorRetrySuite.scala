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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.parquet._
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{
  MessageType => ShadedMessageType,
  Type => ShadedType
}
import org.apache.iceberg.types.Types
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

/**
 * Verifies the snapshot/restore around `withRetryNoSplit` in
 * GpuParquetReaderPostProcessor.process under simulated retry. Position generation commits its
 * counter advance to the processor as soon as GPU sequence generation succeeds; if a later field
 * action in the same safeMap iteration OOMs, withRetryNoSplit reruns the whole block. Without the
 * snapshot/restore the rerun would see already-advanced counters and emit wrong positions or IDs.
 */
class GpuPostProcessorRetrySuite extends RmmSparkRetrySuiteBase {

  private def createBlockMetaData(rowCount: Long): BlockMetaData = {
    val block = new BlockMetaData()
    block.setRowCount(rowCount)
    block
  }

  private def assertCountersSurviveRetry(
      fieldId: Int,
      fieldName: String,
      offset: Long): Unit = {
    // A second projected field that does NOT exist in the file schema and is not in
    // idToConstant. ActionBuildingVisitor lowers this to FillNull, whose execute() allocates
    // a fresh GPU column. That second allocation is what we OOM, so position generation is the
    // earlier action that has already committed counter state when the retry fires.
    // Use an ordinary field ID to avoid accidentally selecting another metadata action.
    val fillFieldId = 1

    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)

    val expectedSchema = new Schema(
      Types.NestedField.optional(fieldId, fieldName, Types.LongType.get()),
      Types.NestedField.optional(fillFieldId, "fill", Types.LongType.get())
    )

    // Two blocks of 500 + 400 rows starting at file-global row 500 — same shape as the
    // non-retry multi-block test in GpuPostProcessorSuite.
    val blockRowCounts = Seq(500L, 400L)
    val blocks = blockRowCounts.map(createBlockMetaData)
    val firstRowIndices = blockRowCounts.scanLeft(500L)(_ + _).dropRight(1)
    val parquetInfo = ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = blocks,
      partValues = InternalRow.empty,
      schema = ShimUtils.getParquetUnshader().unshade(parquetSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = firstRowIndices
    )
    val constants = new JHashMap[Integer, Any]()
    if (fieldName == "_row_id") {
      constants.put(fieldId, offset)
    }
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      constants,
      expectedSchema,
      parquetSchema,
      Map.empty)

    def emptyBatch(rows: Int): ColumnarBatch =
      new ColumnarBatch(Array.empty[SparkColumnVector], rows)

    def assertColumnRange(batch: ColumnarBatch, expectedStart: Long): Unit = {
      val column = batch.column(0).asInstanceOf[GpuColumnVector]
      withResource(column.copyToHost()) { hostCol =>
        val base = hostCol.getBase
        var i = 0
        while (i < batch.numRows()) {
          val actual = base.getLong(i)
          val expected = expectedStart + i
          assert(actual == expected,
            s"$fieldName[$i] expected $expected, got $actual")
          i += 1
        }
      }
    }

    // Skip 1 GPU allocation (RangeSequence output, which commits counters as soon as
    // it succeeds), then OOM on the next allocation (FillNull's columnVectorFromNull).
    // withRetryNoSplit catches it and reruns the whole block; snapshot/restore must reset
    // position counters back to their pre-block values so the rerun uses 500..799, not
    // 800..1099.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 1)
    withResource(processor.process(emptyBatch(300))) { batch =>
      assert(batch.numRows() == 300)
      assertColumnRange(batch, offset + 500L)
    }

    // Follow-up call without OOM injection: counters should now be exactly where a single
    // successful 300-row call would leave them. If the snapshot/restore double-restored or
    // failed to re-advance, the next batch's start would be wrong.
    withResource(processor.process(emptyBatch(200))) { batch =>
      assert(batch.numRows() == 200)
      assertColumnRange(batch, offset + 800L)
    }
  }

  test("FetchRowPosition counters survive a retry when a later action OOMs") {
    assertCountersSurviveRetry(MetadataColumns.ROW_POSITION.fieldId(), "_pos", offset = 0L)
  }

  test("Inherited row ID counters survive a retry when a later action OOMs") {
    val majorMinor = IcebergProvider.detectedVersion.split("\\.").take(2).mkString(".")
    assume(Set("1.10", "1.11").contains(majorMinor),
      "The selected Iceberg runtime does not support row lineage inheritance")
    assertCountersSurviveRetry(ShimUtils.rowIdFieldId(), "_row_id", offset = 1000L)
  }

  test("FetchRowPosition retries range metadata allocation across a row-group gap") {
    val batchRows = 8
    val rowsPerFullBlock = batchRows / 2
    val secondBlockStart = 1000000L
    val rowPosId = MetadataColumns.ROW_POSITION.fieldId()
    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.optional(rowPosId, "_pos", Types.LongType.get()))

    // The gap between the two blocks forces two ranges in one native output column.
    val blocks = Seq(rowsPerFullBlock.toLong, rowsPerFullBlock.toLong + 50L)
      .map(createBlockMetaData)
    val parquetInfo = ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = blocks,
      partValues = InternalRow.empty,
      schema = unshade(parquetSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = Seq(500L, secondBlockStart)
    )
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      parquetSchema,
      Map.empty)

    def emptyBatch(rows: Int): ColumnarBatch =
      new ColumnarBatch(Array.empty[SparkColumnVector], rows)

    def assertPositions(batch: ColumnarBatch, expected: Seq[Long]): Unit = {
      val posCol = batch.column(0).asInstanceOf[GpuColumnVector]
      withResource(posCol.copyToHost()) { hostCol =>
        val base = hostCol.getBase
        expected.zipWithIndex.foreach { case (position, index) =>
          assert(base.getLong(index) == position,
            s"_pos[$index] expected $position, got ${base.getLong(index)}")
        }
      }
    }

    // Skip output and starts allocations, then fail the ends metadata allocation.
    // Position counters must remain unchanged until the whole column is generated.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 2)
    withResource(processor.process(emptyBatch(batchRows))) { batch =>
      assertPositions(batch,
        (500L until 500L + rowsPerFullBlock) ++
          (secondBlockStart until secondBlockStart + rowsPerFullBlock))
    }

    // Verify that the successful retry committed the counters exactly once.
    withResource(processor.process(emptyBatch(50))) { batch =>
      assertPositions(batch,
        secondBlockStart + rowsPerFullBlock until secondBlockStart + rowsPerFullBlock + 50L)
    }
  }
}
