/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  test("test simple GPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test simple CPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    // Inject CPU OOM after skipping the first few CPU allocations. The skipCount ensures
    // the OOM is thrown at a point where our retry logic can handle it (during row conversion,
    // after builder state has been captured).
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test CPU OOM retry preserves all rows for non-RequireSingleBatch") {
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    // Inject a CPU OOM during conversion and verify that retry still produces
    // the complete set of rows when the iterator is allowed to emit multiple batches.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test first-row CPU OOM with TargetSize goal falls back to retry") {
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    // skipCount=1 lets the first CPU allocation (data buffer) succeed, then fires OOM on the
    // second (validity buffer), so the row is not committed (rowCount == 0). This exercises
    // the blockUntilMemoryFreed path. skipCount=0 does not work: blockThreadUntilReady() has
    // nothing to spill and re-throws the OOM when no prior allocations exist.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 1)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test first-row CPU OOM with RequireSingleBatch falls back to retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    // skipCount=1: same reasoning as the TargetSize test above — fires on the validity
    // buffer allocation during the first row, keeping rowCount == 0 for the OOM.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 1)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  // Note: SplitAndRetryOOM with rowCount == 0 is propagated directly (can't split a single
  // row). A dedicated CpuSplitAndRetryOOM test for per-row convert() with rowCount == 0 is not
  // feasible because RMM allocator-level OOM injection cannot reliably target it — it tends to
  // hit builders.tryBuild() instead. The GPU split-and-retry test below verifies propagation.
  // SplitAndRetryOOM with rowCount > 0 (emit-early) is covered by the test below.

  test("test CPU SplitAndRetryOOM emit-early for non-RequireSingleBatch") {
    // Same injection as "test CPU OOM retry preserves all rows" but with SplitAndRetryOOM:
    // skipCount=3 reliably fires inside convertRows after at least one row is committed,
    // triggering the emit-early path (rowCount > 0, non-RequireSingleBatch).
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test simple OOM split and retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }
}
