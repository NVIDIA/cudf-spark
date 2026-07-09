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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

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

  test("test empty schema preserves row count") {
    val emptySchema = StructType(Nil)
    val rowIter: Iterator[InternalRow] = Iterator(InternalRow.empty)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, emptySchema, RequireSingleBatch, batchSize,
      new GpuRowToColumnConverter(emptySchema))
    withResource(row2ColIter.next()) { batch =>
      assertResult(1)(batch.numRows())
      assertResult(0)(batch.numCols())
    }
    assert(!row2ColIter.hasNext)
  }

  test("test GPU OOM split and retry produces multiple batches") {
    val numRows = 10
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    val batches = new ArrayBuffer[ColumnarBatch]()
    val allValues = new ArrayBuffer[Int]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            allValues += hostCol.getInt(i)
          }
        }
      }

      assert(batches.length >= 2,
        s"Expected at least 2 batches after split, got ${batches.length}")
      assertResult(numRows)(allValues.length)
      assertResult((1 to numRows).toSeq)(allValues.toSeq)
    } finally {
      batches.foreach(_.close())
    }
  }

  test("test pending GPU OOM split batches reacquire semaphore") {
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("RowToColumnarIteratorRetrySuite")
        .getOrCreate()
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    TrampolineUtil.setTaskContext(context)
    try {
      val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
      val goal = TargetSize(batchSize)
      val row2ColIter = new RowToColumnarIterator(
        rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)

      withResource(row2ColIter.next()) { batch =>
        assertResult(5)(batch.numRows())
      }
      GpuSemaphore.releaseIfNecessary(context)
      val (_, releasedAt) = GpuSemaphore.getLastSemAcqAndRelTime(context)
      assert(releasedAt > 0)

      withResource(row2ColIter.next()) { batch =>
        assertResult(5)(batch.numRows())
      }
      val (reacquiredAt, _) = GpuSemaphore.getLastSemAcqAndRelTime(context)
      assert(reacquiredAt > releasedAt)
      assert(!row2ColIter.hasNext)
    } finally {
      GpuSemaphore.releaseIfNecessary(context)
      TrampolineUtil.unsetTaskContext()
      ScalableTaskCompletion.reset()
      spark.stop()
    }
  }

  test("test GPU OOM split and retry with multiple splits") {
    val numRows = 16
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 3,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    val batches = new ArrayBuffer[ColumnarBatch]()
    val allValues = new ArrayBuffer[Int]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            allValues += hostCol.getInt(i)
          }
        }
      }

      assert(batches.length >= 4,
        s"Expected at least 4 batches after 3 sequential splits, got ${batches.length}")
      assertResult(numRows)(allValues.length)
      assertResult((1 to numRows).toSeq)(allValues.toSeq)
    } finally {
      batches.foreach(_.close())
    }
  }

  test("test GPU OOM split with single row cannot split further") {
    val rowIter: Iterator[InternalRow] = Iterator(InternalRow(1))
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("test GPU OOM split and retry with nested types") {
    val nestedSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("str", StringType),
      StructField("arr", ArrayType(IntegerType, containsNull = true)),
      StructField("nested_struct", StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", StringType)
      ))),
      StructField("map", MapType(StringType, IntegerType, valueContainsNull = true)),
      StructField("arr_of_struct", ArrayType(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )),
        containsNull = true
      )),
      StructField("arr_of_arr", ArrayType(
        ArrayType(IntegerType, containsNull = true),
        containsNull = true
      )),
      StructField("struct_of_arr", StructType(Seq(
        StructField("nums", ArrayType(IntegerType, containsNull = true)),
        StructField("strs", ArrayType(StringType, containsNull = true))
      )))
    ))
    val numRows = 10
    val rowIter: Iterator[InternalRow] = (1 to numRows).map { i =>
      val str = UTF8String.fromString(s"test_string_$i")
      val arr = ArrayData.toArrayData(Array(i, i + 1, null, i + 2))
      val nestedStruct = new GenericInternalRow(
        Array[Any](i * 10, UTF8String.fromString(s"s_$i")))
      val keys = ArrayData.toArrayData(Array(
        UTF8String.fromString(s"key_${i}_a"),
        UTF8String.fromString(s"key_${i}_b")
      ))
      val values = ArrayData.toArrayData(Array(i, null))
      val mapData = new ArrayBasedMapData(keys, values)
      val struct1 = new GenericInternalRow(Array[Any](i, UTF8String.fromString(s"a_$i")))
      val struct2 = new GenericInternalRow(Array[Any](i + 1, UTF8String.fromString(s"b_$i")))
      val arrOfStruct = ArrayData.toArrayData(Array(struct1, null, struct2))
      val innerArr1 = ArrayData.toArrayData(Array(i, i + 1))
      val innerArr2 = ArrayData.toArrayData(Array(i + 2, null, i + 3))
      val arrOfArr = ArrayData.toArrayData(Array(innerArr1, null, innerArr2))
      val numsArr = ArrayData.toArrayData(Array(i * 100, i * 100 + 1, null))
      val strsArr = ArrayData.toArrayData(Array(
        UTF8String.fromString(s"x_$i"),
        null,
        UTF8String.fromString(s"y_$i")
      ))
      val structOfArr = new GenericInternalRow(Array[Any](numsArr, strsArr))

      new GenericInternalRow(Array[Any](
        i, str, arr, nestedStruct, mapData, arrOfStruct, arrOfArr, structOfArr))
    }.toIterator

    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, nestedSchema, goal, batchSize, new GpuRowToColumnConverter(nestedSchema))

    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    val batches = new ArrayBuffer[ColumnarBatch]()
    val idValues = new ArrayBuffer[Int]()
    val strValues = new ArrayBuffer[String]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            idValues += hostCol.getInt(i)
          }
        }
        withResource(batch.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            strValues += hostCol.getBase.getJavaString(i)
          }
        }
      }

      assert(batches.length >= 2,
        s"Expected at least 2 batches after split with nested types, got ${batches.length}")
      assertResult(numRows)(idValues.length)
      assertResult(numRows)(strValues.length)
      assertResult((1 to numRows).toSeq)(idValues.toSeq)
      val expectedStrings = (1 to numRows).map(i => s"test_string_$i")
      assertResult(expectedStrings)(strValues.toSeq)
    } finally {
      batches.foreach(_.close())
    }
  }
}
