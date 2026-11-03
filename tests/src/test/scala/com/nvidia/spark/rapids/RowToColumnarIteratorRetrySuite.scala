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

import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsHostColumnBuilderSuite._
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  // RmmSparkRetrySuiteBase dedicates the test thread to this task.
  private val taskId = 1L
  private val intStringSchema = StructType(Seq(
    StructField("i", IntegerType), StructField("s", StringType)))
  private val intArraySchema = StructType(Seq(
    StructField("i", IntegerType), StructField("a", ArrayType(StringType))))
  private val intArrayBinarySchema = StructType(Seq(StructField("i", IntegerType),
    StructField("a", ArrayType(StringType)), StructField("b", BinaryType)))
  private val smallBatchBytes = 64L * 1024
  // The cache build's goal: no size target, so a batch ends only where it has to split.
  private val cacheBuildGoal = TargetSize(Long.MaxValue)
  // The split case: 2047 values fit a string limit one byte short of 2048 values, so row 2048
  // starts a second batch. Rows 2047 and 2048 both have a null INT in one validity byte, so the
  // rollback of row 2048 must keep row 2047's null.
  private val splitCaseRows = 2300
  private val rowsBeforeLimit = 2047
  // Values shared by the carried-row and finite-target split tests: four fit a 4096-byte limit,
  // and row 5, whose INT is null, crosses it.
  private val sharedValueSize = 1000
  private val sharedNullIntRow = 5

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
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).iterator
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
      val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).iterator
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
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).iterator
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
    }.iterator

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

  Seq(true, false).foreach { retry =>
    test("a lowered string limit splits the cache build before the row that crosses it, " +
        retryMode(retry)) {
      val valueSize = 1024
      withLimits(limits(stringBytes = splitCaseStringLimit(valueSize))) {
        assertSplitAtLimit(r2c(splitCaseData(valueSize).iterator(splitCaseRows),
          intStringSchema, cacheBuildGoal, smallBatchBytes, retry), valueSize)
      }
    }

    test("a lowered string limit under RequireSingleBatch fails with the builder's message, " +
        retryMode(retry)) {
      val valueSize = 1024
      val limit = splitCaseStringLimit(valueSize)
      withLimits(limits(stringBytes = limit)) {
        val iter = r2c(splitCaseData(valueSize).iterator(splitCaseRows), intStringSchema,
          RequireSingleBatch, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assertResult(limitMessage("The string data size in bytes",
          (rowsBeforeLimit + 1L) * valueSize, limit, DType.STRING))(e.getMessage)
      }
    }

    test("a first row whose array alone exceeds a lowered limit fails with the single-row " +
        "message, " + retryMode(retry)) {
      def arrayRow(n: Int, elements: Array[Any]): InternalRow =
        new GenericInternalRow(Array[Any](n, new GenericArrayData(elements)))
      val element = repeatedDigits(1, 1000)
      val rows = Iterator(
        arrayRow(1, Array.fill[Any](5)(UTF8String.fromBytes(element))),
        arrayRow(2, Array[Any](UTF8String.fromString("2"))))
      withLimits(limits(stringBytes = 4096)) {
        val iter = r2c(rows, intArraySchema, cacheBuildGoal, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assertResult(singleRowMessage("The string data size in bytes", 5000, 4096,
          DType.STRING))(e.getMessage)
      }
    }

    Seq(3, 0).foreach { prefixRows =>
      val where = if (prefixRows > 0) "after a legal prefix" else "on the first row"
      test(s"a column limit exception from the input's next() $where passes through " +
          "unchanged, " + retryMode(retry)) {
        val failure =
          new ColumnLimitExceededException("thrown by the input iterator", "a test remedy")
        val input = new FailingInput(new IntStringRows(
          intIsNull = _ => false, stringIsNull = _ => false, valueSize = _ => 8)
          .iterator(prefixRows), failure)
        val iter = r2c(input, intStringSchema, cacheBuildGoal, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assert(e eq failure, s"expected the input's own exception, got: ${e.getMessage}")
        assertResult(prefixRows + 1, "calls to the input's next()")(input.nextCalls)
      }
    }

    test("a row too large for empty builders after a legal prefix fails with the single-row " +
        "message, " + retryMode(retry)) {
      RmmSpark.getAndResetNumRetryThrow(taskId)
      val valueSize = (n: Int) => if (n == 4) 5000 else sharedValueSize
      val source = unsafeRows(4, valueSize)
      val expected = sharedIntStringData(valueSize)
      withLimits(limits(stringBytes = 4096)) {
        val iter = r2c(source, intStringSchema, cacheBuildGoal, smallBatchBytes, retry)
        withResource(iter.next()) { batch =>
          assertResult(3, "rows before the row too large for any batch")(batch.numRows())
          assertIntStringBatch(batch, 1, expected)
        }
        val e = interceptLimit(iter.next())
        assertResult(singleRowMessage("The string data size in bytes", 5000, 4096,
          DType.STRING))(e.getMessage)
      }
      // Row 4 came from the source once; its retry in empty builders used the carried copy.
      assertResult(4, "calls to the source's next()")(source.nextCalls)
      assertResult(0, "retry OOMs")(RmmSpark.getAndResetNumRetryThrow(taskId))
    }
  }

  test("TargetSize ends batches at the row estimate and at the byte target") {
    // The schema's row estimate for 64 KiB.
    val shortRows = 2319
    def sizedRows(valueSize: Int => Int): IntStringRows = new IntStringRows(
      intIsNull = _ => false, stringIsNull = _ => false, valueSize = valueSize)
    // Cases of (input, rows, byte target, batch sizes).
    // The sizes were recorded on main and are pre-existing behavior.
    Seq(
      // The short rows end the first batch at the row estimate, and rows of a 1 KiB string end
      // the later ones at the byte target.
      (new IntStringRows(intIsNull = n => n <= shortRows && n % 3 == 0,
        stringIsNull = n => n <= shortRows && n % 3 == 0,
        valueSize = n => if (n <= shortRows) 0 else 1024), shortRows + 320, smallBatchBytes,
        Seq(shortRows, 64, 64, 64, 64, 64)),
      // A row of an INT and a 1 KiB string counts 1032.25 bytes, so 64 rows reach 66064 exactly
      // and no 65th is admitted. One byte higher, the 65th row is admitted and ends the batch.
      (sizedRows(_ => 1024), 200, 66064L, Seq(64, 64, 64, 8)),
      (sizedRows(_ => 1024), 200, 66065L, Seq(65, 65, 65, 5)),
      // A first row larger than the target is a batch of its own. The row estimate refined from
      // it then ends each later batch at the number of rows emitted so far.
      (sizedRows(n => if (n == 1) 128 * 1024 else 1024), 6, smallBatchBytes, Seq(1, 1, 2, 2))
    ).foreach { case (data, numRows, target, expectedSizes) =>
      Seq(true, false).foreach { retry =>
        val sizes = drainBatches(r2c(data.iterator(numRows), intStringSchema,
            TargetSize(target), target, retry)) { (batch, firstRow) =>
          assertIntStringBatch(batch, firstRow, data)
        }
        assertResult(expectedSizes,
          s"batches of $numRows rows at a target of $target, ${retryMode(retry)}")(sizes)
      }
    }
  }

  test("a lowered string or binary limit splits the input under a finite target") {
    val stringData = sharedIntStringData(_ => sharedValueSize)
    // Each input, made fresh for every run, with the check of its batches, which returns the INT
    // null count.
    val inputs = Seq(
      ("UnsafeRow strings", intStringSchema, () => unsafeRows(10, _ => sharedValueSize),
        (batch: ColumnarBatch, firstRow: Int) =>
          assertIntStringBatch(batch, firstRow, stringData)(0)),
      ("GenericInternalRow binaries", intArrayBinarySchema, () => intArrayBinaryRows(10),
        (batch: ColumnarBatch, firstRow: Int) => assertIntArrayBinaryBatch(batch, firstRow)))
    for ((name, rowSchema, newSource, check) <- inputs; retry <- Seq(true, false)) {
      val source = newSource()
      val intNullCounts = ArrayBuffer[Long]()
      val sizes = withLimits(limits(stringBytes = 4096, fixedWidthElements = 4096)) {
        drainBatches(r2c(source, rowSchema, TargetSize(smallBatchBytes), smallBatchBytes,
            retry)) { (batch, firstRow) =>
          intNullCounts += check(batch, firstRow)
        }
      }
      // Four rows fit each lowered limit, so the batches hold 4, 4 and 2 rows, and row 5's INT
      // null is counted in the second batch only. The source is asked for each row once.
      val clue = s"$name, ${retryMode(retry)}"
      assertResult(Seq(4, 4, 2), s"batch sizes, $clue")(sizes)
      assertResult(Seq(0L, 1L, 0L), s"INT null counts, $clue")(intNullCounts.toList)
      assertResult(10, s"calls to the source's next(), $clue")(source.nextCalls)
    }
  }

  private def retryMode(enabled: Boolean): String =
    if (enabled) "r2c retry on" else "r2c retry off"

  private def r2c(
      rows: Iterator[InternalRow],
      rowSchema: StructType,
      goal: CoalesceSizeGoal,
      batchSizeBytes: Long,
      enableRetry: Boolean): RowToColumnarIterator =
    new RowToColumnarIterator(rows, rowSchema, goal, batchSizeBytes,
      new GpuRowToColumnConverter(rowSchema), enableRetry)

  private def singleRowMessage(what: String, attempted: Long, limit: Long,
      dType: DType): String =
    s"A single row cannot fit in a batch on its own: $what would be $attempted, exceeding the " +
      s"limit of $limit for a column of cuDF type $dType; reduce the size of that row's values"

  /** Intercepts the limit exception, closing any batch the call returns instead. */
  private def interceptLimit(nextBatch: => ColumnarBatch): ColumnLimitExceededException = {
    val e = intercept[ColumnLimitExceededException] {
      withResource(nextBatch)(_ => ())
    }
    assertResult(classOf[ColumnLimitExceededException])(e.getClass)
    e
  }

  private def splitCaseData(valueSize: Int): IntStringRows = new IntStringRows(
    intIsNull = n => n % 10 == 7 || n == rowsBeforeLimit + 1,
    stringIsNull = _ => false,
    valueSize = _ => valueSize)

  private def splitCaseStringLimit(valueSize: Int): Long = (rowsBeforeLimit + 1L) * valueSize - 1

  /**
   * Drains the split case and checks that it ends the first batch before row 2048: two batches,
   * every value equal to the input, every null count equal to its mask, and no retry OOM, so
   * the split never waited in blockUntilMemoryFreed.
   */
  private def assertSplitAtLimit(iter: Iterator[ColumnarBatch], valueSize: Int): Unit = {
    RmmSpark.getAndResetNumRetryThrow(taskId)
    val expected = splitCaseData(valueSize)
    val sizes = drainBatches(iter) { (batch, firstRow) =>
      assertIntStringBatch(batch, firstRow, expected)
    }
    assertResult(Seq(rowsBeforeLimit, splitCaseRows - rowsBeforeLimit), "batch sizes")(sizes)
    assertResult(0, "retry OOMs")(RmmSpark.getAndResetNumRetryThrow(taskId))
  }

  private def sharedIntStringData(valueSize: Int => Int): IntStringRows = new IntStringRows(
    intIsNull = _ == sharedNullIntRow, stringIsNull = _ => false, valueSize = valueSize)

  /** The shared values as UnsafeRows. */
  private def unsafeRows(numRows: Int, valueSize: Int => Int): CountingInput = {
    val projection = UnsafeProjection.create(intStringSchema)
    new CountingInput(
      sharedIntStringData(valueSize).iterator(numRows).map(row => projection(row).copy()))
  }

  /** Rows of an INT, an ARRAY<STRING> of two copies of the row number and a BINARY. */
  private def intArrayBinaryRows(numRows: Int): CountingInput =
    new CountingInput((1 to numRows).iterator.map { n =>
      val digits = UTF8String.fromString(n.toString)
      new GenericInternalRow(Array[Any](if (n == sharedNullIntRow) null else n,
        new GenericArrayData(Array[Any](digits, digits)), repeatedDigits(n, sharedValueSize)))
    })

  /** Drains the iterator, checking each batch with its first input row; returns the sizes. */
  private def drainBatches(iter: Iterator[ColumnarBatch])(
      check: (ColumnarBatch, Int) => Unit): Seq[Int] = {
    val sizes = ArrayBuffer[Int]()
    var firstRow = 1
    while (iter.hasNext) {
      withResource(iter.next()) { batch =>
        check(batch, firstRow)
        sizes += batch.numRows()
        firstRow += batch.numRows()
      }
    }
    sizes.toList
  }

  private def withHostColumns[T](batch: ColumnarBatch)(body: Array[HostColumnVector] => T): T =
    withResource(GpuColumnVector.extractBases(batch).safeMap(_.copyToHost()))(body)

  /**
   * Checks a batch of the INT and STRING schema against the expected rows from firstRow on, and
   * each column's null count against its mask. Returns the columns' null counts.
   */
  private def assertIntStringBatch(
      batch: ColumnarBatch,
      firstRow: Int,
      expected: IntStringRows): Array[Long] = {
    withHostColumns(batch) { columns =>
      columns.foreach(column => assertNullCountsMatchMasks(column))
      val ints = columns(0)
      val strings = columns(1)
      (0 until batch.numRows()).foreach { i =>
        val n = firstRow + i
        val row = expected.row(n)
        assertResult(row.isNullAt(0), s"INT null at row $n")(ints.isNull(i))
        if (!row.isNullAt(0)) {
          assertResult(row.getInt(0), s"INT at row $n")(ints.getInt(i))
        }
        assertResult(row.isNullAt(1), s"string null at row $n")(strings.isNull(i))
        if (!row.isNullAt(1)) {
          // Compared as a flag, so that a failure does not print values of up to 128 KiB.
          val equal = UTF8String.fromBytes(strings.getUTF8(i)) == row.getUTF8String(1)
          assert(equal, s"string at row $n")
        }
      }
      columns.map(_.getNullCount)
    }
  }

  /** Checks a batch of intArrayBinaryRows; returns the INT null count. */
  private def assertIntArrayBinaryBatch(batch: ColumnarBatch, firstRow: Int): Long = {
    withHostColumns(batch) { columns =>
      columns.foreach(column => assertNullCountsMatchMasks(column))
      val ints = columns(0)
      val arrays = columns(1)
      val elements = arrays.getChildColumnView(0)
      val binaries = columns(2)
      (0 until batch.numRows()).foreach { i =>
        val n = firstRow + i
        assertResult(n == sharedNullIntRow, s"INT null at row $n")(ints.isNull(i))
        if (n != sharedNullIntRow) {
          assertResult(n, s"INT at row $n")(ints.getInt(i))
        }
        val array = (arrays.getStartListOffset(i) until arrays.getEndListOffset(i))
          .map(j => elements.getJavaString(j))
        assertResult(Seq(n.toString, n.toString), s"array at row $n")(array)
        val equal = java.util.Arrays.equals(binaries.getBytesFromList(i),
          repeatedDigits(n, sharedValueSize))
        assert(equal, s"binary at row $n")
      }
      ints.getNullCount
    }
  }

  /** The ASCII digits of n, repeated for size bytes. */
  private def repeatedDigits(n: Int, size: Int): Array[Byte] = {
    val digits = n.toString.getBytes(StandardCharsets.US_ASCII)
    Array.tabulate[Byte](size)(i => digits(i % digits.length))
  }

  /**
   * Rows of a nullable INT and a nullable STRING, numbered from 1: the INT is the row number and
   * the string repeats its digits for valueSize(n) bytes.
   */
  private class IntStringRows(
      intIsNull: Int => Boolean,
      stringIsNull: Int => Boolean,
      valueSize: Int => Int) {
    def row(n: Int): InternalRow = new GenericInternalRow(Array[Any](
      if (intIsNull(n)) null else n,
      if (stringIsNull(n)) null else UTF8String.fromBytes(repeatedDigits(n, valueSize(n)))))

    def iterator(numRows: Int): Iterator[InternalRow] = (1 to numRows).iterator.map(n => row(n))
  }

  /** Returns the given rows, counting the calls to next(). */
  private class CountingInput(rows: Iterator[InternalRow]) extends Iterator[InternalRow] {
    var nextCalls = 0

    override def hasNext: Boolean = rows.hasNext

    override def next(): InternalRow = {
      nextCalls += 1
      rows.next()
    }
  }

  /** Returns the prefix rows, then throws the given exception from next(). */
  private class FailingInput(prefix: Iterator[InternalRow], failure: RuntimeException)
      extends Iterator[InternalRow] {
    var nextCalls = 0

    override def hasNext: Boolean = true

    override def next(): InternalRow = {
      nextCalls += 1
      if (prefix.hasNext) prefix.next() else throw failure
    }
  }
}
