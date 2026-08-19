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

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.window._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.{Ascending, CurrentRow, ExprId, RangeFrame, RowFrame, SortOrder, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.rapids.aggregate.GpuCount
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class WindowRetrySuite
    extends RmmSparkRetrySuiteBase
        with MockitoSugar {
  private def buildInputBatch() = {
    val windowTable = new Table.TestBuilder()
      .column(1.asInstanceOf[java.lang.Integer], 1, 1, 1)
      .column(5L, null.asInstanceOf[java.lang.Long], 3L, 3L)
      .build()
    withResource(windowTable) { tbl =>
      GpuColumnVector.from(tbl, Seq(IntegerType, LongType).toArray[DataType])
    }
  }

  def setupWindowIterator(
      frame: GpuSpecifiedWindowFrame,
      orderSpec: Seq[SortOrder] = Seq.empty,
      boundPartitionSpec: Seq[GpuExpression] = Seq.empty): GpuWindowIterator = {
    val spec = GpuWindowSpecDefinition(Seq.empty, orderSpec, frame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral.create(1, IntegerType))), spec)
    val it = new GpuWindowIterator(
      input = Seq(buildInputBatch()).iterator,
      boundWindowOps = Seq(GpuAlias(count, "count")()),
      boundPartitionSpec,
      boundOrderSpec = orderSpec,
      outputTypes = Array(DataTypes.LongType),
      numOutputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      opTime = NoopMetric
    )
    // pre-load a spillable batch before injecting the OOM
    // and wrap in mockito
    val spillableBatch = spy(it.getNext())
    it.onDeck = Some(spillableBatch)
    it
  }

  test("row based window handles GpuRetryOOM") {
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(UnboundedFollowing))
    val it = setupWindowIterator(frame)
    val inputBatch = it.onDeck.get
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    withResource(it.next()) { batch =>
      assertResult(4)(batch.numRows())
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        assertResult(4)(hostCol.getRowCount)
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          assertResult(4)(hostCol.getLong(row))
        }
      }
      verify(inputBatch, times(2)).getColumnarBatch
      verify(inputBatch, times(1)).close()
    }
  }

  test("optimized-row based window handles GpuRetryOOM") {
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val it = setupWindowIterator(frame)
    val inputBatch = it.onDeck.get
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    withResource(it.next()) { batch =>
      assertResult(4)(batch.numRows())
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        assertResult(4)(hostCol.getRowCount)
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          assertResult(row + 1)(hostCol.getLong(row))
        }
      }
      verify(inputBatch, times(2)).getColumnarBatch
      verify(inputBatch, times(1)).close()
    }
  }

  test("ranged based window handles GpuRetryOOM") {
    val frame = GpuSpecifiedWindowFrame(
      RangeFrame,
      GpuLiteral.create(-1, IntegerType),
      GpuSpecialFrameBoundary(CurrentRow))
    val child = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "test")
    val orderSpec = SortOrder(child, Ascending) :: Nil
    val it = setupWindowIterator(frame, orderSpec = orderSpec)
    val inputBatch = it.onDeck.get
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    withResource(it.next()) { batch =>
      assertResult(4)(batch.numRows())
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        assertResult(4)(hostCol.getRowCount)
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          assertResult(4)(hostCol.getLong(row))
        }
      }
      verify(inputBatch, times(2)).getColumnarBatch
      verify(inputBatch, times(1)).close()
    }
  }

  test("GpuSplitAndRetryOOM is not handled in doAggs") {
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val it = setupWindowIterator(frame)
    val inputBatch = it.onDeck.get
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      it.next()
    }
    verify(inputBatch, times(1)).getColumnarBatch
    verify(inputBatch, times(1)).close()
  }

  test("row based group by window handles GpuRetryOOM") {
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val it = setupWindowIterator(frame, boundPartitionSpec =
      Seq(GpuBoundReference(1, DataTypes.LongType, false)(ExprId.apply(0), "tbd")))
    val inputBatch = it.onDeck.get
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    withResource(it.next()) { batch =>
      assertResult(4)(batch.numRows())
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        assertResult(4)(hostCol.getRowCount)
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          if (row == 0) { // 5
            assertResult(1)(hostCol.getLong(row))
          } else if (row == 1) { // null
            assertResult(1)(hostCol.getLong(row))
          } else if (row == 2) { // 3
            assertResult(1)(hostCol.getLong(row))
          } else if (row == 3) { // 3
            assertResult(2)(hostCol.getLong(row))
          }
        }
      }
      verify(inputBatch, times(2)).getColumnarBatch
      verify(inputBatch, times(1)).close()
    }
  }

  test("row-based group by running window handles GpuSplitAndRetryOOM") {
    val runningFrame = GpuSpecifiedWindowFrame(RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding), GpuSpecialFrameBoundary(CurrentRow))
    val boundOrderSpec = SortOrder(
      GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "int"), Ascending) :: Nil
    val boundPartSpec = Seq(
      GpuBoundReference(1, LongType, nullable = true)(ExprId(1), "long"))
    val spec = GpuWindowSpecDefinition(boundPartSpec, boundOrderSpec, runningFrame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral.create(1, IntegerType))), spec)
    val cb = buildInputBatch()

    val runningIter = new GpuRunningWindowIterator(
      Seq(cb).iterator, Seq(GpuAlias(count, "count")()), boundPartSpec, boundOrderSpec,
      Array(LongType), NoopMetric, NoopMetric, NoopMetric)
    withResource(runningIter) { _ =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      // there should be two batches, each has two rows
      withResource(runningIter.next()) { first =>
        assertResult(1)(first.numCols())
        assertResult(2)(first.numRows())
        withResource(first.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hc =>
          // one row one partition
          Seq(1L, 1L).zipWithIndex.foreach { case (cnt, pos) =>
            assert(cnt == hc.getLong(pos))
          }
        }
      }
      withResource(runningIter.next()) { second =>
        assertResult(1)(second.numCols())
        assertResult(2)(second.numRows())
        withResource(second.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hc =>
          // count for partition [3, 3] are [1L, 2L] by running frame.
          Seq(1L, 2L).zipWithIndex.foreach { case (cnt, pos) =>
            assert(cnt == hc.getLong(pos))
          }
        }
      }
      assert(runningIter.isEmpty)
    }
  }

  private def buildLargeBatchForBoundedWindow() = {
    // Build a batch with 8 rows to have enough data for bounded window with preceding/following
    val windowTable = new Table.TestBuilder()
      .column(1.asInstanceOf[java.lang.Integer], 2, 3, 4, 5, 6, 7, 8)
      .column(10L.asInstanceOf[java.lang.Long], 20L, 30L, 40L, 50L, 60L, 70L, 80L)
      .build()
    withResource(windowTable) { tbl =>
      GpuColumnVector.from(tbl, Seq(IntegerType, LongType).toArray[DataType])
    }
  }

  test("batched bounded window handles GpuRetryOOM") {
    // Create a bounded window frame: ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    val boundedFrame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral.create(-2, IntegerType),
      GpuLiteral.create(2, IntegerType))
    val spec = GpuWindowSpecDefinition(Seq.empty, Seq.empty, boundedFrame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral.create(1, IntegerType))), spec)

    var oomRetriedForCacheConcat = false
    // Create the iterator with bounded window (minPreceding=-2, maxFollowing=2)
    val it = new GpuBatchedBoundedWindowIterator(
      input = Seq(buildLargeBatchForBoundedWindow(), buildLargeBatchForBoundedWindow()).iterator,
      boundWindowOps = Seq(GpuAlias(count, "count")()),
      boundPartitionSpec = Seq.empty,
      boundOrderSpec = Seq.empty,
      outputTypes = Array(DataTypes.LongType),
      minPreceding = -2,
      maxFollowing = 2,
      numOutputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      opTime = NoopMetric
    ) { // override the 3 methods who support oom-retry.
      override def getNextInputBatchWithRetry: SpillableColumnarBatch = {
        if (hasCache) {
          // Inject a retry OOM when needing to concat the cache and the input batch.
          RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
          oomRetriedForCacheConcat = true
        }
        super.getNextInputBatchWithRetry
      }

      override def computeWindowWithRetry(scb: SpillableColumnarBatch): SpillableColumnarBatch = {
        // Inject a retry OOM for the window computation
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        super.computeWindowWithRetry(scb)
      }

      override def trimWithRetry(scb: SpillableColumnarBatch, offTheTop: Int,
          offTheBottom: Int): ColumnarBatch = {
        // Inject a retry OOM for the trimming operation
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        super.trimWithRetry(scb, offTheTop, offTheBottom)
      }
    }

    try {
      withResource(it.next()) { batch =>
        // we expect 6 output rows for the first input batch
        assertResult(6)(batch.numRows())
        assertResult(1)(batch.numCols())
        // Verify the count values for bounded window
        // Row 0: count of rows in range [max(0,0-2), min(7,0+2)] = [0,2] = 3 rows
        // Row 1: count of rows in range [max(0,1-2), min(7,1+2)] = [0,3] = 4 rows
        // Row 2: count of rows in range [max(0,2-2), min(7,2+2)] = [0,4] = 5 rows
        // Row 3-4: count of 5 rows (full window)
        // Row 5: count of rows in range [3,7] = 5 rows
        // Row 6-7: unprocessed for next batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          assertResult(6)(hostCol.getRowCount)
          val expectedCounts = Seq(3L, 4L, 5L, 5L, 5L, 5L)
          (0 until hostCol.getRowCount.toInt).foreach { row =>
            assertResult(expectedCounts(row))(hostCol.getLong(row))
          }
        }
      }
      withResource(it.next()) { batch => // second batch
        // we expect 10 (=8+2) output rows for the second last) input batch.
        assertResult(10)(batch.numRows())
        assertResult(1)(batch.numCols())
        // Verify the count values for bounded window,
        // Row 0-1: count of 5 rows for rows from the cache (full window with
        //          preceding and unprocessed rows from the previous rows)
        // Row 2-6: count of 5 rows for rows from the input batch (full window with
        //          preceding and unprocessed rows from the previous rows)
        // Row 7: count of rows in range [5,9] = 5 rows
        // Row 8: count of rows in range [6,9] = 4 rows
        // Row 9: count of rows in range [7,9] = 3 rows
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          assertResult(10)(hostCol.getRowCount)
          val expectedCounts = Seq(5L, 5L, 5L, 5L, 5L, 5L, 5L, 5L, 4L, 3L)
          (0 until hostCol.getRowCount.toInt).foreach { row =>
            assertResult(expectedCounts(row))(hostCol.getLong(row))
          }
        }
      }

      assert(!it.hasNext) // Iterator should be exhausted
      assert(oomRetriedForCacheConcat)
    } finally {
      it.clearCached()
    }
  }

  test("batched bounded window handles GpuSplitAndRetryOOM") {
    // GpuSplitAndRetryOOM should propagate since we use withRetryNoSplit (no split policy)
    val boundedFrame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuLiteral.create(-2, IntegerType),
      GpuLiteral.create(2, IntegerType))
    val spec = GpuWindowSpecDefinition(Seq.empty, Seq.empty, boundedFrame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral.create(1, IntegerType))), spec)

    val it = new GpuBatchedBoundedWindowIterator(
      input = Seq(buildLargeBatchForBoundedWindow()).iterator,
      boundWindowOps = Seq(GpuAlias(count, "count")()),
      boundPartitionSpec = Seq.empty,
      boundOrderSpec = Seq.empty,
      outputTypes = Array(DataTypes.LongType),
      minPreceding = -2,
      maxFollowing = 2,
      numOutputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      opTime = NoopMetric
    )

    try {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)

      // GpuSplitAndRetryOOM should be thrown since withRetryNoSplit doesn't support splitting
      assertThrows[GpuSplitAndRetryOOM] {
        it.next()
      }
    } finally {
      it.clearCached()
    }
  }
}
