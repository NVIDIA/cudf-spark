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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, ExprId,
  SortOrder}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class LimitRetrySuite extends RmmSparkRetrySuiteBase {

  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val attrs = AttributeReference(ref.name, ref.dataType, ref.nullable)()
  private val gpuSorter = new GpuSorter(Seq(SortOrder(ref, Ascending)), Array(attrs),
    Map.empty[String, GpuMetric])
  private val NUM_ROWS = 100

  private def buildBatch(ints: Seq[Int]): ColumnarBatch = {
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), ints.length)
  }

  private def buildBatch1: ColumnarBatch = {
    buildBatch(0 until NUM_ROWS by 2)
  }

  private def buildBatch2: ColumnarBatch = {
    buildBatch(1 until NUM_ROWS by 2)
  }

  test("GPU topn with split and retry OOM") {
    val limit = 20
    Seq(0, 5).foreach { offset =>
      val topNIter = GpuTopN(limit, gpuSorter, Seq(buildBatch1, buildBatch2).toIterator,
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
        offset)
      val numRows = limit - offset
      var curValue = offset
      var pos = 0
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assert(topNIter.hasNext)
      withResource(topNIter.next()) { scb =>
        withResource(scb.getColumnarBatch()) { cb =>
          withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
            while (pos < hCol.getRowCount.toInt) {
              assertResult(curValue)(hCol.getInt(pos))
              pos += 1
              curValue += 1
            }
          }
        }
      }
      assertResult(numRows)(pos)
      // only one batch
      assert(!topNIter.hasNext)
    }
  }

  /** A TopN over a rows-only child can only order by an expression that reads no column. */
  private val literalSorter = new GpuSorter(Seq(SortOrder(GpuLiteral(1, IntegerType), Ascending)),
    Array.empty[Attribute], Map.empty[String, GpuMetric])

  /** Runs GpuTopN over rows-only batches and returns (rows, columns) for each output batch. */
  private def rowsOnlyTopN(rowCounts: Seq[Int], limit: Int, offset: Int): Seq[(Int, Int)] = {
    val topNIter = GpuTopN(limit, literalSorter,
      rowCounts.map(n => new ColumnarBatch(Array.empty, n)).toIterator,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
      offset)
    topNIter.map { scb =>
      withResource(scb) { _ =>
        withResource(scb.getColumnarBatch())(cb => (cb.numRows, cb.numCols))
      }
    }.toList
  }

  test("GPU topn over rows-only batches keeps the rows the CPU keeps") {
    Seq(
      (Seq(1000, 1000), 5, 0), // two batches in one task
      (Seq.fill(5)(1000), 5, 2),
      (Seq(3, 4), 20, 0), // the limit exceeds the rows
      (Seq(3, 4), 20, 5),
      (Seq(2, 2), 10, 5), // the offset exceeds the rows
      (Seq(0, 7, 0), 5, 0),
      (Seq(0, 0), 5, 0),
      (Seq(1000), 5, 0),
      (Seq(10, Int.MaxValue - 5), 10, 0), // the two inputs together pass Int.MaxValue rows
      // Each input alone fills a limit of 2^30.
      (Seq(1 << 30, 1 << 30), 1 << 30, 0),
      (Seq(1 << 30, 1 << 30), 1 << 30, 7),
      // The largest limit Spark 3.5 plans as a top-k, under topKSortFallbackThreshold.
      (Seq(Int.MaxValue, Int.MaxValue, Int.MaxValue), Int.MaxValue - 16, 0)
    ).foreach { case (rowCounts, limit, offset) =>
      val expected = math.max(0L, math.min(limit, rowCounts.map(_.toLong).sum) - offset).toInt
      assertResult(Seq((expected, 0)), s"rows $rowCounts, limit $limit, offset $offset")(
        rowsOnlyTopN(rowCounts, limit, offset))
    }
  }

  test("GPU topn over rows-only batches allocates nothing a split and retry OOM can stop") {
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertResult(Seq((5, 0)))(rowsOnlyTopN(Seq(1000, 1000), 5, 0))
    assertResult(0)(RmmSpark.getAndResetNumSplitRetryThrow(1))
    // The OOM is still armed, so the next device allocation takes it.
    assertThrows[GpuSplitAndRetryOOM] {
      withResource(ColumnVector.fromInts(1))(_ => ())
    }
  }

  test("GPU limit with retry OOM") {
    val totalRows = 24
    Seq((20, 5), (50, 5)).foreach { case (limit, offset) =>
      val limitIter = new GpuBaseLimitIterator(
        // 3 batches as input, and each has 8 rows
        (0 until totalRows).grouped(8).map(buildBatch(_)).toList.toIterator,
        limit, offset, Array[DataType](IntegerType),
        NoopMetric, NoopMetric, NoopMetric)
      var leftRows = if (limit > totalRows) totalRows - offset else limit - offset
      var curValue = offset
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      while(limitIter.hasNext) {
        var pos = 0
        withResource(limitIter.next()) { cb =>
          withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
            while (pos < hCol.getRowCount.toInt) {
              assertResult(curValue)(hCol.getInt(pos))
              pos += 1
              curValue += 1
            }
          }
          leftRows -= cb.numRows()
        }
      }
      // all the rows are consumed
      assertResult(0)(leftRows)
    }
  }

}
