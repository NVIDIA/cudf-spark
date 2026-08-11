/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, GpuSplitAndRetryOOM, RmmSpark}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSortRetrySuite extends RmmSparkRetrySuiteBase with MockitoSugar {

  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val sortOrder = SortOrder(ref, Ascending)
  private val attrs = AttributeReference(ref.name, ref.dataType, ref.nullable)()
  private val gpuSorter = new GpuSorter(Seq(sortOrder), Array(attrs), Map.empty[String, GpuMetric])
  private val NUM_ROWS = 100

  private def batchIter(batches: Int): Iterator[ColumnarBatch] =
    ((0 until batches)).map { _ =>
      buildBatch
    }.toIterator

  private def buildBatch: ColumnarBatch = {
    val ints = (NUM_ROWS / 2 until NUM_ROWS) ++ (0 until NUM_ROWS / 2)
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), NUM_ROWS)
  }

  test("GPU out-of-core sort without OOM failures") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 1024)
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS * 2)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort with retry when first-pass-split GpuRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 1024,
      firstPassSplitExp = new GpuRetryOOM())
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS * 2)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort throws when first-pass-split GpuSplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 1024,
      firstPassSplitExp = new GpuSplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[GpuSplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  test("GPU out-of-core sort with retry when merge-sort-split GpuRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 400,
      mergeSortExp = new GpuRetryOOM())
    withResource(outCoreIter) { _ =>
      var numRows = 0
      while(outCoreIter.hasNext) {
        withResource(outCoreIter.next()) { cb =>
          numRows += cb.numRows()
        }
      }
      assertResult(NUM_ROWS * 2)(numRows)
    }
  }

  test("GPU out-of-core sort throws when merge-sort-split GpuSplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 400,
      mergeSortExp = new GpuSplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[GpuSplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  test("GPU out-of-core sort with retry when concat-output GpuRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 400,
      concatOutExp = new GpuRetryOOM())
    withResource(outCoreIter) { _ =>
      var numRows = 0
      while (outCoreIter.hasNext) {
        withResource(outCoreIter.next()) { cb =>
          numRows += cb.numRows()
        }
      }
      assertResult(NUM_ROWS * 2)(numRows)
    }
  }

  test("GPU out-of-core sort throws when concat-output GpuSplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      batchIter(2),
      gpuSorter,
      targetSize = 400,
      concatOutExp = new GpuSplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[GpuSplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  private class GpuOutOfCoreSortIteratorThatThrows(
      iter: Iterator[ColumnarBatch],
      sorter: GpuSorter,
      targetSize: Long,
      firstPassSplitExp: Throwable = null,
      mergeSortExp: Throwable = null,
      concatOutExp: Throwable = null,
      expMaxCount: Int = 1)
    extends GpuOutOfCoreSortIterator(iter, sorter, targetSize,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric){

    private var expCnt = expMaxCount

    override def onFirstPassSplit(): Unit = if (firstPassSplitExp != null && expCnt > 0) {
      expCnt -= 1
      throw firstPassSplitExp
    }

    override def onMergeSortSplit(): Unit = if (mergeSortExp != null && expCnt > 0) {
      expCnt -= 1
      throw mergeSortExp
    }

    override def onConcatOutput(): Unit = if (concatOutExp != null && expCnt > 0) {
      expCnt -= 1
      throw concatOutExp
    }
  }

  test("GPU each batch sort with GpuRetryOOM") {
    val eachBatchIter = GpuSortEachBatchIterator(
      batchIter(2),
      gpuSorter,
      singleBatch = false)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    while (eachBatchIter.hasNext) {
      var pos = 0
      var curValue = 0
      withResource(eachBatchIter.next()) { cb =>
        assertResult(NUM_ROWS)(cb.numRows())
        withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
          while (pos < hCol.getRowCount.toInt) {
            assertResult(curValue)(hCol.getInt(pos))
            pos += 1
            curValue += 1
          }
        }
      }
    }
  }

  test("GPU each batch sort throws GpuSplitAndRetryOOM") {
    val inputIter = batchIter(2)
    try {
      val eachBatchIter = GpuSortEachBatchIterator(
        inputIter,
        gpuSorter,
        singleBatch = false)
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertThrows[GpuSplitAndRetryOOM] {
        eachBatchIter.next()
      }
    } finally {
      inputIter.foreach(_.close())
    }
  }
}
