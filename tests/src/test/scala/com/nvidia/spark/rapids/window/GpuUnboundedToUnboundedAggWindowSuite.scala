/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, NoopMetric, RmmSparkRetrySuiteBase, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.RmmSpark
import java.util
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, ShortType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuUnboundedToUnboundedAggWindowSuite extends RmmSparkRetrySuiteBase with MockitoSugar {
  def basicRepeatTest(numOutputRows: Long, rowsPerRideAlongBatch: Int,
      aggGroups: Int, targetSizeBytes: Int,
      retrySplit: Boolean = false, retryRepeat: Boolean = false) : Unit = {
    // First I need to setup the operations. I am trying to test repeat in isolation
    // so we are not going to build them up using the front door
    val aggOutput = Seq(AttributeReference("my_max", IntegerType, nullable = true)(),
      AttributeReference("_count", LongType, nullable = true)())

    val rideAlongOutput = Seq(AttributeReference("a", ShortType, nullable = true)())
    val repeatOutput = GpuUnboundedToUnboundedAggWindowIterator.repeatOps(aggOutput)

    val finalProject = GpuUnboundedToUnboundedAggWindowIterator.computeFinalProject(
      rideAlongOutput, repeatOutput, repeatOutput ++ rideAlongOutput, Map.empty)

    val conf = GpuUnboundedToUnboundedAggStages(Seq.empty, Seq.empty, Seq.empty,
                                                Seq.empty, finalProject)

    def makeRepeatCb(): SpillableColumnarBatch = {
      val data = ArrayBuffer[Int]()
      val counts = ArrayBuffer[Long]()
      var rowRemainingForRepeat = numOutputRows
      var groupId = 0
      val rowsPerGroup = math.ceil(numOutputRows.toDouble / aggGroups).toLong
      while(rowRemainingForRepeat > 0) {
        data.append(groupId)
        val rowsInGroup = math.min(rowRemainingForRepeat, rowsPerGroup)
        counts.append(rowsInGroup)
        groupId += 1
        rowRemainingForRepeat -= rowsInGroup
      }
      val table = withResource(ColumnVector.fromInts(data.toSeq: _*)) { dataCv =>
        withResource(ColumnVector.fromLongs(counts.toSeq: _*)) { countsCv =>
          new Table(dataCv, countsCv)
        }
      }
      withResource(table) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(table, Array[DataType](IntegerType, LongType)),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }

    def makeRideAlongCb(startRow: Long, numRows: Int): SpillableColumnarBatch = {
      val values = (0 until numRows).map(row => (startRow + row).toShort)
      val table = withResource(ColumnVector.fromShorts(values: _*)) { data =>
        new Table(data)
      }
      withResource(table) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(table, Array[DataType](ShortType)),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }

    val rideAlongList = new util.LinkedList[SpillableColumnarBatch]
    var rowsRemaining = numOutputRows
    while (rowsRemaining > 0) {
      val rowsToAdd = math.min(rowsRemaining, rowsPerRideAlongBatch)
      val startRow = numOutputRows - rowsRemaining
      rowsRemaining -= rowsToAdd
      rideAlongList.add(makeRideAlongCb(startRow, rowsToAdd.toInt))
    }
    val injectRetry = retrySplit || retryRepeat
    val aggResult = if (injectRetry) spy(makeRepeatCb()) else makeRepeatCb()
    if (injectRetry) {
      var injected = false
      doAnswer { invocation =>
        val cb = invocation.callRealMethod().asInstanceOf[ColumnarBatch]
        closeOnExcept(cb) { _ =>
          if (!injected) {
            // Arm after materializing the aggregate input so earlier ride-along
            // splits, concatenations, or unspills cannot consume the injected OOM.
            assert(RmmSpark.getAndResetNumRetryThrow(1) == 0)
            injected = true
            RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
              RmmSpark.OomInjectionType.GPU.ordinal, 0)
          }
          cb
        }
      }.when(aggResult).getColumnarBatch()
    }
    val inputIter = Seq(SecondPassAggResult(rideAlongList, aggResult)).toIterator
    val splitIter = new GpuUnboundedToUnboundedAggSliceBySizeIterator(inputIter, conf,
      targetSizeBytes, NoopMetric)
    val repeatIter = new GpuUnboundedToUnboundedAggFinalIterator(splitIter, conf,
      NoopMetric, NoopMetric, NoopMetric)

    var numRowsActuallyOutput = 0L
    var numBatchesActuallyOutput = 0
    while (repeatIter.hasNext) {
      withResource(repeatIter.next()) { result =>
        assert(result.numCols() == 2)
        val rowsPerGroup = math.ceil(numOutputRows.toDouble / aggGroups).toLong
        withResource(result.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { aggs =>
          withResource(result.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { rideAlong =>
            (0 until result.numRows()).foreach { row =>
              assert(aggs.getInt(row) == ((numRowsActuallyOutput + row) / rowsPerGroup).toInt)
              assert(rideAlong.getShort(row) == (numRowsActuallyOutput + row).toShort)
            }
          }
        }
        numRowsActuallyOutput += result.numRows()
        numBatchesActuallyOutput += 1
      }
    }
    assert(numRowsActuallyOutput == numOutputRows)
    if (injectRetry) {
      assert(RmmSpark.getAndResetNumRetryThrow(1) == 1)
      verify(aggResult, times(2)).getColumnarBatch()
      if (retrySplit) {
        assert(numBatchesActuallyOutput > 1)
      } else {
        assert(numBatchesActuallyOutput == 1)
      }
    }
  }

  test("single batch repeat test") {
    basicRepeatTest(1000, 1000, 2, 1024 * 1024 * 1024)
  }

  test("multi batch no split repeat test") {
    basicRepeatTest(1000, 100, 2, 1024 * 1024 * 1024)
  }

  test("single batch with split repeat test") {
    basicRepeatTest(1000, 1000, 2, 4 * 1024)
  }

  test("aggregate split handles GpuRetryOOM") {
    basicRepeatTest(1000, 1000, 2, 4 * 1024, retrySplit = true)
  }

  test("repeat handles GpuRetryOOM") {
    basicRepeatTest(1000, 1000, 2, 1024 * 1024 * 1024, retryRepeat = true)
  }

  test("multi batch with split repeat test") {
    basicRepeatTest(1000, 100, 2, 4 * 1024)
  }

  test("single batch split on agg boundary") {
    basicRepeatTest(1000, 1000, 1000, 1024)
  }

  test("single batch single agg repeat test") {
    basicRepeatTest(1000, 1000, 1, 1024 * 1024 * 1024)
  }

  test("multi batch no split single agg repeat test") {
    basicRepeatTest(1000, 100, 1, 1024 * 1024 * 1024)
  }

  test("single batch with split single agg repeat test") {
    basicRepeatTest(1000, 1000, 1, 4 * 1024)
  }

  test("multi batch with split single agg repeat test") {
    basicRepeatTest(1000, 100, 1, 4 * 1024)
  }

}
