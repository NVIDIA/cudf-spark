/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Scalar, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, NoopMetric, RmmSparkRetrySuiteBase, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import java.util

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, ShortType}

class GpuUnboundedToUnboundedAggWindowSuite extends RmmSparkRetrySuiteBase {
  def basicRepeatTest(numOutputRows: Long, rowsPerRideAlongBatch: Int,
      aggGroups: Int, targetSizeBytes: Int) : Unit = {
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

    def makeRideAlongCb(numRows: Int): SpillableColumnarBatch = {
      // very basic test to verify that the repeat stage works properly.
      val table = withResource(Scalar.fromShort(5.toShort)) { s =>
        withResource(ColumnVector.fromScalar(s, numRows)) { data1 =>
          new Table(data1)
        }
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
      rowsRemaining -= rowsToAdd
      rideAlongList.add(makeRideAlongCb(rowsToAdd.toInt))
    }
    val inputIter = Seq(SecondPassAggResult(rideAlongList, makeRepeatCb())).toIterator
    val splitIter = new GpuUnboundedToUnboundedAggSliceBySizeIterator(inputIter, conf,
      targetSizeBytes, NoopMetric)
    val repeatIter = new GpuUnboundedToUnboundedAggFinalIterator(splitIter, conf,
      NoopMetric, NoopMetric, NoopMetric)

    var numRowsActuallyOutput = 0L
    while (repeatIter.hasNext) {
      withResource(repeatIter.next()) { result =>
        numRowsActuallyOutput += result.numRows()
        assert(result.numCols() == 2)
      }
    }
    assert(numRowsActuallyOutput == numOutputRows)
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
