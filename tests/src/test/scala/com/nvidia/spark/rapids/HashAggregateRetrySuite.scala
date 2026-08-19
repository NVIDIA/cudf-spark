/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{CudfException, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.aggregate.{CudfAggregate, CudfSum}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

class HashAggregateRetrySuite
    extends RmmSparkRetrySuiteBase
        with MockitoSugar {
  private def buildReductionBatch(): SpillableColumnarBatch = {
    val reductionTable = new Table.TestBuilder()
      .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
      .build()
    withResource(reductionTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1))
    }
  }

  private def buildGroupByBatch(): SpillableColumnarBatch = {
    val groupByTable = new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 1, 1)
        .column(1L.asInstanceOf[java.lang.Long], 2L, 3L, 4L)
        .build()
    withResource(groupByTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(IntegerType, LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1))
    }
  }

  def doReduction(input: SpillableColumnarBatch): SpillableColumnarBatch = {
    val mockMetrics = mock[GpuHashAggregateMetrics]
    when(mockMetrics.opTime).thenReturn(NoopMetric)
    when(mockMetrics.concatTime).thenReturn(NoopMetric)
    when(mockMetrics.numAggOps).thenReturn(NoopMetric)
    val aggHelper = spy(new AggHelper(
      Seq.empty, Seq.empty, Seq.empty,
      forceMerge = false, new SQLConf(), isSorted = false, Map.empty[String, GpuMetric]))

    // mock out a reduction on the first column
    val aggs = new ArrayBuffer[CudfAggregate]()
    val aggOrdinals = new ArrayBuffer[Int]()
    aggs.append(new CudfSum(LongType))
    aggOrdinals.append(0)
    when(aggHelper.cudfAggregates).thenReturn(aggs)
    when(aggHelper.aggOrdinals).thenReturn(aggOrdinals)

    // attempt a cuDF reduction
    GpuAggregateIterator.aggregate(
      aggHelper, input, mockMetrics)
  }

  def makeGroupByAggHelper(forceMerge: Boolean): AggHelper = {
    val aggHelper = spy(new AggHelper(
      Seq.empty, Seq.empty, Seq.empty,
      forceMerge = forceMerge, new SQLConf(),
      isSorted = false, Map.empty[String, GpuMetric]))

    // mock out a group by with the first column as key, and second column
    // as a group by sum
    val groupingOrdinals = new Array[Int](1)
    groupingOrdinals(0) = 0 // groupby the 0th column
    val aggs = new ArrayBuffer[CudfAggregate]()
    aggs.append(new CudfSum(LongType))
    val aggOrdinals = new ArrayBuffer[Int]()
    aggOrdinals.append(1)
    val postStepDataTypes = new ArrayBuffer[DataType]()
    postStepDataTypes.append(IntegerType) // group by col
    postStepDataTypes.append(aggs(0).dataType) // sum

    when(aggHelper.cudfAggregates).thenReturn(aggs)
    when(aggHelper.aggOrdinals).thenReturn(aggOrdinals)
    when(aggHelper.groupingOrdinals).thenReturn(groupingOrdinals)
    when(aggHelper.postStepDataTypes).thenReturn(postStepDataTypes)
    aggHelper
  }

  def doGroupBy(
      input: SpillableColumnarBatch,
      forceMerge: Boolean = false): SpillableColumnarBatch = {
    val mockMetrics = mock[GpuHashAggregateMetrics]
    when(mockMetrics.opTime).thenReturn(NoopMetric)
    when(mockMetrics.concatTime).thenReturn(NoopMetric)
    when(mockMetrics.numAggOps).thenReturn(NoopMetric)

    // attempt a cuDF group by
    GpuAggregateIterator.aggregate(
      makeGroupByAggHelper(forceMerge = false),
      input,
      mockMetrics)
  }

  test("computeAndAggregate reduction with retry") {
    val reductionBatch = buildReductionBatch()
    withResource(reductionBatch.incRefCount()) { _ =>
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doReduction(reductionBatch)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(1)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          withResource(gcv.getBase.copyToHost()) { hcv =>
            assertResult(9)(hcv.getLong(0))
          }
        }
      }
      // we need to request a ColumnarBatch twice here for the retry
      // why is this invoking the underlying method
      verify(reductionBatch, times(2)).getColumnarBatch
    }
  }

  test("computeAndAggregate reduction with two retries") {
    val reductionBatch = buildReductionBatch()
    withResource(reductionBatch.incRefCount()) { _ =>
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doReduction(reductionBatch)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(1)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          withResource(gcv.getBase.copyToHost()) { hcv =>
            assertResult(9)(hcv.getLong(0))
          }
        }
      }
      // we need to request a ColumnarBatch three times, because of 1 regular attempt,
      // and two retries
      verify(reductionBatch, times(3)).getColumnarBatch
    }
  }

  test("computeAndAggregate reduction with cudf exception") {
    val reductionBatch = buildReductionBatch()
    withResource(reductionBatch.incRefCount()) { _ =>
      RmmSpark.forceCudfException(RmmSpark.getCurrentThreadId)
      assertThrows[CudfException] {
        doReduction(reductionBatch)
      }
      // columnar batch was obtained once, but since this was not a retriable exception
      // we don't retry it
      verify(reductionBatch, times(1)).getColumnarBatch
    }
  }

  test("computeAndAggregate group by with retry") {
    val groupByBatch = buildGroupByBatch()
    withResource(groupByBatch.incRefCount()) { _ =>
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doGroupBy(groupByBatch)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(3)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
          var rowsLeftToMatch = 3
          withResource(aggv.getBase.copyToHost()) { aggvh =>
            withResource(gcv.getBase.copyToHost()) { grph =>
              (0 until 3).foreach { row =>
                if (grph.isNull(row)) {
                  assertResult(2L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 5) {
                  assertResult(1L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 1) {
                  assertResult(7L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                }
              }
            }
          }
          assertResult(0)(rowsLeftToMatch)
        }
      }
      // we need to request a ColumnarBatch twice here for the retry
      verify(groupByBatch, times(2)).getColumnarBatch
    }
  }

  test("computeAndAggregate reduction with split and retry") {
    val reductionBatch = buildReductionBatch()
    withResource(reductionBatch.incRefCount()) { _ =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doReduction(reductionBatch)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(1)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]

          withResource(gcv.getBase.copyToHost()) { hcv =>
            assertResult(9L)(hcv.getLong(0))
          }
        }
      }
      // the second time we access this batch is to split it
      verify(reductionBatch, times(2)).getColumnarBatch
    }
  }

  test("computeAndAggregate group by with split retry") {
    val groupByBatch = buildGroupByBatch()
    withResource(groupByBatch.incRefCount()) { _ =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doGroupBy(groupByBatch)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(3)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
          var rowsLeftToMatch = 3
          withResource(aggv.getBase.copyToHost()) { aggvh =>
            withResource(gcv.getBase.copyToHost()) { grph =>
              (0 until 3).foreach { row =>
                if (grph.isNull(row)) {
                  assertResult(2L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 5) {
                  assertResult(1L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 1) {
                  assertResult(7L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                }
              }
            }
          }
          assertResult(0)(rowsLeftToMatch)
        }
      }
      // the second time we access this batch is to split it
      verify(groupByBatch, times(2)).getColumnarBatch
    }
  }

  test("computeAndAggregate group by with retry and forceMerge") {
    // with forceMerge we expect 1 batch to be returned at all costs
    val groupByBatch = buildGroupByBatch()
    withResource(groupByBatch.incRefCount()) { _ =>
      // we force a split because that would cause us to compute two aggs
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = doGroupBy(groupByBatch, forceMerge = true)
      withResource(result) { spillable =>
        withResource(spillable.getColumnarBatch) { cb =>
          assertResult(3)(cb.numRows)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
          var rowsLeftToMatch = 3
          withResource(aggv.getBase.copyToHost()) { aggvh =>
            withResource(gcv.getBase.copyToHost()) { grph =>
              (0 until 3).foreach { row =>
                if (grph.isNull(row)) {
                  assertResult(2L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 5) {
                  assertResult(1L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                } else if (grph.getInt(row) == 1) {
                  assertResult(7L)(aggvh.getLong(row))
                  rowsLeftToMatch -= 1
                }
              }
            }
          }
          assertResult(0)(rowsLeftToMatch)
        }
      }
      // we need to request a ColumnarBatch twice here for the retry
      verify(groupByBatch, times(2)).getColumnarBatch
    }
  }
}
