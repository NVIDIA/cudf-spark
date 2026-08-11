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

import scala.collection.mutable

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

class RangeRetrySuite extends RmmSparkRetrySuiteBase {
  private val maxRows = 20
  private val start = BigInt(0)
  private val end = BigInt(50)
  private val step = 2

  test("GPU range iterator no OOMs") {
    val rangeIter = new GpuRangeIterator(start, end, step, maxRows, null, NoopMetric)
    // It should produce two batches, and rows numbers are 20 (max rows number)
    // and 5 (remaining ones).
    val actualRowsNumbers = mutable.ArrayBuffer(20L, 5L)
    var curValue = start.toLong
    rangeIter.foreach { batch =>
      withResource(batch) { _ =>
        // check data
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
          (0 until hCol.getRowCount.toInt).foreach { pos =>
            assertResult(curValue)(hCol.getLong(pos))
            curValue += step
          }
        }
        // check metadata
        assertResult(actualRowsNumbers.remove(0))(batch.numRows())
      }
    }
    assert(actualRowsNumbers.isEmpty)
  }

  test("GPU range iterator with split and retry OOM") {
    val rangeIter = new GpuRangeIterator(start, end, step, maxRows, null, NoopMetric)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    // It should produce two batches, and rows numbers are
    // 10 (=20/2) after retry, and
    // 15 (25-10), the remaining ones.
    val actualRowsNumbers = mutable.ArrayBuffer(10L, 15L)
    var curValue = start.toLong
    rangeIter.foreach { batch =>
      withResource(batch) { _ =>
        // check data
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
          (0 until hCol.getRowCount.toInt).foreach { pos =>
            assertResult(curValue)(hCol.getLong(pos))
            curValue += step
          }
        }
        // check metadata
        assertResult(actualRowsNumbers.remove(0))(batch.numRows())
      }
    }
    assert(actualRowsNumbers.isEmpty)
  }

  test("GPU range iterator generates emtpy range") {
    // start == end
    assert {
      new GpuRangeIterator(start, start, step, maxRows, null, NoopMetric).isEmpty
    }
    // start < end but step < 0
    assert {
      val negativeStep = -2
      new GpuRangeIterator(start, end, negativeStep, maxRows, null, NoopMetric).isEmpty
    }
  }

  test("GPU range iterator generates one element range") {
    val oneEnd = BigInt(2)
    val rangeIter = new GpuRangeIterator(start, oneEnd, step, maxRows, null, NoopMetric)
    withResource(rangeIter.next()) { batch =>
      assertResult(1)(batch.numRows())
    }
    assert(rangeIter.isEmpty)
  }

  test("GPU range iterator with invalid inputs") {
    assertThrows[AssertionError] {
      val invalidEnd = BigInt(49)
      new GpuRangeIterator(start, invalidEnd, step, maxRows, null, NoopMetric)
    }
  }
}
