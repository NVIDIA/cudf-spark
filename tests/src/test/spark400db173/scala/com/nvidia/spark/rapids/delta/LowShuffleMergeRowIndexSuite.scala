/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.{RmmRapidsRetryIterator, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

class LowShuffleMergeRowIndexSuite extends RmmSparkRetrySuiteBase {
  private val ranges = Array((0L, 3), (3L, 2), (0L, 4))
  private val expected = Seq(0L, 1L, 2L, 3L, 4L, 0L, 1L, 2L, 3L)

  private def checkIndexes(column: ColumnVector): Unit = {
    withResource(column.copyToHost()) { host =>
      assert((0 until host.getRowCount.toInt).map(index => host.getLong(index.toLong)) == expected)
    }
  }

  test("metadata-only row indexes retain row-group offsets and restart for each file") {
    withResource(LowShuffleMergeRowIndexes.buildColumn(ranges)) { column =>
      checkIndexes(column)
    }
    withResource(LowShuffleMergeRowIndexes.buildColumn(Array.empty[(Long, Int)])) { column =>
      assert(column.getRowCount == 0)
    }
    withResource(LowShuffleMergeRowIndexes.buildColumn(Array((7L, 2)))) { column =>
      withResource(column.copyToHost()) { host =>
        assert(Seq(host.getLong(0), host.getLong(1)) == Seq(7L, 8L))
      }
    }
  }

  test("OOM after an earlier row-group allocation releases partial columns before retry") {
    val allocatedBefore = Rmm.getTotalBytesAllocated
    // Exercise both initial and later allocations, including a partial row-group sequence.
    // Scalar construction can allocate too, so a single fixed skip can miss partial columns.
    for (allocationsToSkip <- 0 until 4) {
      RmmSpark.getAndResetNumRetryThrow(/* taskId */ 1)
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, allocationsToSkip)
      withResource(RmmRapidsRetryIterator.withRetryNoSplit[ColumnVector] {
        LowShuffleMergeRowIndexes.buildColumn(ranges)
      }) { column =>
        checkIndexes(column)
      }
      assert(RmmSpark.getAndResetNumRetryThrow(/* taskId */ 1) > 0)
      assert(Rmm.getTotalBytesAllocated == allocatedBefore,
        s"row-group columns leaked GPU memory after skipping $allocationsToSkip allocations")
    }
  }
}
