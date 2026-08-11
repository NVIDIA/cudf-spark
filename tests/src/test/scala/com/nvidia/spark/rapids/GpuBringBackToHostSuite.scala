/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.LongType

class GpuBringBackToHostSuite extends SparkQueryCompareTestSuite {
  test("doExecute") {
    val numRows = 1024 * 1024
    val rows = withGpuSparkSession { _ =>
      val plan = GpuBringBackToHost(
        GpuRangeExec(start=0, end=numRows, step=1, numSlices=1,
          output=Seq(AttributeReference("id", LongType)()),
          targetSizeBytes=Math.max(numRows / 10, 1)))
      plan.executeCollect()
    }

    var rowId = 0
    rows.foreach { row =>
      assertResult(1)(row.numFields)
      assertResult(rowId)(row.getLong(0))
      rowId += 1
    }
    assertResult(numRows)(rowId)
  }

  test("doExecuteColumnar returns a columnar batch with a valid numRows") {
    withGpuSparkSession { spark =>
      val data = mixedDf(spark, numSlices = 1)
      val plan = GpuBringBackToHost(
        GpuRowToColumnarExec(data.queryExecution.sparkPlan, RequireSingleBatch))
      val rdd = plan.doExecuteColumnar()
      val part = rdd.partitions.head
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      val batches = rdd.compute(part, context)

      // Only one batch should be returned, which contains all data.
      assert(batches.hasNext)
      val batch = batches.next()
      assert(batch.numCols() == data.columns.length)
      assert(batch.numRows() == data.count())
      assert(!batches.hasNext)
    }
  }
}