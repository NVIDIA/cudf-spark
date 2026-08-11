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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, GpuLiteral, LocalGpuMetric,
  NoopMetric, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec._
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Verifies MERGE WriteSummary metrics are published only for the successful
 * withRetryNoSplit attempt. The injected OOM is placed after attemptMetrics.record
 * (during applyOutputs/project) so a regression that publishes inside the retry
 * callback would double-count and fail the assertions.
 *
 * Spark 4.1+ only: earlier shims use NoopMetric staging when WriteSummary is unused.
 */
class GpuMergeBatchIteratorRetrySuite extends RmmSparkRetrySuiteBase {

  private def buildBatch(): ColumnarBatch = {
    val table = new Table.TestBuilder()
      .column(Integer.valueOf(42))
      .build()
    withResource(table) { tbl =>
      GpuColumnVector.from(tbl, Array[DataType](IntegerType))
    }
  }

  // Allocations before attemptMetrics.record in this setup:
  // 2x presence GpuLiteral eval, matched and-mask, notMatched not+and masks,
  // Keep condition literal, and/replaceNulls/not/and for masks, then filter.
  // The next GPU allocation is applyOutputs/project after record.
  private val oomSkipBeforeProjectAfterRecord = 10

  test("MERGE metrics publish only the successful retry attempt") {
    val published = MergeRowMetrics(
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric(),
      new LocalGpuMetric())
    val insert = GpuKeep(
      GpuLiteral.create(true, BooleanType),
      Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "id")),
      ACTION_INSERT)
    val it = new GpuMergeBatchIterator(
      Array(IntegerType),
      Seq(buildBatch()).iterator,
      isTargetRowPresent = GpuLiteral.create(false, BooleanType),
      isSourceRowPresent = GpuLiteral.create(true, BooleanType),
      matchedInstructionExecs = Nil,
      notMatchedInstructionExecs = Seq(insert),
      notMatchedBySourceInstructionExecs = Nil,
      numOutputRows = NoopMetric,
      numOutputBatches = NoopMetric,
      opTime = NoopMetric,
      mergeMetrics = published)

    RmmSpark.forceRetryOOM(
      RmmSpark.getCurrentThreadId,
      1,
      RmmSpark.OomInjectionType.GPU.ordinal,
      oomSkipBeforeProjectAfterRecord)
    withResource(it.next()) { batch =>
      assert(batch.numRows() === 1)
    }
    assert(RmmSpark.getAndResetNumRetryThrow(/* taskId = */ 1) > 0)
    // One successful attempt inserts the single input row once.
    assert(published.numTargetRowsInserted.value === 1L)
    assert(published.numTargetRowsCopied.value === 0L)
    assert(published.numTargetRowsDeleted.value === 0L)
    assert(published.numTargetRowsUpdated.value === 0L)
  }
}
