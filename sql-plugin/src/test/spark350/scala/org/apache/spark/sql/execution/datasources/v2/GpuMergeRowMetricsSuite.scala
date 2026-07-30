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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.{FQSuiteName, LocalGpuMetric}

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec._
import org.apache.spark.sql.types.BooleanType
import org.scalatest.funsuite.AnyFunSuite

class GpuMergeRowMetricsSuite extends AnyFunSuite with FQSuiteName {

  private def metrics(): MergeRowMetrics = MergeRowMetrics(
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric(),
    new LocalGpuMetric())

  private val trueLit = Literal(true, BooleanType)

  test("record Keep actions onto MergeSummary metric fields") {
    val m = metrics()
    m.record(GpuKeep(trueLit, Nil, ACTION_COPY), 2, sourcePresent = true)
    m.record(GpuKeep(trueLit, Nil, ACTION_INSERT), 3, sourcePresent = true)
    m.record(GpuKeep(trueLit, Nil, ACTION_UPDATE), 4, sourcePresent = true)
    m.record(GpuKeep(trueLit, Nil, ACTION_DELETE), 5, sourcePresent = false)

    assert(m.numTargetRowsCopied.value === 2)
    assert(m.numTargetRowsInserted.value === 3)
    assert(m.numTargetRowsUpdated.value === 4)
    assert(m.numTargetRowsMatchedUpdated.value === 4)
    assert(m.numTargetRowsDeleted.value === 5)
    assert(m.numTargetRowsNotMatchedBySourceDeleted.value === 5)
    assert(m.numTargetRowsMatchedDeleted.value === 0)
  }

  test("record Discard and Split like Spark MergeRowsExec") {
    val m = metrics()
    m.record(GpuDiscard(trueLit), 2, sourcePresent = true)
    m.record(GpuSplit(trueLit, Nil, Nil), 3, sourcePresent = false)

    assert(m.numTargetRowsDeleted.value === 2)
    assert(m.numTargetRowsMatchedDeleted.value === 2)
    assert(m.numTargetRowsUpdated.value === 3)
    assert(m.numTargetRowsNotMatchedBySourceUpdated.value === 3)
  }

  test("ACTION_UNKNOWN Keep rows are skipped to avoid ambiguous counts") {
    val m = metrics()
    m.record(GpuKeep(trueLit, Nil, ACTION_UNKNOWN), 10, sourcePresent = true)
    assert(m.numTargetRowsCopied.value === 0)
    assert(m.numTargetRowsInserted.value === 0)
    assert(m.numTargetRowsUpdated.value === 0)
    assert(m.numTargetRowsDeleted.value === 0)
  }

  test("zero-row batches do not change metrics") {
    val m = metrics()
    m.record(GpuKeep(trueLit, Nil, ACTION_INSERT), 0, sourcePresent = true)
    m.record(GpuDiscard(trueLit), 0, sourcePresent = true)
    assert(m.numTargetRowsInserted.value === 0)
    assert(m.numTargetRowsDeleted.value === 0)
  }
}
