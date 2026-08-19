/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

/*
 * A parent trait for all the classes of Pandas UDF plans.
 *
 * Put all the common things in this trait, to reduce the duplicate code in each
 * child class of the Pandas UDF plan.
 *
 */
trait GpuPythonExecBase extends GpuExec {

  override final protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW),
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES)
  )

  /**
   * Return the common metrics in order of
   *   NUM_INPUT_ROWS
   *   NUM_INPUT_BATCHES
   *   NUM_OUTPUT_ROWS
   *   NUM_OUTPUT_BATCHES
   * as a tuple.
   */
  protected def commonGpuMetrics(): (GpuMetric, GpuMetric, GpuMetric, GpuMetric) = (
    gpuLongMetric(NUM_INPUT_ROWS),
    gpuLongMetric(NUM_INPUT_BATCHES),
    gpuLongMetric(NUM_OUTPUT_ROWS),
    gpuLongMetric(NUM_OUTPUT_BATCHES))

}

