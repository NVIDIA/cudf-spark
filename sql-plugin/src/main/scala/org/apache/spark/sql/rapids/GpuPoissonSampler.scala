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
package org.apache.spark.sql.rapids

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{GatherUtils, GpuMetric, NvtxIdWithMetrics, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.random.PoissonSampler

class GpuPoissonSampler(fraction: Double, useGapSamplingIfPossible: Boolean,
                        numOutputRows: GpuMetric, numOutputBatches: GpuMetric, opTime: GpuMetric)
  extends PoissonSampler[ColumnarBatch](fraction, useGapSamplingIfPossible) {

  override def clone: PoissonSampler[ColumnarBatch] =
    new GpuPoissonSampler(fraction, useGapSamplingIfPossible,
      numOutputRows, numOutputBatches, opTime)

  override def sample(batchIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    if (fraction <= 0.0) {
      Iterator.empty
    } else {
      batchIterator.map { columnarBatch =>
        NvtxIdWithMetrics(NvtxRegistry.SAMPLE_EXEC, opTime) {
          withResource(columnarBatch) { cb =>
            // collect sampled row idx
            // samples idx in batch one by one, so it's same with CPU version
            val sampledRows = sample(cb.numRows())

            numOutputBatches += 1
            numOutputRows += sampledRows.length
            GatherUtils.gather(cb, sampledRows)
          }
        }
      }
    }
  }

  // collect the sampled row indexes, Note one row can be sampled multiple times
  private def sample(numRows: Int): ArrayBuffer[Int] = {
    val buf = new ArrayBuffer[Int]
    var rowIdx = 0
    while (rowIdx < numRows) {
      // invoke PoissonSampler sample
      val rowCount = super.sample()
      if (rowCount > 0) {
        var i = 0
        while (i < rowCount) {
          buf += rowIdx
          i = i + 1
        }
      }
      rowIdx += 1
    }
    buf
  }
}
