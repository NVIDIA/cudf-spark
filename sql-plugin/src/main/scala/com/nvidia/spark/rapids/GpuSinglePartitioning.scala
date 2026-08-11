/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case object GpuSinglePartitioning extends GpuExpression with ShimExpression
    with GpuPartitioning {
  /**
   * Returns the result of evaluating this expression on the entire `ColumnarBatch`.
   * The result of calling this may be a single [[GpuColumnVector]] or a scalar value.
   * Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a literal that Add would have to be able to handle.
   *
   * By convention any [[GpuColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it
   * into a `ColumnarBatch` and closing the batch or by closing the vector directly if it is a
   * temporary value.
   */
  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    if (batch.numCols == 0) {
      Array(batch).zipWithIndex
    } else {
      // Nothing needs to be sliced but a contiguous table is needed for GPU shuffle which
      // slice will produce.
      sliceInternalGpuOrCpuAndClose(
        batch.numRows,
        Array(0),
        GpuColumnVector.extractColumns(batch))
    }
  }

  // these next overrides are required since `GpuSinglePartitioning` is used as a static class
  // and the RAPIDS Shuffle Manager can be enabled and disabled dynamically
  override def usesGPUShuffle: Boolean =
    GpuShuffleEnv.useGPUShuffle(new RapidsConf(SQLConf.get))

  override def usesMultiThreadedShuffle: Boolean =
    GpuShuffleEnv.useMultiThreadedShuffle(new RapidsConf(SQLConf.get))

  override def usesKudoGPUSlicing: Boolean =
    new RapidsConf(SQLConf.get).shuffleKudoGpuSerializerEnabled

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions: Int = 1

  override def children: Seq[Expression] = Seq.empty

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }
}
