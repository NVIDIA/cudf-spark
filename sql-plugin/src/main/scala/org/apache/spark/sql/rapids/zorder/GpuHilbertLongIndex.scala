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

package org.apache.spark.sql.rapids.zorder

import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuProjectExec, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.ZOrder
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Expression used to replace DataBrick's HilbertLongIndex operator for zorder. We don't know
 * exactly what their code is doing, so this is a lot of guess work based off of the
 * InterleaveBits implementation in open source.
 */
case class GpuHilbertLongIndex(numBits: Int, children: Seq[Expression])
    extends GpuExpression with ShimExpression with ExpectsInputTypes {

  private val n: Int = children.size

  override def inputTypes: Seq[DataType] = Seq.fill(n)(IntegerType)

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val ret = withResource(GpuProjectExec.project(batch, children)) { inputs =>
      NvtxRegistry.HILBERT_INDEX {
        val bases = GpuColumnVector.extractBases(inputs)
        // Null values are replaced with 0 as a part of interleaveBits to match what delta does,
        // but null values should never show up in practice because this is fed by
        // GpuPartitionerExpr, which is not nullable.
        ZOrder.hilbertIndex(numBits, batch.numRows(), bases: _*)
      }
    }
    GpuColumnVector.from(ret, dataType)
  }
}
