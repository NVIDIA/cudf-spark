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
 * Expression based off of deltalake's InterleaveBits operator for zorder.  This is modified
 * slightly to allow more types (LONG, SHORT and BYTE) so that we could look into improving
 * the performance or extending functionality in the future.
 */
case class GpuInterleaveBits(children: Seq[Expression])
    extends GpuExpression with ShimExpression with ExpectsInputTypes {

  private val n: Int = children.size

  // Support LONG, INT, SHORT, or BYTE, so long as all are the same...
  private val childrenType = if (n == 0) {
    IntegerType
  } else {
    children.head.dataType match {
      case it: IntegralType => it
      case _ => IntegerType
    }
  }

  override def inputTypes: Seq[DataType] = Seq.fill(n)(childrenType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val ret = withResource(GpuProjectExec.project(batch, children)) { inputs =>
      NvtxRegistry.INTERLEAVE_BITS {
        val bases = GpuColumnVector.extractBases(inputs)
        // Null values are replaced with 0 as a part of interleaveBits to match what delta does,
        // but null values should never show up in practice because this is fed by
        // GpuPartitionerExpr, which is not nullable.
        ZOrder.interleaveBits(batch.numRows(), bases: _*)
      }
    }
    GpuColumnVector.from(ret, dataType)
  }
}
