/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuRangePartitioner, GpuUnaryExpression, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuPartitionerExpr(child: Expression, partitioner: GpuRangePartitioner)
    extends GpuUnaryExpression {

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    if (partitioner.rangeBounds.isEmpty) {
      // A single partition has no boundaries to search, and cudf cannot search an empty table.
      // This happens when the sampled keys of the data being clustered have at most one
      // distinct value, such as a one-row table or a small table partition.
      withResource(Scalar.fromInt(0)) { zero =>
        ColumnVector.fromScalar(zero, input.getRowCount.toInt)
      }
    } else {
      NvtxRegistry.GPU_PARTITIONER {
        withResource(new ColumnarBatch(Array(input.incRefCount()))) { cb =>
          cb.setNumRows(input.getRowCount.toInt)
          partitioner.computePartitionIndexes(cb)
        }
      }
    }
  }
}
