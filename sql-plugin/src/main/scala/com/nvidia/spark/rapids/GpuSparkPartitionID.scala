/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An expression that returns the current partition id just like
 * `org.apache.spark.sql.catalyst.expressions.SparkPartitionID`.
 *
 * The partition index used here is the parent RDD partition index injected by the
 * hosting operator via [[GpuNondeterministic.initialize]] — NOT
 * `TaskContext.getPartitionId()`. This preserves SPARK-14393's invariant that
 * values are stable across `coalesce`/`union`.
 */
case class GpuSparkPartitionID() extends GpuLeafExpression with GpuNondeterministic {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  @transient private[this] var partitionId: Int = _

  override val prettyName = "SPARK_PARTITION_ID"

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    partitionId = partitionIndex
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    ensureInitialized()
    val numRows = batch.numRows()
    withResource(Scalar.fromInt(partitionId)) { part =>
      GpuColumnVector.from(ColumnVector.fromScalar(part, numRows), dataType)
    }
  }
}
